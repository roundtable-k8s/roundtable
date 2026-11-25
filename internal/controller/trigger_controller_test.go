/*
Copyright 2026 dapperdivers.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/go-logr/logr"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/chainrun"
	"github.com/roundtable-k8s/roundtable/pkg/metrics"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// firings reads roundtable_trigger_firings_total for a trigger of the test table.
func firings(trigger, source, outcome string) float64 {
	return testutil.ToFloat64(metrics.TriggerFiringsTotal.WithLabelValues("trg-rt", trigger, source, outcome))
}

var _ = Describe("Trigger controller", func() {
	const (
		ns       = "default"
		rtName   = "trg-rt"
		prefix   = "trg"
		tmplName = "trg-target"
		stream   = "trg_tasks_EVENTS"
	)

	var (
		srv   *natsserver.Server
		cli   natspkg.Client
		js    jetstream.JetStream
		r     *TriggerReconciler
		clock time.Time
	)

	runsOf := func(trigger string) []aiv1alpha1.Chain {
		list := &aiv1alpha1.ChainList{}
		Expect(k8sClient.List(ctx, list, client.InNamespace(ns),
			client.MatchingLabels{aiv1alpha1.LabelTrigger: trigger})).To(Succeed())
		return list.Items
	}
	getTrigger := func(name string) *aiv1alpha1.Trigger {
		t := &aiv1alpha1.Trigger{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, t)).To(Succeed())
		return t
	}
	reconcileTrigger := func(name string) reconcile.Result {
		res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
		Expect(err).NotTo(HaveOccurred())
		return res
	}
	publish := func(subject, msgID, data string) {
		var opts []jetstream.PublishOpt
		if msgID != "" {
			opts = append(opts, jetstream.WithMsgID(msgID))
		}
		_, err := js.Publish(ctx, subject, []byte(data), opts...)
		Expect(err).NotTo(HaveOccurred())
	}
	natsTrigger := func(name, subject string, mutate func(*aiv1alpha1.TriggerSpec)) *aiv1alpha1.Trigger {
		t := &aiv1alpha1.Trigger{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: aiv1alpha1.TriggerSpec{
				Source:        aiv1alpha1.TriggerSource{NATS: &aiv1alpha1.TriggerNATSSource{Subject: subject}},
				Target:        aiv1alpha1.TriggerTarget{ChainRef: &corev1.LocalObjectReference{Name: tmplName}},
				Input:         `{"service": "{{ .Event.Data.service }}", "subject": "{{ .Event.Subject }}"}`,
				RoundTableRef: &corev1.LocalObjectReference{Name: rtName},
			},
		}
		if mutate != nil {
			mutate(&t.Spec)
		}
		Expect(k8sClient.Create(ctx, t)).To(Succeed())
		reconcileTrigger(name)
		return getTrigger(name)
	}
	consumerExists := func(durable string) bool {
		_, err := js.Consumer(ctx, stream, durable)
		return err == nil
	}

	BeforeEach(func() {
		opts := natstest.DefaultTestOptions
		opts.Port = -1
		opts.JetStream = true
		opts.StoreDir = GinkgoT().TempDir()
		srv = natstest.RunServer(&opts)
		cli = natspkg.NewClient(natspkg.Config{URL: srv.ClientURL()}, logr.Discard())
		Expect(cli.Connect()).To(Succeed())
		nc, err := nats.Connect(srv.ClientURL())
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(nc.Close)
		js, err = jetstream.New(nc)
		Expect(err).NotTo(HaveOccurred())

		clock = time.Now()
		provider := natspkg.NewProviderWithClient(cli, logr.Discard())
		r = &TriggerReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: record.NewFakeRecorder(100),
			NATS:     provider,
			Now:      func() time.Time { return clock },
		}

		rt := &aiv1alpha1.RoundTable{
			ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: ns},
			Spec: aiv1alpha1.RoundTableSpec{NATS: aiv1alpha1.RoundTableNATS{
				SubjectPrefix: prefix, TasksStream: "trg_tasks", ResultsStream: "trg_results", CreateStreams: true,
			}},
		}
		Expect(k8sClient.Create(ctx, rt)).To(Succeed())
		// The RoundTable controller creates the events stream next to the
		// tasks and results streams.
		Expect((&RoundTableReconciler{NATS: provider}).ensureStreams(ctx, rt)).To(Succeed())
		Expect(k8sClient.Create(ctx, &aiv1alpha1.Chain{
			ObjectMeta: metav1.ObjectMeta{Name: tmplName, Namespace: ns},
			Spec: aiv1alpha1.ChainSpec{
				RoundTableRef: &corev1.LocalObjectReference{Name: rtName},
				Input:         `{"default": true}`,
				Schedule:      "0 * * * *",
				Steps:         []aiv1alpha1.ChainStep{{Name: "do", KnightRef: corev1.LocalObjectReference{Name: "lead"}, Task: "do {{ .Input }}", TimeoutSeconds: 120}},
			},
		})).To(Succeed())
	})

	AfterEach(func() {
		r.init()
		r.consumers.stopAll()
		triggers := &aiv1alpha1.TriggerList{}
		Expect(k8sClient.List(ctx, triggers, client.InNamespace(ns))).To(Succeed())
		for i := range triggers.Items {
			t := &triggers.Items[i]
			t.Finalizers = nil
			Expect(client.IgnoreNotFound(k8sClient.Update(ctx, t))).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, t))).To(Succeed())
		}
		Expect(k8sClient.DeleteAllOf(ctx, &aiv1alpha1.Chain{}, client.InNamespace(ns),
			client.MatchingLabels{chainrun.LabelRunSource: chainrun.SourceTrigger})).To(Succeed())
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &aiv1alpha1.Chain{ObjectMeta: metav1.ObjectMeta{Name: tmplName, Namespace: ns}}))).To(Succeed())
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &aiv1alpha1.RoundTable{ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: ns}}))).To(Succeed())
		_ = cli.Close()
		srv.Shutdown()
	})

	Context("validation", func() {
		base := func(name string) *aiv1alpha1.Trigger {
			return &aiv1alpha1.Trigger{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
				Spec: aiv1alpha1.TriggerSpec{
					Target: aiv1alpha1.TriggerTarget{ChainRef: &corev1.LocalObjectReference{Name: tmplName}},
				},
			}
		}

		It("rejects missing and multiple sources", func() {
			t := base("v-none")
			Expect(k8sClient.Create(ctx, t)).To(MatchError(ContainSubstring("set exactly one of")))

			t = base("v-two")
			t.Spec.Source.Schedule = &aiv1alpha1.TriggerScheduleSource{Cron: "* * * * *"}
			t.Spec.Source.NATS = &aiv1alpha1.TriggerNATSSource{Subject: prefix + ".events.x"}
			t.Spec.RoundTableRef = &corev1.LocalObjectReference{Name: rtName}
			Expect(k8sClient.Create(ctx, t)).To(MatchError(ContainSubstring("set exactly one of")))
		})

		It("validates a webhook source", func() {
			secret := &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "hook"}, Key: "token"}

			t := base("v-wh-noref")
			t.Spec.Source.Webhook = &aiv1alpha1.TriggerWebhookSource{Auth: aiv1alpha1.TriggerWebhookAuth{BearerTokenSecretRef: secret}}
			Expect(k8sClient.Create(ctx, t)).To(MatchError(ContainSubstring("roundTableRef is required for source.webhook")))

			t = base("v-wh-noauth")
			t.Spec.RoundTableRef = &corev1.LocalObjectReference{Name: rtName}
			t.Spec.Source.Webhook = &aiv1alpha1.TriggerWebhookSource{}
			Expect(k8sClient.Create(ctx, t)).To(MatchError(ContainSubstring("set exactly one of auth.bearerTokenSecretRef or auth.hmacSecretRef")))

			t = base("v-wh-both")
			t.Spec.RoundTableRef = &corev1.LocalObjectReference{Name: rtName}
			t.Spec.Source.Webhook = &aiv1alpha1.TriggerWebhookSource{Auth: aiv1alpha1.TriggerWebhookAuth{BearerTokenSecretRef: secret, HMACSecretRef: secret}}
			Expect(k8sClient.Create(ctx, t)).To(MatchError(ContainSubstring("set exactly one of auth.bearerTokenSecretRef or auth.hmacSecretRef")))

			t = base("v-wh")
			t.Spec.RoundTableRef = &corev1.LocalObjectReference{Name: rtName}
			t.Spec.Source.Webhook = &aiv1alpha1.TriggerWebhookSource{Auth: aiv1alpha1.TriggerWebhookAuth{HMACSecretRef: secret}}
			Expect(k8sClient.Create(ctx, t)).To(Succeed())
		})

		It("requires roundTableRef for a NATS source and defaults the rest", func() {
			t := base("v-nats")
			t.Spec.Source.NATS = &aiv1alpha1.TriggerNATSSource{Subject: prefix + ".events.x"}
			Expect(k8sClient.Create(ctx, t)).To(MatchError(ContainSubstring("roundTableRef is required")))

			t = base("v-sched")
			t.Spec.Source.Schedule = &aiv1alpha1.TriggerScheduleSource{Cron: "* * * * *"}
			Expect(k8sClient.Create(ctx, t)).To(Succeed())
			got := getTrigger("v-sched")
			Expect(got.Spec.Concurrency.Policy).To(Equal(aiv1alpha1.TriggerConcurrencyAllow))
			Expect(got.Spec.TTLSecondsAfterFinished).To(Equal(ptr.To[int32](86400)))
		})
	})

	Context("webhook source", func() {
		webhookTrigger := func(name, eventType string) *aiv1alpha1.Trigger {
			t := &aiv1alpha1.Trigger{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
				Spec: aiv1alpha1.TriggerSpec{
					Source: aiv1alpha1.TriggerSource{Webhook: &aiv1alpha1.TriggerWebhookSource{
						Auth: aiv1alpha1.TriggerWebhookAuth{BearerTokenSecretRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "hook"}, Key: "token"}},
						EventType: eventType,
					}},
					Target:        aiv1alpha1.TriggerTarget{ChainRef: &corev1.LocalObjectReference{Name: tmplName}},
					Input:         `{"alert": "{{ .Event.Data.alert }}", "subject": "{{ .Event.Subject }}"}`,
					RoundTableRef: &corev1.LocalObjectReference{Name: rtName},
				},
			}
			Expect(k8sClient.Create(ctx, t)).To(Succeed())
			reconcileTrigger(name)
			return getTrigger(name)
		}
		publishWithHeaders := func(subject, msgID, data string, headers map[string]string) {
			msg := nats.NewMsg(subject)
			msg.Data = []byte(data)
			for k, v := range headers {
				msg.Header.Set(k, v)
			}
			_, err := js.PublishMsg(ctx, msg, jetstream.WithMsgID(msgID))
			Expect(err).NotTo(HaveOccurred())
		}

		It("reads the fixed webhook subject and starts a run per delivery", func() {
			t := webhookTrigger("alerts", "")
			subject := natspkg.WebhookEventSubject(prefix, ns, "alerts")
			Expect(t.Finalizers).To(ContainElement(aiv1alpha1.FinalizerTrigger))
			Expect(t.Status.Consumer).To(Equal(natspkg.TriggerConsumerName(ns, "alerts")))
			Expect(meta.IsStatusConditionTrue(t.Status.Conditions, aiv1alpha1.ConditionTriggerReady)).To(BeTrue())
			Expect(meta.FindStatusCondition(t.Status.Conditions, aiv1alpha1.ConditionTriggerReady).Message).To(ContainSubstring(subject))

			publishWithHeaders(subject, "d1", `{"alert":"disk"}`, map[string]string{"Content-Type": "application/json"})
			Eventually(func() int { return len(runsOf("alerts")) }, 10*time.Second, 100*time.Millisecond).Should(Equal(1))
			run := runsOf("alerts")[0]
			Expect(run.Spec.Input).To(Equal(`{"alert": "disk", "subject": "` + subject + `"}`))
			Expect(run.Annotations[aiv1alpha1.AnnotationTriggerKey]).To(Equal("d1"))

			// Another Trigger's webhook subject is not read.
			publishWithHeaders(natspkg.WebhookEventSubject(prefix, ns, "other"), "d2", `{"alert":"cpu"}`, nil)
			Consistently(func() int { return len(runsOf("alerts")) }, 2*time.Second, 200*time.Millisecond).Should(Equal(1))
		})

		It("fires only on the configured eventType", func() {
			webhookTrigger("typed", "com.example.deploy")
			subject := natspkg.WebhookEventSubject(prefix, ns, "typed")
			publishWithHeaders(subject, "e1", `{"alert":"a"}`, map[string]string{"Ce-Type": "com.example.other"})
			publishWithHeaders(subject, "e2", `{"alert":"b"}`, nil)
			publishWithHeaders(subject, "e3", `{"alert":"c"}`, map[string]string{"Ce-Type": "com.example.deploy"})
			Eventually(func() int { return len(runsOf("typed")) }, 10*time.Second, 100*time.Millisecond).Should(Equal(1))
			Expect(runsOf("typed")[0].Spec.Input).To(ContainSubstring(`"alert": "c"`))
			Consistently(func() int { return len(runsOf("typed")) }, time.Second, 200*time.Millisecond).Should(Equal(1))
			// Filtered events are acked, not left for redelivery, and not
			// recorded as firings.
			Expect(getTrigger("typed").Status.RecentFirings).To(HaveLen(1))
			cons, err := js.Consumer(ctx, stream, natspkg.TriggerConsumerName(ns, "typed"))
			Expect(err).NotTo(HaveOccurred())
			info, err := cons.Info(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(info.NumAckPending).To(BeZero())
		})
	})

	Context("NATS source", func() {
		It("starts exactly one run per message with the rendered input and dedupes Nats-Msg-Id", func() {
			t := natsTrigger("on-deploy", prefix+".events.deploys.>", nil)
			Expect(t.Finalizers).To(ContainElement(aiv1alpha1.FinalizerTrigger))
			Expect(t.Status.Consumer).To(Equal(natspkg.TriggerConsumerName(ns, "on-deploy")))
			Expect(t.Status.Stream).To(Equal(stream))
			Expect(meta.IsStatusConditionTrue(t.Status.Conditions, aiv1alpha1.ConditionTriggerReady)).To(BeTrue())
			Expect(consumerExists(t.Status.Consumer)).To(BeTrue())

			publish(prefix+".events.deploys.prod", "m1", `{"service":"api"}`)
			Eventually(func() int { return len(runsOf("on-deploy")) }, 10*time.Second, 100*time.Millisecond).Should(Equal(1))
			run := runsOf("on-deploy")[0]
			Expect(run.Spec.Input).To(Equal(`{"service": "api", "subject": "trg.events.deploys.prod"}`))
			Expect(run.Spec.Schedule).To(BeEmpty())
			Expect(run.Labels[chainrun.LabelRunSource]).To(Equal(chainrun.SourceTrigger))
			Expect(run.Annotations[aiv1alpha1.AnnotationTriggerKey]).To(Equal("m1"))
			Expect(run.Annotations[aiv1alpha1.AnnotationTriggerSubject]).To(Equal("trg.events.deploys.prod"))
			Expect(run.Annotations[aiv1alpha1.AnnotationChainRun]).NotTo(BeEmpty())
			Expect(metav1.IsControlledBy(&run, t)).To(BeTrue())

			Eventually(func(g Gomega) {
				st := getTrigger("on-deploy").Status
				g.Expect(st.LastFiredAt).NotTo(BeNil())
				g.Expect(st.RecentFirings).To(HaveLen(1))
				g.Expect(st.RecentFirings[0].Run).To(Equal(run.Name))
				g.Expect(st.ActiveRuns).To(ConsistOf(run.Name))
			}, 5*time.Second, 100*time.Millisecond).Should(Succeed())

			// The same Nats-Msg-Id again starts nothing.
			publish(prefix+".events.deploys.prod", "m1", `{"service":"api"}`)
			// Another subject outside the filter starts nothing either.
			publish(prefix+".events.other", "m2", `{"service":"web"}`)
			Consistently(func() int { return len(runsOf("on-deploy")) }, 2*time.Second, 200*time.Millisecond).Should(Equal(1))
			Expect(firings("on-deploy", "nats", "started")).To(Equal(1.0))
			// The stream drops a repeated Nats-Msg-Id before the Trigger sees it.
			Expect(firings("on-deploy", "nats", "deduped")).To(Equal(0.0))
			Expect(testutil.ToFloat64(metrics.TriggerLastFiredTimestampSeconds.WithLabelValues(rtName, "on-deploy"))).To(BeNumerically(">", 0))
		})

		It("starts nothing for a repeated dedupeKey", func() {
			natsTrigger("dedupe", prefix+".events.builds", func(s *aiv1alpha1.TriggerSpec) {
				s.DedupeKey = "{{ .Event.Data.build }}"
			})
			publish(prefix+".events.builds", "a", `{"service":"api","build":"42"}`)
			Eventually(func() int { return len(runsOf("dedupe")) }, 10*time.Second, 100*time.Millisecond).Should(Equal(1))
			publish(prefix+".events.builds", "b", `{"service":"api","build":"42"}`)
			Eventually(func(g Gomega) {
				f := getTrigger("dedupe").Status.RecentFirings
				g.Expect(f).To(HaveLen(2))
				g.Expect(f[0].SkipReason).To(ContainSubstring("duplicate dedupe key"))
				g.Expect(f[0].Key).To(Equal("42"))
			}, 10*time.Second, 100*time.Millisecond).Should(Succeed())
			Expect(runsOf("dedupe")).To(HaveLen(1))
			Expect(firings("dedupe", "nats", "started")).To(Equal(1.0))
			Expect(firings("dedupe", "nats", "deduped")).To(Equal(1.0))
		})

		It("skips with Forbid while a run is active", func() {
			natsTrigger("forbid", prefix+".events.jobs", func(s *aiv1alpha1.TriggerSpec) {
				s.Concurrency.Policy = aiv1alpha1.TriggerConcurrencyForbid
			})
			publish(prefix+".events.jobs", "j1", `{"service":"a"}`)
			Eventually(func() int { return len(runsOf("forbid")) }, 10*time.Second, 100*time.Millisecond).Should(Equal(1))
			first := runsOf("forbid")[0].Name

			publish(prefix+".events.jobs", "j2", `{"service":"b"}`)
			Eventually(func(g Gomega) {
				f := getTrigger("forbid").Status.RecentFirings
				g.Expect(f).To(HaveLen(2))
				g.Expect(f[0].SkipReason).To(Equal("concurrency Forbid: run " + first + " is active"))
			}, 10*time.Second, 100*time.Millisecond).Should(Succeed())
			Expect(runsOf("forbid")).To(HaveLen(1))

			// Once the run finished, the next event starts a new one.
			c := &aiv1alpha1.Chain{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: first}, c)).To(Succeed())
			c.Status.Phase = aiv1alpha1.ChainPhaseSucceeded
			c.Status.LastRunTrigger = c.Annotations[aiv1alpha1.AnnotationChainRun]
			c.Status.CompletedAt = &metav1.Time{Time: clock}
			Expect(k8sClient.Status().Update(ctx, c)).To(Succeed())
			publish(prefix+".events.jobs", "j3", `{"service":"c"}`)
			Eventually(func() int { return len(runsOf("forbid")) }, 10*time.Second, 100*time.Millisecond).Should(Equal(2))
		})

		It("replaces the active run with Replace", func() {
			natsTrigger("replace", prefix+".events.rep", func(s *aiv1alpha1.TriggerSpec) {
				s.Concurrency.Policy = aiv1alpha1.TriggerConcurrencyReplace
			})
			publish(prefix+".events.rep", "r1", `{"service":"a"}`)
			Eventually(func() int { return len(runsOf("replace")) }, 10*time.Second, 100*time.Millisecond).Should(Equal(1))
			first := runsOf("replace")[0].Name
			publish(prefix+".events.rep", "r2", `{"service":"b"}`)
			Eventually(func(g Gomega) {
				runs := runsOf("replace")
				g.Expect(runs).To(HaveLen(1))
				g.Expect(runs[0].Name).NotTo(Equal(first))
				g.Expect(runs[0].Spec.Input).To(ContainSubstring(`"service": "b"`))
			}, 10*time.Second, 100*time.Millisecond).Should(Succeed())
		})

		It("records a bad template and does not retry the event", func() {
			t := natsTrigger("bad", prefix+".events.bad", func(s *aiv1alpha1.TriggerSpec) {
				s.Input = `{{ .Event.Data.missing }}`
			})
			publish(prefix+".events.bad", "x1", `{"service":"api"}`)
			Eventually(func(g Gomega) {
				f := getTrigger("bad").Status.RecentFirings
				g.Expect(f).To(HaveLen(1))
				g.Expect(f[0].SkipReason).To(ContainSubstring("input template"))
				g.Expect(f[0].Run).To(BeEmpty())
			}, 10*time.Second, 100*time.Millisecond).Should(Succeed())
			Consistently(func(g Gomega) {
				g.Expect(runsOf("bad")).To(BeEmpty())
				g.Expect(getTrigger("bad").Status.RecentFirings).To(HaveLen(1))
			}, 2*time.Second, 200*time.Millisecond).Should(Succeed())
			cons, err := js.Consumer(ctx, stream, t.Status.Consumer)
			Expect(err).NotTo(HaveOccurred())
			info, err := cons.Info(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(info.NumAckPending).To(BeZero())
			Expect(info.NumRedelivered).To(BeZero())
			Expect(info.AckFloor.Stream).To(BeNumerically(">=", 1))
		})

		It("rejects a subject outside the table's events", func() {
			t := natsTrigger("outside", prefix+".tasks.x", nil)
			cond := meta.FindStatusCondition(t.Status.Conditions, aiv1alpha1.ConditionTriggerReady)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(aiv1alpha1.ReasonTriggerInvalidSpec))
			Expect(consumerExists(natspkg.TriggerConsumerName(ns, "outside"))).To(BeFalse())
		})

		It("restarts its message loop after the consumer is deleted", func() {
			t := natsTrigger("revive", prefix+".events.revive", nil)
			Expect(reconcileTrigger("revive").RequeueAfter).To(Equal(RequeueVerySlow))
			Expect(js.DeleteConsumer(ctx, stream, t.Status.Consumer)).To(Succeed())
			// The loop exits once its consumer is gone.
			Eventually(func() bool {
				r.consumers.mu.Lock()
				defer r.consumers.mu.Unlock()
				for _, l := range r.consumers.loops {
					select {
					case <-l.done:
						return true
					default:
					}
				}
				return false
			}, 15*time.Second, 100*time.Millisecond).Should(BeTrue())

			reconcileTrigger("revive")
			Expect(consumerExists(t.Status.Consumer)).To(BeTrue())
			publish(prefix+".events.revive", "r1", `{"service":"api"}`)
			Eventually(func() int { return len(runsOf("revive")) }, 10*time.Second, 100*time.Millisecond).Should(Equal(1))
		})

		It("deletes its consumer when suspended and when deleted", func() {
			t := natsTrigger("cleanup", prefix+".events.c", nil)
			durable := t.Status.Consumer
			Expect(consumerExists(durable)).To(BeTrue())

			t.Spec.Suspend = true
			Expect(k8sClient.Update(ctx, t)).To(Succeed())
			reconcileTrigger("cleanup")
			Expect(consumerExists(durable)).To(BeFalse())
			t = getTrigger("cleanup")
			Expect(t.Status.Consumer).To(BeEmpty())
			Expect(meta.FindStatusCondition(t.Status.Conditions, aiv1alpha1.ConditionTriggerReady).Reason).To(Equal(aiv1alpha1.ReasonTriggerSuspended))

			t.Spec.Suspend = false
			Expect(k8sClient.Update(ctx, t)).To(Succeed())
			reconcileTrigger("cleanup")
			Expect(consumerExists(durable)).To(BeTrue())

			Expect(k8sClient.Delete(ctx, getTrigger("cleanup"))).To(Succeed())
			reconcileTrigger("cleanup")
			Expect(consumerExists(durable)).To(BeFalse())
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: "cleanup"}, &aiv1alpha1.Trigger{})
				return apierrors.IsNotFound(err)
			}, 5*time.Second, 100*time.Millisecond).Should(BeTrue())
		})

		It("deletes finished runs after ttlSecondsAfterFinished", func() {
			natsTrigger("ttl", prefix+".events.ttl", func(s *aiv1alpha1.TriggerSpec) {
				s.TTLSecondsAfterFinished = ptr.To[int32](60)
			})
			publish(prefix+".events.ttl", "t1", `{"service":"a"}`)
			Eventually(func() int { return len(runsOf("ttl")) }, 10*time.Second, 100*time.Millisecond).Should(Equal(1))
			c := runsOf("ttl")[0]
			c.Status.Phase = aiv1alpha1.ChainPhaseSucceeded
			c.Status.LastRunTrigger = c.Annotations[aiv1alpha1.AnnotationChainRun]
			c.Status.CompletedAt = &metav1.Time{Time: clock}
			Expect(k8sClient.Status().Update(ctx, &c)).To(Succeed())

			res := reconcileTrigger("ttl")
			Expect(res.RequeueAfter).To(BeNumerically("~", 60*time.Second, time.Second))
			Expect(runsOf("ttl")).To(HaveLen(1))
			Expect(getTrigger("ttl").Status.ActiveRuns).To(BeEmpty())

			clock = clock.Add(61 * time.Second)
			reconcileTrigger("ttl")
			Eventually(func() int { return len(runsOf("ttl")) }, 5*time.Second, 100*time.Millisecond).Should(BeZero())
		})
	})

	Context("schedule source", func() {
		scheduleTrigger := func(name string, mutate func(*aiv1alpha1.TriggerSpec)) *aiv1alpha1.Trigger {
			t := &aiv1alpha1.Trigger{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
				Spec: aiv1alpha1.TriggerSpec{
					Source: aiv1alpha1.TriggerSource{Schedule: &aiv1alpha1.TriggerScheduleSource{Cron: "*/5 * * * *"}},
					Target: aiv1alpha1.TriggerTarget{ChainRef: &corev1.LocalObjectReference{Name: tmplName}},
					Input:  `{"at": "{{ .Event.Time.UTC.Format "15:04" }}"}`,
				},
			}
			if mutate != nil {
				mutate(&t.Spec)
			}
			Expect(k8sClient.Create(ctx, t)).To(Succeed())
			return getTrigger(name)
		}
		// nextFive is the first */5 minute strictly after t.
		nextFive := func(t time.Time) time.Time {
			return t.UTC().Truncate(5 * time.Minute).Add(5 * time.Minute)
		}

		It("fires once per schedule time with .Event.Time and requeues for the next", func() {
			t := scheduleTrigger("cron", nil)
			due := nextFive(t.CreationTimestamp.Time)

			clock = due.Add(-time.Second)
			res := reconcileTrigger("cron")
			Expect(runsOf("cron")).To(BeEmpty())
			Expect(res.RequeueAfter).To(BeNumerically("~", 2*time.Second, time.Second))
			Expect(getTrigger("cron").Status.NextScheduledAt.Time).To(BeTemporally("==", due))
			Expect(meta.IsStatusConditionTrue(getTrigger("cron").Status.Conditions, aiv1alpha1.ConditionTriggerReady)).To(BeTrue())

			clock = due.Add(10 * time.Second)
			res = reconcileTrigger("cron")
			runs := runsOf("cron")
			Expect(runs).To(HaveLen(1))
			Expect(runs[0].Spec.Input).To(Equal(fmt.Sprintf(`{"at": "%s"}`, due.Format("15:04"))))
			Expect(runs[0].Annotations[aiv1alpha1.AnnotationTriggerKey]).To(Equal(due.Format(time.RFC3339)))
			st := getTrigger("cron").Status
			Expect(st.LastScheduledAt.Time).To(BeTemporally("==", due))
			Expect(st.NextScheduledAt.Time).To(BeTemporally("==", due.Add(5*time.Minute)))
			Expect(st.RecentFirings).To(HaveLen(1))
			Expect(st.RecentFirings[0].Run).To(Equal(runs[0].Name))
			Expect(res.RequeueAfter).To(BeNumerically("~", 5*time.Minute-10*time.Second, 2*time.Second))

			// Reconciling again before the next time starts nothing.
			reconcileTrigger("cron")
			Expect(runsOf("cron")).To(HaveLen(1))
			Expect(getTrigger("cron").Status.RecentFirings).To(HaveLen(1))
			Expect(firings("cron", "schedule", "started")).To(Equal(1.0))
		})

		It("catches up a missed time once, within startingDeadlineSeconds", func() {
			t := scheduleTrigger("catchup", func(s *aiv1alpha1.TriggerSpec) {
				s.Source.Schedule.StartingDeadlineSeconds = ptr.To[int64](120)
			})
			due := nextFive(t.CreationTimestamp.Time)

			// Down across three schedule times: one catch-up run for the latest.
			latest := due.Add(10 * time.Minute)
			clock = latest.Add(time.Minute)
			reconcileTrigger("catchup")
			runs := runsOf("catchup")
			Expect(runs).To(HaveLen(1))
			Expect(runs[0].Annotations[aiv1alpha1.AnnotationTriggerKey]).To(Equal(latest.Format(time.RFC3339)))

			// Missed by more than the deadline: recorded, nothing started.
			clock = latest.Add(5*time.Minute + 3*time.Minute)
			reconcileTrigger("catchup")
			Expect(runsOf("catchup")).To(HaveLen(1))
			st := getTrigger("catchup").Status
			Expect(st.RecentFirings[0].SkipReason).To(ContainSubstring("startingDeadlineSeconds"))
			Expect(st.LastScheduledAt.Time).To(BeTemporally("==", latest.Add(5*time.Minute)))
		})

		It("keeps the target's input when spec.input is empty and reports a missing target", func() {
			t := scheduleTrigger("noinput", func(s *aiv1alpha1.TriggerSpec) { s.Input = "" })
			clock = nextFive(t.CreationTimestamp.Time).Add(time.Second)
			reconcileTrigger("noinput")
			runs := runsOf("noinput")
			Expect(runs).To(HaveLen(1))
			Expect(runs[0].Spec.Input).To(Equal(`{"default": true}`))

			scheduleTrigger("missing", func(s *aiv1alpha1.TriggerSpec) { s.Target.ChainRef.Name = "nope" })
			reconcileTrigger("missing")
			cond := meta.FindStatusCondition(getTrigger("missing").Status.Conditions, aiv1alpha1.ConditionTriggerReady)
			Expect(cond.Reason).To(Equal(aiv1alpha1.ReasonTriggerTargetNotFound))
		})

		annotate := func(name string, set map[string]string, del ...string) {
			t := getTrigger(name)
			patch := client.MergeFrom(t.DeepCopy())
			if t.Annotations == nil {
				t.Annotations = map[string]string{}
			}
			maps.Copy(t.Annotations, set)
			for _, k := range del {
				delete(t.Annotations, k)
			}
			Expect(k8sClient.Patch(ctx, t, patch)).To(Succeed())
		}

		It("is suspended by the suspended annotation while spec.suspend is false, and resumes when it is removed", func() {
			t := scheduleTrigger("annsuspend", nil)
			Expect(t.Spec.Suspend).To(BeFalse())
			due := nextFive(t.CreationTimestamp.Time)
			annotate("annsuspend", map[string]string{aiv1alpha1.AnnotationTriggerSuspended: "token"})

			clock = due.Add(10 * time.Second)
			reconcileTrigger("annsuspend")
			Expect(runsOf("annsuspend")).To(BeEmpty())
			t = getTrigger("annsuspend")
			Expect(t.Status.SuspendedBy).To(Equal(aiv1alpha1.TriggerSuspendedByAnnotation))
			Expect(t.Status.NextScheduledAt).To(BeNil())
			cond := meta.FindStatusCondition(t.Status.Conditions, aiv1alpha1.ConditionTriggerReady)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(aiv1alpha1.ReasonTriggerSuspended))
			Expect(cond.Message).To(ContainSubstring(aiv1alpha1.AnnotationTriggerSuspended))
			Expect(cond.Message).To(ContainSubstring("by token"))

			// Both set: status names both sources.
			t.Spec.Suspend = true
			Expect(k8sClient.Update(ctx, t)).To(Succeed())
			reconcileTrigger("annsuspend")
			Expect(getTrigger("annsuspend").Status.SuspendedBy).To(Equal("spec,annotation"))
			t = getTrigger("annsuspend")
			t.Spec.Suspend = false
			Expect(k8sClient.Update(ctx, t)).To(Succeed())

			// Removing the annotation resumes firing.
			annotate("annsuspend", nil, aiv1alpha1.AnnotationTriggerSuspended)
			reconcileTrigger("annsuspend")
			Expect(runsOf("annsuspend")).To(HaveLen(1))
			t = getTrigger("annsuspend")
			Expect(t.Status.SuspendedBy).To(BeEmpty())
			Expect(meta.IsStatusConditionTrue(t.Status.Conditions, aiv1alpha1.ConditionTriggerReady)).To(BeTrue())
		})

		It("fires once now for the run-now annotation, even while suspended, and records the caller", func() {
			t := scheduleTrigger("runnow", func(s *aiv1alpha1.TriggerSpec) {
				s.Suspend = true
				s.DedupeKey = "ignored-for-manual-runs"
			})
			clock = t.CreationTimestamp.Add(time.Second)
			annotate("runnow", map[string]string{
				aiv1alpha1.AnnotationTriggerRunNow:       "req-1",
				aiv1alpha1.AnnotationTriggerRunNowCaller: "token",
			})
			reconcileTrigger("runnow")
			runs := runsOf("runnow")
			Expect(runs).To(HaveLen(1))
			key := aiv1alpha1.TriggerRunNowKey("req-1")
			Expect(runs[0].Name).To(Equal(chainrun.TriggerRunName(tmplName, "runnow", key)))
			Expect(runs[0].Annotations[aiv1alpha1.AnnotationTriggerKey]).To(Equal(key))
			Expect(runs[0].Annotations[aiv1alpha1.AnnotationCaller]).To(Equal("token"))
			Expect(runs[0].Labels[aiv1alpha1.LabelTrigger]).To(Equal("runnow"))
			Expect(runs[0].Spec.Input).To(Equal(fmt.Sprintf(`{"at": "%s"}`, clock.UTC().Format("15:04"))))
			t = getTrigger("runnow")
			Expect(t.Annotations).NotTo(HaveKey(aiv1alpha1.AnnotationTriggerRunNow))
			Expect(t.Annotations).NotTo(HaveKey(aiv1alpha1.AnnotationTriggerRunNowCaller))
			Expect(t.Status.RecentFirings[0].Key).To(Equal(key))
			Expect(t.Status.RecentFirings[0].Run).To(Equal(runs[0].Name))
			Expect(t.Status.SuspendedBy).To(Equal(aiv1alpha1.TriggerSuspendedBySpec))

			// Reconciling again starts nothing more.
			reconcileTrigger("runnow")
			Expect(runsOf("runnow")).To(HaveLen(1))
		})

		It("applies the concurrency policy to a run-now firing", func() {
			scheduleTrigger("runnowforbid", func(s *aiv1alpha1.TriggerSpec) {
				s.Concurrency.Policy = aiv1alpha1.TriggerConcurrencyForbid
			})
			annotate("runnowforbid", map[string]string{aiv1alpha1.AnnotationTriggerRunNow: "a"})
			reconcileTrigger("runnowforbid")
			Expect(runsOf("runnowforbid")).To(HaveLen(1))

			annotate("runnowforbid", map[string]string{aiv1alpha1.AnnotationTriggerRunNow: "b"})
			reconcileTrigger("runnowforbid")
			Expect(runsOf("runnowforbid")).To(HaveLen(1))
			st := getTrigger("runnowforbid").Status
			Expect(st.RecentFirings[0].Key).To(Equal(aiv1alpha1.TriggerRunNowKey("b")))
			Expect(st.RecentFirings[0].SkipReason).To(ContainSubstring("concurrency Forbid"))
			Expect(getTrigger("runnowforbid").Annotations).NotTo(HaveKey(aiv1alpha1.AnnotationTriggerRunNow))
		})
	})
	Context("knight target", func() {
		const knightName = "trg-knight"
		var nc *nats.Conn

		BeforeEach(func() {
			var err error
			nc, err = nats.Connect(srv.ClientURL())
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(nc.Close)
			Expect(k8sClient.Create(ctx, &aiv1alpha1.Knight{
				ObjectMeta: metav1.ObjectMeta{Name: knightName, Namespace: ns,
					Labels: map[string]string{aiv1alpha1.LabelRoundTable: rtName}},
				Spec: aiv1alpha1.KnightSpec{
					Domain: "ops",
					Model:  "claude-sonnet-4-20250514",
					Skills: []string{"ops"},
					NATS: aiv1alpha1.KnightNATS{
						URL:      srv.ClientURL(),
						Subjects: []string{prefix + ".tasks.ops.>"},
						Stream:   "trg_tasks",
					},
				},
			})).To(Succeed())
			DeferCleanup(func() {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &aiv1alpha1.Knight{
					ObjectMeta: metav1.ObjectMeta{Name: knightName, Namespace: ns}}))).To(Succeed())
			})
		})

		knightTarget := func(task string) aiv1alpha1.TriggerTarget {
			return aiv1alpha1.TriggerTarget{Knight: &aiv1alpha1.TriggerKnightTarget{Name: knightName, Task: task}}
		}
		// tasks subscribes to the knight's task subject and returns the
		// payloads published there so far.
		tasks := func() func() []natspkg.TaskPayload {
			sub, err := nc.SubscribeSync(natspkg.TaskSubject(prefix, "ops", knightName))
			Expect(err).NotTo(HaveOccurred())
			Expect(nc.Flush()).To(Succeed())
			DeferCleanup(func() { _ = sub.Unsubscribe() })
			var got []natspkg.TaskPayload
			return func() []natspkg.TaskPayload {
				for {
					msg, err := sub.NextMsg(10 * time.Millisecond)
					if err != nil {
						return got
					}
					var p natspkg.TaskPayload
					Expect(json.Unmarshal(msg.Data, &p)).To(Succeed())
					got = append(got, p)
				}
			}
		}
		publishResult := func(taskID string, result string) {
			_, err := js.Publish(ctx, natspkg.ResultSubject(prefix, taskID), []byte(result))
			Expect(err).NotTo(HaveOccurred())
		}
		scheduleKnightTrigger := func(name string, mutate func(*aiv1alpha1.TriggerSpec)) *aiv1alpha1.Trigger {
			t := &aiv1alpha1.Trigger{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
				Spec: aiv1alpha1.TriggerSpec{
					Source: aiv1alpha1.TriggerSource{Schedule: &aiv1alpha1.TriggerScheduleSource{Cron: "*/5 * * * *", TimeZone: "UTC"}},
					Target: knightTarget(`Brief me for {{ .Event.Time.UTC.Format "15:04" }} ({{ .Trigger.Name }})`),
				},
			}
			if mutate != nil {
				mutate(&t.Spec)
			}
			Expect(k8sClient.Create(ctx, t)).To(Succeed())
			return getTrigger(name)
		}
		nextFive := func(t time.Time) time.Time {
			return t.UTC().Truncate(5 * time.Minute).Add(5 * time.Minute)
		}

		It("rejects both or no targets and what a knight target does not support", func() {
			t := scheduleKnightTrigger("kv-ok", nil)
			Expect(t.Spec.Target.Knight.TimeoutSeconds).To(HaveValue(Equal(int32(3600))))

			bad := func(name string, mutate func(*aiv1alpha1.TriggerSpec)) error {
				t := &aiv1alpha1.Trigger{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
					Spec: aiv1alpha1.TriggerSpec{
						Source: aiv1alpha1.TriggerSource{Schedule: &aiv1alpha1.TriggerScheduleSource{Cron: "* * * * *"}},
						Target: knightTarget("hi"),
					},
				}
				mutate(&t.Spec)
				return k8sClient.Create(ctx, t)
			}
			Expect(bad("kv-both", func(s *aiv1alpha1.TriggerSpec) {
				s.Target.ChainRef = &corev1.LocalObjectReference{Name: tmplName}
			})).To(MatchError(ContainSubstring("set exactly one of target.chainRef, target.knight or target.mission")))
			Expect(bad("kv-none", func(s *aiv1alpha1.TriggerSpec) {
				s.Target.Knight = nil
			})).To(MatchError(ContainSubstring("set exactly one of target.chainRef, target.knight or target.mission")))
			Expect(bad("kv-replace", func(s *aiv1alpha1.TriggerSpec) {
				s.Concurrency.Policy = aiv1alpha1.TriggerConcurrencyReplace
			})).To(MatchError(ContainSubstring("Replace is not supported with target.knight")))
			Expect(bad("kv-input", func(s *aiv1alpha1.TriggerSpec) {
				s.Input = "x"
			})).To(MatchError(ContainSubstring("spec.input is for target.chainRef")))
		})

		It("sends exactly one task with the rendered text and records its outcome", func() {
			got := tasks()
			t := scheduleKnightTrigger("kt-cron", nil)
			due := nextFive(t.CreationTimestamp.Time)
			clock = due.Add(10 * time.Second)
			res := reconcileTrigger("kt-cron")

			var sent []natspkg.TaskPayload
			Eventually(func() int { sent = got(); return len(sent) }).Should(Equal(1))
			Expect(sent[0].Task).To(Equal(fmt.Sprintf("Brief me for %s (kt-cron)", due.Format("15:04"))))
			Expect(sent[0].TaskID).To(HavePrefix("invoke-" + knightName + "-"))
			st := getTrigger("kt-cron").Status
			Expect(st.RecentFirings).To(HaveLen(1))
			Expect(st.RecentFirings[0].Run).To(Equal(sent[0].TaskID))
			Expect(st.RecentFirings[0].Outcome).To(BeEmpty())
			Expect(st.ActiveRuns).To(Equal([]string{sent[0].TaskID}))
			Expect(st.LastFiredAt).NotTo(BeNil())
			Expect(res.RequeueAfter).To(BeNumerically("<=", triggerTaskPoll))
			Expect(runsOf("kt-cron")).To(BeEmpty())
			Expect(firings("kt-cron", "schedule", "started")).To(Equal(1.0))

			// The task is recorded where the gateway keeps its tasks.
			svc, err := r.taskService(ctx)
			Expect(err).NotTo(HaveOccurred())
			rec, err := svc.GetTask(ctx, sent[0].TaskID)
			Expect(err).NotTo(HaveOccurred())
			Expect(rec.Knight).To(Equal(knightName))
			Expect(rec.RoundTable).To(Equal(rtName))
			Expect(rec.Done()).To(BeFalse())

			// Firing the same schedule time again sends nothing.
			_, err = r.fire(ctx, types.NamespacedName{Namespace: ns, Name: "kt-cron"},
				TriggerEvent{Time: due, Headers: map[string]string{}}, due.Format(time.RFC3339), "", nil, nil)
			Expect(err).NotTo(HaveOccurred())
			Consistently(func() []natspkg.TaskPayload { return got() }, "200ms").Should(HaveLen(1))
			Expect(getTrigger("kt-cron").Status.RecentFirings[0].SkipReason).To(ContainSubstring("duplicate dedupe key"))

			publishResult(sent[0].TaskID, `{"taskId":"`+sent[0].TaskID+`","output":"all quiet"}`)
			reconcileTrigger("kt-cron")
			st = getTrigger("kt-cron").Status
			Expect(st.ActiveRuns).To(BeEmpty())
			Expect(st.RecentFirings[1].Run).To(Equal(sent[0].TaskID))
			Expect(st.RecentFirings[1].Outcome).To(Equal(aiv1alpha1.TriggerFiringSucceeded))
			rec, err = svc.GetTask(ctx, sent[0].TaskID)
			Expect(err).NotTo(HaveOccurred())
			Expect(rec.Output).To(Equal("all quiet"))
		})

		It("applies Forbid while a task is active and records a task without result as lost", func() {
			got := tasks()
			t := scheduleKnightTrigger("kt-forbid", func(s *aiv1alpha1.TriggerSpec) {
				s.Concurrency.Policy = aiv1alpha1.TriggerConcurrencyForbid
				s.Target.Knight.TimeoutSeconds = ptr.To[int32](600)
			})
			due := nextFive(t.CreationTimestamp.Time)
			clock = due.Add(time.Second)
			reconcileTrigger("kt-forbid")
			var sent []natspkg.TaskPayload
			Eventually(func() int { sent = got(); return len(sent) }).Should(Equal(1))

			// The next time finds the task active: nothing is sent.
			clock = due.Add(5*time.Minute + time.Second)
			reconcileTrigger("kt-forbid")
			Consistently(func() []natspkg.TaskPayload { return got() }, "200ms").Should(HaveLen(1))
			st := getTrigger("kt-forbid").Status
			Expect(st.RecentFirings[0].SkipReason).To(ContainSubstring("concurrency Forbid"))

			// No result within timeoutSeconds: lost, and no longer active.
			clock = due.Add(11 * time.Minute)
			reconcileTrigger("kt-forbid")
			st = getTrigger("kt-forbid").Status
			Expect(st.ActiveRuns).To(BeEmpty())
			Expect(got()).To(HaveLen(1))
			Expect(st.RecentFirings).To(ContainElement(SatisfyAll(
				HaveField("Run", sent[0].TaskID), HaveField("Outcome", aiv1alpha1.TriggerFiringLost))))
		})

		It("dedupes NATS events by dedupeKey and holds maxActive", func() {
			got := tasks()
			natsTrigger("kt-nats", prefix+".events.alerts.>", func(s *aiv1alpha1.TriggerSpec) {
				s.Input = ""
				s.Target = knightTarget(`look at {{ .Event.Data.alert }}`)
				s.DedupeKey = `{{ .Event.Data.alert }}`
				s.Concurrency.MaxActive = 2
			})
			Expect(meta.IsStatusConditionTrue(getTrigger("kt-nats").Status.Conditions, aiv1alpha1.ConditionTriggerReady)).To(BeTrue())

			publish(prefix+".events.alerts.a", "1", `{"alert": "disk"}`)
			publish(prefix+".events.alerts.a", "2", `{"alert": "disk"}`)
			publish(prefix+".events.alerts.b", "3", `{"alert": "cpu"}`)
			Eventually(func() int { return len(getTrigger("kt-nats").Status.RecentFirings) }, "10s").Should(Equal(3))
			sent := got()
			Expect(sent).To(HaveLen(2))
			Expect([]string{sent[0].Task, sent[1].Task}).To(ConsistOf("look at disk", "look at cpu"))

			publish(prefix+".events.alerts.c", "4", `{"alert": "mem"}`)
			Eventually(func() string { return getTrigger("kt-nats").Status.RecentFirings[0].SkipReason }, "10s").
				Should(ContainSubstring("maxActive 2 reached"))
			Expect(got()).To(HaveLen(2))
			skipped := 0
			for _, f := range getTrigger("kt-nats").Status.RecentFirings {
				if strings.Contains(f.SkipReason, "duplicate dedupe key") {
					skipped++
				}
			}
			Expect(skipped).To(Equal(1))
		})

		It("fires once now for the run-now annotation and records the caller on the task", func() {
			got := tasks()
			t := scheduleKnightTrigger("kt-runnow", func(s *aiv1alpha1.TriggerSpec) { s.Suspend = true })
			clock = t.CreationTimestamp.Add(time.Second)
			patch := client.MergeFrom(t.DeepCopy())
			t.Annotations = map[string]string{
				aiv1alpha1.AnnotationTriggerRunNow:       "req-k",
				aiv1alpha1.AnnotationTriggerRunNowCaller: "roundtable/ui",
			}
			Expect(k8sClient.Patch(ctx, t, patch)).To(Succeed())
			reconcileTrigger("kt-runnow")

			var sent []natspkg.TaskPayload
			Eventually(func() int { sent = got(); return len(sent) }).Should(Equal(1))
			t = getTrigger("kt-runnow")
			Expect(t.Annotations).NotTo(HaveKey(aiv1alpha1.AnnotationTriggerRunNow))
			Expect(t.Status.RecentFirings).To(HaveLen(1))
			Expect(t.Status.RecentFirings[0].Key).To(Equal(aiv1alpha1.TriggerRunNowKey("req-k")))
			Expect(t.Status.RecentFirings[0].Run).To(Equal(sent[0].TaskID))

			svc, err := r.taskService(ctx)
			Expect(err).NotTo(HaveOccurred())
			rec, err := svc.GetTask(ctx, sent[0].TaskID)
			Expect(err).NotTo(HaveOccurred())
			Expect(rec.Caller).To(Equal("roundtable/ui"))
		})

		It("reports a missing knight or one at another table and sends nothing", func() {
			got := tasks()
			t := scheduleKnightTrigger("kt-missing", func(s *aiv1alpha1.TriggerSpec) { s.Target.Knight.Name = "nobody" })
			clock = nextFive(t.CreationTimestamp.Time).Add(time.Second)
			reconcileTrigger("kt-missing")
			cond := meta.FindStatusCondition(getTrigger("kt-missing").Status.Conditions, aiv1alpha1.ConditionTriggerReady)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(aiv1alpha1.ReasonTriggerTargetNotFound))
			Expect(getTrigger("kt-missing").Status.RecentFirings).To(BeEmpty())

			Expect(k8sClient.Create(ctx, &aiv1alpha1.RoundTable{
				ObjectMeta: metav1.ObjectMeta{Name: "trg-other", Namespace: ns},
				Spec:       aiv1alpha1.RoundTableSpec{NATS: aiv1alpha1.RoundTableNATS{SubjectPrefix: "other"}},
			})).To(Succeed())
			DeferCleanup(func() {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &aiv1alpha1.RoundTable{
					ObjectMeta: metav1.ObjectMeta{Name: "trg-other", Namespace: ns}}))).To(Succeed())
			})
			t = scheduleKnightTrigger("kt-table", func(s *aiv1alpha1.TriggerSpec) {
				s.RoundTableRef = &corev1.LocalObjectReference{Name: "trg-other"}
			})
			clock = nextFive(t.CreationTimestamp.Time).Add(time.Second)
			reconcileTrigger("kt-table")
			cond = meta.FindStatusCondition(getTrigger("kt-table").Status.Conditions, aiv1alpha1.ConditionTriggerReady)
			Expect(cond.Reason).To(Equal(aiv1alpha1.ReasonTriggerTargetWrongTable))
			Expect(getTrigger("kt-table").Status.RecentFirings).To(BeEmpty())
			Consistently(func() []natspkg.TaskPayload { return got() }, "200ms").Should(BeEmpty())
		})
	})

	Context("mission target", func() {
		const plannerName = "trg-planner"
		missionTarget := func() aiv1alpha1.TriggerTarget {
			return aiv1alpha1.TriggerTarget{Mission: &aiv1alpha1.TriggerMissionTarget{
				Objective:       `Review {{ .Trigger.Name }} at {{ .Event.Time.UTC.Format "15:04" }}`,
				SuccessCriteria: "a short list",
				CostBudgetUSD:   "2.50",
				TimeoutSeconds:  600,
				TTLSeconds:      3600,
				Planner:         &aiv1alpha1.TriggerMissionPlanner{Knight: plannerName, MaxChains: 3},
			}}
		}
		scheduleMissionTrigger := func(name string, mutate func(*aiv1alpha1.TriggerSpec)) *aiv1alpha1.Trigger {
			t := &aiv1alpha1.Trigger{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
				Spec: aiv1alpha1.TriggerSpec{
					Source:        aiv1alpha1.TriggerSource{Schedule: &aiv1alpha1.TriggerScheduleSource{Cron: "*/5 * * * *", TimeZone: "UTC"}},
					Target:        missionTarget(),
					RoundTableRef: &corev1.LocalObjectReference{Name: rtName},
				},
			}
			if mutate != nil {
				mutate(&t.Spec)
			}
			Expect(k8sClient.Create(ctx, t)).To(Succeed())
			return getTrigger(name)
		}
		nextFive := func(t time.Time) time.Time {
			return t.UTC().Truncate(5 * time.Minute).Add(5 * time.Minute)
		}
		missionsOf := func(trigger string) []aiv1alpha1.Mission {
			list := &aiv1alpha1.MissionList{}
			Expect(k8sClient.List(ctx, list, client.InNamespace(ns),
				client.MatchingLabels{aiv1alpha1.LabelTrigger: trigger})).To(Succeed())
			return list.Items
		}
		setPhase := func(m *aiv1alpha1.Mission, phase aiv1alpha1.MissionPhase) {
			m.Status.Phase = phase
			Expect(k8sClient.Status().Update(ctx, m)).To(Succeed())
		}

		BeforeEach(func() {
			Expect(k8sClient.Create(ctx, &aiv1alpha1.Knight{
				ObjectMeta: metav1.ObjectMeta{Name: plannerName, Namespace: ns},
				Spec: aiv1alpha1.KnightSpec{
					Domain: "ops", Model: "claude-sonnet-4-20250514", Skills: []string{"ops"},
					NATS: aiv1alpha1.KnightNATS{URL: srv.ClientURL(), Subjects: []string{prefix + ".tasks.ops.>"}, Stream: "trg_tasks"},
				},
			})).To(Succeed())
			DeferCleanup(func() {
				Expect(k8sClient.DeleteAllOf(ctx, &aiv1alpha1.Mission{}, client.InNamespace(ns))).To(Succeed())
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &aiv1alpha1.Knight{
					ObjectMeta: metav1.ObjectMeta{Name: plannerName, Namespace: ns}}))).To(Succeed())
			})
		})

		It("fires once now for the run-now annotation on a suspended schedule and records the caller on the Mission", func() {
			t := scheduleMissionTrigger("mt-runnow", func(s *aiv1alpha1.TriggerSpec) { s.Suspend = true })
			clock = t.CreationTimestamp.Add(time.Second)
			patch := client.MergeFrom(t.DeepCopy())
			t.Annotations = map[string]string{
				aiv1alpha1.AnnotationTriggerRunNow:       "req-m",
				aiv1alpha1.AnnotationTriggerRunNowCaller: "roundtable/ui",
			}
			Expect(k8sClient.Patch(ctx, t, patch)).To(Succeed())
			reconcileTrigger("mt-runnow")

			ms := missionsOf("mt-runnow")
			Expect(ms).To(HaveLen(1))
			Expect(ms[0].Annotations).To(HaveKeyWithValue(aiv1alpha1.AnnotationCaller, "roundtable/ui"))
			t = getTrigger("mt-runnow")
			Expect(t.Annotations).NotTo(HaveKey(aiv1alpha1.AnnotationTriggerRunNow))
			Expect(t.Status.RecentFirings).To(HaveLen(1))
			Expect(t.Status.RecentFirings[0].Key).To(Equal(aiv1alpha1.TriggerRunNowKey("req-m")))
			Expect(t.Status.RecentFirings[0].Run).To(Equal(ms[0].Name))
		})

		It("creates one Mission per firing with the rendered objective and budget, skips under Forbid, and records its phase", func() {
			t := scheduleMissionTrigger("mt-cron", func(s *aiv1alpha1.TriggerSpec) {
				s.Concurrency.Policy = aiv1alpha1.TriggerConcurrencyForbid
			})
			Expect(t.Spec.Target.Mission.MetaMission).To(HaveValue(BeTrue()))
			due := nextFive(t.CreationTimestamp.Time)
			clock = due.Add(10 * time.Second)
			reconcileTrigger("mt-cron")

			ms := missionsOf("mt-cron")
			Expect(ms).To(HaveLen(1))
			m := ms[0]
			Expect(m.Name).To(HavePrefix("mt-cron-"))
			Expect(m.Spec.Objective).To(Equal("Review mt-cron at " + due.Format("15:04")))
			Expect(m.Spec.SuccessCriteria).To(Equal("a short list"))
			Expect(m.Spec.CostBudgetUSD).To(Equal("2.5"))
			Expect(m.Spec.MetaMission).To(BeTrue())
			Expect(m.Spec.Planner.KnightRef.Name).To(Equal(plannerName))
			Expect(m.Spec.Planner.MaxChains).To(Equal(int32(3)))
			Expect(aiv1alpha1.RefName(m.Spec.RoundTableRef)).To(Equal(rtName))
			Expect(metav1.IsControlledBy(&m, getTrigger("mt-cron"))).To(BeTrue())
			st := getTrigger("mt-cron").Status
			Expect(st.RecentFirings).To(HaveLen(1))
			Expect(st.RecentFirings[0].Run).To(Equal(m.Name))
			Expect(st.ActiveRuns).To(Equal([]string{m.Name}))

			// The next time finds the Mission unfinished: nothing is created.
			clock = due.Add(5*time.Minute + time.Second)
			reconcileTrigger("mt-cron")
			Expect(missionsOf("mt-cron")).To(HaveLen(1))
			Expect(getTrigger("mt-cron").Status.RecentFirings[0].SkipReason).To(ContainSubstring("concurrency Forbid"))

			// Finished: the firing gets its outcome and phase, and the
			// next time creates a new Mission.
			// The mission controller moves a finished Mission on to CleaningUp;
			// the final phase lives in the Complete condition.
			meta.SetStatusCondition(&m.Status.Conditions, metav1.Condition{
				Type: aiv1alpha1.ConditionMissionComplete, Status: metav1.ConditionTrue,
				Reason: aiv1alpha1.ReasonMissionSucceeded,
			})
			setPhase(&m, aiv1alpha1.MissionPhaseCleaningUp)
			clock = due.Add(10*time.Minute + time.Second)
			reconcileTrigger("mt-cron")
			st = getTrigger("mt-cron").Status
			Expect(st.ActiveRuns).To(ContainElement(Not(Equal(m.Name))))
			Expect(st.RecentFirings).To(ContainElement(SatisfyAll(
				HaveField("Run", m.Name),
				HaveField("Outcome", aiv1alpha1.TriggerFiringSucceeded),
				HaveField("Phase", aiv1alpha1.MissionPhaseSucceeded))))
			Expect(missionsOf("mt-cron")).To(HaveLen(2))
		})

		It("rejects two target kinds and what a mission target does not support", func() {
			bad := func(name string, mutate func(*aiv1alpha1.TriggerSpec)) error {
				t := &aiv1alpha1.Trigger{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
					Spec: aiv1alpha1.TriggerSpec{
						Source:        aiv1alpha1.TriggerSource{Schedule: &aiv1alpha1.TriggerScheduleSource{Cron: "* * * * *"}},
						Target:        missionTarget(),
						RoundTableRef: &corev1.LocalObjectReference{Name: rtName},
					},
				}
				mutate(&t.Spec)
				return k8sClient.Create(ctx, t)
			}
			Expect(bad("mv-chain", func(s *aiv1alpha1.TriggerSpec) {
				s.Target.ChainRef = &corev1.LocalObjectReference{Name: tmplName}
			})).To(MatchError(ContainSubstring("set exactly one of target.chainRef, target.knight or target.mission")))
			Expect(bad("mv-knight", func(s *aiv1alpha1.TriggerSpec) {
				s.Target.Knight = &aiv1alpha1.TriggerKnightTarget{Name: "k", Task: "t"}
			})).To(MatchError(ContainSubstring("set exactly one of target.chainRef, target.knight or target.mission")))
			Expect(bad("mv-table", func(s *aiv1alpha1.TriggerSpec) {
				s.RoundTableRef = nil
			})).To(MatchError(ContainSubstring("roundTableRef is required for target.mission")))
			Expect(bad("mv-replace", func(s *aiv1alpha1.TriggerSpec) {
				s.Concurrency.Policy = aiv1alpha1.TriggerConcurrencyReplace
			})).To(MatchError(ContainSubstring("Replace is not supported with target.mission")))
			Expect(bad("mv-input", func(s *aiv1alpha1.TriggerSpec) {
				s.Input = "x"
			})).To(MatchError(ContainSubstring("spec.input is for target.chainRef")))
		})

		It("records a request the gateway builder rejects and creates nothing", func() {
			t := scheduleMissionTrigger("mt-bad", func(s *aiv1alpha1.TriggerSpec) {
				s.Target.Mission.CostBudgetUSD = "1000000"
			})
			clock = nextFive(t.CreationTimestamp.Time).Add(time.Second)
			reconcileTrigger("mt-bad")
			Expect(missionsOf("mt-bad")).To(BeEmpty())
			Expect(getTrigger("mt-bad").Status.RecentFirings[0].SkipReason).To(ContainSubstring("costBudgetUSD"))
		})

		It("skips a firing over the table's maxMissions with the reason", func() {
			rt := &aiv1alpha1.RoundTable{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: rtName}, rt)).To(Succeed())
			rt.Spec.Policies = &aiv1alpha1.RoundTablePolicies{MaxMissions: 1}
			Expect(k8sClient.Update(ctx, rt)).To(Succeed())

			t := scheduleMissionTrigger("mt-limit", nil)
			due := nextFive(t.CreationTimestamp.Time)
			clock = due.Add(time.Second)
			reconcileTrigger("mt-limit")
			Expect(missionsOf("mt-limit")).To(HaveLen(1))

			clock = due.Add(5*time.Minute + time.Second)
			reconcileTrigger("mt-limit")
			Expect(missionsOf("mt-limit")).To(HaveLen(1))
			Expect(getTrigger("mt-limit").Status.RecentFirings[0].SkipReason).To(ContainSubstring("policies.maxMissions 1"))
		})
	})
})
