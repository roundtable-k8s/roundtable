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
	"context"
	"encoding/json"
	"errors"
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
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/chainrun"
	"github.com/roundtable-k8s/roundtable/internal/gateway"
	"github.com/roundtable-k8s/roundtable/pkg/metrics"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// firings reads roundtable_trigger_firings_total for a trigger of the test table.
func firings(trigger, source, outcome string) float64 {
	return testutil.ToFloat64(metrics.TriggerFiringsTotal.WithLabelValues("trg-rt", trigger, source, outcome))
}

// finishedRuns reads roundtable_trigger_runs_finished_total for a schedule trigger of the test table.
func finishedRuns(trigger, outcome string) float64 {
	return testutil.ToFloat64(metrics.TriggerRunsFinishedTotal.WithLabelValues("trg-rt", trigger, "schedule", outcome))
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
	getCronJob := func(name string) *batchv1.CronJob {
		cj := &batchv1.CronJob{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, cj)).To(Succeed())
		return cj
	}
	getSA := func(name string) *corev1.ServiceAccount {
		sa := &corev1.ServiceAccount{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, sa)).To(Succeed())
		return sa
	}
	// fireSchedule publishes a scheduled firing the way the gateway's fire
	// endpoint does for the Trigger's CronJob: firing key = schedule time.
	// msgID overrides the Nats-Msg-Id (a run after the duplicate window).
	fireSchedule := func(trigger string, at time.Time, msgID string) {
		key := at.UTC().Format(time.RFC3339)
		msg := nats.NewMsg(natspkg.ScheduleEventSubject(prefix, ns, trigger))
		msg.Header.Set(natspkg.ScheduledTimeHeader, key)
		msg.Header.Set(natspkg.EventKeyHeader, key)
		if msgID == "" {
			msgID = key
		}
		_, err := js.PublishMsg(ctx, msg, jetstream.WithMsgID(msgID))
		Expect(err).NotTo(HaveOccurred())
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
			Schedule: TriggerScheduleConfig{
				Image: "operator:test", GatewayURL: "http://gateway.test", Audience: "roundtable-gateway"},
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
		Expect(k8sClient.DeleteAllOf(ctx, &batchv1.CronJob{}, client.InNamespace(ns),
			client.HasLabels{aiv1alpha1.LabelTrigger})).To(Succeed())
		Expect(k8sClient.DeleteAllOf(ctx, &corev1.ServiceAccount{}, client.InNamespace(ns),
			client.HasLabels{aiv1alpha1.LabelTrigger})).To(Succeed())
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
			Expect(testutil.ToFloat64(metrics.TriggerLastFiredTimestampSeconds.WithLabelValues(ns, rtName, "on-deploy"))).To(BeNumerically(">", 0))
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

		It("starts a run for a message with no key after the events stream is recreated", func() {
			t := natsTrigger("recreate", prefix+".events.recreate", nil)
			publish(prefix+".events.recreate", "", `{"service":"api"}`)
			Eventually(func() int { return len(runsOf("recreate")) }, 10*time.Second, 100*time.Millisecond).Should(Equal(1))
			first := runsOf("recreate")[0]

			// Recreating the stream restarts its sequence at 1 and removes the
			// consumer; the next reconcile recreates both and the loop.
			Expect(js.DeleteStream(ctx, stream)).To(Succeed())
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
			rt := &aiv1alpha1.RoundTable{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: rtName}, rt)).To(Succeed())
			Expect((&RoundTableReconciler{NATS: r.NATS}).ensureStreams(ctx, rt)).To(Succeed())
			reconcileTrigger("recreate")
			Expect(consumerExists(t.Status.Consumer)).To(BeTrue())

			publish(prefix+".events.recreate", "", `{"service":"api"}`)
			Eventually(func() int { return len(runsOf("recreate")) }, 10*time.Second, 100*time.Millisecond).Should(Equal(2))
			for _, run := range runsOf("recreate") {
				if run.Name != first.Name {
					Expect(run.Annotations[aiv1alpha1.AnnotationTriggerKey]).To(HaveSuffix("/1"))
				}
			}
		})

		It("treats a run under the former seq-<n> key as a duplicate", func() {
			natsTrigger("legacy", prefix+".events.legacy", nil)
			tmpl := &aiv1alpha1.Chain{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: tmplName}, tmpl)).To(Succeed())
			old := &aiv1alpha1.Chain{
				ObjectMeta: metav1.ObjectMeta{Name: chainrun.TriggerRunName(tmplName, "legacy", "seq-1"), Namespace: ns,
					Labels: map[string]string{aiv1alpha1.LabelTrigger: "legacy"}},
				Spec: tmpl.Spec,
			}
			old.Spec.Schedule = ""
			Expect(k8sClient.Create(ctx, old)).To(Succeed())

			publish(prefix+".events.legacy", "", `{"service":"api"}`)
			Eventually(func(g Gomega) {
				f := getTrigger("legacy").Status.RecentFirings
				g.Expect(f).To(HaveLen(1))
				g.Expect(f[0].SkipReason).To(ContainSubstring("legacy key seq-1"))
			}, 10*time.Second, 100*time.Millisecond).Should(Succeed())
			Expect(runsOf("legacy")).To(HaveLen(1))
		})

		It("runs a message whose former seq-<n> run is older than the stream", func() {
			natsTrigger("oldlegacy", prefix+".events.oldlegacy", nil)
			tmpl := &aiv1alpha1.Chain{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: tmplName}, tmpl)).To(Succeed())
			old := &aiv1alpha1.Chain{
				ObjectMeta: metav1.ObjectMeta{Name: chainrun.TriggerRunName(tmplName, "oldlegacy", "seq-1"), Namespace: ns,
					Labels: map[string]string{aiv1alpha1.LabelTrigger: "oldlegacy"}},
				Spec: tmpl.Spec,
			}
			old.Spec.Schedule = ""
			Expect(k8sClient.Create(ctx, old)).To(Succeed())
			// The stream is recreated after that run: wait out the
			// one-second resolution of creationTimestamp.
			time.Sleep(2 * time.Second)
			Expect(js.DeleteStream(ctx, stream)).To(Succeed())
			rt := &aiv1alpha1.RoundTable{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: rtName}, rt)).To(Succeed())
			Expect((&RoundTableReconciler{NATS: r.NATS}).ensureStreams(ctx, rt)).To(Succeed())
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
			reconcileTrigger("oldlegacy")

			publish(prefix+".events.oldlegacy", "", `{"service":"api"}`)
			Eventually(func() int { return len(runsOf("oldlegacy")) }, 10*time.Second, 100*time.Millisecond).Should(Equal(2))
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
		nextFive := func(t time.Time) time.Time {
			return t.UTC().Truncate(5 * time.Minute).Add(5 * time.Minute)
		}

		It("renders an owned CronJob and ServiceAccount and fires once per schedule time", func() {
			t := scheduleTrigger("cron", func(s *aiv1alpha1.TriggerSpec) {
				s.Concurrency.Policy = aiv1alpha1.TriggerConcurrencyForbid
			})
			due := nextFive(t.CreationTimestamp.Time)
			clock = due.Add(-time.Minute)
			reconcileTrigger("cron")
			t = getTrigger("cron")

			sa := getSA("trigger-cron")
			Expect(metav1.IsControlledBy(sa, t)).To(BeTrue())
			Expect(sa.AutomountServiceAccountToken).To(HaveValue(BeFalse()))

			cj := getCronJob("trigger-cron")
			Expect(metav1.IsControlledBy(cj, t)).To(BeTrue())
			Expect(cj.Spec.Schedule).To(Equal("*/5 * * * *"))
			Expect(cj.Spec.TimeZone).To(HaveValue(Equal("Etc/UTC")))
			Expect(cj.Spec.StartingDeadlineSeconds).To(BeNil())
			Expect(cj.Spec.ConcurrencyPolicy).To(Equal(batchv1.ForbidConcurrent))
			Expect(cj.Spec.Suspend).To(HaveValue(BeFalse()))
			Expect(cj.Spec.SuccessfulJobsHistoryLimit).To(HaveValue(Equal(int32(1))))
			Expect(cj.Spec.FailedJobsHistoryLimit).To(HaveValue(Equal(int32(2))))
			job := cj.Spec.JobTemplate.Spec
			Expect(job.ActiveDeadlineSeconds).To(HaveValue(BeNumerically(">", 0)))
			pod := job.Template.Spec
			Expect(pod.ServiceAccountName).To(Equal("trigger-cron"))
			Expect(pod.AutomountServiceAccountToken).To(HaveValue(BeFalse()))
			Expect(pod.RestartPolicy).To(Equal(corev1.RestartPolicyNever))
			Expect(pod.SecurityContext.RunAsNonRoot).To(HaveValue(BeTrue()))
			Expect(pod.Containers).To(HaveLen(1))
			c := pod.Containers[0]
			Expect(c.Image).To(Equal("operator:test"))
			Expect(c.Command).To(Equal([]string{"/manager"}))
			Expect(c.Args).To(Equal([]string{"fire-trigger", "--gateway=http://gateway.test",
				"--trigger=" + ns + "/cron", "--token-file=/var/run/secrets/roundtable/gateway/token",
				"--job-name=$(ROUNDTABLE_JOB_NAME)"}))
			Expect(c.Env).To(HaveLen(1))
			Expect(c.Env[0].Name).To(Equal("ROUNDTABLE_JOB_NAME"))
			Expect(c.Env[0].ValueFrom.FieldRef.FieldPath).To(Equal("metadata.labels['batch.kubernetes.io/job-name']"))
			Expect(c.SecurityContext.AllowPrivilegeEscalation).To(HaveValue(BeFalse()))
			Expect(c.SecurityContext.Capabilities.Drop).To(ConsistOf(corev1.Capability("ALL")))
			Expect(pod.Volumes).To(HaveLen(1))
			proj := pod.Volumes[0].Projected.Sources[0].ServiceAccountToken
			Expect(proj.Audience).To(Equal("roundtable-gateway"))

			// The firings arrive on the Trigger's own subject of the target
			// Chain's table.
			Expect(t.Finalizers).To(ContainElement(aiv1alpha1.FinalizerTrigger))
			Expect(consumerExists(natspkg.TriggerConsumerName(ns, "cron"))).To(BeTrue())
			cond := meta.FindStatusCondition(t.Status.Conditions, aiv1alpha1.ConditionTriggerReady)
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Message).To(ContainSubstring("CronJob trigger-cron"))
			Expect(cond.Message).To(ContainSubstring(natspkg.ScheduleEventSubject(prefix, ns, "cron")))
			Expect(t.Status.NextScheduledAt.Time).To(BeTemporally("==", due))
			Expect(runsOf("cron")).To(BeEmpty())

			// A CronJob run: exactly one run, with .Event.Time the schedule time.
			fireSchedule("cron", due, "")
			Eventually(func() int { return len(runsOf("cron")) }, 10*time.Second, 100*time.Millisecond).Should(Equal(1))
			run := runsOf("cron")[0]
			Expect(run.Spec.Input).To(Equal(fmt.Sprintf(`{"at": "%s"}`, due.Format("15:04"))))
			Expect(run.Annotations[aiv1alpha1.AnnotationTriggerKey]).To(Equal(due.Format(time.RFC3339)))
			Eventually(func() []aiv1alpha1.TriggerFiring { return getTrigger("cron").Status.RecentFirings }).Should(HaveLen(1))

			// A second run for the same schedule time (after the stream's
			// duplicate window) is deduplicated by the firing key.
			fireSchedule("cron", due, "retry")
			Eventually(func() []aiv1alpha1.TriggerFiring { return getTrigger("cron").Status.RecentFirings }, 10*time.Second).Should(HaveLen(2))
			Expect(getTrigger("cron").Status.RecentFirings[0].SkipReason).To(ContainSubstring("duplicate dedupe key"))
			Expect(runsOf("cron")).To(HaveLen(1))
			Expect(firings("cron", "schedule", "started")).To(Equal(1.0))

			// Reconciling again changes nothing on the CronJob.
			rv := getCronJob("trigger-cron").ResourceVersion
			reconcileTrigger("cron")
			Expect(getCronJob("trigger-cron").ResourceVersion).To(Equal(rv))
		})

		It("sets ScheduleHealthy False for a failed firing Job and clears it on a later success", func() {
			scheduleTrigger("health", nil)
			reconcileTrigger("health")
			cj := getCronJob("trigger-health")
			Expect(meta.IsStatusConditionTrue(getTrigger("health").Status.Conditions, aiv1alpha1.ConditionTriggerScheduleHealthy)).To(BeTrue())

			newJob := func(name string) *batchv1.Job {
				j := &batchv1.Job{
					ObjectMeta: metav1.ObjectMeta{
						Name: name, Namespace: ns,
						OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(cj, batchv1.SchemeGroupVersion.WithKind("CronJob"))},
					},
					Spec: *cj.Spec.JobTemplate.Spec.DeepCopy(),
				}
				Expect(k8sClient.Create(ctx, j)).To(Succeed())
				return j
			}
			failJob := func(j *batchv1.Job) {
				now := metav1.Now()
				j.Status.StartTime = &now
				j.Status.Conditions = []batchv1.JobCondition{
					{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded", LastTransitionTime: now},
					{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded", Message: "Job has reached the specified backoff limit", LastTransitionTime: now},
				}
				Expect(k8sClient.Status().Update(ctx, j)).To(Succeed())
			}
			j1 := newJob("trigger-health-1")
			failJob(j1)
			reconcileTrigger("health")
			cond := meta.FindStatusCondition(getTrigger("health").Status.Conditions, aiv1alpha1.ConditionTriggerScheduleHealthy)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(aiv1alpha1.ReasonTriggerFireJobFailing))
			Expect(cond.Message).To(ContainSubstring("trigger-health-1"))
			// Ready is unchanged.
			Expect(meta.IsStatusConditionTrue(getTrigger("health").Status.Conditions, aiv1alpha1.ConditionTriggerReady)).To(BeTrue())

			// One Warning event per failed Job, however often it is reconciled.
			reconcileTrigger("health")
			rec := r.Recorder.(*record.FakeRecorder)
			var failed int
			for len(rec.Events) > 0 {
				if strings.Contains(<-rec.Events, "FireJobFailed") {
					failed++
				}
			}
			Expect(failed).To(Equal(1))

			// A later Job that did not fail clears it.
			time.Sleep(1100 * time.Millisecond) // creationTimestamp has second resolution
			newJob("trigger-health-2")
			reconcileTrigger("health")
			Expect(meta.IsStatusConditionTrue(getTrigger("health").Status.Conditions, aiv1alpha1.ConditionTriggerScheduleHealthy)).To(BeTrue())

			// The CronJob's last successful time is mirrored.
			ok := metav1.NewTime(clock.Add(-time.Minute).Truncate(time.Second))
			cj = getCronJob("trigger-health")
			cj.Status.LastSuccessfulTime = &ok
			Expect(k8sClient.Status().Update(ctx, cj)).To(Succeed())
			reconcileTrigger("health")
			Expect(getTrigger("health").Status.LastSuccessfulScheduledAt.Time).To(BeTemporally("==", ok.Time))
		})

		It("reads only this Trigger's labelled firing Jobs for ScheduleHealthy", func() {
			scheduleTrigger("scoped", nil)
			reconcileTrigger("scoped")
			cj := getCronJob("trigger-scoped")
			newJob := func(name string, lbls map[string]string, owned bool) {
				j := &batchv1.Job{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: lbls},
					Spec:       *cj.Spec.JobTemplate.Spec.DeepCopy(),
				}
				// The API server defaults a Job without labels from its pod
				// template's labels, so drop those too for an unlabelled Job.
				if lbls == nil {
					j.Spec.Template.Labels = nil
				}
				if owned {
					j.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(cj, batchv1.SchemeGroupVersion.WithKind("CronJob"))}
				}
				Expect(k8sClient.Create(ctx, j)).To(Succeed())
				now := metav1.Now()
				j.Status.StartTime = &now
				j.Status.Conditions = []batchv1.JobCondition{
					{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded", LastTransitionTime: now},
					{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded", LastTransitionTime: now},
				}
				Expect(k8sClient.Status().Update(ctx, j)).To(Succeed())
			}
			// A failed Job of another Trigger and a failed unlabelled Job (a
			// tools-builder style Job) in the same namespace must not count.
			newJob("scoped-other", map[string]string{aiv1alpha1.LabelTrigger: "someone-else"}, false)
			newJob("scoped-unlabelled", nil, false)
			// A failed Job owned by this CronJob but missing the Trigger label
			// is not read either: the label selector is the scope.
			newJob("scoped-owned-unlabelled", nil, true)
			rec := &jobListRecorder{Client: r.Client}
			r.Client = rec
			DeferCleanup(func() { r.Client = rec.Client })
			reconcileTrigger("scoped")
			Expect(rec.selectors).NotTo(BeEmpty())
			for _, s := range rec.selectors {
				Expect(s.String()).To(Equal(aiv1alpha1.LabelTrigger + "=scoped"))
			}
			Expect(meta.IsStatusConditionTrue(getTrigger("scoped").Status.Conditions, aiv1alpha1.ConditionTriggerScheduleHealthy)).To(BeTrue())
		})

		It("emits FiringSkipped for a schedule firing skipped by Forbid", func() {
			t := scheduleTrigger("skipev", func(s *aiv1alpha1.TriggerSpec) {
				s.Concurrency.Policy = aiv1alpha1.TriggerConcurrencyForbid
			})
			due := nextFive(t.CreationTimestamp.Time)
			clock = due.Add(-time.Minute)
			reconcileTrigger("skipev")
			fireSchedule("skipev", due, "")
			Eventually(func() int { return len(runsOf("skipev")) }, 10*time.Second, 100*time.Millisecond).Should(Equal(1))

			second := due.Add(5 * time.Minute)
			fireSchedule("skipev", second, "")
			Eventually(func() []aiv1alpha1.TriggerFiring { return getTrigger("skipev").Status.RecentFirings }, 10*time.Second).Should(HaveLen(2))
			rec := r.Recorder.(*record.FakeRecorder)
			var got string
			Eventually(func() string {
				for len(rec.Events) > 0 {
					if e := <-rec.Events; strings.Contains(e, "FiringSkipped") {
						got = e
					}
				}
				return got
			}, 10*time.Second, 100*time.Millisecond).ShouldNot(BeEmpty())
			Expect(got).To(ContainSubstring("concurrency Forbid"))
			Expect(got).To(ContainSubstring(second.UTC().Format(time.RFC3339)))
		})

		It("skips a schedule firing consumed after startingDeadlineSeconds and starts one within it", func() {
			deadline := func(s *int64) func(*aiv1alpha1.TriggerSpec) {
				return func(spec *aiv1alpha1.TriggerSpec) {
					spec.Source.Schedule.StartingDeadlineSeconds = s
				}
			}
			hour := int64(3600)

			// Consumed two hours after its slot: recorded as skipped, no run.
			late := scheduleTrigger("late", deadline(&hour))
			due := nextFive(late.CreationTimestamp.Time)
			clock = due.Add(-time.Minute)
			reconcileTrigger("late")
			clock = due.Add(2 * time.Hour)
			fireSchedule("late", due, "late")
			Eventually(func() []aiv1alpha1.TriggerFiring { return getTrigger("late").Status.RecentFirings }, 10*time.Second).Should(HaveLen(1))
			Expect(getTrigger("late").Status.RecentFirings[0].SkipReason).To(Equal(
				"missed schedule time " + due.UTC().Format(time.RFC3339) + " by more than startingDeadlineSeconds"))
			Expect(runsOf("late")).To(BeEmpty())

			// Consumed within the deadline: a run starts.
			inTime := scheduleTrigger("intime", deadline(&hour))
			due = nextFive(inTime.CreationTimestamp.Time)
			clock = due.Add(-time.Minute)
			reconcileTrigger("intime")
			clock = due.Add(30 * time.Minute)
			fireSchedule("intime", due, "intime")
			Eventually(func() int { return len(runsOf("intime")) }, 10*time.Second, 100*time.Millisecond).Should(Equal(1))

			// Without the field the late firing is not skipped.
			unset := scheduleTrigger("unset", nil)
			due = nextFive(unset.CreationTimestamp.Time)
			clock = due.Add(-time.Minute)
			reconcileTrigger("unset")
			clock = due.Add(2 * time.Hour)
			fireSchedule("unset", due, "unset")
			Eventually(func() int { return len(runsOf("unset")) }, 10*time.Second, 100*time.Millisecond).Should(Equal(1))
		})

		It("dedupes a late redelivery of a slot that already ran instead of skipping it as missed", func() {
			hour := int64(3600)
			t := scheduleTrigger("redeliver", func(spec *aiv1alpha1.TriggerSpec) {
				spec.Source.Schedule.StartingDeadlineSeconds = &hour
			})
			due := nextFive(t.CreationTimestamp.Time)
			clock = due.Add(-time.Minute)
			reconcileTrigger("redeliver")
			clock = due.Add(time.Minute)
			fireSchedule("redeliver", due, "redeliver")
			Eventually(func() int { return len(runsOf("redeliver")) }, 10*time.Second, 100*time.Millisecond).Should(Equal(1))

			// Redelivered two hours later, past the deadline: the run exists.
			clock = due.Add(2 * time.Hour)
			fireSchedule("redeliver", due, "redeliver-again")
			Eventually(func() []aiv1alpha1.TriggerFiring { return getTrigger("redeliver").Status.RecentFirings }, 10*time.Second).Should(HaveLen(2))
			skipped := getTrigger("redeliver").Status.RecentFirings[0]
			Expect(skipped.SkipReason).To(ContainSubstring("duplicate dedupe key"))
			Expect(skipped.SkipReason).NotTo(ContainSubstring("startingDeadlineSeconds"))
			Expect(runsOf("redeliver")).To(HaveLen(1))
		})

		It("updates the CronJob on a schedule change and suspend, and reads lastScheduledAt from it", func() {
			scheduleTrigger("upd", nil)
			reconcileTrigger("upd")

			t := getTrigger("upd")
			t.Spec.Source.Schedule = &aiv1alpha1.TriggerScheduleSource{
				Cron: "0 7 * * 1-5", TimeZone: "America/New_York", StartingDeadlineSeconds: ptr.To[int64](3600)}
			t.Spec.Concurrency.Policy = aiv1alpha1.TriggerConcurrencyReplace
			Expect(k8sClient.Update(ctx, t)).To(Succeed())
			reconcileTrigger("upd")
			cj := getCronJob("trigger-upd")
			Expect(cj.Spec.Schedule).To(Equal("0 7 * * 1-5"))
			Expect(cj.Spec.TimeZone).To(HaveValue(Equal("America/New_York")))
			Expect(cj.Spec.StartingDeadlineSeconds).To(HaveValue(Equal(int64(3600))))
			Expect(cj.Spec.ConcurrencyPolicy).To(Equal(batchv1.ReplaceConcurrent))

			// The CronJob's last schedule time is the Trigger's.
			last := metav1.NewTime(clock.Add(-time.Hour).Truncate(time.Second))
			cj.Status.LastScheduleTime = &last
			Expect(k8sClient.Status().Update(ctx, cj)).To(Succeed())
			reconcileTrigger("upd")
			Expect(getTrigger("upd").Status.LastScheduledAt.Time).To(BeTemporally("==", last.Time))

			t = getTrigger("upd")
			t.Spec.Suspend = true
			Expect(k8sClient.Update(ctx, t)).To(Succeed())
			reconcileTrigger("upd")
			Expect(getCronJob("trigger-upd").Spec.Suspend).To(HaveValue(BeTrue()))
			st := getTrigger("upd").Status
			cond := meta.FindStatusCondition(st.Conditions, aiv1alpha1.ConditionTriggerReady)
			Expect(cond.Reason).To(Equal(aiv1alpha1.ReasonTriggerSuspended))
			Expect(cond.Message).To(ContainSubstring("CronJob trigger-upd is suspended"))
			Expect(st.NextScheduledAt).To(BeNil())
			Expect(consumerExists(natspkg.TriggerConsumerName(ns, "upd"))).To(BeFalse())

			t = getTrigger("upd")
			t.Spec.Suspend = false
			Expect(k8sClient.Update(ctx, t)).To(Succeed())
			reconcileTrigger("upd")
			Expect(getCronJob("trigger-upd").Spec.Suspend).To(HaveValue(BeFalse()))
			Expect(meta.IsStatusConditionTrue(getTrigger("upd").Status.Conditions, aiv1alpha1.ConditionTriggerReady)).To(BeTrue())
		})

		It("propagates pod placement set and then cleared to the existing CronJob", func() {
			scheduleTrigger("place", nil)
			reconcileTrigger("place")
			Expect(getCronJob("trigger-place").Spec.JobTemplate.Spec.Template.Spec.Tolerations).To(BeEmpty())

			saved := r.Schedule
			DeferCleanup(func() { r.Schedule = saved })
			r.Schedule.NodeSelector = map[string]string{"pool": "ops"}
			r.Schedule.Tolerations = []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}}
			r.Schedule.PriorityClassName = "high"
			r.Schedule.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "regcred"}}
			reconcileTrigger("place")
			pod := getCronJob("trigger-place").Spec.JobTemplate.Spec.Template.Spec
			Expect(pod.NodeSelector).To(Equal(map[string]string{"pool": "ops"}))
			Expect(pod.Tolerations).To(HaveLen(1))
			Expect(pod.PriorityClassName).To(Equal("high"))
			Expect(pod.ImagePullSecrets).To(HaveLen(1))

			// Unchanged: a further reconcile does not update the CronJob.
			rv := getCronJob("trigger-place").ResourceVersion
			reconcileTrigger("place")
			Expect(getCronJob("trigger-place").ResourceVersion).To(Equal(rv))

			// Cleared: the values are removed from the existing CronJob.
			r.Schedule.NodeSelector = nil
			r.Schedule.Tolerations = nil
			r.Schedule.PriorityClassName = ""
			r.Schedule.ImagePullSecrets = nil
			reconcileTrigger("place")
			pod = getCronJob("trigger-place").Spec.JobTemplate.Spec.Template.Spec
			Expect(pod.NodeSelector).To(BeEmpty())
			Expect(pod.Tolerations).To(BeEmpty())
			Expect(pod.PriorityClassName).To(BeEmpty())
			Expect(pod.ImagePullSecrets).To(BeEmpty())

			rv = getCronJob("trigger-place").ResourceVersion
			reconcileTrigger("place")
			Expect(getCronJob("trigger-place").ResourceVersion).To(Equal(rv))
		})

		It("propagates a cleared startingDeadlineSeconds and image pull policy to the existing CronJob", func() {
			scheduleTrigger("clr", func(s *aiv1alpha1.TriggerSpec) {
				s.Source.Schedule.StartingDeadlineSeconds = ptr.To[int64](3600)
			})
			saved := r.Schedule
			DeferCleanup(func() { r.Schedule = saved })
			r.Schedule.ImagePullPolicy = corev1.PullAlways
			reconcileTrigger("clr")
			cj := getCronJob("trigger-clr")
			Expect(cj.Spec.StartingDeadlineSeconds).To(HaveValue(Equal(int64(3600))))
			Expect(firePullPolicy(cj.Spec.JobTemplate.Spec.Template.Spec)).To(Equal(corev1.PullAlways))

			// Cleared on the Trigger and in the operator config: both reach the CronJob.
			t := getTrigger("clr")
			t.Spec.Source.Schedule.StartingDeadlineSeconds = nil
			Expect(k8sClient.Update(ctx, t)).To(Succeed())
			r.Schedule.ImagePullPolicy = ""
			reconcileTrigger("clr")
			cj = getCronJob("trigger-clr")
			Expect(cj.Spec.StartingDeadlineSeconds).To(BeNil())
			// The API server defaults the empty policy for the image; that
			// defaulted value is the one the operator must not churn on.
			Expect(firePullPolicy(cj.Spec.JobTemplate.Spec.Template.Spec)).To(Equal(corev1.PullIfNotPresent))

			// Unchanged, including the server-defaulted policy: no update.
			rv := cj.ResourceVersion
			reconcileTrigger("clr")
			Expect(getCronJob("trigger-clr").ResourceVersion).To(Equal(rv))
		})

		It("deletes the CronJob and ServiceAccount when the schedule source is removed", func() {
			scheduleTrigger("gone", nil)
			reconcileTrigger("gone")
			Expect(getCronJob("trigger-gone")).NotTo(BeNil())

			t := getTrigger("gone")
			t.Spec.Source = aiv1alpha1.TriggerSource{NATS: &aiv1alpha1.TriggerNATSSource{Subject: prefix + ".events.gone"}}
			t.Spec.RoundTableRef = &corev1.LocalObjectReference{Name: rtName}
			Expect(k8sClient.Update(ctx, t)).To(Succeed())
			reconcileTrigger("gone")
			Expect(apierrors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: "trigger-gone"}, &batchv1.CronJob{}))).To(BeTrue())
			Expect(apierrors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: "trigger-gone"}, &corev1.ServiceAccount{}))).To(BeTrue())
			Expect(getTrigger("gone").Status.LastScheduledAt).To(BeNil())
			Expect(meta.IsStatusConditionTrue(getTrigger("gone").Status.Conditions, aiv1alpha1.ConditionTriggerReady)).To(BeTrue())
		})

		It("is not Ready without a gateway URL or when its names are taken, and leaves foreign objects alone", func() {
			r.Schedule.GatewayURL = ""
			scheduleTrigger("nogw", nil)
			reconcileTrigger("nogw")
			cond := meta.FindStatusCondition(getTrigger("nogw").Status.Conditions, aiv1alpha1.ConditionTriggerReady)
			Expect(cond.Reason).To(Equal(aiv1alpha1.ReasonTriggerCronJobError))
			Expect(cond.Message).To(ContainSubstring("gateway URL"))
			Expect(apierrors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: "trigger-nogw"}, &batchv1.CronJob{}))).To(BeTrue())

			r.Schedule.GatewayURL = "http://gateway.test"
			Expect(k8sClient.Create(ctx, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "trigger-taken", Namespace: ns}})).To(Succeed())
			DeferCleanup(func() {
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &corev1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{Name: "trigger-taken", Namespace: ns}}))).To(Succeed())
			})
			scheduleTrigger("taken", nil)
			reconcileTrigger("taken")
			cond = meta.FindStatusCondition(getTrigger("taken").Status.Conditions, aiv1alpha1.ConditionTriggerReady)
			Expect(cond.Reason).To(Equal(aiv1alpha1.ReasonTriggerCronJobError))
			Expect(cond.Message).To(ContainSubstring("not owned by this Trigger"))

			// Switching away from the schedule does not delete what the
			// Trigger never owned.
			t := getTrigger("taken")
			t.Spec.Source = aiv1alpha1.TriggerSource{NATS: &aiv1alpha1.TriggerNATSSource{Subject: prefix + ".events.taken"}}
			t.Spec.RoundTableRef = &corev1.LocalObjectReference{Name: rtName}
			Expect(k8sClient.Update(ctx, t)).To(Succeed())
			reconcileTrigger("taken")
			Expect(getSA("trigger-taken")).NotTo(BeNil())
		})

		It("keeps the target's input when spec.input is empty and reports a missing target", func() {
			t := scheduleTrigger("noinput", func(s *aiv1alpha1.TriggerSpec) { s.Input = "" })
			reconcileTrigger("noinput")
			fireSchedule("noinput", nextFive(t.CreationTimestamp.Time), "")
			Eventually(func() int { return len(runsOf("noinput")) }, 10*time.Second, 100*time.Millisecond).Should(Equal(1))
			Expect(runsOf("noinput")[0].Spec.Input).To(Equal(`{"default": true}`))

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
			reconcileTrigger("annsuspend")
			Expect(getCronJob("trigger-annsuspend").Spec.Suspend).To(HaveValue(BeFalse()))
			annotate("annsuspend", map[string]string{aiv1alpha1.AnnotationTriggerSuspended: "token"})

			reconcileTrigger("annsuspend")
			Expect(getCronJob("trigger-annsuspend").Spec.Suspend).To(HaveValue(BeTrue()))
			t = getTrigger("annsuspend")
			Expect(t.Status.SuspendedBy).To(Equal(aiv1alpha1.TriggerSuspendedByAnnotation))
			Expect(t.Status.NextScheduledAt).To(BeNil())
			Expect(consumerExists(natspkg.TriggerConsumerName(ns, "annsuspend"))).To(BeFalse())
			cond := meta.FindStatusCondition(t.Status.Conditions, aiv1alpha1.ConditionTriggerReady)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(aiv1alpha1.ReasonTriggerSuspended))
			Expect(cond.Message).To(ContainSubstring(aiv1alpha1.AnnotationTriggerSuspended))
			Expect(cond.Message).To(ContainSubstring("by token"))
			Expect(cond.Message).To(ContainSubstring("CronJob trigger-annsuspend is suspended"))

			// Both set: status names both sources.
			t.Spec.Suspend = true
			Expect(k8sClient.Update(ctx, t)).To(Succeed())
			reconcileTrigger("annsuspend")
			Expect(getTrigger("annsuspend").Status.SuspendedBy).To(Equal("spec,annotation"))
			t = getTrigger("annsuspend")
			t.Spec.Suspend = false
			Expect(k8sClient.Update(ctx, t)).To(Succeed())

			// Removing the annotation resumes the CronJob and firing.
			annotate("annsuspend", nil, aiv1alpha1.AnnotationTriggerSuspended)
			reconcileTrigger("annsuspend")
			Expect(getCronJob("trigger-annsuspend").Spec.Suspend).To(HaveValue(BeFalse()))
			t = getTrigger("annsuspend")
			Expect(t.Status.SuspendedBy).To(BeEmpty())
			Expect(meta.IsStatusConditionTrue(t.Status.Conditions, aiv1alpha1.ConditionTriggerReady)).To(BeTrue())
			fireSchedule("annsuspend", nextFive(t.CreationTimestamp.Time), "")
			Eventually(func() int { return len(runsOf("annsuspend")) }, 10*time.Second, 100*time.Millisecond).Should(Equal(1))
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

		It("never skips a run-now firing for startingDeadlineSeconds, however late the clock", func() {
			deadline := int64(60)
			t := scheduleTrigger("runnow-late", func(s *aiv1alpha1.TriggerSpec) {
				s.Source.Schedule.StartingDeadlineSeconds = &deadline
			})
			clock = t.CreationTimestamp.Add(5 * time.Hour)
			annotate("runnow-late", map[string]string{aiv1alpha1.AnnotationTriggerRunNow: "req-late"})
			reconcileTrigger("runnow-late")
			Expect(runsOf("runnow-late")).To(HaveLen(1))
			Expect(getTrigger("runnow-late").Status.RecentFirings[0].SkipReason).To(BeEmpty())
		})

		It("records a skipped firing and clears a run-now request for a missing Chain", func() {
			t := scheduleTrigger("runnow-nochain", func(s *aiv1alpha1.TriggerSpec) {
				s.Target.ChainRef = &corev1.LocalObjectReference{Name: "no-such-chain"}
			})
			clock = t.CreationTimestamp.Add(time.Second)
			annotate("runnow-nochain", map[string]string{aiv1alpha1.AnnotationTriggerRunNow: "req-x"})
			reconcileTrigger("runnow-nochain")
			t = getTrigger("runnow-nochain")
			Expect(t.Annotations).NotTo(HaveKey(aiv1alpha1.AnnotationTriggerRunNow))
			Expect(t.Status.RecentFirings).To(HaveLen(1))
			Expect(t.Status.RecentFirings[0].SkipReason).To(ContainSubstring("no-such-chain"))
			Expect(meta.FindStatusCondition(t.Status.Conditions, aiv1alpha1.ConditionTriggerReady)).NotTo(BeNil())
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

		It("shortens a CronJob name that would be too long", func() {
			long := strings.Repeat("a", 63)
			name := triggerCronJobName(long)
			Expect(name).To(HaveLen(cronJobNameMax))
			Expect(name).To(HavePrefix("trigger-aaa"))
			Expect(triggerCronJobName(long[:44])).To(Equal("trigger-" + long[:44]))
			Expect(triggerCronJobName(long)).NotTo(Equal(triggerCronJobName(long[:62])))
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
			reconcileTrigger("kt-cron")
			fireSchedule("kt-cron", due, "")

			var sent []natspkg.TaskPayload
			Eventually(func() int { sent = got(); return len(sent) }, "10s").Should(Equal(1))
			Eventually(func() []aiv1alpha1.TriggerFiring { return getTrigger("kt-cron").Status.RecentFirings }).Should(HaveLen(1))
			res := reconcileTrigger("kt-cron")
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
			Expect(finishedRuns("kt-cron", "succeeded")).To(Equal(1.0))
			Expect(finishedRuns("kt-cron", "failed")).To(Equal(0.0))
			// A second reconcile does not count the finished run again.
			reconcileTrigger("kt-cron")
			Expect(finishedRuns("kt-cron", "succeeded")).To(Equal(1.0))
			rec, err = svc.GetTask(ctx, sent[0].TaskID)
			Expect(err).NotTo(HaveOccurred())
			Expect(rec.Output).To(Equal("all quiet"))
		})

		It("counts a task that finishes failed once", func() {
			got := tasks()
			t := scheduleKnightTrigger("kt-fail", nil)
			due := nextFive(t.CreationTimestamp.Time)
			clock = due.Add(10 * time.Second)
			reconcileTrigger("kt-fail")
			fireSchedule("kt-fail", due, "")
			var sent []natspkg.TaskPayload
			Eventually(func() int { sent = got(); return len(sent) }, "10s").Should(Equal(1))
			Eventually(func() []aiv1alpha1.TriggerFiring { return getTrigger("kt-fail").Status.RecentFirings }).Should(HaveLen(1))
			Expect(finishedRuns("kt-fail", "failed")).To(Equal(0.0))

			publishResult(sent[0].TaskID, `{"taskId":"`+sent[0].TaskID+`","error":"boom","success":false}`)
			reconcileTrigger("kt-fail")
			Expect(getTrigger("kt-fail").Status.RecentFirings).To(ContainElement(SatisfyAll(
				HaveField("Run", sent[0].TaskID), HaveField("Outcome", aiv1alpha1.TriggerFiringFailed))))
			Expect(finishedRuns("kt-fail", "failed")).To(Equal(1.0))
			reconcileTrigger("kt-fail")
			Expect(finishedRuns("kt-fail", "failed")).To(Equal(1.0))
			Expect(finishedRuns("kt-fail", "succeeded")).To(Equal(0.0))
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
			fireSchedule("kt-forbid", due, "")
			var sent []natspkg.TaskPayload
			Eventually(func() int { sent = got(); return len(sent) }, "10s").Should(Equal(1))
			Eventually(func() []aiv1alpha1.TriggerFiring { return getTrigger("kt-forbid").Status.RecentFirings }).Should(HaveLen(1))

			// The next time finds the task active: nothing is sent.
			clock = due.Add(5*time.Minute + time.Second)
			fireSchedule("kt-forbid", due.Add(5*time.Minute), "")
			Eventually(func() []aiv1alpha1.TriggerFiring { return getTrigger("kt-forbid").Status.RecentFirings }, "10s").Should(HaveLen(2))
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
			Expect(finishedRuns("kt-forbid", "lost")).To(Equal(1.0))
			reconcileTrigger("kt-forbid")
			Expect(finishedRuns("kt-forbid", "lost")).To(Equal(1.0))
		})

		It("records why a task failed or was lost, with one Warning Event each", func() {
			got := tasks()
			scheduleKnightTrigger("kt-reason", func(s *aiv1alpha1.TriggerSpec) {
				s.Concurrency.Policy = aiv1alpha1.TriggerConcurrencyAllow
				s.Target.Knight.TimeoutSeconds = ptr.To[int32](3600)
			})
			t := getTrigger("kt-reason")
			due := nextFive(t.CreationTimestamp.Time)
			var sent []natspkg.TaskPayload
			for i := range 4 {
				at := due.Add(time.Duration(i) * 5 * time.Minute)
				clock = at.Add(time.Second)
				reconcileTrigger("kt-reason")
				fireSchedule("kt-reason", at, "")
				Eventually(func() int { sent = got(); return len(sent) }, "10s").Should(Equal(i + 1))
				Eventually(func() []aiv1alpha1.TriggerFiring { return getTrigger("kt-reason").Status.RecentFirings }).Should(HaveLen(i + 1))
			}
			publishResult(sent[0].TaskID, `{"taskId":"`+sent[0].TaskID+`","error":"boom"}`)
			publishResult(sent[1].TaskID, `{"taskId":"`+sent[1].TaskID+`","error":"Task refused: plan","usage_limited":true}`)
			publishResult(sent[2].TaskID, `{"taskId":"`+sent[2].TaskID+`","output":"fine"}`)
			// The fourth gets no result.
			clock = due.Add(2 * time.Hour)
			Eventually(func() bool {
				reconcileTrigger("kt-reason")
				return len(getTrigger("kt-reason").Status.ActiveRuns) == 0
			}, "10s").Should(BeTrue())

			byRun := map[string]aiv1alpha1.TriggerFiring{}
			for _, f := range getTrigger("kt-reason").Status.RecentFirings {
				byRun[f.Run] = f
			}
			Expect(byRun[sent[0].TaskID]).To(SatisfyAll(
				HaveField("Outcome", aiv1alpha1.TriggerFiringFailed), HaveField("Reason", "boom")))
			Expect(byRun[sent[1].TaskID]).To(SatisfyAll(
				HaveField("Outcome", aiv1alpha1.TriggerFiringFailed), HaveField("Reason", "usage limit: Task refused: plan")))
			Expect(byRun[sent[2].TaskID]).To(SatisfyAll(
				HaveField("Outcome", aiv1alpha1.TriggerFiringSucceeded), HaveField("Reason", BeEmpty())))
			Expect(byRun[sent[3].TaskID]).To(SatisfyAll(
				HaveField("Outcome", aiv1alpha1.TriggerFiringLost), HaveField("Reason", "no result within 3600s")))

			// Reconciling again emits nothing more.
			reconcileTrigger("kt-reason")
			rec := r.Recorder.(*record.FakeRecorder)
			var failed, lost int
			for len(rec.Events) > 0 {
				ev := <-rec.Events
				switch {
				case strings.Contains(ev, "Warning TaskFailed"):
					failed++
				case strings.Contains(ev, "Warning TaskLost"):
					lost++
				}
			}
			Expect(failed).To(Equal(2))
			Expect(lost).To(Equal(1))
		})

		It("fires under Forbid when the listed task has finished since the last reconcile", func() {
			got := tasks()
			t := scheduleKnightTrigger("kt-forbid-fresh", func(s *aiv1alpha1.TriggerSpec) {
				s.Concurrency.Policy = aiv1alpha1.TriggerConcurrencyForbid
			})
			due := nextFive(t.CreationTimestamp.Time)
			clock = due.Add(time.Second)
			reconcileTrigger("kt-forbid-fresh")
			fireSchedule("kt-forbid-fresh", due, "")
			var sent []natspkg.TaskPayload
			Eventually(func() int { sent = got(); return len(sent) }, "10s").Should(Equal(1))
			Eventually(func() []aiv1alpha1.TriggerFiring { return getTrigger("kt-forbid-fresh").Status.RecentFirings }).Should(HaveLen(1))
			Expect(getTrigger("kt-forbid-fresh").Status.ActiveRuns).To(Equal([]string{sent[0].TaskID}))

			// The result arrives, but no reconcile has collected it yet.
			publishResult(sent[0].TaskID, `{"taskId":"`+sent[0].TaskID+`","output":"done"}`)
			clock = due.Add(5*time.Minute + time.Second)
			fireSchedule("kt-forbid-fresh", due.Add(5*time.Minute), "")
			Eventually(func() int { sent = got(); return len(sent) }, "10s").Should(Equal(2))
			Expect(sent[1].TaskID).NotTo(Equal(sent[0].TaskID))
		})

		It("keeps the other tasks' outcomes when one task lookup fails", func() {
			got := tasks()
			t := scheduleKnightTrigger("kt-lookup", nil)
			due := nextFive(t.CreationTimestamp.Time)
			clock = due.Add(time.Second)
			reconcileTrigger("kt-lookup")
			fireSchedule("kt-lookup", due, "")
			fireSchedule("kt-lookup", due.Add(5*time.Minute), "")
			var sent []natspkg.TaskPayload
			Eventually(func() int { sent = got(); return len(sent) }, "10s").Should(Equal(2))
			Eventually(func() []string { return getTrigger("kt-lookup").Status.ActiveRuns }, "10s").Should(HaveLen(2))

			svc, err := r.taskService(ctx)
			Expect(err).NotTo(HaveOccurred())
			// Swap in a copy under the lock rather than mutating the shared
			// service, which other goroutines may be reading.
			failing := gateway.NewService(r.knightResolver(), failingGetBus{Bus: svc.Bus, fail: sent[0].TaskID}, gateway.Options{
				AllowedNamespaces:  []string{"*"},
				DefaultKnightImage: r.DefaultKnightImage,
			}, logr.Discard())
			r.tasksMu.Lock()
			r.tasks = failing
			r.tasksMu.Unlock()
			DeferCleanup(func() {
				r.tasksMu.Lock()
				r.tasks = svc
				r.tasksMu.Unlock()
			})

			publishResult(sent[1].TaskID, `{"taskId":"`+sent[1].TaskID+`","output":"ok"}`)
			res := reconcileTrigger("kt-lookup")
			Expect(res.RequeueAfter).To(Equal(triggerTaskPoll))
			st := getTrigger("kt-lookup").Status
			Expect(st.ActiveRuns).To(Equal([]string{sent[0].TaskID}))
			Expect(st.RecentFirings).To(ContainElement(SatisfyAll(
				HaveField("Run", sent[1].TaskID), HaveField("Outcome", aiv1alpha1.TriggerFiringSucceeded))))
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

		It("records a skipped firing and clears a run-now request for a missing knight", func() {
			got := tasks()
			t := scheduleKnightTrigger("kt-runnow-missing", func(s *aiv1alpha1.TriggerSpec) { s.Target.Knight.Name = "nobody" })
			clock = t.CreationTimestamp.Add(time.Second)
			patch := client.MergeFrom(t.DeepCopy())
			t.Annotations = map[string]string{aiv1alpha1.AnnotationTriggerRunNow: "req-m"}
			Expect(k8sClient.Patch(ctx, t, patch)).To(Succeed())
			reconcileTrigger("kt-runnow-missing")
			t = getTrigger("kt-runnow-missing")
			Expect(t.Annotations).NotTo(HaveKey(aiv1alpha1.AnnotationTriggerRunNow))
			Expect(t.Status.RecentFirings).To(HaveLen(1))
			Expect(t.Status.RecentFirings[0].SkipReason).To(ContainSubstring("nobody"))
			cond := meta.FindStatusCondition(t.Status.Conditions, aiv1alpha1.ConditionTriggerReady)
			Expect(cond).NotTo(BeNil())
			Expect(got()).To(BeEmpty())
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
			fireSchedule("mt-cron", due, "")

			Eventually(func() []aiv1alpha1.Mission { return missionsOf("mt-cron") }, "10s").Should(HaveLen(1))
			Eventually(func() []aiv1alpha1.TriggerFiring { return getTrigger("mt-cron").Status.RecentFirings }).Should(HaveLen(1))
			ms := missionsOf("mt-cron")
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
			fireSchedule("mt-cron", due.Add(5*time.Minute), "")
			Eventually(func() []aiv1alpha1.TriggerFiring { return getTrigger("mt-cron").Status.RecentFirings }, "10s").Should(HaveLen(2))
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
			fireSchedule("mt-cron", due.Add(10*time.Minute), "")
			Eventually(func() []aiv1alpha1.Mission { return missionsOf("mt-cron") }, "10s").Should(HaveLen(2))
			Eventually(func() []aiv1alpha1.TriggerFiring { return getTrigger("mt-cron").Status.RecentFirings }).Should(HaveLen(3))
			st = getTrigger("mt-cron").Status
			Expect(st.ActiveRuns).To(ContainElement(Not(Equal(m.Name))))
			Expect(st.RecentFirings).To(ContainElement(SatisfyAll(
				HaveField("Run", m.Name),
				HaveField("Outcome", aiv1alpha1.TriggerFiringSucceeded),
				HaveField("Phase", aiv1alpha1.MissionPhaseSucceeded))))
			Expect(missionsOf("mt-cron")).To(HaveLen(2))
			Expect(finishedRuns("mt-cron", "succeeded")).To(Equal(1.0))
			reconcileTrigger("mt-cron")
			Expect(finishedRuns("mt-cron", "succeeded")).To(Equal(1.0))
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
			due := nextFive(t.CreationTimestamp.Time)
			clock = due.Add(time.Second)
			reconcileTrigger("mt-bad")
			fireSchedule("mt-bad", due, "")
			Eventually(func() []aiv1alpha1.TriggerFiring { return getTrigger("mt-bad").Status.RecentFirings }, "10s").Should(HaveLen(1))
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
			fireSchedule("mt-limit", due, "")
			Eventually(func() []aiv1alpha1.Mission { return missionsOf("mt-limit") }, "10s").Should(HaveLen(1))
			Eventually(func() []aiv1alpha1.TriggerFiring { return getTrigger("mt-limit").Status.RecentFirings }).Should(HaveLen(1))

			clock = due.Add(5*time.Minute + time.Second)
			fireSchedule("mt-limit", due.Add(5*time.Minute), "")
			Eventually(func() []aiv1alpha1.TriggerFiring { return getTrigger("mt-limit").Status.RecentFirings }, "10s").Should(HaveLen(2))
			Expect(missionsOf("mt-limit")).To(HaveLen(1))
			Expect(getTrigger("mt-limit").Status.RecentFirings[0].SkipReason).To(ContainSubstring("policies.maxMissions 1"))
		})
	})
})

// jobListRecorder wraps a client and records the label selector of every
// JobList it is asked for.
type jobListRecorder struct {
	client.Client
	selectors []labels.Selector
}

func (j *jobListRecorder) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*batchv1.JobList); ok {
		lo := &client.ListOptions{}
		lo.ApplyOptions(opts)
		sel := lo.LabelSelector
		if sel == nil {
			sel = labels.Everything()
		}
		j.selectors = append(j.selectors, sel)
	}
	return j.Client.List(ctx, list, opts...)
}

// failingGetBus fails reads of one task record, like a transient KV error.
type failingGetBus struct {
	gateway.Bus
	fail string
}

func (b failingGetBus) GetRecord(ctx context.Context, taskID string) (*gateway.TaskRecord, error) {
	if taskID == b.fail {
		return nil, errors.New("kv unavailable")
	}
	return b.Bus.GetRecord(ctx, taskID)
}
