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
	"testing"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

func TestParseChainScheduleMode(t *testing.T) {
	for in, want := range map[string]ChainScheduleMode{
		"":          ChainScheduleModeInProcess,
		"inprocess": ChainScheduleModeInProcess,
		"trigger":   ChainScheduleModeTrigger,
	} {
		if got, err := ParseChainScheduleMode(in); err != nil || got != want {
			t.Errorf("ParseChainScheduleMode(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := ParseChainScheduleMode("cronjob"); err == nil {
		t.Error("ParseChainScheduleMode(cronjob) succeeded")
	}
}

var _ = Describe("Chain schedule through an owned Trigger", func() {
	const (
		namespace = "default"
		rtName    = "sched-trg-table"
		knight    = "sched-trg-knight"
		chainName = "sched-trg-chain"
		trgName   = chainName + "-schedule"
	)
	ctx := context.Background()
	nn := types.NamespacedName{Name: chainName, Namespace: namespace}
	trgNN := types.NamespacedName{Name: trgName, Namespace: namespace}

	var (
		r        *ChainReconciler
		recorder *record.FakeRecorder
	)

	reconcileN := func(n int) {
		for range n {
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())
		}
	}
	getChain := func() *aiv1alpha1.Chain {
		c := &aiv1alpha1.Chain{}
		Expect(k8sClient.Get(ctx, nn, c)).To(Succeed())
		return c
	}
	getTrigger := func() *aiv1alpha1.Trigger {
		t := &aiv1alpha1.Trigger{}
		Expect(k8sClient.Get(ctx, trgNN, t)).To(Succeed())
		return t
	}
	triggerGone := func() bool {
		err := k8sClient.Get(ctx, trgNN, &aiv1alpha1.Trigger{})
		return apierrors.IsNotFound(err)
	}
	updateSpec := func(mut func(*aiv1alpha1.ChainSpec)) {
		c := getChain()
		mut(&c.Spec)
		Expect(k8sClient.Update(ctx, c)).To(Succeed())
	}
	deadline := int64(600)
	create := func() {
		Expect(k8sClient.Create(ctx, &aiv1alpha1.Chain{
			ObjectMeta: metav1.ObjectMeta{Name: chainName, Namespace: namespace},
			Spec: aiv1alpha1.ChainSpec{
				RoundTableRef:           aiv1alpha1.LocalRef(rtName),
				Schedule:                "0 7 * * 1-5",
				TimeZone:                "Europe/Berlin",
				StartingDeadlineSeconds: &deadline,
				Input:                   `{"note": "{{ literal }}"}`,
				TimeoutSeconds:          3600,
				Steps: []aiv1alpha1.ChainStep{{
					Name: "brief", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "brief {{ .Input }}", TimeoutSeconds: 120,
				}},
			},
		})).To(Succeed())
		reconcileN(3) // finalizer, status init, settle
	}
	events := func() []string {
		var out []string
		for {
			select {
			case e := <-recorder.Events:
				out = append(out, e)
			default:
				return out
			}
		}
	}

	BeforeEach(func() {
		recorder = record.NewFakeRecorder(100)
		r = &ChainReconciler{
			Client:       k8sClient,
			Scheme:       k8sClient.Scheme(),
			Recorder:     recorder,
			NATS:         natspkg.NewProviderWithClient(&pickupFakeNATS{fakeNATSClient: newFakeNATSClient()}, logr.Discard()),
			ScheduleMode: ChainScheduleModeTrigger,
		}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: rtName, Namespace: namespace}, &aiv1alpha1.RoundTable{}); apierrors.IsNotFound(err) {
			Expect(k8sClient.Create(ctx, &aiv1alpha1.RoundTable{
				ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: namespace},
				Spec: aiv1alpha1.RoundTableSpec{NATS: aiv1alpha1.RoundTableNATS{
					URL: "nats://localhost:4222", SubjectPrefix: "schedtrg", TasksStream: "schedtrg_tasks", ResultsStream: "schedtrg_results",
				}},
			})).To(Succeed())
		}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: knight, Namespace: namespace}, &aiv1alpha1.Knight{}); apierrors.IsNotFound(err) {
			Expect(k8sClient.Create(ctx, &aiv1alpha1.Knight{
				ObjectMeta: metav1.ObjectMeta{Name: knight, Namespace: namespace},
				Spec: aiv1alpha1.KnightSpec{
					Domain: "ops", Model: "claude-sonnet-4", Skills: []string{"ops"},
					NATS:        aiv1alpha1.KnightNATS{URL: "nats://x:4222", Subjects: []string{"schedtrg.tasks.ops.>"}, Stream: "schedtrg_tasks", ResultsStream: "schedtrg_results"},
					Concurrency: 1, TaskTimeoutSeconds: 120,
				},
			})).To(Succeed())
		}
	})

	AfterEach(func() {
		c := &aiv1alpha1.Chain{}
		if err := k8sClient.Get(ctx, nn, c); err == nil {
			c.Finalizers = nil
			_ = k8sClient.Update(ctx, c)
			_ = k8sClient.Delete(ctx, c)
		}
		_ = k8sClient.Delete(ctx, &aiv1alpha1.Trigger{ObjectMeta: metav1.ObjectMeta{Name: trgName, Namespace: namespace}})
		Eventually(triggerGone).Should(BeTrue())
		if r.cron != nil {
			<-r.cron.Stop().Done()
		}
	})

	It("creates an owned schedule Trigger from the Chain's schedule fields", func() {
		create()

		trg := getTrigger()
		Expect(metav1.IsControlledBy(trg, getChain())).To(BeTrue())
		Expect(trg.Labels).To(HaveKeyWithValue("app.kubernetes.io/managed-by", "roundtable-operator"))
		Expect(trg.Spec.Source.Schedule).To(Equal(&aiv1alpha1.TriggerScheduleSource{
			Cron: "0 7 * * 1-5", TimeZone: "Europe/Berlin", StartingDeadlineSeconds: &deadline,
		}))
		Expect(trg.Spec.Target.ChainRef).To(Equal(&corev1.LocalObjectReference{Name: chainName}))
		Expect(trg.Spec.Concurrency.Policy).To(Equal(aiv1alpha1.TriggerConcurrencyForbid))
		Expect(trg.Spec.RoundTableRef).To(Equal(&corev1.LocalObjectReference{Name: rtName}))
		Expect(trg.Spec.Suspend).To(BeFalse())
		// Left empty so each run gets the Chain's input verbatim, "{{" and all.
		Expect(trg.Spec.Input).To(BeEmpty())
		Expect(events()).To(ContainElement(ContainSubstring("ScheduleTriggerCreated")))

		// No in-process cron entry: the Trigger's CronJob fires instead.
		Expect(r.cronEntries).NotTo(HaveKey(nn.String()))
		Expect(getChain().Status.Phase).To(Equal(aiv1alpha1.ChainPhaseIdle))
	})

	It("mirrors the Trigger's lastScheduledAt and nextScheduledAt onto the Chain", func() {
		create()

		last := metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
		next := metav1.NewTime(time.Now().Add(time.Hour).Truncate(time.Second))
		trg := getTrigger()
		trg.Status.LastScheduledAt, trg.Status.NextScheduledAt = &last, &next
		Expect(k8sClient.Status().Update(ctx, trg)).To(Succeed())
		reconcileN(2)

		c := getChain()
		Expect(c.Status.LastScheduledAt).NotTo(BeNil())
		Expect(c.Status.LastScheduledAt.Unix()).To(Equal(last.Unix()))
		Expect(c.Status.NextScheduledAt).NotTo(BeNil())
		Expect(c.Status.NextScheduledAt.Unix()).To(Equal(next.Unix()))
		// The past fire is the CronJob's to catch up, not an in-process run.
		Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhaseIdle))
	})

	It("updates the Trigger when the schedule fields change, and suspends it with the Chain", func() {
		create()

		updateSpec(func(s *aiv1alpha1.ChainSpec) {
			s.Schedule = "30 6 * * *"
			s.TimeZone = ""
			s.StartingDeadlineSeconds = nil
		})
		reconcileN(2)
		trg := getTrigger()
		Expect(trg.Spec.Source.Schedule).To(Equal(&aiv1alpha1.TriggerScheduleSource{Cron: "30 6 * * *"}))

		updateSpec(func(s *aiv1alpha1.ChainSpec) { s.Suspend = true })
		reconcileN(2)
		Expect(getTrigger().Spec.Suspend).To(BeTrue())
		Expect(getChain().Status.Phase).To(Equal(aiv1alpha1.ChainPhaseSuspended))

		updateSpec(func(s *aiv1alpha1.ChainSpec) { s.Suspend = false })
		reconcileN(2)
		Expect(getTrigger().Spec.Suspend).To(BeFalse())
	})

	It("restores a hand-edited Trigger spec", func() {
		create()
		trg := getTrigger()
		trg.Spec.Source.Schedule.Cron = "* * * * *"
		Expect(k8sClient.Update(ctx, trg)).To(Succeed())
		reconcileN(1)
		Expect(getTrigger().Spec.Source.Schedule.Cron).To(Equal("0 7 * * 1-5"))
	})

	It("deletes the Trigger when spec.schedule is cleared", func() {
		create()
		Expect(triggerGone()).To(BeFalse())

		updateSpec(func(s *aiv1alpha1.ChainSpec) { s.Schedule = "" })
		reconcileN(2)
		Expect(triggerGone()).To(BeTrue())
		Expect(getChain().Status.NextScheduledAt).To(BeNil())
	})

	It("deletes the Trigger when the Chain is deleted", func() {
		create()
		Expect(k8sClient.Delete(ctx, getChain())).To(Succeed())
		reconcileN(1)
		Expect(triggerGone()).To(BeTrue())
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, nn, &aiv1alpha1.Chain{}))).To(BeTrue())
	})

	It("creates no Trigger in inprocess mode", func() {
		r.ScheduleMode = ChainScheduleModeInProcess
		create()
		Expect(triggerGone()).To(BeTrue())
		Expect(r.cronEntries).To(HaveKey(nn.String()))
		Expect(getChain().Status.NextScheduledAt).NotTo(BeNil())
	})

	It("never runs a schedule both ways when the mode is switched", func() {
		r.ScheduleMode = ChainScheduleModeInProcess
		create()
		Expect(r.cronEntries).To(HaveKey(nn.String()))

		// inprocess -> trigger: the cron entry goes, the Trigger comes.
		r.ScheduleMode = ChainScheduleModeTrigger
		reconcileN(1)
		Expect(r.cronEntries).NotTo(HaveKey(nn.String()))
		Expect(triggerGone()).To(BeFalse())

		// trigger -> inprocess: the owned Trigger goes, the cron entry comes.
		r.ScheduleMode = ChainScheduleModeInProcess
		reconcileN(1)
		Expect(triggerGone()).To(BeTrue())
		Expect(r.cronEntries).To(HaveKey(nn.String()))
	})

	It("leaves a Trigger of the same name it does not own alone and reports it", func() {
		foreign := &aiv1alpha1.Trigger{
			ObjectMeta: metav1.ObjectMeta{Name: trgName, Namespace: namespace},
			Spec: aiv1alpha1.TriggerSpec{
				Source: aiv1alpha1.TriggerSource{Schedule: &aiv1alpha1.TriggerScheduleSource{Cron: "0 12 * * *"}},
				Target: aiv1alpha1.TriggerTarget{ChainRef: &corev1.LocalObjectReference{Name: "other"}},
			},
		}
		Expect(k8sClient.Create(ctx, foreign)).To(Succeed())

		create()
		trg := getTrigger()
		Expect(trg.OwnerReferences).To(BeEmpty())
		Expect(trg.Spec.Source.Schedule.Cron).To(Equal("0 12 * * *"))
		Expect(trg.Spec.Target.ChainRef.Name).To(Equal("other"))
		Expect(events()).To(ContainElement(ContainSubstring("ScheduleTriggerConflict")))
		Expect(r.cronEntries).NotTo(HaveKey(nn.String()))

		// Clearing the schedule, switching to inprocess or deleting the Chain
		// never deletes it.
		updateSpec(func(s *aiv1alpha1.ChainSpec) { s.Schedule = "" })
		reconcileN(1)
		r.ScheduleMode = ChainScheduleModeInProcess
		reconcileN(1)
		Expect(k8sClient.Delete(ctx, getChain())).To(Succeed())
		reconcileN(1)
		Expect(triggerGone()).To(BeFalse())
	})
})
