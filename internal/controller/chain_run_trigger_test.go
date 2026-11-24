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
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go"
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

// pickupFakeNATS is a fake NATS client with a controllable consumer ack floor
// and no results.
type pickupFakeNATS struct {
	*fakeNATSClient
	floor     uint64
	delivered uint64
	seq       uint64
}

func (f *pickupFakeNATS) PublishJSONSeq(subject string, v any) (uint64, error) {
	if err := f.PublishJSON(subject, v); err != nil {
		return 0, err
	}
	f.seq++
	return f.seq, nil
}
func (f *pickupFakeNATS) ConsumerAckFloor(string, string) (uint64, error) { return f.floor, nil }
func (f *pickupFakeNATS) ConsumerDeliveredSeq(string, string) (uint64, error) {
	return f.delivered, nil
}
func (f *pickupFakeNATS) PollMessage(string, time.Duration, ...natspkg.SubscribeOption) (*nats.Msg, error) {
	return nil, nil
}

var _ = Describe("Chain run triggers and step timeout clock", func() {
	const (
		namespace = "default"
		rtName    = "trigger-table"
		knight    = "trigger-knight"
		chainName = "trigger-chain"
	)
	ctx := context.Background()
	nn := types.NamespacedName{Name: chainName, Namespace: namespace}

	var (
		fake *pickupFakeNATS
		r    *ChainReconciler
	)

	reconcileN := func(n int) {
		for range n {
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())
		}
	}
	get := func() *aiv1alpha1.Chain {
		c := &aiv1alpha1.Chain{}
		Expect(k8sClient.Get(ctx, nn, c)).To(Succeed())
		return c
	}
	annotate := func(key, val string) {
		c := get()
		if c.Annotations == nil {
			c.Annotations = map[string]string{}
		}
		c.Annotations[key] = val
		Expect(k8sClient.Update(ctx, c)).To(Succeed())
	}
	// setStatus overwrites status (keeping observedGeneration current).
	setStatus := func(mut func(s *aiv1alpha1.ChainStatus)) {
		c := get()
		mut(&c.Status)
		c.Status.ObservedGeneration = c.Generation
		Expect(k8sClient.Status().Update(ctx, c)).To(Succeed())
	}
	create := func(steps []aiv1alpha1.ChainStep, mut func(*aiv1alpha1.ChainSpec)) {
		spec := aiv1alpha1.ChainSpec{RoundTableRef: aiv1alpha1.LocalRef(rtName), Steps: steps, TimeoutSeconds: 86400, Input: "issue-122"}
		if mut != nil {
			mut(&spec)
		}
		Expect(k8sClient.Create(ctx, &aiv1alpha1.Chain{
			ObjectMeta: metav1.ObjectMeta{Name: chainName, Namespace: namespace}, Spec: spec,
		})).To(Succeed())
		reconcileN(3) // finalizer, status init, settle
		Expect(get().Status.Phase).To(Equal(aiv1alpha1.ChainPhaseIdle))
	}
	oneStep := []aiv1alpha1.ChainStep{{Name: "review", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "review {{ .Input }}", TimeoutSeconds: 120}}

	BeforeEach(func() {
		fake = &pickupFakeNATS{fakeNATSClient: newFakeNATSClient()}
		r = &ChainReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: record.NewFakeRecorder(100),
			NATS:     natspkg.NewProviderWithClient(fake, logr.Discard()),
		}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: rtName, Namespace: namespace}, &aiv1alpha1.RoundTable{}); apierrors.IsNotFound(err) {
			Expect(k8sClient.Create(ctx, &aiv1alpha1.RoundTable{
				ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: namespace},
				Spec: aiv1alpha1.RoundTableSpec{NATS: aiv1alpha1.RoundTableNATS{
					URL: "nats://localhost:4222", SubjectPrefix: "trig", TasksStream: "trig_tasks", ResultsStream: "trig_results",
				}},
			})).To(Succeed())
		}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: knight, Namespace: namespace}, &aiv1alpha1.Knight{}); apierrors.IsNotFound(err) {
			Expect(k8sClient.Create(ctx, &aiv1alpha1.Knight{
				ObjectMeta: metav1.ObjectMeta{Name: knight, Namespace: namespace},
				Spec: aiv1alpha1.KnightSpec{
					Domain: "ops", Model: "claude-sonnet-4", Skills: []string{"ops"},
					NATS:        aiv1alpha1.KnightNATS{URL: "nats://x:4222", Subjects: []string{"trig.tasks.ops.>"}, Stream: "trig_tasks", ResultsStream: "trig_results"},
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
	})

	finishedRun := func(phase aiv1alpha1.ChainPhase, stepPhase aiv1alpha1.ChainStepPhase) {
		setStatus(func(s *aiv1alpha1.ChainStatus) {
			now := metav1.Now()
			s.Phase = phase
			s.RunID = "old-run"
			s.StartedAt, s.CompletedAt = &now, &now
			s.StepStatuses = []aiv1alpha1.ChainStepStatus{{Name: "review", Phase: stepPhase, Output: "old output", TaskID: "old-task", StartedAt: &now}}
		})
	}

	DescribeTable("run annotation starts a fresh run from a finished chain",
		func(phase aiv1alpha1.ChainPhase, stepPhase aiv1alpha1.ChainStepPhase) {
			create(oneStep, nil)
			finishedRun(phase, stepPhase)
			annotate(aiv1alpha1.AnnotationChainRun, "go-1")
			reconcileN(1)

			c := get()
			Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhaseRunning))
			Expect(c.Status.RunID).NotTo(BeEmpty())
			Expect(c.Status.RunID).NotTo(Equal("old-run"))
			Expect(c.Status.LastRunTrigger).To(Equal("go-1"))
			Expect(c.Status.CompletedAt).To(BeNil())
			Expect(c.Status.StepStatuses).To(HaveLen(1))
			Expect(c.Status.StepStatuses[0].Phase).To(Equal(aiv1alpha1.ChainStepPhasePending))
			Expect(c.Status.StepStatuses[0].Output).To(BeEmpty())
			Expect(c.Status.StepStatuses[0].TaskID).To(BeEmpty())

			// Running → the first step is published under the new run.
			reconcileN(1)
			c = get()
			Expect(c.Status.StepStatuses[0].Phase).To(Equal(aiv1alpha1.ChainStepPhaseRunning))
			Expect(c.Status.StepStatuses[0].TaskID).To(ContainSubstring(c.Status.RunID))
			Expect(c.Status.StepStatuses[0].TaskSeq).To(BeNumerically(">", 0))

			// Finish the new run, re-reconcile with the SAME annotation value:
			// it must not trigger again.
			runID := c.Status.RunID
			setStatus(func(s *aiv1alpha1.ChainStatus) { s.Phase = aiv1alpha1.ChainPhaseSucceeded })
			reconcileN(2)
			c = get()
			Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhaseSucceeded))
			Expect(c.Status.RunID).To(Equal(runID))

			// A new value triggers another fresh run.
			annotate(aiv1alpha1.AnnotationChainRun, "go-2")
			reconcileN(1)
			c = get()
			Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhaseRunning))
			Expect(c.Status.RunID).NotTo(Equal(runID))
			Expect(c.Status.LastRunTrigger).To(Equal("go-2"))
		},
		Entry("after Succeeded", aiv1alpha1.ChainPhaseSucceeded, aiv1alpha1.ChainStepPhaseSucceeded),
		Entry("after Failed", aiv1alpha1.ChainPhaseFailed, aiv1alpha1.ChainStepPhaseFailed),
	)

	It("never resumes the old run after a spec.input change, and the annotation then starts fresh", func() {
		create(oneStep, nil)
		finishedRun(aiv1alpha1.ChainPhaseSucceeded, aiv1alpha1.ChainStepPhaseSucceeded)

		c := get()
		c.Spec.Input = "issue-126"
		Expect(k8sClient.Update(ctx, c)).To(Succeed())
		reconcileN(1)

		c = get()
		Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhaseIdle))
		Expect(c.Status.RunID).To(BeEmpty())
		Expect(c.Status.StepStatuses[0].Phase).To(Equal(aiv1alpha1.ChainStepPhasePending))
		Expect(c.Status.StepStatuses[0].Output).To(BeEmpty())

		annotate(aiv1alpha1.AnnotationChainRun, "after-input")
		reconcileN(2)
		c = get()
		Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhaseRunning))
		Expect(c.Status.RunID).NotTo(BeEmpty())
		Expect(c.Status.StepStatuses[0].Phase).To(Equal(aiv1alpha1.ChainStepPhaseRunning))
		var payload string
		for _, raw := range fake.published {
			payload = string(raw)
		}
		Expect(payload).To(ContainSubstring("issue-126"))
	})

	It("does not start a run while one is in flight, and fires it once finished", func() {
		create(oneStep, nil)
		annotate(aiv1alpha1.AnnotationChainRun, "first")
		reconcileN(2)
		runID := get().Status.RunID

		annotate(aiv1alpha1.AnnotationChainRun, "second")
		reconcileN(1)
		Expect(get().Status.RunID).To(Equal(runID))
		Expect(get().Status.LastRunTrigger).To(Equal("first"))

		setStatus(func(s *aiv1alpha1.ChainStatus) { s.Phase = aiv1alpha1.ChainPhaseSucceeded })
		reconcileN(1)
		Expect(get().Status.LastRunTrigger).To(Equal("second"))
		Expect(get().Status.RunID).NotTo(Equal(runID))
	})

	It("retry-failed resumes only the failed and skipped steps of the same run", func() {
		steps := []aiv1alpha1.ChainStep{
			{Name: "scan", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "scan", TimeoutSeconds: 120},
			{Name: "review", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "review", TimeoutSeconds: 120, DependsOn: []string{"scan"}},
			{Name: "report", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "report", TimeoutSeconds: 120, DependsOn: []string{"review"}},
		}
		create(steps, nil)
		setStatus(func(s *aiv1alpha1.ChainStatus) {
			now := metav1.Now()
			s.Phase = aiv1alpha1.ChainPhaseFailed
			s.RunID = "run-1"
			s.StartedAt, s.CompletedAt = &now, &now
			s.StepStatuses = []aiv1alpha1.ChainStepStatus{
				{Name: "scan", Phase: aiv1alpha1.ChainStepPhaseSucceeded, Output: "scan-out", TaskID: "t1", StartedAt: &now},
				{Name: "review", Phase: aiv1alpha1.ChainStepPhaseFailed, Error: "boom", TaskID: "t2", StartedAt: &now, Retries: 1},
				{Name: "report", Phase: aiv1alpha1.ChainStepPhaseSkipped},
			}
		})

		annotate(aiv1alpha1.AnnotationChainRetryFailed, "r1")
		reconcileN(1)

		c := get()
		Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhaseRunning))
		Expect(c.Status.RunID).To(Equal("run-1"))
		Expect(c.Status.LastRetryTrigger).To(Equal("r1"))
		Expect(c.Status.StepStatuses[0].Phase).To(Equal(aiv1alpha1.ChainStepPhaseSucceeded))
		Expect(c.Status.StepStatuses[0].Output).To(Equal("scan-out"))
		Expect(c.Status.StepStatuses[1].Phase).To(Equal(aiv1alpha1.ChainStepPhasePending))
		Expect(c.Status.StepStatuses[1].Error).To(BeEmpty())
		Expect(c.Status.StepStatuses[1].Retries).To(BeZero())
		Expect(c.Status.StepStatuses[2].Phase).To(Equal(aiv1alpha1.ChainStepPhasePending))

		// Only the failed step is dispatched; scan is not re-run.
		reconcileN(1)
		c = get()
		Expect(c.Status.StepStatuses[0].TaskID).To(Equal("t1"))
		Expect(c.Status.StepStatuses[1].Phase).To(Equal(aiv1alpha1.ChainStepPhaseRunning))
		Expect(c.Status.StepStatuses[2].Phase).To(Equal(aiv1alpha1.ChainStepPhasePending))

		// Same value again after another failure: ignored.
		setStatus(func(s *aiv1alpha1.ChainStatus) { s.Phase = aiv1alpha1.ChainPhaseFailed })
		reconcileN(1)
		Expect(get().Status.Phase).To(Equal(aiv1alpha1.ChainPhaseFailed))
	})

	It("retry-failed on a chain with nothing to retry is a recorded no-op", func() {
		create(oneStep, nil)
		finishedRun(aiv1alpha1.ChainPhaseSucceeded, aiv1alpha1.ChainStepPhaseSucceeded)
		annotate(aiv1alpha1.AnnotationChainRetryFailed, "x")
		reconcileN(1)
		c := get()
		Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhaseSucceeded))
		Expect(c.Status.RunID).To(Equal("old-run"))
		Expect(c.Status.LastRetryTrigger).To(Equal("x"))
	})

	Context("step timeout clock", func() {
		queued := func(ago time.Duration) {
			setStatus(func(s *aiv1alpha1.ChainStatus) {
				started := metav1.NewTime(time.Now().Add(-ago))
				s.Phase = aiv1alpha1.ChainPhaseRunning
				s.RunID = "run-q"
				s.StartedAt = &started
				s.StepStatuses = []aiv1alpha1.ChainStepStatus{{
					Name: "review", Phase: aiv1alpha1.ChainStepPhaseRunning,
					TaskID: "chain-x-review.run-q-1", TaskSeq: 5, StartedAt: &started,
				}}
			})
		}

		It("does not count queue time: the timeout starts at pickup", func() {
			create(oneStep, nil) // step timeout 120s, default max queue wait 1800s
			queued(10 * time.Minute)
			fake.delivered = 0 // knight busy, not picked up yet
			reconcileN(1)
			c := get()
			Expect(c.Status.StepStatuses[0].Phase).To(Equal(aiv1alpha1.ChainStepPhaseRunning), "10m queued > 120s timeout must not fail it")
			Expect(c.Status.StepStatuses[0].PickedUpAt).To(BeNil())

			fake.delivered = 5 // knight receives the task (still un-acked): picked up now
			reconcileN(1)
			c = get()
			Expect(c.Status.StepStatuses[0].Phase).To(Equal(aiv1alpha1.ChainStepPhaseRunning))
			Expect(c.Status.StepStatuses[0].PickedUpAt).NotTo(BeNil())

			// 200s after pickup the 120s step timeout fires.
			setStatus(func(s *aiv1alpha1.ChainStatus) {
				t := metav1.NewTime(time.Now().Add(-200 * time.Second))
				s.StepStatuses[0].PickedUpAt = &t
			})
			reconcileN(1)
			c = get()
			Expect(c.Status.StepStatuses[0].Phase).To(Equal(aiv1alpha1.ChainStepPhaseFailed))
			Expect(c.Status.StepStatuses[0].Error).To(ContainSubstring("timed out"))
		})

		It("detects pickup from the delivered sequence while the ack floor is still below the task, then times out by timeoutSeconds", func() {
			create(oneStep, func(s *aiv1alpha1.ChainSpec) { s.MaxQueueWaitSeconds = 60 })
			queued(30 * time.Second)
			fake.floor, fake.delivered = 4, 5 // delivered, not yet acked
			reconcileN(1)
			Expect(get().Status.StepStatuses[0].PickedUpAt).NotTo(BeNil())

			// 10 minutes after publish (> maxQueueWait 60s) but only 30s after
			// pickup (< 120s step timeout): must keep running.
			setStatus(func(s *aiv1alpha1.ChainStatus) {
				st := metav1.NewTime(time.Now().Add(-10 * time.Minute))
				pu := metav1.NewTime(time.Now().Add(-30 * time.Second))
				s.StepStatuses[0].StartedAt, s.StepStatuses[0].PickedUpAt = &st, &pu
			})
			reconcileN(1)
			Expect(get().Status.StepStatuses[0].Phase).To(Equal(aiv1alpha1.ChainStepPhaseRunning))

			// 200s after pickup the 120s step timeout fires, not "not picked up".
			setStatus(func(s *aiv1alpha1.ChainStatus) {
				pu := metav1.NewTime(time.Now().Add(-200 * time.Second))
				s.StepStatuses[0].PickedUpAt = &pu
			})
			reconcileN(1)
			c := get()
			Expect(c.Status.StepStatuses[0].Phase).To(Equal(aiv1alpha1.ChainStepPhaseFailed))
			Expect(c.Status.StepStatuses[0].Error).To(ContainSubstring("timed out"))
			Expect(c.Status.StepStatuses[0].Error).NotTo(ContainSubstring("not picked up"))
		})

		It("fails a task the knight never picks up once maxQueueWait elapses", func() {
			create(oneStep, func(s *aiv1alpha1.ChainSpec) { s.MaxQueueWaitSeconds = 60 })
			queued(30 * time.Second)
			reconcileN(1)
			Expect(get().Status.StepStatuses[0].Phase).To(Equal(aiv1alpha1.ChainStepPhaseRunning))

			queued(2 * time.Minute)
			reconcileN(1)
			c := get()
			Expect(c.Status.StepStatuses[0].Phase).To(Equal(aiv1alpha1.ChainStepPhaseFailed))
			Expect(c.Status.StepStatuses[0].Error).To(ContainSubstring("not picked up"))
		})

		It("falls back to publish time for tasks without a recorded sequence", func() {
			create(oneStep, nil)
			queued(10 * time.Minute)
			setStatus(func(s *aiv1alpha1.ChainStatus) { s.StepStatuses[0].TaskSeq = 0 })
			reconcileN(1)
			Expect(get().Status.StepStatuses[0].Phase).To(Equal(aiv1alpha1.ChainStepPhaseFailed))
		})
	})
})
