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

// quotaRefusalReason is the exact refusal shape a claude-knight publishes
// (claude-knight src/main.ts: result and error carry
// "Task refused: <checkReserve reason>", with usage_limited true).
func quotaRefusalResult(resetsAt time.Time) *natspkg.TaskResult {
	return &natspkg.TaskResult{
		TaskID:       "",
		Result:       "Task refused: Claude plan five_hour window 91% used — at the knight reserve of 80% (resets " + resetsAt.UTC().Format("2006-01-02T15:04:05.000Z") + ")",
		Error:        "Task refused: Claude plan five_hour window 91% used — at the knight reserve of 80% (resets " + resetsAt.UTC().Format("2006-01-02T15:04:05.000Z") + ")",
		UsageLimited: true,
	}
}

// quotaFakeNATS records every published task with its subject and serves
// results by subject.
type quotaFakeNATS struct {
	*fanoutFakeNATS
	subjects []string
}

func (f *quotaFakeNATS) PublishJSONSeq(subject string, v any) (uint64, error) {
	seq, err := f.fanoutFakeNATS.PublishJSONSeq(subject, v)
	if err != nil {
		return seq, err
	}
	f.subjects = append(f.subjects, subject)
	return seq, nil
}

func (f *quotaFakeNATS) taskSubjects() []string { return f.subjects }

var _ = Describe("Chain plan-quota refusals (#321)", func() {
	const (
		namespace    = "default"
		rtName       = "quota-table"
		claudeKnight = "quota-claude"
		piKnight     = "quota-pi"
		chainName    = "quota-chain"
		prefix       = "qua"
	)
	ctx := context.Background()
	nn := types.NamespacedName{Name: chainName, Namespace: namespace}

	var (
		fake     *quotaFakeNATS
		recorder *record.FakeRecorder
		r        *ChainReconciler
	)

	reconcileN := func(n int) []reconcile.Result {
		out := make([]reconcile.Result, 0, n)
		for range n {
			res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())
			out = append(out, res)
		}
		return out
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
	status := func(c *aiv1alpha1.Chain, name string) aiv1alpha1.ChainStepStatus {
		for _, ss := range c.Status.StepStatuses {
			if ss.Name == name {
				return ss
			}
		}
		Fail("no status for step " + name)
		return aiv1alpha1.ChainStepStatus{}
	}
	// start creates the chain and triggers a run; the implement step is
	// dispatched to the claude coder.
	start := func(fallback bool, model string) {
		step := aiv1alpha1.ChainStep{
			Name: "implement", KnightRef: corev1.LocalObjectReference{Name: claudeKnight},
			Task: "implement it", TimeoutSeconds: 120,
		}
		if model != "" {
			step.Model = model
		}
		if fallback {
			step.FallbackRef = corev1.LocalObjectReference{Name: piKnight}
		}
		Expect(k8sClient.Create(ctx, &aiv1alpha1.Chain{
			ObjectMeta: metav1.ObjectMeta{Name: chainName, Namespace: namespace},
			Spec: aiv1alpha1.ChainSpec{
				RoundTableRef: aiv1alpha1.LocalRef(rtName), TimeoutSeconds: 86400,
				Steps: []aiv1alpha1.ChainStep{
					step,
					{Name: "merge", KnightRef: corev1.LocalObjectReference{Name: claudeKnight}, Task: "merge", TimeoutSeconds: 120, DependsOn: []string{"implement"}},
				},
			},
		})).To(Succeed())
		reconcileN(3)
		Expect(get().Status.Phase).To(Equal(aiv1alpha1.ChainPhaseIdle))
		annotate(aiv1alpha1.AnnotationChainRun, "go")
		reconcileN(2)
		Expect(status(get(), "implement").Phase).To(Equal(aiv1alpha1.ChainStepPhaseRunning))
		Expect(fake.taskSubjects()).To(HaveLen(1))
	}
	finish := func(t natspkg.TaskPayload, res *natspkg.TaskResult) {
		res.TaskID = t.TaskID
		fake.results[natspkg.ResultSubject(prefix, t.TaskID)] = *res
	}
	finishLast := func(res *natspkg.TaskResult) {
		ts := fake.tasks
		Expect(ts).NotTo(BeEmpty())
		finish(ts[len(ts)-1], res)
	}
	payloadAt := func(i int) map[string]any {
		m := map[string]any{}
		data, err := json.Marshal(fake.tasks[i])
		Expect(err).NotTo(HaveOccurred())
		Expect(json.Unmarshal(data, &m)).To(Succeed())
		return m
	}

	BeforeEach(func() {
		fake = &quotaFakeNATS{fanoutFakeNATS: &fanoutFakeNATS{fakeNATSClient: newFakeNATSClient(), results: map[string]natspkg.TaskResult{}}}
		recorder = record.NewFakeRecorder(100)
		r = &ChainReconciler{
			Client:             k8sClient,
			Scheme:             k8sClient.Scheme(),
			Recorder:           recorder,
			NATS:               natspkg.NewProviderWithClient(fake, logr.Discard()),
			DefaultKnightImage: "ghcr.io/roundtable-k8s/pi-knight:v2",
		}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: rtName, Namespace: namespace}, &aiv1alpha1.RoundTable{}); apierrors.IsNotFound(err) {
			Expect(k8sClient.Create(ctx, &aiv1alpha1.RoundTable{
				ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: namespace},
				Spec: aiv1alpha1.RoundTableSpec{
					NATS: aiv1alpha1.RoundTableNATS{
						URL: "nats://localhost:4222", SubjectPrefix: prefix, TasksStream: "qua_tasks", ResultsStream: "qua_results",
					},
					ModelAliases: map[string]string{"deep": "opus"},
				},
			})).To(Succeed())
		}
		knight := func(name, image string) {
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &aiv1alpha1.Knight{}); apierrors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, &aiv1alpha1.Knight{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
					Spec: aiv1alpha1.KnightSpec{
						Domain: "ops", Model: "claude-sonnet-4", Image: image, Skills: []string{"ops"},
						NATS:        aiv1alpha1.KnightNATS{URL: "nats://x:4222", Subjects: []string{"qua.tasks.ops.>"}, Stream: "qua_tasks", ResultsStream: "qua_results"},
						Concurrency: 1, TaskTimeoutSeconds: 120,
					},
				})).To(Succeed())
			}
		}
		knight(claudeKnight, "ghcr.io/roundtable-k8s/claude-knight:v1")
		knight(piKnight, "") // falls back to DefaultKnightImage (pi-knight)
	})

	AfterEach(func() {
		c := &aiv1alpha1.Chain{}
		if err := k8sClient.Get(ctx, nn, c); err == nil {
			c.Finalizers = nil
			_ = k8sClient.Update(ctx, c)
			_ = k8sClient.Delete(ctx, c)
		}
		Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(ctx, nn, &aiv1alpha1.Chain{}))
		}).Should(BeTrue())
	})

	It("re-runs a quota-refused implement step on the fallback coder without a model override", func() {
		start(true, "deep")
		// The first task went to the claude coder with the resolved alias.
		Expect(payloadAt(0)).To(HaveKeyWithValue("model", "opus"))
		Expect(fake.taskSubjects()[0]).To(Equal(natspkg.TaskSubject(prefix, "ops", claudeKnight)))

		resetsAt := time.Now().Add(time.Hour)
		finishLast(quotaRefusalResult(resetsAt))
		reconcileN(1)

		c := get()
		ss := status(c, "implement")
		Expect(ss.Phase).To(Equal(aiv1alpha1.ChainStepPhaseRunning))
		Expect(ss.Knight).To(Equal(piKnight))
		Expect(ss.Reason).To(Equal(stepReasonFallbackQuota))
		// Re-run on the fallback coder: no model override, its own default.
		Expect(fake.taskSubjects()).To(HaveLen(2))
		Expect(fake.taskSubjects()[1]).To(Equal(natspkg.TaskSubject(prefix, "ops", piKnight)))
		Expect(payloadAt(1)).NotTo(HaveKey("model"))
		// The chain did not fail or escalate.
		Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhaseRunning))
		var events []string
		for len(recorder.Events) > 0 {
			events = append(events, <-recorder.Events)
		}
		Expect(events).To(ContainElement(ContainSubstring("StepFallbackQuota")))

		// The fallback finishes the work; the chain continues as if nothing happened.
		finish(fake.tasks[1], &natspkg.TaskResult{Output: "PRs: https://github.com/o/r/pull/1"})
		reconcileN(2)
		c = get()
		Expect(status(c, "implement").Phase).To(Equal(aiv1alpha1.ChainStepPhaseSucceeded))
		Expect(status(c, "merge").Phase).To(Equal(aiv1alpha1.ChainStepPhaseRunning))
	})

	It("re-runs a quota-refused fix round on the fallback coder without a model override", func() {
		Expect(k8sClient.Create(ctx, &aiv1alpha1.Chain{
			ObjectMeta: metav1.ObjectMeta{Name: chainName, Namespace: namespace},
			Spec: aiv1alpha1.ChainSpec{
				RoundTableRef: aiv1alpha1.LocalRef(rtName), TimeoutSeconds: 86400,
				Steps: []aiv1alpha1.ChainStep{
					{Name: "review", KnightRef: corev1.LocalObjectReference{Name: claudeKnight}, Task: "review", TimeoutSeconds: 120},
					{Name: "fixloop", DependsOn: []string{"review"},
						FallbackRef: corev1.LocalObjectReference{Name: piKnight},
						Loop: &aiv1alpha1.ChainLoop{
							From: "review", Until: `last.output.contains("Verdict: ship it")`, MaxIterations: 3, OnExhausted: aiv1alpha1.ChainLoopExhaustedContinue,
							Steps: []aiv1alpha1.ChainLoopStep{
								{Name: "fix", KnightRef: corev1.LocalObjectReference{Name: claudeKnight}, Model: "deep", Task: "fix {{ .Loop.Last.Output }}"},
							},
						}},
				},
			},
		})).To(Succeed())
		reconcileN(3)
		annotate(aiv1alpha1.AnnotationChainRun, "go")
		reconcileN(2)
		Expect(fake.tasks).NotTo(BeEmpty()) // the review, on the claude coder
		finish(fake.tasks[0], &natspkg.TaskResult{Output: "PRs: https://github.com/o/r/pull/1\nVerdict: needs changes"})
		reconcileN(1)
		Expect(fake.taskSubjects()).To(HaveLen(2)) // fix round 1 on the claude coder
		Expect(payloadAt(1)).To(HaveKeyWithValue("model", "opus"))

		// The claude coder refuses the fix round for plan quota.
		finish(fake.tasks[1], quotaRefusalResult(time.Now().Add(time.Hour)))
		reconcileN(1)

		c := get()
		Expect(status(c, "fixloop").Phase).To(Equal(aiv1alpha1.ChainStepPhaseRunning))
		Expect(fake.taskSubjects()).To(HaveLen(3))
		Expect(fake.taskSubjects()[2]).To(Equal(natspkg.TaskSubject(prefix, "ops", piKnight)))
		Expect(payloadAt(2)).NotTo(HaveKey("model"))

		finish(fake.tasks[2], &natspkg.TaskResult{Output: "fixed, Verdict: ship it"})
		reconcileN(2)
		Expect(status(get(), "fixloop").Loop.Outcome).To(Equal(aiv1alpha1.ChainLoopConverged))
	})

	It("dispatches the round after a fallback-rescued round to the primary coder with its model alias", func() {
		Expect(k8sClient.Create(ctx, &aiv1alpha1.Chain{
			ObjectMeta: metav1.ObjectMeta{Name: chainName, Namespace: namespace},
			Spec: aiv1alpha1.ChainSpec{
				RoundTableRef: aiv1alpha1.LocalRef(rtName), TimeoutSeconds: 86400,
				Steps: []aiv1alpha1.ChainStep{
					{Name: "review", KnightRef: corev1.LocalObjectReference{Name: claudeKnight}, Task: "review", TimeoutSeconds: 120},
					{Name: "fixloop", DependsOn: []string{"review"},
						FallbackRef: corev1.LocalObjectReference{Name: piKnight},
						Loop: &aiv1alpha1.ChainLoop{
							From: "review", Until: `last.output.contains("Verdict: ship it")`, MaxIterations: 3, OnExhausted: aiv1alpha1.ChainLoopExhaustedContinue,
							Steps: []aiv1alpha1.ChainLoopStep{
								{Name: "fix", KnightRef: corev1.LocalObjectReference{Name: claudeKnight}, Model: "deep", Task: "fix {{ .Loop.Last.Output }}"},
							},
						}},
				},
			},
		})).To(Succeed())
		reconcileN(3)
		annotate(aiv1alpha1.AnnotationChainRun, "go")
		reconcileN(2)
		finish(fake.tasks[0], &natspkg.TaskResult{Output: "PRs: https://github.com/o/r/pull/1\nVerdict: needs changes"})
		reconcileN(1)
		Expect(fake.taskSubjects()).To(HaveLen(2)) // fix round 1 on the claude coder

		// Round 1 is quota-refused and rescued by the fallback coder.
		finish(fake.tasks[1], quotaRefusalResult(time.Now().Add(time.Hour)))
		reconcileN(1)
		Expect(fake.taskSubjects()).To(HaveLen(3))
		Expect(fake.taskSubjects()[2]).To(Equal(natspkg.TaskSubject(prefix, "ops", piKnight)))

		// Round 1 does not converge: the next round goes back to the primary
		// coder with its model alias — the fallback does not stick.
		finish(fake.tasks[2], &natspkg.TaskResult{Output: "fixed, Verdict: needs changes"})
		reconcileN(1)
		Expect(fake.taskSubjects()).To(HaveLen(4))
		Expect(fake.taskSubjects()[3]).To(Equal(natspkg.TaskSubject(prefix, "ops", claudeKnight)))
		Expect(payloadAt(3)).To(HaveKeyWithValue("model", "opus"))
		Expect(status(get(), "fixloop").Loop.Iteration).To(Equal(int32(2)))
	})

	It("defers a quota-refused step until the reset and requeues then, without escalating", func() {
		start(false, "")
		Expect(payloadAt(0)).NotTo(HaveKey("model"))

		resetsAt := time.Now().Add(time.Hour)
		finishLast(quotaRefusalResult(resetsAt))
		results := reconcileN(1)

		c := get()
		ss := status(c, "implement")
		// The step is Deferred for the reset time plus jitter, not Failed.
		Expect(ss.Phase).To(Equal(aiv1alpha1.ChainStepPhaseDeferred))
		Expect(ss.Reason).To(Equal(stepReasonQuotaRefused))
		Expect(ss.Error).To(ContainSubstring("Task refused: Claude plan"))
		Expect(ss.DeferUntil).NotTo(BeNil())
		want := resetsAt.Add(quotaDeferJitter).Round(time.Second)
		Expect(ss.DeferUntil.Time).To(BeTemporally("~", want, 2*time.Second))
		// The chain is requeued for the reset time, stays Running (no
		// failure, no escalation) and dispatches nothing else meanwhile.
		Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhaseRunning))
		Expect(results).To(HaveLen(1))
		Expect(results[0].RequeueAfter).To(BeNumerically(">", 50*time.Minute))
		Expect(fake.taskSubjects()).To(HaveLen(1))
		// The issue keeps rt:in-progress: the deferred chain is still an
		// active pickup chain, not handed back as needs-human.
		Expect(pickupChainActive(c)).To(BeTrue())
		Expect(status(c, "merge").Phase).To(Equal(aiv1alpha1.ChainStepPhasePending))
		var events []string
		for len(recorder.Events) > 0 {
			events = append(events, <-recorder.Events)
		}
		Expect(events).NotTo(ContainElement(ContainSubstring("Failed")))
	})

	It("re-runs a deferred step on its primary knight after the reset", func() {
		start(false, "")

		finishLast(quotaRefusalResult(time.Now().Add(time.Hour)))
		reconcileN(1)
		Expect(status(get(), "implement").Phase).To(Equal(aiv1alpha1.ChainStepPhaseDeferred))

		// The window resets (the deferUntil passes): the step runs again.
		c := get()
		past := metav1.NewTime(time.Now().Add(-time.Minute))
		for i := range c.Status.StepStatuses {
			if c.Status.StepStatuses[i].Name == "implement" {
				c.Status.StepStatuses[i].DeferUntil = &past
			}
		}
		Expect(k8sClient.Status().Update(ctx, c)).To(Succeed())
		reconcileN(1)

		c = get()
		Expect(status(c, "implement").Phase).To(Equal(aiv1alpha1.ChainStepPhaseRunning))
		Expect(status(c, "implement").Knight).To(Equal(claudeKnight))
		Expect(fake.taskSubjects()).To(HaveLen(2))
		Expect(fake.taskSubjects()[1]).To(Equal(natspkg.TaskSubject(prefix, "ops", claudeKnight)))
	})
})

// TestQuotaRefusalDetection unit-tests the refusal detection against the
// exact claude-knight result shapes.
func TestQuotaRefusalDetection(t *testing.T) {
	resets := time.Now().Add(5 * time.Hour).UTC()
	refused := func(msg string) *natspkg.TaskResult {
		return &natspkg.TaskResult{Error: msg, Result: msg, UsageLimited: true}
	}
	cases := map[string]struct {
		res  *natspkg.TaskResult
		want bool
	}{
		"claude reserve refusal": {refused("Task refused: Claude plan five_hour window 91% used — at the knight reserve of 80% (resets " +
			resets.Format(time.RFC3339Nano) + ")"), true},
		"claude rejected window": {refused("Task refused: Claude plan five_hour limit reached (resets " +
			resets.Format(time.RFC3339) + ")"), true},
		"refusal without reset time":       {refused("Task refused: Claude plan five_hour window 91% used"), false},
		"ordinary error":                   {&natspkg.TaskResult{Error: "boom"}, false},
		"success":                          {&natspkg.TaskResult{Output: "done"}, false},
		"usage-limited in-run, not refuse": {&natspkg.TaskResult{Error: "ran out mid-task", UsageLimited: true}, false},
		"nil":                              {nil, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, ok := quotaRefusal(tc.res)
			if ok != tc.want {
				t.Fatalf("quotaRefusal() = %v, want refusable=%v", ok, tc.want)
			}
			if tc.want && got.IsZero() {
				t.Fatal("expected a reset time")
			}
			if !tc.want && !got.IsZero() {
				t.Fatalf("unexpected reset time %v", got)
			}
		})
	}
}
