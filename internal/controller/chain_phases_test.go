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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// Unit tests for the reconcileRunning phases dispatchReadySteps and
// collectResults, on a fake NATS client and a fake Kubernetes client.

const (
	phasesNS     = "default"
	phasesKnight = "phases-knight"
	phasesPrefix = "pht"
)

var phasesNATS = natsConfig{SubjectPrefix: phasesPrefix, TasksStream: "pht_tasks", ResultsStream: "pht_results", Table: "phases-table"}

// newPhasesReconciler returns a ChainReconciler on a fake NATS client and a
// fake Kubernetes client holding the knights.
func newPhasesReconciler(t *testing.T, knights ...string) (*ChainReconciler, *fanoutFakeNATS) {
	t.Helper()
	s := k8sruntime.NewScheme()
	if err := aiv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	b := fake.NewClientBuilder().WithScheme(s)
	for _, k := range append([]string{phasesKnight}, knights...) {
		b = b.WithObjects(&aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{Name: k, Namespace: phasesNS},
			Spec:       aiv1alpha1.KnightSpec{Domain: "ops", Model: "claude-sonnet-4"},
		})
	}
	nc := &fanoutFakeNATS{fakeNATSClient: newFakeNATSClient(), results: map[string]natspkg.TaskResult{}}
	return &ChainReconciler{
		Client:             b.Build(),
		Scheme:             s,
		Recorder:           record.NewFakeRecorder(100),
		NATS:               natspkg.NewProviderWithClient(nc, logr.Discard()),
		DefaultKnightImage: "ghcr.io/roundtable-k8s/claude-knight:test",
	}, nc
}

// phasesChain returns a running chain with the given steps and step statuses.
func phasesChain(steps []aiv1alpha1.ChainStep, statuses ...aiv1alpha1.ChainStepStatus) *aiv1alpha1.Chain {
	for i := range steps {
		if steps[i].KnightRef.Name == "" {
			steps[i].KnightRef = corev1.LocalObjectReference{Name: phasesKnight}
		}
		if steps[i].TimeoutSeconds == 0 {
			steps[i].TimeoutSeconds = 600
		}
	}
	return &aiv1alpha1.Chain{
		ObjectMeta: metav1.ObjectMeta{Name: "phases", Namespace: phasesNS},
		Spec:       aiv1alpha1.ChainSpec{Steps: steps, TimeoutSeconds: 3600},
		Status: aiv1alpha1.ChainStatus{
			Phase:        aiv1alpha1.ChainPhaseRunning,
			RunID:        "run-1",
			StepStatuses: statuses,
		},
	}
}

func pending(name string) aiv1alpha1.ChainStepStatus {
	return aiv1alpha1.ChainStepStatus{Name: name, Phase: aiv1alpha1.ChainStepPhasePending}
}

func stepStatus(t *testing.T, chain *aiv1alpha1.Chain, name string) aiv1alpha1.ChainStepStatus {
	t.Helper()
	for _, ss := range chain.Status.StepStatuses {
		if ss.Name == name {
			return ss
		}
	}
	t.Fatalf("no status for step %q", name)
	return aiv1alpha1.ChainStepStatus{}
}

func publishedSteps(nc *fanoutFakeNATS) []string {
	out := make([]string, 0, len(nc.tasks))
	for _, task := range nc.tasks {
		out = append(out, task.StepName)
	}
	return out
}

func TestDispatchReadyStepsDependencies(t *testing.T) {
	ago := metav1.NewTime(time.Now().Add(-time.Minute))
	r, nc := newPhasesReconciler(t)
	chain := phasesChain([]aiv1alpha1.ChainStep{
		{Name: "running", Task: "t"},
		{Name: "failed-soft", Task: "t", ContinueOnFailure: true},
		{Name: "failed-hard", Task: "t"},
		{Name: "free", Task: "do {{ .Input }}"},
		{Name: "waits", Task: "t", DependsOn: []string{"running"}},
		{Name: "after-soft", Task: "t", DependsOn: []string{"failed-soft"}},
		{Name: "after-hard", Task: "t", DependsOn: []string{"failed-hard"}},
		{Name: "after-missing", Task: "t", DependsOn: []string{"nope"}},
	},
		aiv1alpha1.ChainStepStatus{Name: "running", Phase: aiv1alpha1.ChainStepPhaseRunning, TaskID: "x", StartedAt: &ago},
		aiv1alpha1.ChainStepStatus{Name: "failed-soft", Phase: aiv1alpha1.ChainStepPhaseFailed},
		aiv1alpha1.ChainStepStatus{Name: "failed-hard", Phase: aiv1alpha1.ChainStepPhaseFailed},
		pending("free"), pending("waits"), pending("after-soft"), pending("after-hard"), pending("after-missing"),
	)
	chain.Spec.Input = "it"

	if _, held := r.dispatchReadySteps(context.Background(), chain, newChainRun(phasesNATS, chain)); held {
		t.Fatal("dispatch held without a deferred step")
	}

	if got := publishedSteps(nc); len(got) != 2 || got[0] != "free" || got[1] != "after-soft" {
		t.Fatalf("published steps = %v, want [free after-soft]", got)
	}
	if nc.tasks[0].Task != "do it" || nc.tasks[0].RunID != "run-1" {
		t.Errorf("free task = %+v, want rendered task and run ID", nc.tasks[0])
	}
	free := stepStatus(t, chain, "free")
	if free.Phase != aiv1alpha1.ChainStepPhaseRunning || free.TaskID == "" || free.TaskSeq == 0 || free.Knight != phasesKnight || free.StartedAt == nil {
		t.Errorf("free status = %+v, want Running with taskId, seq, knight and start time", free)
	}
	for _, name := range []string{"waits", "after-hard", "after-missing"} {
		if ss := stepStatus(t, chain, name); ss.Phase != aiv1alpha1.ChainStepPhasePending || ss.TaskID != "" {
			t.Errorf("%s status = %+v, want still Pending (dependencies not met)", name, ss)
		}
	}
}

func TestDispatchReadyStepsRetryBackoff(t *testing.T) {
	for name, tc := range map[string]struct {
		completedAgo time.Duration
		stepBackoff  *aiv1alpha1.StepRetry
		wantDispatch bool
	}{
		"chain backoff not passed": {completedAgo: time.Minute, wantDispatch: false},
		"chain backoff passed":     {completedAgo: 10 * time.Minute, wantDispatch: true},
		"step backoff overrides":   {completedAgo: time.Minute, stepBackoff: &aiv1alpha1.StepRetry{MaxAttempts: 3, BackoffSeconds: 30}, wantDispatch: true},
	} {
		t.Run(name, func(t *testing.T) {
			r, nc := newPhasesReconciler(t)
			done := metav1.NewTime(time.Now().Add(-tc.completedAgo))
			chain := phasesChain([]aiv1alpha1.ChainStep{{Name: "flaky", Task: "t", Retry: tc.stepBackoff}},
				aiv1alpha1.ChainStepStatus{Name: "flaky", Phase: aiv1alpha1.ChainStepPhasePending, Retries: 1, CompletedAt: &done})
			chain.Spec.RetryPolicy = &aiv1alpha1.ChainRetryPolicy{MaxRetries: 3, BackoffSeconds: 300}

			r.dispatchReadySteps(context.Background(), chain, newChainRun(phasesNATS, chain))

			ss := stepStatus(t, chain, "flaky")
			if dispatched := len(nc.tasks) == 1 && ss.Phase == aiv1alpha1.ChainStepPhaseRunning; dispatched != tc.wantDispatch {
				t.Fatalf("dispatched = %v (tasks %v, phase %s), want %v", dispatched, publishedSteps(nc), ss.Phase, tc.wantDispatch)
			}
		})
	}
}

func TestDispatchReadyStepsWhen(t *testing.T) {
	r, nc := newPhasesReconciler(t)
	chain := phasesChain([]aiv1alpha1.ChainStep{
		{Name: "skipped", Task: "t", When: "false"},
		{Name: "runs", Task: "t", When: "true"},
		{Name: "broken", Task: "t", When: `steps.nope.output == "x"`},
	}, pending("skipped"), pending("runs"), pending("broken"))

	r.dispatchReadySteps(context.Background(), chain, newChainRun(phasesNATS, chain))

	if got := publishedSteps(nc); len(got) != 1 || got[0] != "runs" {
		t.Fatalf("published steps = %v, want [runs]", got)
	}
	if ss := stepStatus(t, chain, "skipped"); ss.Phase != aiv1alpha1.ChainStepPhaseSkipped || ss.Reason != stepReasonWhenFalse {
		t.Errorf("skipped status = %+v, want Skipped with reason %s", ss, stepReasonWhenFalse)
	}
	if ss := stepStatus(t, chain, "broken"); ss.Phase != aiv1alpha1.ChainStepPhaseFailed || ss.CompletedAt == nil {
		t.Errorf("broken status = %+v, want Failed with a when error", ss)
	}
}

func TestDispatchReadyStepsHoldsForDeferredStep(t *testing.T) {
	r, nc := newPhasesReconciler(t)
	until := metav1.NewTime(time.Now().Add(time.Hour))
	chain := phasesChain([]aiv1alpha1.ChainStep{{Name: "deferred", Task: "t"}, {Name: "free", Task: "t"}},
		aiv1alpha1.ChainStepStatus{Name: "deferred", Phase: aiv1alpha1.ChainStepPhaseDeferred, Reason: stepReasonQuotaRefused, DeferUntil: &until},
		pending("free"))

	wait, held := r.dispatchReadySteps(context.Background(), chain, newChainRun(phasesNATS, chain))

	if !held || wait <= 50*time.Minute || wait > time.Hour {
		t.Fatalf("held = %v, wait = %s, want held for about an hour", held, wait)
	}
	if len(nc.tasks) != 0 {
		t.Fatalf("published %v while a step is quota-deferred", publishedSteps(nc))
	}
}

// running returns a plain step status that was published (and picked up) a
// minute ago with the given task ID.
func running(name, taskID string) aiv1alpha1.ChainStepStatus {
	ago := metav1.NewTime(time.Now().Add(-time.Minute))
	return aiv1alpha1.ChainStepStatus{Name: name, Phase: aiv1alpha1.ChainStepPhaseRunning, TaskID: taskID, StartedAt: &ago, PickedUpAt: &ago, Knight: phasesKnight}
}

func TestCollectResultsUsageLimited(t *testing.T) {
	resetsAt := time.Now().Add(2 * time.Hour)
	r, nc := newPhasesReconciler(t, "phases-fallback")
	chain := phasesChain([]aiv1alpha1.ChainStep{
		{Name: "primary-only", Task: "t"},
		{Name: "with-fallback", Task: "t", FallbackRef: corev1.LocalObjectReference{Name: "phases-fallback"}},
	}, running("primary-only", "task-a"), running("with-fallback", "task-b"))
	nc.results[natspkg.ResultSubject(phasesPrefix, "task-a")] = *quotaRefusalResult(resetsAt)
	nc.results[natspkg.ResultSubject(phasesPrefix, "task-b")] = *quotaRefusalResult(resetsAt)

	r.collectResults(context.Background(), chain, newChainRun(phasesNATS, chain))

	deferred := stepStatus(t, chain, "primary-only")
	if deferred.Phase != aiv1alpha1.ChainStepPhaseDeferred || deferred.Reason != stepReasonQuotaRefused || deferred.DeferUntil == nil || deferred.CompletedAt != nil {
		t.Errorf("primary-only status = %+v, want Deferred (QuotaRefused) with deferUntil", deferred)
	} else if !deferred.DeferUntil.After(resetsAt.Add(-time.Second)) {
		t.Errorf("deferUntil = %s, want at or after the plan reset %s", deferred.DeferUntil, resetsAt)
	}
	fb := stepStatus(t, chain, "with-fallback")
	if fb.Phase != aiv1alpha1.ChainStepPhasePending || fb.Reason != stepReasonFallbackQuota || fb.Knight != "phases-fallback" || fb.Error != "" {
		t.Errorf("with-fallback status = %+v, want Pending on the fallback knight", fb)
	}
}

func TestCollectResultsOutcomes(t *testing.T) {
	r, nc := newPhasesReconciler(t)
	old := metav1.NewTime(time.Now().Add(-10 * time.Minute))
	queued := aiv1alpha1.ChainStepStatus{Name: "queued", Phase: aiv1alpha1.ChainStepPhaseRunning, TaskID: "task-q", TaskSeq: 99, StartedAt: &old}
	chain := phasesChain([]aiv1alpha1.ChainStep{
		{Name: "ok", Task: "t"},
		{Name: "flaky", Task: "t", Retry: &aiv1alpha1.StepRetry{MaxAttempts: 2}},
		{Name: "broken", Task: "t"},
		{Name: "empty", Task: "t"},
		{Name: "waiting", Task: "t"},
		{Name: "no-task", Task: "t"},
		{Name: "queued", Task: "t"},
	},
		running("ok", "task-ok"), running("flaky", "task-flaky"), running("broken", "task-broken"),
		running("empty", "task-empty"), running("waiting", "task-waiting"), running("no-task", ""), queued,
	)
	chain.Spec.MaxQueueWaitSeconds = 60
	nc.results[natspkg.ResultSubject(phasesPrefix, "task-ok")] = natspkg.TaskResult{Output: "done", Model: "claude-opus-4"}
	nc.results[natspkg.ResultSubject(phasesPrefix, "task-flaky")] = natspkg.TaskResult{Error: "boom"}
	nc.results[natspkg.ResultSubject(phasesPrefix, "task-broken")] = natspkg.TaskResult{Error: "boom"}
	nc.results[natspkg.ResultSubject(phasesPrefix, "task-empty")] = natspkg.TaskResult{Output: "[No output from agent]"}

	r.collectResults(context.Background(), chain, newChainRun(phasesNATS, chain))

	for name, want := range map[string]struct {
		phase   aiv1alpha1.ChainStepPhase
		retries int32
	}{
		"ok":      {phase: aiv1alpha1.ChainStepPhaseSucceeded},
		"flaky":   {phase: aiv1alpha1.ChainStepPhasePending, retries: 1},
		"broken":  {phase: aiv1alpha1.ChainStepPhaseFailed},
		"empty":   {phase: aiv1alpha1.ChainStepPhaseFailed},
		"waiting": {phase: aiv1alpha1.ChainStepPhaseRunning},
		"no-task": {phase: aiv1alpha1.ChainStepPhaseRunning},
		"queued":  {phase: aiv1alpha1.ChainStepPhaseFailed},
	} {
		if ss := stepStatus(t, chain, name); ss.Phase != want.phase || ss.Retries != want.retries {
			t.Errorf("%s: phase %s retries %d, want %s retries %d (status %+v)", name, ss.Phase, ss.Retries, want.phase, want.retries, ss)
		}
	}
	if ok := stepStatus(t, chain, "ok"); ok.Output != "done" || ok.Model != "claude-opus-4" || ok.CompletedAt == nil {
		t.Errorf("ok status = %+v, want output, model and completion time", ok)
	}
	if broken := stepStatus(t, chain, "broken"); broken.Error != "boom" {
		t.Errorf("broken error = %q, want boom", broken.Error)
	}
	if empty := stepStatus(t, chain, "empty"); empty.Error != errEmptyOutput {
		t.Errorf("empty error = %q, want %q", empty.Error, errEmptyOutput)
	}
	if q := stepStatus(t, chain, "queued"); q.PickedUpAt != nil {
		t.Errorf("queued pickedUpAt = %v, want nil (knight never picked it up)", q.PickedUpAt)
	}
}
