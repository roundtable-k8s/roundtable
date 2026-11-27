package controller

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// #776: a result already in the stream wins over the step timeout.

func longAgo() *metav1.Time {
	t := metav1.NewTime(time.Now().Add(-3 * time.Hour))
	return &t
}

func TestCollectStepResultBeatsTimeout(t *testing.T) {
	r, nc := newPhasesReconciler(t)
	stale := func(name, id string) aiv1alpha1.ChainStepStatus {
		return aiv1alpha1.ChainStepStatus{Name: name, Phase: aiv1alpha1.ChainStepPhaseRunning, TaskID: id, StartedAt: longAgo(), PickedUpAt: longAgo(), Knight: phasesKnight}
	}
	chain := phasesChain([]aiv1alpha1.ChainStep{{Name: "ok", Task: "t"}, {Name: "bad", Task: "t"}, {Name: "none", Task: "t"}},
		stale("ok", "task-ok"), stale("bad", "task-bad"), stale("none", "task-none"))
	nc.results[natspkg.ResultSubject(phasesPrefix, "task-ok")] = natspkg.TaskResult{Output: "green"}
	nc.results[natspkg.ResultSubject(phasesPrefix, "task-bad")] = natspkg.TaskResult{Error: "knight said no"}

	r.collectResults(context.Background(), chain, newChainRun(phasesNATS, chain))

	if ok := stepStatus(t, chain, "ok"); ok.Phase != aiv1alpha1.ChainStepPhaseSucceeded || ok.Output != "green" {
		t.Errorf("ok = %+v, want Succeeded with the published output", ok)
	}
	if bad := stepStatus(t, chain, "bad"); bad.Phase != aiv1alpha1.ChainStepPhaseFailed || bad.Error != "knight said no" {
		t.Errorf("bad = %+v, want the knight's error, not the timeout", bad)
	}
	if none := stepStatus(t, chain, "none"); none.Phase != aiv1alpha1.ChainStepPhaseFailed || none.Error != "step timed out after 600s" {
		t.Errorf("none = %+v, want the timeout message", none)
	}
}

func TestFanOutItemResultBeatsTimeout(t *testing.T) {
	r, nc := newPhasesReconciler(t)
	item := func(i int32, id string) aiv1alpha1.ChainItemStatus {
		return aiv1alpha1.ChainItemStatus{Index: i, Phase: aiv1alpha1.ChainStepPhaseRunning, TaskID: id, StartedAt: longAgo(), PickedUpAt: longAgo()}
	}
	spec := aiv1alpha1.ChainStep{Name: "fan", Task: "t", ForEach: &aiv1alpha1.ChainForEach{From: "input"}, FailurePolicy: aiv1alpha1.ChainFailurePolicyContinue}
	chain := phasesChain([]aiv1alpha1.ChainStep{spec})
	ss := aiv1alpha1.ChainStepStatus{Name: "fan", Phase: aiv1alpha1.ChainStepPhaseRunning, StartedAt: longAgo(), FanOut: &aiv1alpha1.ChainFanOutStatus{
		Total: 3, ItemPhases: "RRR", Items: []aiv1alpha1.ChainItemStatus{item(0, "t0"), item(1, "t1"), item(2, "t2")},
	}}
	chain.Status.StepStatuses = []aiv1alpha1.ChainStepStatus{ss}
	nc.results[natspkg.ResultSubject(phasesPrefix, "t0")] = natspkg.TaskResult{Output: "item ok"}
	nc.results[natspkg.ResultSubject(phasesPrefix, "t1")] = natspkg.TaskResult{Error: "item boom"}

	r.reconcileFanOut(context.Background(), phasesNATS, chain, &chain.Spec.Steps[0], &chain.Status.StepStatuses[0])

	items := chain.Status.StepStatuses[0].FanOut.Items
	if items[0].Phase != aiv1alpha1.ChainStepPhaseSucceeded || items[0].Output != "item ok" {
		t.Errorf("item 0 = %+v, want Succeeded with output", items[0])
	}
	if items[1].Phase != aiv1alpha1.ChainStepPhaseFailed || items[1].Error != "item boom" {
		t.Errorf("item 1 = %+v, want the knight's error", items[1])
	}
	if items[2].Phase != aiv1alpha1.ChainStepPhaseFailed || items[2].Error != "step timed out after 600s" {
		t.Errorf("item 2 = %+v, want the timeout message", items[2])
	}
}

func TestLoopBodyResultBeatsTimeout(t *testing.T) {
	for name, tc := range map[string]struct {
		result    *natspkg.TaskResult
		wantPhase aiv1alpha1.ChainStepPhase
		wantErr   string
		wantOut   string
	}{
		"success": {&natspkg.TaskResult{Output: "fixed"}, aiv1alpha1.ChainStepPhaseSucceeded, "", "fixed"},
		"failure": {&natspkg.TaskResult{Error: "body boom"}, aiv1alpha1.ChainStepPhaseFailed, "body boom", ""},
		"none":    {nil, aiv1alpha1.ChainStepPhaseFailed, "step timed out after 120s", ""},
	} {
		t.Run(name, func(t *testing.T) {
			r, nc := newPhasesReconciler(t)
			body := aiv1alpha1.ChainLoopStep{Name: "fix", KnightRef: aiv1alpha1.ChainStep{}.KnightRef, Task: "fix"}
			body.KnightRef.Name = phasesKnight
			spec := aiv1alpha1.ChainStep{Name: "lp", Loop: &aiv1alpha1.ChainLoop{Steps: []aiv1alpha1.ChainLoopStep{body}}}
			chain := phasesChain([]aiv1alpha1.ChainStep{spec})
			ss := &aiv1alpha1.ChainStepStatus{Name: "lp", Phase: aiv1alpha1.ChainStepPhaseRunning}
			bs := &aiv1alpha1.ChainLoopStepStatus{Name: "fix", Phase: aiv1alpha1.ChainStepPhaseRunning, Iteration: 1, TaskID: "task-body", StartedAt: longAgo(), PickedUpAt: longAgo()}
			if tc.result != nil {
				nc.results[natspkg.ResultSubject(phasesPrefix, "task-body")] = *tc.result
			}

			r.collectBodyTask(context.Background(), phasesNATS, chain, &chain.Spec.Steps[0], &body, bs, ss)

			if bs.Phase != tc.wantPhase || bs.Error != tc.wantErr || bs.Output != tc.wantOut {
				t.Errorf("body = phase %s error %q output %q, want %s %q %q", bs.Phase, bs.Error, bs.Output, tc.wantPhase, tc.wantErr, tc.wantOut)
			}
		})
	}
}
