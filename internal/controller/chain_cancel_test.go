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

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func cancelFixture(t *testing.T, annotations map[string]string, statuses ...aiv1alpha1.ChainStepStatus) (*ChainReconciler, *fanoutFakeNATS, *aiv1alpha1.Chain) {
	t.Helper()
	r, nc := newPhasesReconciler(t)
	chain := phasesChain([]aiv1alpha1.ChainStep{
		{Name: "a", Task: "t"},
		{Name: "b", Task: "t", DependsOn: []string{"a"}},
		{Name: "c", Task: "t", DependsOn: []string{"a"}},
	}, statuses...)
	chain.Annotations = annotations
	chain.Finalizers = []string{chainFinalizer}
	chain.Status.StartedAt = &metav1.Time{Time: metav1.Now().Time}
	chain.Status.ObservedGeneration = chain.Generation
	// Status writes need the status subresource; the chain goes in with its status.
	r.Client = fake.NewClientBuilder().WithScheme(r.Scheme).WithObjects(chain.DeepCopy()).
		WithStatusSubresource(&aiv1alpha1.Chain{}).Build()
	return r, nc, chain
}

func reconcileChain(t *testing.T, r *ChainReconciler, chain *aiv1alpha1.Chain) *aiv1alpha1.Chain {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: chain.Namespace, Name: chain.Name}}); err != nil {
		t.Fatal(err)
	}
	got := &aiv1alpha1.Chain{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: chain.Namespace, Name: chain.Name}, got); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestCancelRunningChain(t *testing.T) {
	r, nc, chain := cancelFixture(t, map[string]string{aiv1alpha1.AnnotationChainCancel: "wrong input"},
		aiv1alpha1.ChainStepStatus{Name: "a", Phase: aiv1alpha1.ChainStepPhaseRunning, TaskID: "t1"}, pending("b"), pending("c"))

	got := reconcileChain(t, r, chain)
	if got.Status.Phase != aiv1alpha1.ChainPhaseFailed || got.Status.CompletedAt == nil || got.Status.RunsFailed != 1 {
		t.Fatalf("status: %+v", got.Status)
	}
	for _, name := range []string{"a", "b", "c"} {
		ss := stepStatus(t, got, name)
		if ss.Phase != aiv1alpha1.ChainStepPhaseFailed || ss.Error != "cancelled: wrong input" || ss.CompletedAt == nil {
			t.Errorf("step %s: %+v", name, ss)
		}
	}
	for _, typ := range []string{aiv1alpha1.ConditionChainCancelled, aiv1alpha1.ConditionChainComplete} {
		if c := meta.FindStatusCondition(got.Status.Conditions, typ); c == nil || c.Status != metav1.ConditionTrue || c.Reason != aiv1alpha1.ReasonChainCancelled {
			t.Errorf("condition %s: %+v", typ, c)
		}
	}
	if _, ok := got.Annotations[aiv1alpha1.AnnotationChainCancel]; ok {
		t.Error("cancel annotation not removed")
	}

	if len(nc.tasks) != 0 {
		t.Errorf("published after cancel: %v", publishedSteps(nc))
	}
}

func TestCancelTerminalChainIsNoOp(t *testing.T) {
	r, _, chain := cancelFixture(t, map[string]string{aiv1alpha1.AnnotationChainCancel: "late"})
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: chain.Namespace, Name: chain.Name}, chain); err != nil {
		t.Fatal(err)
	}
	chain.Status.Phase = aiv1alpha1.ChainPhaseSucceeded
	chain.Status.StepStatuses = []aiv1alpha1.ChainStepStatus{
		{Name: "a", Phase: aiv1alpha1.ChainStepPhaseSucceeded}, {Name: "b", Phase: aiv1alpha1.ChainStepPhaseSucceeded},
		{Name: "c", Phase: aiv1alpha1.ChainStepPhaseSucceeded}}
	if err := r.Status().Update(context.Background(), chain); err != nil {
		t.Fatal(err)
	}
	got := reconcileChain(t, r, chain)
	if got.Status.Phase != aiv1alpha1.ChainPhaseSucceeded || got.Status.RunsFailed != 0 ||
		meta.FindStatusCondition(got.Status.Conditions, aiv1alpha1.ConditionChainCancelled) != nil {
		t.Errorf("terminal run changed: %+v", got.Status)
	}
	if _, ok := got.Annotations[aiv1alpha1.AnnotationChainCancel]; ok {
		t.Error("stale cancel annotation kept")
	}
}

func TestCancelStopsLoopAndFanOut(t *testing.T) {
	r, _, chain := cancelFixture(t, map[string]string{aiv1alpha1.AnnotationChainCancel: ""},
		aiv1alpha1.ChainStepStatus{Name: "a", Phase: aiv1alpha1.ChainStepPhaseRunning, Loop: &aiv1alpha1.ChainLoopStatus{Iteration: 2}},
		aiv1alpha1.ChainStepStatus{Name: "b", Phase: aiv1alpha1.ChainStepPhaseRunning, FanOut: &aiv1alpha1.ChainFanOutStatus{Total: 3, ItemPhases: "RPP"}},
		aiv1alpha1.ChainStepStatus{Name: "c", Phase: aiv1alpha1.ChainStepPhaseSucceeded})
	got := reconcileChain(t, r, chain)
	if a := stepStatus(t, got, "a"); a.Phase != aiv1alpha1.ChainStepPhaseFailed || a.Loop.Outcome != aiv1alpha1.ChainLoopFailed || a.Error != "cancelled" {
		t.Errorf("loop step: %+v", a)
	}
	if b := stepStatus(t, got, "b"); b.Phase != aiv1alpha1.ChainStepPhaseFailed || b.FanOut.ItemPhases != "RKK" {
		t.Errorf("fan-out step: %+v", b)
	}
	if c := stepStatus(t, got, "c"); c.Phase != aiv1alpha1.ChainStepPhaseSucceeded {
		t.Errorf("finished step touched: %+v", c)
	}
}
