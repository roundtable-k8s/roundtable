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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// A Running chain with a step in flight requeues itself to poll the result.
// The requeue must carry an explicit priority: an unset one keeps the low
// priority the item got from the start-up cache list, starving the poll
// behind other work (#700).
func TestReconcile_RequeueCarriesExplicitPriority(t *testing.T) {
	r, _, chain := cancelFixture(t, nil, running("a", "task-a"), pending("b"), pending("c"))
	// Reach the step poll: the chain needs a RoundTable and its knight, which the cancel
	// fixture leaves out.
	key := types.NamespacedName{Namespace: chain.Namespace, Name: chain.Name}
	if err := r.Get(context.Background(), key, chain); err != nil {
		t.Fatal(err)
	}
	chain.Spec.RoundTableRef = &corev1.LocalObjectReference{Name: "table"}
	if err := r.Update(context.Background(), chain); err != nil {
		t.Fatal(err)
	}
	if err := r.Create(context.Background(), &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{Name: phasesKnight, Namespace: chain.Namespace},
		Spec:       aiv1alpha1.KnightSpec{Domain: "ops", Model: "claude-sonnet-4"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.Create(context.Background(), &aiv1alpha1.RoundTable{
		ObjectMeta: metav1.ObjectMeta{Name: "table", Namespace: chain.Namespace},
	}); err != nil {
		t.Fatal(err)
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	if err != nil {
		t.Fatalf("fixture must reach the step poll, got error: %v", err)
	}
	if res.RequeueAfter <= 0 && !res.Requeue { //nolint:staticcheck
		t.Fatalf("fixture must requeue to poll the in-flight step, got %+v", res)
	}
	if res.Priority == nil || *res.Priority != requeuePriority {
		t.Fatalf("requeueing result has Priority=%v, want %d (result %+v)", res.Priority, requeuePriority, res)
	}
}

// A result that does not requeue keeps its priority untouched.
func TestReconcile_NoRequeueLeavesPriorityUnset(t *testing.T) {
	r, _, chain := cancelFixture(t, nil)
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: chain.Namespace, Name: "gone"}})
	if err != nil || res.Priority != nil {
		t.Fatalf("got %+v, %v; want empty result", res, err)
	}
}

// Chain and Mission reconcilers share resetRequeuePriority: any requeue or
// error gets the explicit priority, a plain result is left alone.
func TestResetRequeuePriority(t *testing.T) {
	for name, res := range map[string]ctrl.Result{
		"RequeueAfter": {RequeueAfter: 5},
		"Requeue":      {Requeue: true}, //nolint:staticcheck
	} {
		got, _ := resetRequeuePriority(res, nil)
		if got.Priority == nil || *got.Priority != requeuePriority {
			t.Errorf("%s: Priority=%v, want %d", name, got.Priority, requeuePriority)
		}
	}
	if got, _ := resetRequeuePriority(ctrl.Result{}, context.Canceled); got.Priority == nil {
		t.Error("error result: Priority unset, want explicit")
	}
	if got, _ := resetRequeuePriority(ctrl.Result{}, nil); got.Priority != nil {
		t.Errorf("plain result: Priority=%v, want nil", got.Priority)
	}
}
