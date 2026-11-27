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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/cli-utils/pkg/kstatus/status"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func roundTableKstatusOf(t *testing.T, rt *aiv1alpha1.RoundTable) status.Status {
	t.Helper()
	m, err := k8sruntime.DefaultUnstructuredConverter.ToUnstructured(rt)
	if err != nil {
		t.Fatal(err)
	}
	u := &unstructured.Unstructured{Object: m}
	u.SetAPIVersion(aiv1alpha1.GroupVersion.String())
	u.SetKind("RoundTable")
	res, err := status.Compute(u)
	if err != nil {
		t.Fatal(err)
	}
	return res.Status
}

func kstatusTestReconciler(t *testing.T, objs ...client.Object) (*RoundTableReconciler, *aiv1alpha1.RoundTable) {
	t.Helper()
	s := k8sruntime.NewScheme()
	if err := aiv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).
		WithStatusSubresource(&aiv1alpha1.RoundTable{}).Build()
	return &RoundTableReconciler{Client: c, Scheme: s, Recorder: record.NewFakeRecorder(50)}, nil
}

// anyPhase skips the phase assertion: the table-owned planner knight is created
// on the first reconcile and not ready yet, so the phase is not what is tested.
const anyPhase aiv1alpha1.RoundTablePhase = "*"

// TestRoundTableKstatus checks what Helm's --wait and Flux health checks see:
// a RoundTable in any phase is Current once the operator has accepted its
// spec, including a spec that was just changed by a chart upgrade.
func TestRoundTableKstatus(t *testing.T) {
	knight := func(name string, phase aiv1alpha1.KnightPhase, ready bool) *aiv1alpha1.Knight {
		return &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec:       aiv1alpha1.KnightSpec{Domain: "ops", Model: "claude-sonnet-4"},
			Status:     aiv1alpha1.KnightStatus{Phase: phase, Ready: ready},
		}
	}
	cases := []struct {
		name      string
		mutate    func(rt *aiv1alpha1.RoundTable)
		knights   []client.Object
		wantPhase aiv1alpha1.RoundTablePhase
	}{
		{"provisioning", func(*aiv1alpha1.RoundTable) {}, nil, anyPhase},
		{"ready", func(*aiv1alpha1.RoundTable) {}, []client.Object{knight("k1", aiv1alpha1.KnightPhaseReady, true)}, anyPhase},
		{"degraded", func(*aiv1alpha1.RoundTable) {}, []client.Object{knight("k1", aiv1alpha1.KnightPhaseDegraded, false)}, anyPhase},
		{"suspended", func(rt *aiv1alpha1.RoundTable) { rt.Spec.Suspend = true }, nil, aiv1alpha1.RoundTablePhaseSuspended},
		{"budgeted", func(rt *aiv1alpha1.RoundTable) {
			rt.Spec.Policies = &aiv1alpha1.RoundTablePolicies{CostBudgetUSD: "1"}
		}, []client.Object{knight("k1", aiv1alpha1.KnightPhaseReady, true)}, anyPhase},
		{"invalidKnightSelector", func(rt *aiv1alpha1.RoundTable) {
			rt.Spec.KnightSelector = &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{
				{Key: "x", Operator: "Bogus"},
			}}
		}, nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt := &aiv1alpha1.RoundTable{
				ObjectMeta: metav1.ObjectMeta{Name: "table", Namespace: "default", Generation: 2},
				Status:     aiv1alpha1.RoundTableStatus{ObservedGeneration: 1},
			}
			tc.mutate(rt)
			if got := roundTableKstatusOf(t, rt); got != status.InProgressStatus {
				t.Fatalf("unobserved spec: kstatus = %s, want InProgress", got)
			}
			r, _ := kstatusTestReconciler(t, append([]client.Object{rt.DeepCopy()}, tc.knights...)...)
			_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "table"}})
			if tc.wantPhase == "" {
				if err == nil {
					t.Fatal("expected the reconcile to fail")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			got := &aiv1alpha1.RoundTable{}
			if err := r.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "table"}, got); err != nil {
				t.Fatal(err)
			}
			if tc.wantPhase != anyPhase && got.Status.Phase != tc.wantPhase {
				t.Errorf("phase = %q, want %q", got.Status.Phase, tc.wantPhase)
			}
			if got.Status.ObservedGeneration != got.Generation {
				t.Errorf("observedGeneration = %d, want %d", got.Status.ObservedGeneration, got.Generation)
			}
			if s := roundTableKstatusOf(t, got); s != status.CurrentStatus {
				t.Errorf("after reconcile (phase %q): kstatus = %s, want Current", got.Status.Phase, s)
			}
		})
	}
}
