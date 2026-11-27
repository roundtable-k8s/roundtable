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
	"sigs.k8s.io/cli-utils/pkg/kstatus/status"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func kstatusOf(t *testing.T, chain *aiv1alpha1.Chain) status.Status {
	t.Helper()
	m, err := k8sruntime.DefaultUnstructuredConverter.ToUnstructured(chain)
	if err != nil {
		t.Fatal(err)
	}
	u := &unstructured.Unstructured{Object: m}
	u.SetAPIVersion(aiv1alpha1.GroupVersion.String())
	u.SetKind("Chain")
	res, err := status.Compute(u)
	if err != nil {
		t.Fatal(err)
	}
	return res.Status
}

// TestChainKstatus checks what Helm's --wait and Flux health checks see:
// a Chain that is running, idle, finished or suspended is Current, even when
// its spec was just changed (a chart upgrade bumps metadata.generation).
func TestChainKstatus(t *testing.T) {
	steps := func() []aiv1alpha1.ChainStep {
		return []aiv1alpha1.ChainStep{{Name: "a", Task: "t"}, {Name: "b", Task: "t", DependsOn: []string{"a"}}}
	}
	cases := []struct {
		name  string
		build func() *aiv1alpha1.Chain
	}{
		{"idle", func() *aiv1alpha1.Chain {
			c := phasesChain(steps(), pending("a"), pending("b"))
			c.Status.Phase = aiv1alpha1.ChainPhaseIdle
			c.Status.RunID = ""
			return c
		}},
		{"running", func() *aiv1alpha1.Chain {
			return phasesChain(steps(), aiv1alpha1.ChainStepStatus{Name: "a", Phase: aiv1alpha1.ChainStepPhaseRunning, TaskID: "t1"}, pending("b"))
		}},
		{"succeeded", func() *aiv1alpha1.Chain {
			c := phasesChain(steps(),
				aiv1alpha1.ChainStepStatus{Name: "a", Phase: aiv1alpha1.ChainStepPhaseSucceeded},
				aiv1alpha1.ChainStepStatus{Name: "b", Phase: aiv1alpha1.ChainStepPhaseSucceeded})
			c.Status.Phase = aiv1alpha1.ChainPhaseSucceeded
			return c
		}},
		{"failed", func() *aiv1alpha1.Chain {
			c := phasesChain(steps(),
				aiv1alpha1.ChainStepStatus{Name: "a", Phase: aiv1alpha1.ChainStepPhaseFailed, Error: "boom"}, pending("b"))
			c.Status.Phase = aiv1alpha1.ChainPhaseFailed
			return c
		}},
		{"suspended", func() *aiv1alpha1.Chain {
			c := phasesChain(steps(), pending("a"), pending("b"))
			c.Spec.Suspend = true
			c.Status.Phase = aiv1alpha1.ChainPhaseSuspended
			return c
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Steady state: spec observed.
			chain := tc.build()
			chain.Generation = 1
			chain.Status.ObservedGeneration = 1
			if got := kstatusOf(t, chain); got != status.CurrentStatus {
				t.Errorf("steady state: kstatus = %s, want Current", got)
			}

			// Spec just changed: after one reconcile the generation is observed.
			chain = tc.build()
			chain.Generation = 2
			chain.Status.ObservedGeneration = 1
			chain.Finalizers = []string{chainFinalizer}
			chain.Spec.RoundTableRef = aiv1alpha1.LocalRef("table")
			if got := kstatusOf(t, chain); got != status.InProgressStatus {
				t.Fatalf("unobserved spec: kstatus = %s, want InProgress", got)
			}
			r, _ := newPhasesReconciler(t)
			r.Client = fake.NewClientBuilder().WithScheme(r.Scheme).WithObjects(chain.DeepCopy(), &aiv1alpha1.RoundTable{
				ObjectMeta: metav1.ObjectMeta{Name: "table", Namespace: phasesNS},
			}, &aiv1alpha1.Knight{
				ObjectMeta: metav1.ObjectMeta{Name: phasesKnight, Namespace: phasesNS},
				Spec:       aiv1alpha1.KnightSpec{Domain: "ops", Model: "claude-sonnet-4"},
			}).WithStatusSubresource(&aiv1alpha1.Chain{}).Build()
			got := reconcileChain(t, r, chain)
			if got.Status.ObservedGeneration != got.Generation {
				t.Errorf("observedGeneration = %d, want %d", got.Status.ObservedGeneration, got.Generation)
			}
			if s := kstatusOf(t, got); s != status.CurrentStatus {
				t.Errorf("after reconcile (phase %s): kstatus = %s, want Current", got.Status.Phase, s)
			}
		})
	}
}

// TestChainKstatusRunningChainWithUnresolvedRoundTable covers a chart upgrade
// that changes a running Chain while its RoundTable cannot be read (e.g. it is
// being replaced in the same release). The reconcile of the run fails and is
// retried with backoff, but the new spec was accepted, so the Chain must not
// stay InProgress for kstatus (Helm --wait, Flux) until the run can continue.
func TestChainKstatusRunningChainWithUnresolvedRoundTable(t *testing.T) {
	chain := phasesChain([]aiv1alpha1.ChainStep{{Name: "a", Task: "t"}},
		aiv1alpha1.ChainStepStatus{Name: "a", Phase: aiv1alpha1.ChainStepPhaseRunning, TaskID: "t1"})
	chain.Generation = 2
	chain.Status.ObservedGeneration = 1
	chain.Finalizers = []string{chainFinalizer}
	chain.Spec.RoundTableRef = aiv1alpha1.LocalRef("missing")

	r, _ := newPhasesReconciler(t)
	r.Client = fake.NewClientBuilder().WithScheme(r.Scheme).WithObjects(chain.DeepCopy(), &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{Name: phasesKnight, Namespace: phasesNS},
		Spec:       aiv1alpha1.KnightSpec{Domain: "ops", Model: "claude-sonnet-4"},
	}).WithStatusSubresource(&aiv1alpha1.Chain{}).Build()

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: chain.Namespace, Name: chain.Name}})
	if err == nil {
		t.Fatal("expected the unresolved RoundTable to fail the reconcile")
	}
	got := &aiv1alpha1.Chain{}
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: chain.Namespace, Name: chain.Name}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != aiv1alpha1.ChainPhaseRunning {
		t.Fatalf("phase = %s, want Running", got.Status.Phase)
	}
	if s := kstatusOf(t, got); s != status.CurrentStatus {
		t.Errorf("kstatus = %s (observedGeneration %d, generation %d), want Current", s, got.Status.ObservedGeneration, got.Generation)
	}
}
