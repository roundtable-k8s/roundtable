package controller

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	missionpkg "github.com/roundtable-k8s/roundtable/internal/mission"
)

// With a cost budget, reconcileActive reads each chain once for the cost
// aggregate (the budget check) and reuses it for the breakdown when the chain
// statuses did not change in between. The other reads are ReconcileChains'
// own (EnsureMissionChain and the monitor Get).
func TestReconcileActiveBudgetAggregatesChainsOnce(t *testing.T) {
	scheme := statusTestScheme()
	start := metav1.NewTime(time.Now())
	chain := &aiv1alpha1.Chain{
		ObjectMeta: metav1.ObjectMeta{Name: "mission-m-c", Namespace: "ns"},
		Status: aiv1alpha1.ChainStatus{
			Phase:   aiv1alpha1.ChainPhaseRunning,
			CostUSD: "0.5000",
			StepStatuses: []aiv1alpha1.ChainStepStatus{
				{Knight: "a", CostUSD: "0.5000"},
			},
		},
	}
	m := &aiv1alpha1.Mission{
		ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "ns"},
		Spec: aiv1alpha1.MissionSpec{
			CostBudgetUSD:  "10",
			TimeoutSeconds: 3600,
			Knights:        []aiv1alpha1.MissionKnight{{Name: "a"}},
			Chains:         []aiv1alpha1.MissionChainRef{{Name: "c", Phase: "Active"}},
		},
		Status: aiv1alpha1.MissionStatus{
			Phase:          aiv1alpha1.MissionPhaseActive,
			KnightStatuses: []aiv1alpha1.MissionKnightStatus{{Name: "a"}},
			StartedAt:      &start,
			ChainStatuses:  []aiv1alpha1.MissionChainStatus{{Name: "c", ChainCRName: "mission-m-c", Phase: aiv1alpha1.ChainPhaseRunning, CostUSD: "0.5000"}},
		},
	}

	gets := 0
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(chain, m).WithStatusSubresource(chain, m).
		WithInterceptorFuncs(interceptor.Funcs{Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*aiv1alpha1.Chain); ok {
				gets++
			}
			return cl.Get(ctx, key, obj, opts...)
		}}).Build()
	r := &MissionReconciler{
		Client:      c,
		Scheme:      scheme,
		Recorder:    record.NewFakeRecorder(10),
		CostTracker: &missionpkg.CostTracker{Client: c},
		Chains:      &missionpkg.ChainManager{Client: c, Scheme: scheme},
	}
	if _, err := r.reconcileActive(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	// 1 budget aggregate + 2 in ReconcileChains; a second aggregate would be 4.
	if gets != 3 {
		t.Errorf("chain Gets = %d, want 3 (one aggregate read)", gets)
	}
	if len(m.Status.CostBreakdown) != 1 || m.Status.CostBreakdown[0].CostUSD != "0.5000" {
		t.Errorf("breakdown = %+v", m.Status.CostBreakdown)
	}
}
