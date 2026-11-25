package mission

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func TestAggregateMissionCost(t *testing.T) {
	scheme := testScheme(t)
	a := &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "ns"},
		Status:     aiv1alpha1.KnightStatus{TotalCost: "1.2500"},
	}
	b := &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "ns"},
		Status:     aiv1alpha1.KnightStatus{TotalCost: "0.7500"},
	}
	ct := &CostTracker{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(a, b).Build()}
	m := &aiv1alpha1.Mission{
		ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "ns"},
		Spec: aiv1alpha1.MissionSpec{Knights: []aiv1alpha1.MissionKnight{
			{Name: "a"}, {Name: "missing", Ephemeral: true}, {Name: "b"},
		}},
	}
	total, err := ct.AggregateMissionCost(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2.0 {
		t.Errorf("total = %v, want 2.0", total)
	}
	bd := m.Status.CostBreakdown
	if len(bd) != 3 {
		t.Fatalf("breakdown len = %d, want 3", len(bd))
	}
	if bd[1].Name != "missing" || bd[1].CostUSD != "0.0000" || !bd[1].Ephemeral {
		t.Errorf("missing knight entry = %+v, want zero-cost ephemeral entry", bd[1])
	}
	if bd[0].CostUSD != "1.2500" || bd[2].CostUSD != "0.7500" {
		t.Errorf("unexpected breakdown %+v", bd)
	}
}

func TestSuspendMissionChains(t *testing.T) {
	scheme := testScheme(t)
	ch := &aiv1alpha1.Chain{ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "ns"}}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ch).Build()
	ct := &CostTracker{Client: cl}
	m := &aiv1alpha1.Mission{
		ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "ns"},
		Status: aiv1alpha1.MissionStatus{ChainStatuses: []aiv1alpha1.MissionChainStatus{
			{Name: "x", ChainCRName: "c1"}, {Name: "y"}, {Name: "z", ChainCRName: "nope"},
		}},
	}
	if err := ct.SuspendMissionChains(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	got := &aiv1alpha1.Chain{}
	if err := cl.Get(t.Context(), clientKey("ns", "c1"), got); err != nil {
		t.Fatal(err)
	}
	if !got.Spec.Suspend {
		t.Error("chain c1 should be suspended")
	}
}
