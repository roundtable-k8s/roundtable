package mission

import (
	"context"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

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

func budgetMission(budget string, chainCosts ...string) *aiv1alpha1.Mission {
	m := &aiv1alpha1.Mission{
		ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "ns"},
		Spec: aiv1alpha1.MissionSpec{
			CostBudgetUSD: budget,
			Knights:       []aiv1alpha1.MissionKnight{{Name: "a"}},
		},
	}
	for _, c := range chainCosts {
		m.Status.ChainStatuses = append(m.Status.ChainStatuses, aiv1alpha1.MissionChainStatus{Name: "c", CostUSD: c})
	}
	return m
}

func TestCheckBudget(t *testing.T) {
	knight := &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "ns"},
		Status:     aiv1alpha1.KnightStatus{TotalCost: "1.5000"},
	}
	tests := []struct {
		name      string
		mission   *aiv1alpha1.Mission
		want      BudgetStatus
		wantTotal string
	}{
		{"no budget", budgetMission(""), BudgetStatus{}, ""},
		{"zero budget", budgetMission("0"), BudgetStatus{}, ""},
		{"under budget", budgetMission("2.0"), BudgetStatus{Total: 1.5, Budget: 2}, "1.5000"},
		{"over budget", budgetMission("1.0"), BudgetStatus{Total: 1.5, Budget: 1, Exceeded: true}, "1.5000"},
		{"chain cost higher", budgetMission("2.0", "2.5"), BudgetStatus{Total: 2.5, Budget: 2, Exceeded: true}, "2.5000"},
		{"unparseable budget", budgetMission("abc"), BudgetStatus{Total: 1.5}, "1.5000"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ct := &CostTracker{Client: fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(knight.DeepCopy()).Build()}
			got, err := ct.CheckBudget(t.Context(), tt.mission)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Errorf("status = %+v, want %+v", got, tt.want)
			}
			if tt.mission.Status.TotalCost != tt.wantTotal {
				t.Errorf("Status.TotalCost = %q, want %q", tt.mission.Status.TotalCost, tt.wantTotal)
			}
			if tt.wantTotal == "" && tt.mission.Status.CostBreakdown != nil {
				t.Errorf("mission touched without budget: %+v", tt.mission.Status.CostBreakdown)
			}
		})
	}
}

func TestCheckBudgetAggregateError(t *testing.T) {
	boom := errors.New("boom")
	cl := fake.NewClientBuilder().WithScheme(testScheme(t)).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return boom
		},
	}).Build()
	m := budgetMission("1.0")
	got, err := (&CostTracker{Client: cl}).CheckBudget(t.Context(), m)
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	if got != (BudgetStatus{}) || m.Status.TotalCost != "" {
		t.Errorf("got %+v, TotalCost %q; want zero values", got, m.Status.TotalCost)
	}
}

func TestApplyChainUsage(t *testing.T) {
	t.Run("no chains", func(t *testing.T) {
		m := &aiv1alpha1.Mission{Status: aiv1alpha1.MissionStatus{TotalCost: "1.0000"}}
		ApplyChainUsage(m)
		if m.Status.Tokens != nil || m.Status.TotalCost != "1.0000" {
			t.Errorf("unexpected status %+v", m.Status)
		}
	})
	t.Run("tokens only", func(t *testing.T) {
		m := &aiv1alpha1.Mission{Status: aiv1alpha1.MissionStatus{
			TotalCost:     "1.0000",
			ChainStatuses: []aiv1alpha1.MissionChainStatus{{Name: "c", Tokens: &aiv1alpha1.TokenTotals{}}},
		}}
		ApplyChainUsage(m)
		if m.Status.Tokens == nil {
			t.Error("tokens not set")
		}
		if m.Status.TotalCost != "1.0000" {
			t.Errorf("TotalCost = %q, want unchanged", m.Status.TotalCost)
		}
	})
	t.Run("cost raise only", func(t *testing.T) {
		m := &aiv1alpha1.Mission{Status: aiv1alpha1.MissionStatus{
			TotalCost:     "1.0000",
			ChainStatuses: []aiv1alpha1.MissionChainStatus{{Name: "c", CostUSD: "3"}},
		}}
		ApplyChainUsage(m)
		if m.Status.TotalCost != "3.0000" {
			t.Errorf("TotalCost = %q, want 3.0000", m.Status.TotalCost)
		}
		m.Status.ChainStatuses[0].CostUSD = "0.5"
		ApplyChainUsage(m)
		if m.Status.TotalCost != "3.0000" {
			t.Errorf("TotalCost lowered to %q", m.Status.TotalCost)
		}
	})
}
