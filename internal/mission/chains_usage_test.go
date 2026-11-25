package mission

import (
	"testing"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func TestSumChainUsage(t *testing.T) {
	m := &aiv1alpha1.Mission{}
	m.Status.ChainStatuses = []aiv1alpha1.MissionChainStatus{
		{Name: "a", CostUSD: "0.500000", Tokens: &aiv1alpha1.TokenTotals{Input: 10, Output: 2}},
		{Name: "b", CostUSD: "1.250000", Tokens: &aiv1alpha1.TokenTotals{Input: 5, Output: 1}},
		{Name: "c"},
	}
	cost, tokens := SumChainUsage(m)
	if cost != 1.75 || tokens == nil || tokens.Input != 15 || tokens.Output != 3 {
		t.Fatalf("cost=%v tokens=%+v", cost, tokens)
	}
	cm := &ChainManager{}
	chain := &aiv1alpha1.Chain{Status: aiv1alpha1.ChainStatus{CostUSD: "2.000000", Tokens: &aiv1alpha1.TokenTotals{Input: 1}}}
	cm.UpdateChainUsage(m, "c", chain)
	if m.Status.ChainStatuses[2].CostUSD != "2.000000" || m.Status.ChainStatuses[2].Tokens.Input != 1 {
		t.Fatalf("%+v", m.Status.ChainStatuses[2])
	}
	if c, _ := SumChainUsage(&aiv1alpha1.Mission{}); c != 0 {
		t.Fatal("empty mission cost")
	}
}
