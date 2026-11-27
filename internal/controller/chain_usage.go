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
	"fmt"
	"strconv"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/pkg/metrics"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// billingUnknown labels cost whose result did not say how it is billed. It
// counts as real spend (billing!="subscription").
const billingUnknown = "unknown"

// parseCostUSD reads a status cost string; empty or malformed is 0.
func parseCostUSD(s string) float64 {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return v
}

func formatCostUSD(v float64) string { return fmt.Sprintf("%.6f", v) }

// nonNegative clamps a reported token count at zero.
func nonNegative(n int64) int64 {
	if n < 0 {
		return 0
	}
	return n
}

// metricsChainName is the chain label of the usage metrics: the template's
// name for a run copy started by issue pickup, a Trigger or the gateway, so
// cardinality stays bounded; the chain's own name otherwise.
func metricsChainName(chain *aiv1alpha1.Chain) string {
	if t := chain.Annotations[aiv1alpha1.AnnotationRunTemplate]; t != "" {
		return t
	}
	return chain.Name
}

// chainMissionName is the mission a chain belongs to: spec.missionRef, else
// the mission label of mission-scoped chains.
func chainMissionName(chain *aiv1alpha1.Chain) string {
	if m := aiv1alpha1.RefName(chain.Spec.MissionRef); m != "" {
		return m
	}
	return chain.Labels[aiv1alpha1.LabelMission]
}

// recordUsage adds one finished task attempt's cost and tokens to the step's
// status totals and to the usage metrics. knight is the knight that ran it.
// Results without usage (older knights) change nothing.
func (r *ChainReconciler) recordUsage(ctx context.Context, chain *aiv1alpha1.Chain, ss *aiv1alpha1.ChainStepStatus, knight string, res *natspkg.TaskResult) {
	if ss == nil || res == nil {
		return
	}
	// A knight reporting a negative count or cost must not lower the totals:
	// each value is clamped at zero before it is added.
	var tok aiv1alpha1.TokenTotals
	if res.Tokens != nil {
		tok = aiv1alpha1.TokenTotals{
			Input: nonNegative(res.Tokens.Input), Output: nonNegative(res.Tokens.Output),
			CacheRead: nonNegative(res.Tokens.CacheRead), CacheWrite: nonNegative(res.Tokens.CacheWrite),
		}
	}
	cost := res.Cost
	if cost < 0 {
		cost = 0
	}
	if cost <= 0 && tok.IsZero() {
		return
	}
	if cost > 0 {
		ss.CostUSD = formatCostUSD(parseCostUSD(ss.CostUSD) + cost)
		addKnightCost(ss, knight, cost)
	}
	if !tok.IsZero() {
		ss.Tokens = ss.Tokens.Add(tok)
	}

	table := r.chainTable(ctx, chain)
	name := metricsChainName(chain)
	billing := res.Billing
	if billing == "" {
		billing = billingUnknown
	}
	if cost > 0 {
		metrics.ChainStepCostUSDTotal.WithLabelValues(table, name, ss.Name, knight, billing).Add(cost)
		if m := chainMissionName(chain); m != "" {
			metrics.MissionCostUSDTotal.WithLabelValues(table, m, billing).Add(cost)
		}
	}
	for _, c := range []struct {
		typ string
		n   int64
	}{
		{"input", tok.Input}, {"output", tok.Output},
		{"cacheRead", tok.CacheRead}, {"cacheWrite", tok.CacheWrite},
	} {
		if c.n > 0 {
			metrics.ChainStepTokensTotal.WithLabelValues(table, name, ss.Name, knight, c.typ).Add(float64(c.n))
		}
	}
}

// refreshChainUsage sets the chain's run totals to the sum of its steps'.
func refreshChainUsage(chain *aiv1alpha1.Chain) {
	var cost float64
	var tokens *aiv1alpha1.TokenTotals
	for _, ss := range chain.Status.StepStatuses {
		cost += parseCostUSD(ss.CostUSD)
		if ss.Tokens != nil {
			tokens = tokens.Add(*ss.Tokens)
		}
	}
	chain.Status.CostUSD = ""
	if cost > 0 {
		chain.Status.CostUSD = formatCostUSD(cost)
	}
	chain.Status.Tokens = tokens
}

// addKnightCost adds cost to the step's per-knight split.
func addKnightCost(ss *aiv1alpha1.ChainStepStatus, knight string, cost float64) {
	for i := range ss.KnightCosts {
		if ss.KnightCosts[i].Name == knight {
			ss.KnightCosts[i].CostUSD = formatCostUSD(parseCostUSD(ss.KnightCosts[i].CostUSD) + cost)
			return
		}
	}
	ss.KnightCosts = append(ss.KnightCosts, aiv1alpha1.StepKnightCost{Name: knight, CostUSD: formatCostUSD(cost)})
}
