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
	"math"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/pkg/metrics"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

// A two-step run where the first step fails once and is retried: status sums
// the attempts, the counters grow once per finished attempt, and a run copy
// reports under its template's name.
func TestChainUsageTwoStepRunWithRetry(t *testing.T) {
	// The series count below is global; reset so series written by other
	// tests (e.g. the Ginkgo suite) under -shuffle do not leak in.
	metrics.ChainStepCostUSDTotal.Reset()
	r := &ChainReconciler{Recorder: record.NewFakeRecorder(50)}
	chain := &aiv1alpha1.Chain{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "pr-pipeline-issue-7",
			Annotations: map[string]string{aiv1alpha1.AnnotationRunTemplate: "tmpl-usage"},
			Labels:      map[string]string{aiv1alpha1.LabelMission: "m-usage"},
		},
		Spec: aiv1alpha1.ChainSpec{
			RoundTableRef: aiv1alpha1.LocalRef("tbl-usage"),
			RetryPolicy:   &aiv1alpha1.ChainRetryPolicy{MaxRetries: 1},
		},
	}
	chain.Status.StepStatuses = []aiv1alpha1.ChainStepStatus{
		{Name: "plan", Knight: "kay"}, {Name: "impl", Knight: "bors"},
	}
	plan, impl := &chain.Status.StepStatuses[0], &chain.Status.StepStatuses[1]
	ctx := context.Background()
	nc := natsConfig{}

	r.applyStepResult(ctx, nc, chain, nil, plan, &natspkg.TaskResult{
		Error: "boom", Cost: 0.5, Billing: "api", Tokens: &natspkg.TokenUsage{Input: 100, Output: 10}})
	if plan.Phase != aiv1alpha1.ChainStepPhasePending || plan.Retries != 1 {
		t.Fatalf("plan should be retrying: %+v", plan)
	}
	r.applyStepResult(ctx, nc, chain, nil, plan, &natspkg.TaskResult{
		Output: "ok", Cost: 0.25, Billing: "api", Tokens: &natspkg.TokenUsage{Input: 50, Output: 5}})
	r.applyStepResult(ctx, nc, chain, nil, impl, &natspkg.TaskResult{
		Output: "done", Cost: 1, Billing: "subscription", Tokens: &natspkg.TokenUsage{Input: 1000, Output: 200}})
	// A result without usage changes nothing.
	r.recordUsage(ctx, chain, impl, "bors", &natspkg.TaskResult{Output: "x"})
	refreshChainUsage(chain)

	if plan.CostUSD != "0.750000" || plan.Tokens == nil || plan.Tokens.Input != 150 || plan.Tokens.Output != 15 {
		t.Errorf("plan totals: %q %+v", plan.CostUSD, plan.Tokens)
	}
	if chain.Status.CostUSD != "1.750000" || chain.Status.Tokens == nil ||
		chain.Status.Tokens.Input != 1150 || chain.Status.Tokens.Output != 215 {
		t.Errorf("chain totals: %q %+v", chain.Status.CostUSD, chain.Status.Tokens)
	}

	// Metrics carry the template's name and the table, never the run copy's.
	if got := testutil.ToFloat64(metrics.ChainStepCostUSDTotal.WithLabelValues("tbl-usage", "tmpl-usage", "plan", "kay", "api")); !near(got, 0.75) {
		t.Errorf("plan cost counter = %v", got)
	}
	if got := testutil.ToFloat64(metrics.ChainStepCostUSDTotal.WithLabelValues("tbl-usage", "tmpl-usage", "impl", "bors", "subscription")); !near(got, 1) {
		t.Errorf("impl cost counter = %v", got)
	}
	if got := testutil.ToFloat64(metrics.ChainStepTokensTotal.WithLabelValues("tbl-usage", "tmpl-usage", "plan", "kay", "input")); !near(got, 150) {
		t.Errorf("plan input tokens counter = %v", got)
	}
	if got := testutil.ToFloat64(metrics.ChainStepTokensTotal.WithLabelValues("tbl-usage", "tmpl-usage", "impl", "bors", "output")); !near(got, 200) {
		t.Errorf("impl output tokens counter = %v", got)
	}
	if got := testutil.ToFloat64(metrics.MissionCostUSDTotal.WithLabelValues("tbl-usage", "m-usage", "api")); !near(got, 0.75) {
		t.Errorf("mission api cost counter = %v", got)
	}
	if got := testutil.ToFloat64(metrics.MissionCostUSDTotal.WithLabelValues("tbl-usage", "m-usage", "subscription")); !near(got, 1) {
		t.Errorf("mission subscription cost counter = %v", got)
	}
	if n := testutil.CollectAndCount(metrics.ChainStepCostUSDTotal, "roundtable_chain_step_cost_usd_total"); n != 2 {
		t.Errorf("expected 2 series, got %d", n)
	}
	if got := testutil.ToFloat64(metrics.ChainStepCostUSDTotal.WithLabelValues("tbl-usage", "pr-pipeline-issue-7", "plan", "kay", "api")); got != 0 {
		t.Errorf("run copy name leaked into the chain label: %v", got)
	}
}

// Cached input in a result is summed into step and chain totals and counted
// per type; a result without cache fields leaves the cache totals unchanged.
func TestChainUsageCachedTokens(t *testing.T) {
	r := &ChainReconciler{Recorder: record.NewFakeRecorder(10)}
	chain := &aiv1alpha1.Chain{
		ObjectMeta: metav1.ObjectMeta{Name: "cache-usage"},
		Spec:       aiv1alpha1.ChainSpec{RoundTableRef: aiv1alpha1.LocalRef("tbl-cache")},
	}
	chain.Status.StepStatuses = []aiv1alpha1.ChainStepStatus{{Name: "a"}, {Name: "b"}}
	a, b := &chain.Status.StepStatuses[0], &chain.Status.StepStatuses[1]
	ctx := context.Background()

	r.recordUsage(ctx, chain, a, "kay", &natspkg.TaskResult{Output: "ok",
		Tokens: &natspkg.TokenUsage{Input: 6, Output: 100, CacheRead: 40000, CacheWrite: 2000}})
	// Cache tokens alone are usage.
	r.recordUsage(ctx, chain, a, "kay", &natspkg.TaskResult{Output: "ok",
		Tokens: &natspkg.TokenUsage{CacheRead: 1000}})
	// An older knight without cache fields.
	r.recordUsage(ctx, chain, b, "bors", &natspkg.TaskResult{Output: "ok",
		Tokens: &natspkg.TokenUsage{Input: 50, Output: 5}})
	refreshChainUsage(chain)

	if want := (aiv1alpha1.TokenTotals{Input: 6, Output: 100, CacheRead: 41000, CacheWrite: 2000}); a.Tokens == nil || *a.Tokens != want {
		t.Errorf("step a tokens = %+v, want %+v", a.Tokens, want)
	}
	if want := (aiv1alpha1.TokenTotals{Input: 50, Output: 5}); b.Tokens == nil || *b.Tokens != want {
		t.Errorf("step b tokens = %+v, want %+v", b.Tokens, want)
	}
	if want := (aiv1alpha1.TokenTotals{Input: 56, Output: 105, CacheRead: 41000, CacheWrite: 2000}); chain.Status.Tokens == nil || *chain.Status.Tokens != want {
		t.Errorf("chain tokens = %+v, want %+v", chain.Status.Tokens, want)
	}
	if got := testutil.ToFloat64(metrics.ChainStepTokensTotal.WithLabelValues("tbl-cache", "cache-usage", "a", "kay", "cacheRead")); !near(got, 41000) {
		t.Errorf("cacheRead counter = %v", got)
	}
	if got := testutil.ToFloat64(metrics.ChainStepTokensTotal.WithLabelValues("tbl-cache", "cache-usage", "a", "kay", "cacheWrite")); !near(got, 2000) {
		t.Errorf("cacheWrite counter = %v", got)
	}
	if got := testutil.ToFloat64(metrics.ChainStepTokensTotal.WithLabelValues("tbl-cache", "cache-usage", "b", "bors", "cacheRead")); got != 0 {
		t.Errorf("older knight cacheRead counter = %v", got)
	}
}

// A knight reporting negative counts or cost cannot lower the totals: the
// negative fields add nothing, the positive ones still count.
func TestChainUsageIgnoresNegativeCounts(t *testing.T) {
	r := &ChainReconciler{Recorder: record.NewFakeRecorder(10)}
	chain := &aiv1alpha1.Chain{
		ObjectMeta: metav1.ObjectMeta{Name: "negative-usage"},
		Spec:       aiv1alpha1.ChainSpec{RoundTableRef: aiv1alpha1.LocalRef("tbl-negative")},
	}
	chain.Status.StepStatuses = []aiv1alpha1.ChainStepStatus{{Name: "a"}}
	a := &chain.Status.StepStatuses[0]
	ctx := context.Background()

	r.recordUsage(ctx, chain, a, "kay", &natspkg.TaskResult{Output: "ok", Cost: 0.25,
		Tokens: &natspkg.TokenUsage{Input: 100, Output: 10, CacheRead: 1000}})
	r.recordUsage(ctx, chain, a, "kay", &natspkg.TaskResult{Output: "ok", Cost: -5,
		Tokens: &natspkg.TokenUsage{Input: -40, Output: 7, CacheRead: -900, CacheWrite: -3}})
	// Only negative values: nothing to record at all.
	r.recordUsage(ctx, chain, a, "kay", &natspkg.TaskResult{Output: "ok", Cost: -1,
		Tokens: &natspkg.TokenUsage{Input: -1}})
	refreshChainUsage(chain)

	if want := (aiv1alpha1.TokenTotals{Input: 100, Output: 17, CacheRead: 1000}); a.Tokens == nil || *a.Tokens != want {
		t.Errorf("step tokens = %+v, want %+v", a.Tokens, want)
	}
	if a.CostUSD != "0.250000" {
		t.Errorf("step cost = %q, want 0.250000", a.CostUSD)
	}
	if chain.Status.Tokens == nil || *chain.Status.Tokens != *a.Tokens {
		t.Errorf("chain tokens = %+v, want %+v", chain.Status.Tokens, a.Tokens)
	}
}

// A step's cost is split by the knight that ran each attempt: a fallback or a
// loop body on another knight does not take over the primary's cost.
func TestRecordUsageKnightCosts(t *testing.T) {
	r := &ChainReconciler{Recorder: record.NewFakeRecorder(10)}
	chain := &aiv1alpha1.Chain{
		ObjectMeta: metav1.ObjectMeta{Name: "knight-costs"},
		Spec:       aiv1alpha1.ChainSpec{RoundTableRef: aiv1alpha1.LocalRef("tbl-kc")},
	}
	ss := &aiv1alpha1.ChainStepStatus{Name: "s"}
	ctx := context.Background()
	r.recordUsage(ctx, chain, ss, "kay", &natspkg.TaskResult{Output: "x", Cost: 0.25})
	r.recordUsage(ctx, chain, ss, "bors", &natspkg.TaskResult{Output: "x", Cost: 0.5})
	r.recordUsage(ctx, chain, ss, "kay", &natspkg.TaskResult{Output: "x", Cost: 0.25})
	if ss.CostUSD != "1.000000" {
		t.Errorf("costUSD = %q", ss.CostUSD)
	}
	got := map[string]string{}
	for _, kc := range ss.KnightCosts {
		got[kc.Name] = kc.CostUSD
	}
	if len(got) != 2 || got["kay"] != "0.500000" || got["bors"] != "0.500000" {
		t.Errorf("knightCosts = %+v", ss.KnightCosts)
	}
}
