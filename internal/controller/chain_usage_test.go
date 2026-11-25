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
