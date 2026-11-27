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
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/pkg/metrics"
)

func stepDurationSamples(t *testing.T, table, chain, step, status string) (count uint64, sum float64) {
	t.Helper()
	obs, err := metrics.ChainStepDurationSeconds.GetMetricWithLabelValues(table, chain, step, status)
	if err != nil {
		t.Fatal(err)
	}
	var m dto.Metric
	if err := obs.(prometheus.Metric).Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
}

func runCounter(table, chain, status string) float64 {
	return testutil.ToFloat64(metrics.ChainRunsTotal.WithLabelValues(table, chain, status))
}

// A terminal run increments roundtable_chain_runs_total once, under the
// status of its outcome, and observes roundtable_chain_step_duration_seconds
// once per step that finished during the reconcile (#441).
func TestChainRunMetricsOnTerminalRun(t *testing.T) {
	ctx := context.Background()
	start := metav1.NewTime(time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC))
	done := func(d time.Duration) *metav1.Time { c := metav1.NewTime(start.Add(d)); return &c }

	cases := []struct {
		name       string
		steps      []aiv1alpha1.ChainStepStatus
		continueOn map[string]bool
		wantStatus string
		wantPhase  aiv1alpha1.ChainPhase
	}{
		{
			name: "succeeded",
			steps: []aiv1alpha1.ChainStepStatus{
				{Name: "plan", Phase: aiv1alpha1.ChainStepPhaseSucceeded, StartedAt: &start, CompletedAt: done(3 * time.Second)},
				{Name: "impl", Phase: aiv1alpha1.ChainStepPhaseSucceeded, StartedAt: &start, CompletedAt: done(10 * time.Second)},
			},
			wantStatus: "succeeded", wantPhase: aiv1alpha1.ChainPhaseSucceeded,
		},
		{
			name: "failed",
			steps: []aiv1alpha1.ChainStepStatus{
				{Name: "plan", Phase: aiv1alpha1.ChainStepPhaseSucceeded, StartedAt: &start, CompletedAt: done(3 * time.Second)},
				{Name: "impl", Phase: aiv1alpha1.ChainStepPhaseFailed, StartedAt: &start, CompletedAt: done(10 * time.Second)},
			},
			wantStatus: "failed", wantPhase: aiv1alpha1.ChainPhaseFailed,
		},
		{
			name: "partially_succeeded",
			steps: []aiv1alpha1.ChainStepStatus{
				{Name: "plan", Phase: aiv1alpha1.ChainStepPhaseSucceeded, StartedAt: &start, CompletedAt: done(3 * time.Second)},
				{Name: "impl", Phase: aiv1alpha1.ChainStepPhaseFailed, StartedAt: &start, CompletedAt: done(10 * time.Second)},
			},
			continueOn: map[string]bool{"impl": true},
			wantStatus: "partially_succeeded", wantPhase: aiv1alpha1.ChainPhasePartiallySucceeded,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			table, name := "tbl-metrics-"+tc.name, "chain-metrics-"+tc.name
			chain := &aiv1alpha1.Chain{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
				Spec:       aiv1alpha1.ChainSpec{RoundTableRef: aiv1alpha1.LocalRef(table)},
			}
			chain.Status.StepStatuses = tc.steps
			r := &ChainReconciler{Recorder: record.NewFakeRecorder(10)}

			// Steps start the reconcile running and finish during it.
			prev := map[string]aiv1alpha1.ChainStepPhase{}
			for _, ss := range tc.steps {
				prev[ss.Name] = aiv1alpha1.ChainStepPhaseRunning
			}
			specMap := map[string]*aiv1alpha1.ChainStep{}
			for n, c := range tc.continueOn {
				specMap[n] = &aiv1alpha1.ChainStep{Name: n, ContinueOnFailure: c}
			}

			before := runCounter(table, name, tc.wantStatus)
			r.observeFinishedSteps(ctx, chain, prev)
			r.completeRun(ctx, chain, specMap)

			if chain.Status.Phase != tc.wantPhase {
				t.Fatalf("phase = %q, want %q", chain.Status.Phase, tc.wantPhase)
			}
			if got := runCounter(table, name, tc.wantStatus) - before; got != 1 {
				t.Errorf("roundtable_chain_runs_total{status=%q} grew by %v, want 1", tc.wantStatus, got)
			}
			for _, other := range []string{"succeeded", "failed", "partially_succeeded"} {
				if other != tc.wantStatus && runCounter(table, name, other) != 0 {
					t.Errorf("status %q counter was incremented for a %s run", other, tc.name)
				}
			}
			for _, ss := range tc.steps {
				count, sum := stepDurationSamples(t, table, name, ss.Name, strings.ToLower(string(ss.Phase)))
				if count != 1 {
					t.Errorf("step %s duration observed %d times, want 1", ss.Name, count)
				}
				if want := ss.CompletedAt.Sub(ss.StartedAt.Time).Seconds(); sum != want {
					t.Errorf("step %s duration sum = %v, want %v", ss.Name, sum, want)
				}
			}
		})
	}
}

// A step that was already terminal before the reconcile, or has no start and
// completion time, adds no duration sample.
func TestObserveFinishedStepsSkipsRepeatsAndUntimedSteps(t *testing.T) {
	start := metav1.NewTime(time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC))
	end := metav1.NewTime(start.Add(5 * time.Second))
	chain := &aiv1alpha1.Chain{
		ObjectMeta: metav1.ObjectMeta{Name: "chain-metrics-skip", Namespace: "default"},
		Spec:       aiv1alpha1.ChainSpec{RoundTableRef: aiv1alpha1.LocalRef("tbl-metrics-skip")},
	}
	chain.Status.StepStatuses = []aiv1alpha1.ChainStepStatus{
		{Name: "old", Phase: aiv1alpha1.ChainStepPhaseSucceeded, StartedAt: &start, CompletedAt: &end},
		{Name: "untimed", Phase: aiv1alpha1.ChainStepPhaseSkipped},
	}
	r := &ChainReconciler{Recorder: record.NewFakeRecorder(10)}
	r.observeFinishedSteps(context.Background(), chain, map[string]aiv1alpha1.ChainStepPhase{
		"old":     aiv1alpha1.ChainStepPhaseSucceeded,
		"untimed": aiv1alpha1.ChainStepPhaseRunning,
	})
	if c, _ := stepDurationSamples(t, "tbl-metrics-skip", "chain-metrics-skip", "old", "succeeded"); c != 0 {
		t.Errorf("already-terminal step observed %d times", c)
	}
	if c, _ := stepDurationSamples(t, "tbl-metrics-skip", "chain-metrics-skip", "untimed", "skipped"); c != 0 {
		t.Errorf("untimed step observed %d times", c)
	}
}
