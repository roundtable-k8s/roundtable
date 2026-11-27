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

package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestKnightsGaugeNameAndLabels(t *testing.T) {
	Knights.Reset()
	Knights.WithLabelValues("t1", "Ready").Set(2)
	const want = `
# HELP roundtable_knights Number of knights by table and phase
# TYPE roundtable_knights gauge
roundtable_knights{phase="Ready",table="t1"} 2
`
	if err := testutil.CollectAndCompare(Knights, strings.NewReader(want)); err != nil {
		t.Error(err)
	}
}

func TestMissionsGaugeNameAndLabels(t *testing.T) {
	Missions.Reset()
	Missions.WithLabelValues("t1", "Active").Set(3)
	const want = `
# HELP roundtable_missions Number of missions by table and phase
# TYPE roundtable_missions gauge
roundtable_missions{phase="Active",table="t1"} 3
`
	if err := testutil.CollectAndCompare(Missions, strings.NewReader(want)); err != nil {
		t.Error(err)
	}
}

func TestTableCost(t *testing.T) {
	TableCostUSD.Reset()
	TableCostUSD.WithLabelValues("t1").Set(1.5)
	const wantCost = `
# HELP roundtable_table_cost_usd Total cost in USD by table
# TYPE roundtable_table_cost_usd gauge
roundtable_table_cost_usd{table="t1"} 1.5
`
	if err := testutil.CollectAndCompare(TableCostUSD, strings.NewReader(wantCost)); err != nil {
		t.Error(err)
	}
}

func TestChainCounters(t *testing.T) {
	ChainRunsTotal.Reset()
	ChainNoOpRunsTotal.Reset()
	ChainRunsTotal.WithLabelValues("t1", "c1", "succeeded").Inc()
	ChainNoOpRunsTotal.WithLabelValues("t1", "c1").Inc()
	const wantRuns = `
# HELP roundtable_chain_runs_total Total chain runs by terminal status
# TYPE roundtable_chain_runs_total counter
roundtable_chain_runs_total{chain="c1",status="succeeded",table="t1"} 1
`
	const wantNoop = `
# HELP roundtable_chain_noop_runs_total Total chain runs that completed without executing any steps
# TYPE roundtable_chain_noop_runs_total counter
roundtable_chain_noop_runs_total{chain="c1",table="t1"} 1
`
	if err := testutil.CollectAndCompare(ChainRunsTotal, strings.NewReader(wantRuns)); err != nil {
		t.Error(err)
	}
	if err := testutil.CollectAndCompare(ChainNoOpRunsTotal, strings.NewReader(wantNoop)); err != nil {
		t.Error(err)
	}
}

func TestChainStepDurationLabels(t *testing.T) {
	ChainStepDurationSeconds.Reset()
	ChainStepDurationSeconds.WithLabelValues("t1", "c1", "s1", "succeeded").Observe(3)
	if n := testutil.CollectAndCount(ChainStepDurationSeconds, "roundtable_chain_step_duration_seconds"); n != 1 {
		t.Errorf("expected 1 series, got %d", n)
	}
}

func TestTriggerFiringsNameAndLabels(t *testing.T) {
	TriggerFiringsTotal.Reset()
	TriggerFiringsTotal.WithLabelValues("t1", "nightly", "schedule", "started").Inc()
	const want = `
# HELP roundtable_trigger_firings_total Trigger events by table, trigger, source and outcome
# TYPE roundtable_trigger_firings_total counter
roundtable_trigger_firings_total{outcome="started",source="schedule",table="t1",trigger="nightly"} 1
`
	if err := testutil.CollectAndCompare(TriggerFiringsTotal, strings.NewReader(want)); err != nil {
		t.Error(err)
	}
}

func TestTriggerLastFiredNameAndLabels(t *testing.T) {
	TriggerLastFiredTimestampSeconds.Reset()
	TriggerLastFiredTimestampSeconds.WithLabelValues("ns1", "t1", "nightly").Set(1700000000)
	const want = `
# HELP roundtable_trigger_last_fired_timestamp_seconds Unix time of the Trigger's last started run, by namespace, table and trigger
# TYPE roundtable_trigger_last_fired_timestamp_seconds gauge
roundtable_trigger_last_fired_timestamp_seconds{namespace="ns1",table="t1",trigger="nightly"} 1.7e+09
`
	if err := testutil.CollectAndCompare(TriggerLastFiredTimestampSeconds, strings.NewReader(want)); err != nil {
		t.Error(err)
	}
}

func TestGatewayRequestsNameAndLabels(t *testing.T) {
	GatewayRequestsTotal.Reset()
	GatewayRequestsTotal.WithLabelValues("GET /v1/knights", "200", "none", "serviceaccount").Inc()
	const want = `
# HELP roundtable_gateway_requests_total Invoke gateway HTTP requests by route, status code, table and caller kind
# TYPE roundtable_gateway_requests_total counter
roundtable_gateway_requests_total{caller_kind="serviceaccount",code="200",route="GET /v1/knights",table="none"} 1
`
	if err := testutil.CollectAndCompare(GatewayRequestsTotal, strings.NewReader(want)); err != nil {
		t.Error(err)
	}
}
