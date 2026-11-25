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
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// Knights is the number of knights per table and phase, recomputed from
	// the Knight list on every RoundTable reconcile.
	Knights = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "roundtable_knights",
			Help: "Number of knights by table and phase",
		},
		[]string{"table", "phase"},
	)

	// KnightWakeSeconds tracks wake latency: the time from a scaled-to-zero
	// knight entering Waking (tasks queued) until it becomes Ready.
	// Labels: table, knight (knight name), namespace
	// Buckets: 1s, 2s, 4s, ... 512s (covers 1s to 5m and beyond)
	KnightWakeSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "roundtable_knight_wake_seconds",
			Help:    "Time from a scaled-to-zero knight entering Waking until it is Ready, in seconds",
			Buckets: prometheus.ExponentialBuckets(1, 2, 10),
		},
		[]string{"table", "knight", "namespace"},
	)

	// ChainRunsTotal counts chain runs that reached a terminal phase.
	// status: succeeded, partially_succeeded, failed
	ChainRunsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "roundtable_chain_runs_total",
			Help: "Total chain runs by terminal status",
		},
		[]string{"table", "chain", "status"},
	)

	// ChainNoOpRunsTotal tracks chain runs that completed without executing
	// a single step (all outputs restored from cache). A nonzero rate usually
	// means stale KV entries are masking a real problem.
	ChainNoOpRunsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "roundtable_chain_noop_runs_total",
			Help: "Total chain runs that completed without executing any steps",
		},
		[]string{"table", "chain"},
	)

	// ChainProviderRetriesTotal counts chain step retries after a transient
	// model provider failure (overloaded, 5xx), #481. reason: overloaded,
	// provider_5xx, network, unknown.
	ChainProviderRetriesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "roundtable_chain_provider_retries_total",
			Help: "Chain step retries after a transient model provider failure",
		},
		[]string{"table", "chain", "step", "reason"},
	)

	// ChainStepDurationSeconds observes how long a chain step took, when it
	// finishes. status: succeeded, failed, skipped
	ChainStepDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "roundtable_chain_step_duration_seconds",
			Help:    "Chain step duration in seconds",
			Buckets: prometheus.ExponentialBuckets(1, 2, 12), // 1s to ~34min
		},
		[]string{"table", "chain", "step", "status"},
	)

	// ChainStepCostUSDTotal is the cost in USD of finished step attempts. The
	// chain label is the template's name for run copies. billing separates
	// real spend from the notional cost of subscription-billed models.
	ChainStepCostUSDTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "roundtable_chain_step_cost_usd_total",
			Help: "Cost in USD of finished chain step attempts",
		},
		[]string{"table", "chain", "step", "knight", "billing"},
	)

	// ChainStepTokensTotal counts tokens of finished step attempts.
	// type: input, output, cacheRead, cacheWrite
	ChainStepTokensTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "roundtable_chain_step_tokens_total",
			Help: "Tokens used by finished chain step attempts",
		},
		[]string{"table", "chain", "step", "knight", "type"},
	)

	// MissionCostUSDTotal is the cost in USD of a mission's finished step attempts.
	MissionCostUSDTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "roundtable_mission_cost_usd_total",
			Help: "Cost in USD of the finished step attempts of a mission's chains",
		},
		[]string{"table", "mission", "billing"},
	)

	// Missions is the number of missions per table and phase, recomputed from
	// the Mission list on every RoundTable reconcile.
	Missions = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "roundtable_missions",
			Help: "Number of missions by table and phase",
		},
		[]string{"table", "phase"},
	)

	// TableCostUSD is the cumulative cost in USD of a table.
	TableCostUSD = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "roundtable_table_cost_usd",
			Help: "Total cost in USD by table",
		},
		[]string{"table"},
	)
)

// TasksDeadLetteredTotal counts tasks written to a table's dead-letter stream.
// Labels: table, knight, reason (lost, failed, timeout, usage_limited, refused).
var TasksDeadLetteredTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "roundtable_tasks_dead_lettered_total",
		Help: "Tasks written to the dead-letter stream, by table, knight and reason",
	},
	[]string{"table", "knight", "reason"},
)

// DeadLettersOpen is the number of dead-letter entries still in state open
// (neither resolved by a later success nor retried). Labels: table, knight.
var DeadLettersOpen = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "roundtable_dead_letters_open",
		Help: "Dead-letter entries in state open, by table and knight",
	},
	[]string{"table", "knight"},
)

// KnightIsolated is 1 for each knight's current Isolated condition reason
// (Isolated, OptedOut, RuntimeClassNotFound, FQDNPolicyUnavailable,
// EgressPolicyFailed, IsolationDisabled), so a dashboard or alert can list
// every knight that is not isolated. Labels: table, knight, reason.
var KnightIsolated = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "roundtable_knight_isolated",
		Help: "1 for the knight's current Isolated condition reason (Isolated when isolated), by table, knight and reason",
	},
	[]string{"table", "knight", "reason"},
)

// KnightConsumerReady is 1 when the knight's durable task consumer exists
// with the expected config (ConsumerReady=True) and 0 otherwise, so an alert
// can catch a knight KEDA cannot wake. Labels: table, knight.
var KnightConsumerReady = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "roundtable_knight_consumer_ready",
		Help: "1 when the knight's durable task consumer exists with the expected config, 0 otherwise, by table and knight",
	},
	[]string{"table", "knight"},
)

// TriggerFiringsTotal counts Trigger events by what became of them. Labels:
// table (the target chain's roundTableRef, none if unset), trigger, source
// (schedule, nats, webhook, github), outcome (started, deduped, skipped,
// error).
var TriggerFiringsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "roundtable_trigger_firings_total",
		Help: "Trigger events by table, trigger, source and outcome",
	},
	[]string{"table", "trigger", "source", "outcome"},
)

// TriggerLastFiredTimestampSeconds is the Unix time a Trigger last started a
// run. Labels: table, trigger.
var TriggerLastFiredTimestampSeconds = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "roundtable_trigger_last_fired_timestamp_seconds",
		Help: "Unix time of the Trigger's last started run, by table and trigger",
	},
	[]string{"table", "trigger"},
)

// GatewayRequestsTotal counts invoke gateway HTTP requests. route is the
// registered route pattern (never the raw path), code the HTTP status, table
// the request's RoundTable (none when not known).
var GatewayRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "roundtable_gateway_requests_total",
		Help: "Invoke gateway HTTP requests by route template, status code and table",
	},
	[]string{"route", "code", "table"},
)

// GatewayRequestDurationSeconds observes invoke gateway request duration.
// Requests can wait for a knight, so the buckets reach ~17min.
var GatewayRequestDurationSeconds = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "roundtable_gateway_request_duration_seconds",
		Help:    "Invoke gateway HTTP request duration in seconds by route template",
		Buckets: prometheus.ExponentialBuckets(0.01, 4, 10),
	},
	[]string{"route"},
)

func init() {
	// Register all custom metrics with the controller-runtime metrics registry
	metrics.Registry.MustRegister(
		Knights,
		KnightWakeSeconds,
		ChainRunsTotal,
		ChainNoOpRunsTotal,
		ChainProviderRetriesTotal,
		ChainStepDurationSeconds,
		ChainStepCostUSDTotal,
		ChainStepTokensTotal,
		MissionCostUSDTotal,
		Missions,
		TableCostUSD,
		KnightIsolated,
		KnightConsumerReady,
		TasksDeadLetteredTotal,
		DeadLettersOpen,
		TriggerFiringsTotal,
		TriggerLastFiredTimestampSeconds,
		GatewayRequestsTotal,
		GatewayRequestDurationSeconds,
	)
}
