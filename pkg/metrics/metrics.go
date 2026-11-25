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
	// Labels: knight (knight name), namespace
	// Buckets: 1s, 2s, 4s, ... 512s (covers 1s to 5m and beyond)
	KnightWakeSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "roundtable_knight_wake_seconds",
			Help:    "Time from a scaled-to-zero knight entering Waking until it is Ready, in seconds",
			Buckets: prometheus.ExponentialBuckets(1, 2, 10),
		},
		[]string{"knight", "namespace"},
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
	// model provider failure (overloaded, 5xx), #481.
	ChainProviderRetriesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "roundtable_chain_provider_retries_total",
			Help: "Chain step retries after a transient model provider failure",
		},
		[]string{"chain", "step"},
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

	// WarmPoolKnights is the number of warm pool knights by state
	// (available, provisioning, claimed).
	WarmPoolKnights = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "roundtable_warm_pool_knights",
			Help: "Warm pool knights by table and state",
		},
		[]string{"table", "state"},
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

func init() {
	// Register all custom metrics with the controller-runtime metrics registry
	metrics.Registry.MustRegister(
		Knights,
		KnightWakeSeconds,
		ChainRunsTotal,
		ChainNoOpRunsTotal,
		ChainProviderRetriesTotal,
		ChainStepDurationSeconds,
		Missions,
		TableCostUSD,
		WarmPoolKnights,
		TasksDeadLetteredTotal,
		DeadLettersOpen,
	)
}
