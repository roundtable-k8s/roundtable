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

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/pkg/metrics"
)

// pendingRunMetrics holds the run counters a reconcile decided on, so they
// are only counted once the terminal status that records the run has been
// persisted. A status-update conflict re-enters the terminal branch on the
// next reconcile; counting eagerly would count the run twice.
type pendingRunMetrics struct {
	status string // chain_runs_total status label; empty when none pending
	noop   bool   // chain_noop_runs_total
}

type pendingRunMetricsKey struct{}

func withPendingRunMetrics(ctx context.Context) (context.Context, *pendingRunMetrics) {
	p := &pendingRunMetrics{}
	return context.WithValue(ctx, pendingRunMetricsKey{}, p), p
}

// recordRunOutcome defers the run counter to the end of the reconcile when
// the context carries a collector, else counts it right away.
func (r *ChainReconciler) recordRunOutcome(ctx context.Context, chain *aiv1alpha1.Chain, status string) {
	if p, ok := ctx.Value(pendingRunMetricsKey{}).(*pendingRunMetrics); ok {
		p.status = status
		return
	}
	metrics.ChainRunsTotal.WithLabelValues(r.chainTable(ctx, chain), chain.Name, status).Inc()
}

// recordNoOpRun is recordRunOutcome for the no-op run counter.
func (r *ChainReconciler) recordNoOpRun(ctx context.Context, chain *aiv1alpha1.Chain) {
	if p, ok := ctx.Value(pendingRunMetricsKey{}).(*pendingRunMetrics); ok {
		p.noop = true
		return
	}
	metrics.ChainNoOpRunsTotal.WithLabelValues(r.chainTable(ctx, chain), chain.Name).Inc()
}

// flushIfPersisted counts what was recorded when the chain's status was
// written during this reconcile (its resourceVersion moved off baseRV).
func (p *pendingRunMetrics) flushIfPersisted(ctx context.Context, r *ChainReconciler, chain *aiv1alpha1.Chain, baseRV string) {
	if chain.ResourceVersion == baseRV || (p.status == "" && !p.noop) {
		return
	}
	table := r.chainTable(ctx, chain)
	if p.status != "" {
		metrics.ChainRunsTotal.WithLabelValues(table, chain.Name, p.status).Inc()
	}
	if p.noop {
		metrics.ChainNoOpRunsTotal.WithLabelValues(table, chain.Name).Inc()
	}
}
