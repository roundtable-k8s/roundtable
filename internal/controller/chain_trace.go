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

// One trace per Chain run (docs/design/telemetry-conventions.md, decision 14).
//
// A run outlives any single reconcile and possibly the operator process, so
// no span object is kept in memory. Instead:
//
//   - When a run starts, the root span's IDs (and the sampling decision) are
//     picked once and stored in status.traceContext / status.traceID.
//   - Every other span's ID is derived from the trace ID and a stable key read
//     from the status (the task ID of a step attempt or fan-out item, the step
//     start time plus round number of a loop round). Tasks are published with
//     that span as their parent, so knight spans join the run's trace.
//   - Each reconcile compares the spans visible in the status before and after
//     it. A span that finished (or disappeared) in between is emitted once,
//     with its stored IDs and its start/end times from the status. Emission
//     only happens after the status was written, so a conflicting write never
//     emits a span the next reconcile would emit again.
//
// The tracer provider must use telemetry.IDGenerator for the emitted spans
// to keep their stored IDs; spans whose IDs cannot be kept are dropped.

import (
	"context"
	"crypto/sha256"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/telemetry"
)

const chainTracerName = "github.com/roundtable-k8s/roundtable/internal/controller"

// Span attribute keys (docs/design/telemetry-conventions.md).
const (
	attrTable          = "roundtable.table"
	attrChain          = "roundtable.chain"
	attrChainStep      = "roundtable.chain.step"
	attrChainStepType  = "roundtable.chain.step.type"
	attrChainItemIndex = "roundtable.chain.item.index"
	attrChainIteration = "roundtable.chain.loop.iteration"
	attrRunID          = "roundtable.run.id"
	attrTaskID         = "roundtable.task.id"
	attrAttempt        = "roundtable.attempt"
	attrKnight         = "roundtable.knight"
)

// Step types recorded in roundtable.chain.step.type.
const (
	stepTypeTask    = "task"
	stepTypeForEach = "forEach"
	stepTypeLoop    = "loop"
)

// w3c encodes status.traceContext; it is always W3C, whatever the global
// propagator is.
var w3c = propagation.TraceContext{}

func (r *ChainReconciler) tracer() trace.Tracer {
	tp := r.TracerProvider
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	return tp.Tracer(chainTracerName)
}

// runSpanContext returns the run's root span context from
// status.traceContext; it is invalid when the run has none.
func runSpanContext(chain *aiv1alpha1.Chain) trace.SpanContext {
	tc := chain.Status.TraceContext
	if tc == nil || tc.Traceparent == "" {
		return trace.SpanContext{}
	}
	carrier := propagation.MapCarrier{"traceparent": tc.Traceparent}
	if tc.Tracestate != "" {
		carrier["tracestate"] = tc.Tracestate
	}
	return trace.SpanContextFromContext(w3c.Extract(context.Background(), carrier))
}

// resetRunTrace drops the trace of the previous run; the next reconcile of
// a running chain starts a new one.
func resetRunTrace(chain *aiv1alpha1.Chain) {
	chain.Status.TraceContext = nil
	chain.Status.TraceID = ""
}

// ensureRunTrace picks the root span's IDs for a running chain that has no
// trace yet and stores them on the status. With no tracer provider (telemetry
// off) nothing is stored.
func (r *ChainReconciler) ensureRunTrace(ctx context.Context, chain *aiv1alpha1.Chain) {
	if chain.Status.TraceContext != nil {
		return
	}
	// The span only draws the root's IDs and sampling decision from the
	// provider. It is never ended, so never exported: the root span is
	// emitted with these IDs when the run ends.
	_, probe := r.tracer().Start(ctx, "chain "+chain.Name, trace.WithNewRoot())
	sc := probe.SpanContext()
	if !sc.IsValid() {
		return
	}
	carrier := propagation.MapCarrier{}
	w3c.Inject(trace.ContextWithSpanContext(context.Background(), sc), carrier)
	chain.Status.TraceContext = &aiv1alpha1.TraceContext{
		Traceparent: carrier.Get("traceparent"),
		Tracestate:  carrier.Get("tracestate"),
	}
	chain.Status.TraceID = sc.TraceID().String()
}

// Span keys: stable strings, read back from the status, from which span IDs
// are derived.

func plainStepSpanKey(step, taskID string) string { return "step/" + step + "/" + taskID }

func groupStepSpanKey(step string, startedAt *metav1.Time) string {
	var unix int64
	if startedAt != nil {
		// Seconds: the status keeps metav1.Time at second precision.
		unix = startedAt.Unix()
	}
	return "step/" + step + "@" + strconv.FormatInt(unix, 10)
}

func itemSpanKey(taskID string) string { return "item/" + taskID }

func roundSpanKey(stepKey string, round int32) string {
	return stepKey + "/round/" + strconv.FormatInt(int64(round), 10)
}

// deriveSpanID returns the span ID for key in trace tid.
func deriveSpanID(tid trace.TraceID, key string) trace.SpanID {
	h := sha256.New()
	h.Write(tid[:])
	h.Write([]byte(key))
	var sid trace.SpanID
	copy(sid[:], h.Sum(nil))
	if !sid.IsValid() {
		sid[len(sid)-1] = 1
	}
	return sid
}

func childSpanContext(root trace.SpanContext, key string) trace.SpanContext {
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    root.TraceID(),
		SpanID:     deriveSpanID(root.TraceID(), key),
		TraceFlags: root.TraceFlags(),
		TraceState: root.TraceState(),
		Remote:     true,
	})
}

// withRunSpan returns ctx parented on the run's span for key, so a task
// published with it is a child of that span. ctx is returned unchanged when
// the run has no trace.
func withRunSpan(ctx context.Context, chain *aiv1alpha1.Chain, key string) context.Context {
	root := runSpanContext(chain)
	if !root.IsValid() {
		return ctx
	}
	return trace.ContextWithRemoteSpanContext(ctx, childSpanContext(root, key))
}

// runSpan is a span of a chain run as seen in the status.
type runSpan struct {
	name     string
	sc       trace.SpanContext
	parent   trace.SpanContext // invalid for the root
	start    time.Time
	end      time.Time
	finished bool
	failed   bool
	errMsg   string
	attrs    []attribute.KeyValue

	// For resolving a fan-out item whose detailed entry was compacted away.
	step      string
	itemIndex int32
	isItem    bool
}

const (
	msgChainEnded = "chain run ended before this finished"
	msgSuperseded = "superseded by a new attempt or run"
)

// stepOutcome reports whether a finished step (or attempt) failed and why.
func stepOutcome(phase aiv1alpha1.ChainStepPhase, errMsg, reason string) (bool, string) {
	switch phase {
	case aiv1alpha1.ChainStepPhaseSucceeded:
		return false, ""
	case aiv1alpha1.ChainStepPhaseRunning:
		return true, msgChainEnded
	}
	for _, m := range []string{errMsg, reason} {
		if m != "" {
			return true, m
		}
	}
	return true, string(phase)
}

// chainRunSpans lists the spans of the chain's current run that the status
// describes, finished or not. now ends spans whose status has no end time.
func chainRunSpans(chain *aiv1alpha1.Chain, now time.Time) map[trace.SpanID]runSpan {
	root := runSpanContext(chain)
	if !root.IsValid() || !root.IsSampled() {
		return nil
	}
	st := &chain.Status
	running := st.Phase == aiv1alpha1.ChainPhaseRunning
	legStart := now
	if st.StartedAt != nil {
		legStart = st.StartedAt.Time
	}
	// Work from before a retry-failed resume belongs to the previous trace.
	floor := legStart.Truncate(time.Second)
	inLeg := func(t *metav1.Time) bool { return t != nil && !t.Time.Before(floor) }
	endOr := func(t *metav1.Time) time.Time {
		if t != nil {
			return t.Time
		}
		return now
	}
	base := []attribute.KeyValue{attribute.String(attrChain, chain.Name), attribute.String(attrRunID, st.RunID)}
	with := func(extra ...attribute.KeyValue) []attribute.KeyValue {
		return append(append(make([]attribute.KeyValue, 0, len(base)+len(extra)), base...), extra...)
	}

	spans := map[trace.SpanID]runSpan{}
	add := func(s runSpan) { spans[s.sc.SpanID()] = s }

	rs := runSpan{name: "chain " + chain.Name, sc: root, start: legStart, end: endOr(st.CompletedAt), finished: !running, attrs: with()}
	if st.Phase == aiv1alpha1.ChainPhaseFailed {
		rs.failed, rs.errMsg = true, "chain failed"
		if c := meta.FindStatusCondition(st.Conditions, aiv1alpha1.ConditionChainComplete); c != nil && c.Message != "" {
			rs.errMsg = c.Message
		}
	}
	add(rs)

	specs := make(map[string]*aiv1alpha1.ChainStep, len(chain.Spec.Steps))
	for i := range chain.Spec.Steps {
		specs[chain.Spec.Steps[i].Name] = &chain.Spec.Steps[i]
	}
	for _, ss := range st.StepStatuses {
		if !inLeg(ss.StartedAt) {
			continue
		}
		stepDone := !running || ss.Phase != aiv1alpha1.ChainStepPhaseRunning
		spec := specs[ss.Name]
		if spec == nil || (spec.ForEach == nil && spec.Loop == nil) {
			if ss.TaskID == "" {
				continue
			}
			s := runSpan{
				name: "step " + ss.Name, sc: childSpanContext(root, plainStepSpanKey(ss.Name, ss.TaskID)), parent: root,
				start: ss.StartedAt.Time, end: endOr(ss.CompletedAt), finished: stepDone,
				attrs: with(attribute.String(attrChainStep, ss.Name), attribute.String(attrChainStepType, stepTypeTask),
					attribute.Int64(attrAttempt, int64(ss.Retries)+1), attribute.String(attrTaskID, ss.TaskID),
					attribute.String(attrKnight, ss.Knight)),
			}
			if stepDone {
				s.failed, s.errMsg = stepOutcome(ss.Phase, ss.Error, ss.Reason)
			}
			add(s)
			continue
		}

		stepType := stepTypeForEach
		if spec.Loop != nil {
			stepType = stepTypeLoop
		}
		stepKey := groupStepSpanKey(ss.Name, ss.StartedAt)
		stepSC := childSpanContext(root, stepKey)
		s := runSpan{
			name: "step " + ss.Name, sc: stepSC, parent: root,
			start: ss.StartedAt.Time, end: endOr(ss.CompletedAt), finished: stepDone,
			attrs: with(attribute.String(attrChainStep, ss.Name), attribute.String(attrChainStepType, stepType),
				attribute.Int64(attrAttempt, int64(ss.Retries)+1)),
		}
		if stepDone {
			s.failed, s.errMsg = stepOutcome(ss.Phase, ss.Error, ss.Reason)
		}
		add(s)

		if fo := ss.FanOut; fo != nil {
			for _, it := range fo.Items {
				if it.TaskID == "" || !inLeg(it.StartedAt) {
					continue
				}
				done := stepDone || it.Phase != aiv1alpha1.ChainStepPhaseRunning
				is := runSpan{
					name: "item " + ss.Name + "[" + strconv.Itoa(int(it.Index)) + "]", sc: childSpanContext(root, itemSpanKey(it.TaskID)), parent: stepSC,
					start: it.StartedAt.Time, end: endOr(it.CompletedAt), finished: done,
					attrs: with(attribute.String(attrChainStep, ss.Name), attribute.Int64(attrChainItemIndex, int64(it.Index)),
						attribute.Int64(attrAttempt, int64(it.Retries)+1), attribute.String(attrTaskID, it.TaskID)),
					step: ss.Name, itemIndex: it.Index, isItem: true,
				}
				if done {
					is.failed, is.errMsg = stepOutcome(it.Phase, it.Error, "")
				}
				add(is)
			}
		}

		if ls := ss.Loop; ls != nil {
			for n := int32(1); n <= ls.Iteration; n++ {
				var start, end time.Time
				widen := func(s, e *metav1.Time) {
					if !inLeg(s) {
						return
					}
					if start.IsZero() || s.Time.Before(start) {
						start = s.Time
					}
					if e != nil && e.After(end) {
						end = e.Time
					}
				}
				for _, h := range ls.History {
					if h.Round == n {
						widen(h.StartedAt, h.FinishedAt)
					}
				}
				for _, bs := range ls.Steps {
					if bs.Iteration == n {
						widen(bs.StartedAt, bs.CompletedAt)
					}
				}
				if start.IsZero() {
					continue // no task of this round in this leg yet
				}
				done := stepDone || n < ls.Iteration
				if !done || end.IsZero() {
					end = now
				}
				rsp := runSpan{
					name: "iteration " + ss.Name + " " + strconv.Itoa(int(n)), sc: childSpanContext(root, roundSpanKey(stepKey, n)), parent: stepSC,
					start: start, end: end, finished: done,
					attrs: with(attribute.String(attrChainStep, ss.Name), attribute.Int64(attrChainIteration, int64(n))),
				}
				if done && n == ls.Iteration && ss.Phase != aiv1alpha1.ChainStepPhaseSucceeded {
					rsp.failed, rsp.errMsg = stepOutcome(ss.Phase, ss.Error, ss.Reason)
				}
				add(rsp)
			}
		}
	}
	return spans
}

// emitRunSpans emits the spans that finished between prev (the status at the
// start of the reconcile) and the chain's current status.
func (r *ChainReconciler) emitRunSpans(ctx context.Context, chain *aiv1alpha1.Chain, prev map[trace.SpanID]runSpan) {
	now := time.Now()
	cur := chainRunSpans(chain, now)
	var table string
	tableOf := func() string {
		if table == "" {
			if table = r.chainTable(ctx, chain); table == "" {
				table = "none"
			}
		}
		return table
	}
	for id, s := range cur {
		if !s.finished {
			continue
		}
		p, seen := prev[id]
		if seen && p.finished {
			continue
		}
		if seen {
			// Keep what the span carried while it was live (attempt
			// number, start), the outcome comes from now.
			s.name, s.start, s.attrs = p.name, p.start, p.attrs
		}
		r.emitRunSpan(ctx, s, tableOf())
	}
	for id, p := range prev {
		if _, ok := cur[id]; ok || p.finished {
			continue
		}
		// Gone from the status while live: a retry replaced it, its fan-out
		// entry was compacted away, or a new run replaced the trace.
		p.end, p.failed, p.errMsg = now, true, msgSuperseded
		if p.isItem {
			if ok, known := fanOutItemSucceeded(chain, p.step, p.itemIndex); known && ok {
				p.failed, p.errMsg = false, ""
			}
		}
		r.emitRunSpan(ctx, p, tableOf())
	}
}

// fanOutItemSucceeded looks up a fan-out item's phase in the compact item phases.
func fanOutItemSucceeded(chain *aiv1alpha1.Chain, step string, idx int32) (ok, known bool) {
	for _, ss := range chain.Status.StepStatuses {
		if ss.Name != step || ss.FanOut == nil || int(idx) >= len(ss.FanOut.ItemPhases) {
			continue
		}
		switch ss.FanOut.ItemPhases[idx] {
		case itemSucceeded:
			return true, true
		case itemFailed:
			return false, true
		}
	}
	return false, false
}

var presetIDsWarning sync.Once

func (r *ChainReconciler) emitRunSpan(ctx context.Context, s runSpan, table string) {
	opts := []trace.SpanStartOption{
		trace.WithTimestamp(s.start),
		trace.WithAttributes(append(s.attrs, attribute.String(attrTable, table))...),
	}
	parent := ctx
	if s.parent.IsValid() {
		parent = trace.ContextWithRemoteSpanContext(ctx, s.parent)
	} else {
		opts = append(opts, trace.WithNewRoot())
	}
	_, span := r.tracer().Start(telemetry.WithPresetIDs(parent, s.sc.TraceID(), s.sc.SpanID()), s.name, opts...)
	if got := span.SpanContext(); got.TraceID() != s.sc.TraceID() || got.SpanID() != s.sc.SpanID() {
		// The provider ignored the stored IDs: exporting this span would
		// orphan the tasks parented on it. Drop it (it is never ended).
		if span.IsRecording() {
			presetIDsWarning.Do(func() {
				logf.FromContext(ctx).Info("Dropped chain run spans: the tracer provider does not use telemetry.IDGenerator")
			})
		}
		return
	}
	if s.failed {
		span.SetStatus(codes.Error, s.errMsg)
	}
	end := s.end
	if end.Before(s.start) {
		end = s.start
	}
	span.End(trace.WithTimestamp(end))
}
