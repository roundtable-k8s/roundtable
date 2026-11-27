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

package mission

// One trace per Mission (docs/design/telemetry-conventions.md, decision 14).
//
// Like a Chain run, a mission outlives any reconcile: the root span's IDs are
// picked once and stored in status.traceContext / status.traceID, the root
// span is emitted when the mission ends, and the other spans (plan, briefing
// tasks) take IDs derived from the trace ID and a stable key. Chains the
// mission creates carry the root span context in the
// telemetry.AnnotationParentTraceparent annotation, so the chain controller
// parents the chain's root span on the mission instead of starting a trace.
// With no tracer provider (telemetry off) nothing is stored or emitted.

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/telemetry"
)

const tracerName = "github.com/roundtable-k8s/roundtable/internal/mission"

// Span names, keys and attributes (docs/design/telemetry-conventions.md).
const (
	attrTable   = "roundtable.table"
	attrMission = "roundtable.mission"
	attrKnight  = "roundtable.knight"
	attrTaskID  = "roundtable.task.id"

	planSpanKey = "plan"
)

// BriefingSpanKey is the span key of a knight's briefing task.
func BriefingSpanKey(knight string) string { return "briefing/" + knight }

var w3c = propagation.TraceContext{}

func tracerOf(tp trace.TracerProvider) trace.Tracer {
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	return tp.Tracer(tracerName)
}

// RootSpanContext returns the mission's root span context from
// status.traceContext; it is invalid when the mission has none.
func RootSpanContext(m *aiv1alpha1.Mission) trace.SpanContext {
	tc := m.Status.TraceContext
	if tc == nil || tc.Traceparent == "" {
		return trace.SpanContext{}
	}
	carrier := propagation.MapCarrier{"traceparent": tc.Traceparent}
	if tc.Tracestate != "" {
		carrier["tracestate"] = tc.Tracestate
	}
	return trace.SpanContextFromContext(w3c.Extract(context.Background(), carrier))
}

// EnsureTrace stores the root span's IDs on the status of a mission that has
// none yet. The change is in memory: the caller's next status write keeps it,
// and a mission whose write is lost simply picks new IDs on the next reconcile.
func EnsureTrace(ctx context.Context, tp trace.TracerProvider, m *aiv1alpha1.Mission) {
	if m.Status.TraceContext != nil {
		return
	}
	// The probe only draws IDs and the sampling decision from the provider;
	// it is never ended, so never exported.
	_, probe := tracerOf(tp).Start(ctx, "mission "+m.Name, trace.WithNewRoot())
	sc := probe.SpanContext()
	if !sc.IsValid() {
		return
	}
	carrier := propagation.MapCarrier{}
	w3c.Inject(trace.ContextWithSpanContext(context.Background(), sc), carrier)
	m.Status.TraceContext = &aiv1alpha1.TraceContext{
		Traceparent: carrier.Get("traceparent"),
		Tracestate:  carrier.Get("tracestate"),
	}
	m.Status.TraceID = sc.TraceID().String()
}

// ParentAnnotations returns the annotations that make a chain created by the
// mission a child of the mission's root span; nil when the mission has no trace.
func ParentAnnotations(m *aiv1alpha1.Mission) map[string]string {
	if tc := m.Status.TraceContext; tc != nil && tc.Traceparent != "" {
		return map[string]string{telemetry.AnnotationParentTraceparent: tc.Traceparent}
	}
	return nil
}

// WithSpan returns ctx parented on the mission's span for key ("" is the root
// span), so a task published with it is a child of that span. ctx is returned
// unchanged when the mission has no trace.
func WithSpan(ctx context.Context, m *aiv1alpha1.Mission, key string) context.Context {
	root := RootSpanContext(m)
	if !root.IsValid() {
		return ctx
	}
	if key == "" {
		return trace.ContextWithRemoteSpanContext(ctx, root)
	}
	return trace.ContextWithRemoteSpanContext(ctx, telemetry.ChildSpanContext(root, key))
}

// SpanEnd describes how a span ended.
type SpanEnd struct {
	Start  time.Time
	End    time.Time
	ErrMsg string // non-empty marks the span as failed
	Attrs  []attribute.KeyValue
}

// Table is the roundtable.table attribute value of a mission.
// It is the roundTableRef, else the ephemeral table recorded in status, else
// "none".
func Table(m *aiv1alpha1.Mission) string {
	if name := TableName(m); name != "" {
		return name
	}
	return "none"
}

// TableName returns the name of the RoundTable a mission runs on: its
// roundTableRef, else the ephemeral table recorded in status; "" if neither.
func TableName(m *aiv1alpha1.Mission) string {
	if name := aiv1alpha1.RefName(m.Spec.RoundTableRef); name != "" {
		return name
	}
	return m.Status.RoundTableName
}

// EmitSpan emits the mission's span for key ("" is the root span, named
// "mission <name>") with its stored IDs. Spans whose IDs the provider did not
// keep are dropped, as for chain runs. It does nothing without a trace.
func EmitSpan(ctx context.Context, tp trace.TracerProvider, m *aiv1alpha1.Mission, key, name string, e SpanEnd) {
	root := RootSpanContext(m)
	if !root.IsValid() || !root.IsSampled() {
		return
	}
	sc, parent := root, trace.SpanContext{}
	if key != "" {
		sc, parent = telemetry.ChildSpanContext(root, key), root
	}
	attrs := append([]attribute.KeyValue{
		attribute.String(attrTable, Table(m)),
		attribute.String(attrMission, m.Name),
	}, e.Attrs...)
	opts := []trace.SpanStartOption{trace.WithTimestamp(e.Start), trace.WithAttributes(attrs...)}
	pctx := ctx
	if parent.IsValid() {
		pctx = trace.ContextWithRemoteSpanContext(ctx, parent)
	} else {
		opts = append(opts, trace.WithNewRoot())
	}
	_, span := tracerOf(tp).Start(telemetry.WithPresetIDs(pctx, sc.TraceID(), sc.SpanID()), name, opts...)
	if got := span.SpanContext(); got.TraceID() != sc.TraceID() || got.SpanID() != sc.SpanID() {
		return // provider ignored the stored IDs; dropped (never ended)
	}
	if e.ErrMsg != "" {
		span.SetStatus(codes.Error, e.ErrMsg)
	}
	end := e.End
	if end.Before(e.Start) {
		end = e.Start
	}
	span.End(trace.WithTimestamp(end))
}

// EmitPlanSpan emits the plan span: the planner round trip, from dispatch to
// the end of planning. errMsg marks it failed.
func EmitPlanSpan(ctx context.Context, tp trace.TracerProvider, m *aiv1alpha1.Mission, errMsg string) {
	start := time.Now()
	if pr := m.Status.PlanningResult; pr != nil && pr.StartedAt != nil {
		start = pr.StartedAt.Time
	} else if m.Status.StartedAt != nil {
		start = m.Status.StartedAt.Time
	}
	attrs := []attribute.KeyValue{}
	if m.Status.PlanningTaskID != "" {
		attrs = append(attrs, attribute.String(attrTaskID, m.Status.PlanningTaskID))
	}
	EmitSpan(ctx, tp, m, planSpanKey, "plan", SpanEnd{Start: start, End: time.Now(), ErrMsg: errMsg, Attrs: attrs})
}

// KnightAttr is the roundtable.knight attribute.
func KnightAttr(name string) attribute.KeyValue { return attribute.String(attrKnight, name) }

// TaskAttr is the roundtable.task.id attribute.
func TaskAttr(id string) attribute.KeyValue { return attribute.String(attrTaskID, id) }
