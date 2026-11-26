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

package telemetry

import (
	"context"
	"crypto/rand"
	"crypto/sha256"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Spans of long-running work (a Chain run, a step) outlive a single
// reconcile and even the operator process. Their trace and span IDs are chosen
// up front and stored on the resource, and the span itself is emitted once,
// when the work ends, with explicit start and end timestamps. The SDK only
// lets its IDGenerator pick IDs, so the IDs travel to it in the context.

type presetIDsKey struct{}

type presetIDs struct {
	traceID trace.TraceID
	spanID  trace.SpanID
}

// WithPresetIDs returns ctx asking IDGenerator to give the next span started
// with it the given IDs. The span ID is only used when the span ends up in
// traceID (its parent's trace, or traceID for a new root).
func WithPresetIDs(ctx context.Context, traceID trace.TraceID, spanID trace.SpanID) context.Context {
	return context.WithValue(ctx, presetIDsKey{}, presetIDs{traceID: traceID, spanID: spanID})
}

func presetFrom(ctx context.Context) (presetIDs, bool) {
	p, ok := ctx.Value(presetIDsKey{}).(presetIDs)
	return p, ok && p.traceID.IsValid() && p.spanID.IsValid()
}

// IDGenerator is an sdktrace.IDGenerator that honours IDs preset with
// WithPresetIDs and otherwise generates random ones (from crypto/rand).
// The operator's tracer provider must be built with
// sdktrace.WithIDGenerator(NewIDGenerator()) for spans emitted after the fact
// (Chain run and step spans) to keep their stored IDs.
type IDGenerator struct{}

var _ sdktrace.IDGenerator = (*IDGenerator)(nil)

// NewIDGenerator returns an IDGenerator.
func NewIDGenerator() *IDGenerator { return &IDGenerator{} }

// NewIDs returns the preset trace and span ID, or random ones.
func (g *IDGenerator) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	if p, ok := presetFrom(ctx); ok {
		return p.traceID, p.spanID
	}
	var tid trace.TraceID
	for !tid.IsValid() {
		_, _ = rand.Read(tid[:])
	}
	return tid, randomSpanID()
}

// NewSpanID returns the preset span ID when it was preset for traceID, or a
// random one.
func (g *IDGenerator) NewSpanID(ctx context.Context, traceID trace.TraceID) trace.SpanID {
	if p, ok := presetFrom(ctx); ok && p.traceID == traceID {
		return p.spanID
	}
	return randomSpanID()
}

func randomSpanID() trace.SpanID {
	var sid trace.SpanID
	for !sid.IsValid() {
		_, _ = rand.Read(sid[:])
	}
	return sid
}

// DeriveSpanID returns the span ID for a stable key in trace tid, so a span
// can be emitted after the fact with an ID its children already carry.
func DeriveSpanID(tid trace.TraceID, key string) trace.SpanID {
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

// ChildSpanContext returns the context of the span for key under root.
func ChildSpanContext(root trace.SpanContext, key string) trace.SpanContext {
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    root.TraceID(),
		SpanID:     DeriveSpanID(root.TraceID(), key),
		TraceFlags: root.TraceFlags(),
		TraceState: root.TraceState(),
		Remote:     true,
	})
}

// AnnotationParentTraceparent marks a Chain created by a Mission: its value
// is the W3C traceparent of the mission's root span, and the chain's run
// trace becomes a part of the mission's trace instead of a new one.
const AnnotationParentTraceparent = "ai.roundtable.io/parent-traceparent"

// LogValues returns the key/value pair that puts a run's trace ID on a log
// line, or nothing when the ID is empty (telemetry off), so lines are
// unchanged then. The operator's spans are emitted after the fact from stored
// IDs, so the ID comes from the resource status, not from a span in ctx.
func LogValues(traceID string) []any {
	if traceID == "" {
		return nil
	}
	return []any{"trace_id", traceID}
}
