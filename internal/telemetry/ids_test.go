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
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestIDGeneratorHonoursPresetIDs(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec), sdktrace.WithIDGenerator(NewIDGenerator()))
	tr := tp.Tracer("test")
	ctx := context.Background()

	tid := trace.TraceID{1, 2, 3}
	root := trace.SpanID{4, 5, 6}
	child := trace.SpanID{7, 8, 9}

	_, s := tr.Start(WithPresetIDs(ctx, tid, root), "root", trace.WithNewRoot())
	s.End()
	if got := s.SpanContext(); got.TraceID() != tid || got.SpanID() != root {
		t.Fatalf("root got %s/%s", got.TraceID(), got.SpanID())
	}

	parent := trace.ContextWithRemoteSpanContext(ctx, s.SpanContext())
	_, c := tr.Start(WithPresetIDs(parent, tid, child), "child")
	c.End()
	if got := c.SpanContext(); got.TraceID() != tid || got.SpanID() != child {
		t.Fatalf("child got %s/%s", got.TraceID(), got.SpanID())
	}

	// A preset for another trace is ignored: the span gets a random ID.
	_, o := tr.Start(WithPresetIDs(parent, trace.TraceID{9}, child), "other")
	o.End()
	if got := o.SpanContext(); got.TraceID() != tid || got.SpanID() == child || !got.SpanID().IsValid() {
		t.Fatalf("mismatched preset should be ignored, got %s/%s", got.TraceID(), got.SpanID())
	}

	// Without a preset, IDs are random and valid.
	_, a := tr.Start(ctx, "a")
	_, b := tr.Start(ctx, "b")
	if !a.SpanContext().IsValid() || a.SpanContext().TraceID() == b.SpanContext().TraceID() {
		t.Fatal("random IDs expected")
	}
}

func TestLogValues(t *testing.T) {
	if got := LogValues(""); len(got) != 0 {
		t.Fatalf("empty trace ID: got %v, want nothing", got)
	}
	got := LogValues("abc")
	if len(got) != 2 || got[0] != "trace_id" || got[1] != "abc" {
		t.Fatalf("got %v", got)
	}
}
