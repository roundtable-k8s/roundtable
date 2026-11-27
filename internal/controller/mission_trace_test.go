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
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/mission"
	"github.com/roundtable-k8s/roundtable/internal/telemetry"
)

// A mission that plans and runs one chain is a single trace: the mission,
// plan and briefing spans and the chain's spans share one trace ID, and the
// chain's root span is a child of the mission's root span.
func TestMissionPlanAndChainShareOneTrace(t *testing.T) {
	ctx := context.Background()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec), sdktrace.WithIDGenerator(telemetry.NewIDGenerator()))

	start := metav1.NewTime(time.Now().Add(-time.Minute))
	m := &aiv1alpha1.Mission{ObjectMeta: metav1.ObjectMeta{Name: "m1", Namespace: "ns"}}
	m.Status.StartedAt = &start
	m.Spec.RoundTableRef = aiv1alpha1.LocalRef("rt")
	mission.EnsureTrace(ctx, tp, m)
	if m.Status.TraceContext == nil || len(m.Status.TraceID) != 32 {
		t.Fatalf("mission has no trace: %+v", m.Status)
	}
	if len(rec.Ended()) != 0 {
		t.Fatalf("EnsureTrace exported a span (the probe is never ended)")
	}

	mission.EmitPlanSpan(ctx, tp, m, "")
	mission.EmitSpan(ctx, tp, m, mission.BriefingSpanKey("k1"), "briefing k1", mission.SpanEnd{Start: start.Time, End: time.Now()})

	// The chain the mission creates.
	chain := &aiv1alpha1.Chain{ObjectMeta: metav1.ObjectMeta{
		Name: "mission-m1-c", Namespace: "ns", Annotations: mission.ParentAnnotations(m),
	}}
	chain.Spec.RoundTableRef = aiv1alpha1.LocalRef("rt")
	chain.Status.StartedAt = &start
	done := metav1.Now()
	chain.Status.CompletedAt = &done
	r := &ChainReconciler{TracerProvider: tp}
	chain.Status.Phase = aiv1alpha1.ChainPhaseRunning
	r.ensureRunTrace(ctx, chain)
	if chain.Status.TraceID != m.Status.TraceID {
		t.Fatalf("chain trace %q is not the mission's %q", chain.Status.TraceID, m.Status.TraceID)
	}
	chain.Status.Phase = aiv1alpha1.ChainPhaseSucceeded
	for _, s := range chainRunSpans(chain, time.Now()) {
		r.emitRunSpan(ctx, s, "rt")
	}

	missionRoot := mission.RootSpanContext(m)
	m.Status.Phase = aiv1alpha1.MissionPhaseSucceeded
	mission.EmitSpan(ctx, tp, m, "", "mission m1", mission.SpanEnd{Start: start.Time, End: time.Now()})

	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range rec.Ended() {
		byName[s.Name()] = s
		if s.SpanContext().TraceID().String() != m.Status.TraceID {
			t.Errorf("span %q is in trace %s, want %s", s.Name(), s.SpanContext().TraceID(), m.Status.TraceID)
		}
	}
	for _, n := range []string{"mission m1", "plan", "briefing k1", "chain mission-m1-c"} {
		if byName[n] == nil {
			t.Fatalf("span %q not emitted; got %v", n, rec.Ended())
		}
	}
	if byName["mission m1"].SpanContext().SpanID() != missionRoot.SpanID() || byName["mission m1"].Parent().IsValid() {
		t.Errorf("mission span is not the stored root")
	}
	for _, n := range []string{"plan", "briefing k1", "chain mission-m1-c"} {
		if got := byName[n].Parent().SpanID(); got != missionRoot.SpanID() {
			t.Errorf("%s parent = %s, want the mission root %s", n, got, missionRoot.SpanID())
		}
	}
}

// Without a tracer provider (telemetry off) nothing is stored and a chain
// keeps behaving as before.
func TestMissionTraceOffStoresNothing(t *testing.T) {
	m := &aiv1alpha1.Mission{ObjectMeta: metav1.ObjectMeta{Name: "m1"}}
	mission.EnsureTrace(context.Background(), sdktraceNoop(), m)
	if m.Status.TraceContext != nil || m.Status.TraceID != "" || mission.ParentAnnotations(m) != nil {
		t.Fatalf("trace stored with telemetry off: %+v", m.Status)
	}
	mission.EmitSpan(context.Background(), sdktraceNoop(), m, "", "mission m1", mission.SpanEnd{})
}

func sdktraceNoop() trace.TracerProvider { return noop.NewTracerProvider() }

// The reconciler emits the mission's root span once, as the mission moves from
// its terminal phase to CleaningUp.
func TestMissionReconcileEmitsSpanOnCleaningUp(t *testing.T) {
	ctx := context.Background()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec), sdktrace.WithIDGenerator(telemetry.NewIDGenerator()))

	start := metav1.NewTime(time.Now().Add(-time.Minute))
	m := &aiv1alpha1.Mission{ObjectMeta: metav1.ObjectMeta{Name: "m1", Namespace: "ns"}}
	m.Spec.RoundTableRef = aiv1alpha1.LocalRef("rt")
	m.Status.StartedAt = &start
	m.Status.Phase = aiv1alpha1.MissionPhaseSucceeded
	mission.EnsureTrace(ctx, tp, m)
	if m.Status.TraceContext == nil {
		t.Fatal("mission has no trace")
	}
	root := mission.RootSpanContext(m)

	c := fake.NewClientBuilder().WithScheme(statusTestScheme()).WithObjects(m).WithStatusSubresource(m).Build()
	r := &MissionReconciler{Client: c, Scheme: statusTestScheme(), Recorder: record.NewFakeRecorder(10), TracerProvider: tp}

	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(m)}
	if _, err := r.reconcileOnce(ctx, req); err != nil {
		t.Fatal(err)
	}
	got := &aiv1alpha1.Mission{}
	if err := c.Get(ctx, req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != aiv1alpha1.MissionPhaseCleaningUp {
		t.Fatalf("phase = %s, want CleaningUp", got.Status.Phase)
	}
	spans := rec.Ended()
	if len(spans) != 1 {
		t.Fatalf("emitted %d spans, want 1: %v", len(spans), spans)
	}
	s := spans[0]
	if s.Name() != "mission m1" || s.SpanContext().SpanID() != root.SpanID() ||
		s.SpanContext().TraceID().String() != m.Status.TraceID || s.Parent().IsValid() {
		t.Errorf("span %q is not the stored mission root", s.Name())
	}
}
