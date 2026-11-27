package telemetry

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"OTEL_SDK_DISABLED", "OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_SERVICE_NAME", "OTEL_RESOURCE_ATTRIBUTES"} {
		t.Setenv(k, "")
	}
}

func setup(t *testing.T) func(context.Context) error {
	t.Helper()
	// Restore a no-op provider rather than the previous global: that one may
	// be the delegating provider, which logs and would keep pointing at the
	// SDK provider shut down by the test.
	t.Cleanup(func() { otel.SetTracerProvider(noop.NewTracerProvider()) })
	shutdown, err := Setup(context.Background(), "v1.2.3")
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	return shutdown
}

func isSDK(tp any) bool { _, ok := tp.(*sdktrace.TracerProvider); return ok }

func TestNoEndpointIsNoop(t *testing.T) {
	clearEnv(t)
	shutdown := setup(t)
	if isSDK(otel.GetTracerProvider()) {
		t.Fatal("expected no-op provider")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestDisabledIsNoop(t *testing.T) {
	clearEnv(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
	t.Setenv("OTEL_SDK_DISABLED", "true")
	shutdown := setup(t)
	if isSDK(otel.GetTracerProvider()) {
		t.Fatal("expected no-op provider")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func serviceAttrs(t *testing.T) map[string]string {
	t.Helper()
	tp, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider)
	if !ok {
		t.Fatal("expected SDK provider")
	}
	rec := &recorder{}
	tp.RegisterSpanProcessor(rec)
	_, span := tp.Tracer("t").Start(context.Background(), "s")
	span.End()
	out := map[string]string{}
	for _, kv := range rec.res {
		out[string(kv.Key)] = kv.Value.String()
	}
	return out
}

type recorder struct{ res []attribute.KeyValue }

func (r *recorder) OnStart(context.Context, sdktrace.ReadWriteSpan) {}
func (r *recorder) OnEnd(s sdktrace.ReadOnlySpan)                   { r.res = s.Resource().Attributes() }
func (r *recorder) Shutdown(context.Context) error                  { return nil }
func (r *recorder) ForceFlush(context.Context) error                { return nil }

func TestEndpointDefaultServiceName(t *testing.T) {
	clearEnv(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment=test")
	shutdown := setup(t)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_ = shutdown(ctx)
	}()
	a := serviceAttrs(t)
	if a["service.name"] != DefaultServiceName || a["service.version"] != "v1.2.3" ||
		a["deployment.environment"] != "test" {
		t.Fatalf("unexpected resource: %v", a)
	}
}

func TestServiceNameOverride(t *testing.T) {
	clearEnv(t)
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://localhost:4317")
	t.Setenv("OTEL_SERVICE_NAME", "custom")
	shutdown := setup(t)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_ = shutdown(ctx)
	}()
	if got := serviceAttrs(t)["service.name"]; got != "custom" {
		t.Fatalf("service.name = %q", got)
	}
}

// Chain run spans are emitted after the fact with IDs stored on the resource;
// the provider Setup builds must honour them or the operator drops the spans.
func TestSetupHonoursPresetIDs(t *testing.T) {
	clearEnv(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
	shutdown := setup(t)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_ = shutdown(ctx)
	}()
	tid := trace.TraceID{1, 2, 3}
	sid := DeriveSpanID(tid, "step/a/task-1")
	_, span := otel.Tracer("t").Start(WithPresetIDs(context.Background(), tid, sid), "s", trace.WithNewRoot())
	defer span.End()
	if got := span.SpanContext(); got.TraceID() != tid || got.SpanID() != sid {
		t.Fatalf("span IDs = %s/%s, want %s/%s", got.TraceID(), got.SpanID(), tid, sid)
	}
}
