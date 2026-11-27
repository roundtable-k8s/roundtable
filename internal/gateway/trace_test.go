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

package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// installTracer swaps in an in-memory provider and the W3C propagator.
func installTracer(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	prevProp := otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(noop.NewTracerProvider())
		otel.SetTextMapPropagator(prevProp)
	})
	return exp
}

func serve(h http.Handler, path string, hdr map[string]string) {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
}

func TestInstrumentServerSpan(t *testing.T) {
	exp := installTracer(t)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/things/{id}", func(w http.ResponseWriter, r *http.Request) {
		setRequestTable(r.Context(), "dev")
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /boom", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) })
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	h := instrument(mux, logr.Discard())

	serve(h, "/v1/things/42", nil)
	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("spans = %d, want 1", len(spans))
	}
	s := spans[0]
	if s.Name != "GET /v1/things/{id}" {
		t.Fatalf("name = %q", s.Name)
	}
	if s.SpanKind != trace.SpanKindServer || s.Parent.IsValid() {
		t.Fatalf("kind %v parent %v", s.SpanKind, s.Parent)
	}
	attrs := map[string]string{}
	for _, a := range s.Attributes {
		attrs[string(a.Key)] = a.Value.String()
	}
	if attrs["http.request.method"] != "GET" || attrs["http.response.status_code"] != "200" || attrs["roundtable.table"] != "dev" {
		t.Fatalf("attrs = %v", attrs)
	}
	if s.Status.Code == codes.Error {
		t.Fatal("200 must not set error status")
	}

	exp.Reset()
	serve(h, "/healthz", nil)
	if n := len(exp.GetSpans()); n != 0 {
		t.Fatalf("healthz produced %d spans", n)
	}

	serve(h, "/boom", nil)
	spans = exp.GetSpans()
	if len(spans) != 1 || spans[0].Status.Code != codes.Error {
		t.Fatalf("5xx spans = %+v", spans)
	}
}

func TestInstrumentHonoursTraceparent(t *testing.T) {
	exp := installTracer(t)
	h := instrument(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}), logr.Discard())
	const tid, sid = "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"
	serve(h, "/x", map[string]string{"traceparent": "00-" + tid + "-" + sid + "-01"})
	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("spans = %d", len(spans))
	}
	if got := spans[0].SpanContext.TraceID().String(); got != tid {
		t.Fatalf("trace id = %s, want %s", got, tid)
	}
	if got := spans[0].Parent.SpanID().String(); got != sid {
		t.Fatalf("parent = %s, want %s", got, sid)
	}
}

// spyBus records the context the gateway publishes a task with.
type spyBus struct {
	Bus
	ctx chan context.Context
}

func (b *spyBus) Publish(ctx context.Context, subject string, p natspkg.TaskPayload, table string) error {
	select {
	case b.ctx <- ctx:
	default:
	}
	return b.Bus.Publish(ctx, subject, p, table)
}

// A request no route matched is named just its HTTP method, so a scanner's
// paths cannot grow the span-name set.
func TestInstrumentUnmatchedSpanNamedByMethod(t *testing.T) {
	exp := installTracer(t)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/things/{id}", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	h := instrument(mux, logr.Discard())

	serve(h, "/wp-admin/setup.php", nil)
	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("spans = %d, want 1", len(spans))
	}
	if spans[0].Name != "GET" {
		t.Fatalf("unmatched span name = %q, want %q", spans[0].Name, "GET")
	}
}

func TestInvokePublishesInRequestSpanAndReturnsTraceID(t *testing.T) {
	exp := installTracer(t)
	e := newEnv(t, Options{})
	spy := &spyBus{Bus: e.svc.Bus, ctx: make(chan context.Context, 1)}
	e.svc.Bus = spy

	resp, body := e.do(t, "POST", "/v1/knights/operator/invoke", testToken, InvokeRequest{Task: "hello"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %v", resp.StatusCode, body)
	}
	pubCtx := <-spy.ctx
	sc := trace.SpanContextFromContext(pubCtx)
	if !sc.IsValid() {
		t.Fatal("publish context carries no span")
	}
	if body["traceId"] != sc.TraceID().String() {
		t.Fatalf("traceId = %v, want %s", body["traceId"], sc.TraceID())
	}
	found := false
	for _, s := range exp.GetSpans() {
		if s.SpanContext.SpanID() == sc.SpanID() && s.SpanKind == trace.SpanKindServer {
			found = true
		}
	}
	if !found {
		t.Fatal("publish context's span is not the request's server span")
	}
}

func TestInvokeNoTraceIDWithoutTracing(t *testing.T) {
	otel.SetTracerProvider(noop.NewTracerProvider())
	e := newEnv(t, Options{})
	_, body := e.do(t, "POST", "/v1/knights/operator/invoke", testToken, InvokeRequest{Task: "hello"})
	if _, ok := body["traceId"]; ok {
		t.Fatalf("traceId present without a provider: %v", body["traceId"])
	}
}
