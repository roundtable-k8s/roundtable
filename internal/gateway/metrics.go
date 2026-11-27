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
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/roundtable-k8s/roundtable/pkg/metrics"
)

const (
	// noTable is the table label of a request whose knight, chain or
	// Trigger is not known (or has no table).
	noTable = "none"
	// unmatchedRoute is the route label of a request no route matched, so a
	// scanner's paths cannot grow the label set.
	unmatchedRoute = "unmatched"
	// healthRoute is not logged: probes would drown the request log.
	healthRoute = "GET /healthz"
)

// tracerName is the instrumentation scope of the gateway's server spans.
const tracerName = "github.com/roundtable-k8s/roundtable/internal/gateway"

// traceIDFrom returns the trace ID of the span in ctx, "" when there is none.
func traceIDFrom(ctx context.Context) string {
	if sc := trace.SpanContextFromContext(ctx); sc.HasTraceID() {
		return sc.TraceID().String()
	}
	return ""
}

// routeTemplate drops the method from a ServeMux pattern ("GET /v1/x" -> "/v1/x").
func routeTemplate(pattern string) string {
	if _, path, ok := strings.Cut(pattern, " "); ok {
		return path
	}
	return pattern
}

type requestInfoKey struct{}

// requestInfo carries what the handlers learn about a request (its table,
// its caller) back to the metrics and logging middleware.
type requestInfo struct {
	table  atomic.Pointer[string]
	caller atomic.Pointer[Caller]
}

// setRequestTable records the RoundTable the request in ctx addresses. An
// empty name is ignored.
func setRequestTable(ctx context.Context, table string) {
	if ri, ok := ctx.Value(requestInfoKey{}).(*requestInfo); ok && table != "" {
		ri.table.Store(&table)
	}
}

// setRequestCaller records the authenticated caller of the request in ctx.
func setRequestCaller(ctx context.Context, c Caller) {
	if ri, ok := ctx.Value(requestInfoKey{}).(*requestInfo); ok {
		ri.caller.Store(&c)
	}
}

// statusRecorder remembers the status code written.
type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (s *statusRecorder) WriteHeader(code int) {
	if s.code == 0 {
		s.code = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.code == 0 {
		s.code = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

// Flush keeps event streams flushing through the recorder.
func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// instrument counts and times each request by route template (the matched
// ServeMux pattern, never the raw path), status code, table and caller kind,
// and logs it with its caller (except health probes).
func instrument(next http.Handler, log logr.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ri := &requestInfo{}
		r = r.WithContext(context.WithValue(r.Context(), requestInfoKey{}, ri))
		rec := &statusRecorder{ResponseWriter: w}

		// Server span: a child of an incoming traceparent, in the request
		// context so the handlers' NATS publishes carry it to the knight.
		// Health probes get none. With no provider installed it is a no-op.
		traced := r.Method != http.MethodGet || r.URL.Path != "/healthz"
		var span trace.Span
		if traced {
			ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
			ctx, span = otel.Tracer(tracerName).Start(ctx, r.Method, trace.WithSpanKind(trace.SpanKindServer))
			r = r.WithContext(ctx)
		}
		next.ServeHTTP(rec, r)

		// The routing muxes set r.Pattern on this request: the innermost
		// match wins, "/" (the auth-wrapped API) means nothing matched or
		// the token was refused.
		route := r.Pattern
		if route == "" || route == "/" {
			route = unmatchedRoute
		}
		code := rec.code
		if code == 0 {
			code = http.StatusOK
		}
		table := noTable
		if t := ri.table.Load(); t != nil {
			table = *t
		}
		kind, caller := callerNone, ""
		if c := ri.caller.Load(); c != nil {
			kind, caller = c.Kind, c.String()
		}
		elapsed := time.Since(start)
		traceID := ""
		if traced {
			if route == unmatchedRoute {
				span.SetName(r.Method)
			} else {
				span.SetName(r.Method + " " + routeTemplate(route))
			}
			attrs := []attribute.KeyValue{
				attribute.String("http.request.method", r.Method),
				attribute.Int("http.response.status_code", code),
			}
			if table != noTable {
				attrs = append(attrs, attribute.String("roundtable.table", table))
			}
			span.SetAttributes(attrs...)
			if code >= http.StatusInternalServerError {
				span.SetStatus(codes.Error, http.StatusText(code))
			}
			if span.IsRecording() {
				traceID = span.SpanContext().TraceID().String()
			}
			span.End()
		}
		metrics.GatewayRequestsTotal.WithLabelValues(route, strconv.Itoa(code), table, kind).Inc()
		metrics.GatewayRequestDurationSeconds.WithLabelValues(route).Observe(elapsed.Seconds())
		if route != healthRoute {
			kv := []any{"method", r.Method, "route", route, "code", code, "durationMs", elapsed.Milliseconds(),
				"caller", caller, "callerKind", kind, "table", table}
			if traceID != "" {
				kv = append(kv, "trace_id", traceID)
			}
			if r.Method == http.MethodGet && code < http.StatusBadRequest && isPollRoute(route) {
				log.V(1).Info("Request", kv...)
			} else {
				log.Info("Request", kv...)
			}
		}
	})
}

// isPollRoute reports whether a route pattern is a task or schedule-run read,
// which clients poll; successful polls log at V(1).
func isPollRoute(route string) bool {
	_, path, ok := strings.Cut(route, " ")
	if !ok {
		path = route
	}
	return strings.HasPrefix(path, "/v1/tasks") ||
		(strings.HasPrefix(path, "/v1/schedules/") && strings.Contains(path, "/runs"))
}
