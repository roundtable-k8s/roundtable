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
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"

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
		metrics.GatewayRequestsTotal.WithLabelValues(route, strconv.Itoa(code), table, kind).Inc()
		metrics.GatewayRequestDurationSeconds.WithLabelValues(route).Observe(elapsed.Seconds())
		if route != healthRoute {
			log.Info("Request", "method", r.Method, "route", route, "code", code, "durationMs", elapsed.Milliseconds(),
				"caller", caller, "callerKind", kind, "table", table)
		}
	})
}
