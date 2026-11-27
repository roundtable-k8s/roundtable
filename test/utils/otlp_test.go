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

package utils

import (
	"strings"
	"testing"
)

// Two export lines as the collector file exporter writes them, mixed with
// collector log output (the exporter writes to /dev/stdout in the e2e test).
//
//nolint:lll // OTLP JSON export lines are long by nature.
const otlpLines = `2026-11-25T10:00:00.000Z	info	service@v0.120.0/service.go:1	Everything is ready.
{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"roundtable-operator"}}]},"scopeSpans":[{"scope":{"name":"x"},"spans":[{"traceId":"0AF7651916CD43DD8448EB211C80319C","spanId":"00f067aa0ba902b7","parentSpanId":"","name":"chain demo","kind":1,"attributes":[{"key":"roundtable.table","value":{"stringValue":"t1"}},{"key":"roundtable.attempt","value":{"intValue":"1"}}]},{"traceId":"0af7651916cd43dd8448eb211c80319c","spanId":"1111111111111111","parentSpanId":"00f067aa0ba902b7","name":"step a","kind":1,"attributes":[]}]}]}]}
{"resourceSpans":[{"resource":{"attributes":[{"key":"roundtable.table","value":{"stringValue":"t1"}}]},"scopeSpans":[{"scope":{},"spans":[{"traceId":"0af7651916cd43dd8448eb211c80319c","spanId":"2222222222222222","parentSpanId":"1111111111111111","name":"chat stub","attributes":[{"key":"roundtable.cost.usd","value":{"doubleValue":0.0125}},{"key":"ok","value":{"boolValue":true}}]},{"traceId":"0af7651916cd43dd8448eb211c80319c","spanId":"2222222222222222","parentSpanId":"1111111111111111","name":"chat stub","attributes":[]},{"traceId":"ffffffffffffffffffffffffffffffff","spanId":"3333333333333333","name":"other"}]}]}]}
`

func TestParseOTLPJSONLines(t *testing.T) {
	spans, err := ParseOTLPJSONLines(otlpLines)
	if err != nil {
		t.Fatal(err)
	}
	if len(spans) != 5 {
		t.Fatalf("got %d spans, want 5", len(spans))
	}
	root := spans[0]
	if root.TraceID != "0af7651916cd43dd8448eb211c80319c" || root.Attrs["roundtable.attempt"] != "1" {
		t.Fatalf("root = %+v", root)
	}
	if got := spans[2].Attrs["roundtable.cost.usd"]; got != "0.0125" {
		t.Fatalf("cost = %q", got)
	}
	if v, ok := spans[2].Attr("roundtable.table"); !ok || v != "t1" {
		t.Fatalf("resource fallback = %q %v", v, ok)
	}
	if spans[2].Attrs["ok"] != "true" {
		t.Fatalf("bool = %q", spans[2].Attrs["ok"])
	}

	trace := SpansInTrace(spans, "0AF7651916CD43DD8448EB211C80319C")
	if len(trace) != 3 {
		t.Fatalf("trace has %d spans, want 3 (duplicate dropped)", len(trace))
	}
	if err := CheckConnected(trace); err != nil {
		t.Fatal(err)
	}
	if !IsDescendant(trace, trace[2], trace[0]) || IsDescendant(trace, trace[0], trace[2]) {
		t.Fatal("IsDescendant")
	}
	if n := len(SpansWithPrefix(trace, "chat ")); n != 1 {
		t.Fatalf("chat spans = %d", n)
	}
	if n := len(SpansNamed(trace, "step a")); n != 1 {
		t.Fatalf("step spans = %d", n)
	}
}

func TestParseOTLPJSONLinesBadLine(t *testing.T) {
	if _, err := ParseOTLPJSONLines(`{"resourceSpans":[`); err == nil {
		t.Fatal("want error for truncated JSON")
	}
}

func TestCheckConnected(t *testing.T) {
	cases := map[string]struct {
		spans []Span
		want  string
	}{
		"two roots": {
			spans: []Span{{SpanID: "a", Name: "chain x"}, {SpanID: "b", Name: "invoke_agent k"}},
			want:  "exactly one root",
		},
		"orphan": {
			spans: []Span{{SpanID: "a", Name: "chain x"}, {SpanID: "b", ParentSpanID: "z", Name: "chat m"}},
			want:  "chat m (parent z)",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := CheckConnected(tc.spans)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestParseTempoTrace(t *testing.T) {
	body := `{"trace":{"resourceSpans":[{"resource":{"attributes":[{"key":"roundtable.table","value":{"stringValue":"t"}}]},` +
		`"scopeSpans":[{"spans":[` +
		`{"traceId":"AAECAwQFBgcICQoLDA0ODw==","spanId":"AAECAwQFBgc=","parentSpanId":"","name":"root","attributes":[{"key":"k","value":{"intValue":"3"}}]},` +
		`{"traceId":"000102030405060708090a0b0c0d0e0f","spanId":"0807060504030201","parentSpanId":"0001020304050607","name":"child"}]}]}]}}`
	spans, err := ParseTempoTrace(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(spans) != 2 {
		t.Fatalf("want 2 spans, got %d", len(spans))
	}
	if spans[0].TraceID != "000102030405060708090a0b0c0d0e0f" || spans[0].SpanID != "0001020304050607" || spans[0].ParentSpanID != "" {
		t.Errorf("base64 ids not normalized: %+v", spans[0])
	}
	if spans[1].ParentSpanID != "0001020304050607" || spans[1].SpanID != "0807060504030201" {
		t.Errorf("hex ids changed: %+v", spans[1])
	}
	if v, _ := spans[0].Attr("roundtable.table"); v != "t" || spans[0].Attrs["k"] != "3" {
		t.Errorf("attributes: %+v", spans[0])
	}
	if err := CheckConnected(spans); err != nil {
		t.Error(err)
	}
}
