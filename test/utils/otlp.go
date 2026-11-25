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
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Span is one exported span, flattened from the OTLP JSON the collector's
// file exporter writes (one ExportTraceServiceRequest per line).
type Span struct {
	TraceID      string
	SpanID       string
	ParentSpanID string
	Name         string
	// Attrs are the span attributes, values rendered as strings.
	Attrs map[string]string
	// Resource are the resource attributes of the emitting process.
	Resource map[string]string
}

// Attr returns the span attribute key, falling back to the resource
// attribute (for example roundtable.table, which knights set on both).
func (s Span) Attr(key string) (string, bool) {
	if v, ok := s.Attrs[key]; ok {
		return v, true
	}
	v, ok := s.Resource[key]
	return v, ok
}

type otlpValue struct {
	StringValue *string          `json:"stringValue"`
	IntValue    *json.RawMessage `json:"intValue"`
	DoubleValue *float64         `json:"doubleValue"`
	BoolValue   *bool            `json:"boolValue"`
	ArrayValue  *struct {
		Values []otlpValue `json:"values"`
	} `json:"arrayValue"`
}

type otlpKeyValue struct {
	Key   string    `json:"key"`
	Value otlpValue `json:"value"`
}

type otlpTraces struct {
	ResourceSpans []struct {
		Resource struct {
			Attributes []otlpKeyValue `json:"attributes"`
		} `json:"resource"`
		ScopeSpans []struct {
			Spans []struct {
				TraceID      string         `json:"traceId"`
				SpanID       string         `json:"spanId"`
				ParentSpanID string         `json:"parentSpanId"`
				Name         string         `json:"name"`
				Attributes   []otlpKeyValue `json:"attributes"`
			} `json:"spans"`
		} `json:"scopeSpans"`
	} `json:"resourceSpans"`
}

func (v otlpValue) String() string {
	switch {
	case v.StringValue != nil:
		return *v.StringValue
	case v.IntValue != nil:
		// OTLP JSON encodes int64 as a string; accept a bare number too.
		return strings.Trim(string(*v.IntValue), `"`)
	case v.DoubleValue != nil:
		return strconv.FormatFloat(*v.DoubleValue, 'f', -1, 64)
	case v.BoolValue != nil:
		return strconv.FormatBool(*v.BoolValue)
	case v.ArrayValue != nil:
		parts := make([]string, 0, len(v.ArrayValue.Values))
		for _, e := range v.ArrayValue.Values {
			parts = append(parts, e.String())
		}
		return "[" + strings.Join(parts, ",") + "]"
	}
	return ""
}

func attrMap(kvs []otlpKeyValue) map[string]string {
	m := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		m[kv.Key] = kv.Value.String()
	}
	return m
}

// ParseOTLPJSONLines reads the spans from OTLP JSON lines. Lines that are not
// JSON objects (the collector's own log output, when the file exporter writes
// to stdout) are skipped. Trace and span IDs are lower-cased hex.
func ParseOTLPJSONLines(out string) ([]Span, error) {
	var spans []Span
	for line := range strings.SplitSeq(out, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, `{"resourceSpans"`) {
			continue
		}
		var req otlpTraces
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			return nil, fmt.Errorf("parse OTLP JSON line: %w", err)
		}
		for _, rs := range req.ResourceSpans {
			res := attrMap(rs.Resource.Attributes)
			for _, ss := range rs.ScopeSpans {
				for _, s := range ss.Spans {
					spans = append(spans, Span{
						TraceID:      strings.ToLower(s.TraceID),
						SpanID:       strings.ToLower(s.SpanID),
						ParentSpanID: strings.ToLower(s.ParentSpanID),
						Name:         s.Name,
						Attrs:        attrMap(s.Attributes),
						Resource:     res,
					})
				}
			}
		}
	}
	return spans, nil
}

// SpansInTrace returns the spans of traceID, de-duplicated by span ID (an
// exporter retry can write a span twice).
func SpansInTrace(spans []Span, traceID string) []Span {
	traceID = strings.ToLower(traceID)
	seen := map[string]bool{}
	var out []Span
	for _, s := range spans {
		if s.TraceID != traceID || seen[s.SpanID] {
			continue
		}
		seen[s.SpanID] = true
		out = append(out, s)
	}
	return out
}

// SpansNamed returns the spans whose name is name.
func SpansNamed(spans []Span, name string) []Span {
	var out []Span
	for _, s := range spans {
		if s.Name == name {
			out = append(out, s)
		}
	}
	return out
}

// SpansWithPrefix returns the spans whose name starts with prefix.
func SpansWithPrefix(spans []Span, prefix string) []Span {
	var out []Span
	for _, s := range spans {
		if strings.HasPrefix(s.Name, prefix) {
			out = append(out, s)
		}
	}
	return out
}

// CheckConnected returns an error unless the spans form one tree: exactly one
// root (no parent) and every other span's parent among the spans.
func CheckConnected(spans []Span) error {
	ids := make(map[string]bool, len(spans))
	for _, s := range spans {
		ids[s.SpanID] = true
	}
	var roots, orphans []string
	for _, s := range spans {
		switch {
		case s.ParentSpanID == "":
			roots = append(roots, s.Name)
		case !ids[s.ParentSpanID]:
			orphans = append(orphans, fmt.Sprintf("%s (parent %s)", s.Name, s.ParentSpanID))
		}
	}
	sort.Strings(roots)
	sort.Strings(orphans)
	if len(roots) != 1 {
		return fmt.Errorf("want exactly one root span, got %d: %v", len(roots), roots)
	}
	if len(orphans) > 0 {
		return fmt.Errorf("spans whose parent is not in the trace: %v", orphans)
	}
	return nil
}

// IsDescendant reports whether span is below ancestor in spans.
func IsDescendant(spans []Span, span, ancestor Span) bool {
	byID := make(map[string]Span, len(spans))
	for _, s := range spans {
		byID[s.SpanID] = s
	}
	for cur, hops := span, 0; cur.ParentSpanID != "" && hops <= len(spans); hops++ {
		if cur.ParentSpanID == ancestor.SpanID {
			return true
		}
		next, ok := byID[cur.ParentSpanID]
		if !ok {
			return false
		}
		cur = next
	}
	return false
}
