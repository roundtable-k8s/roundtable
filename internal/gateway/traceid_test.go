package gateway

import (
	"encoding/json"
	"strings"
	"testing"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

const testTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"

func TestValidTraceID(t *testing.T) {
	for id, want := range map[string]string{
		testTraceID:                         testTraceID,
		"":                                  "",
		"short":                             "",
		strings.Repeat("0", 32):             "",
		strings.ToUpper(testTraceID):        "",
		strings.Repeat("g", 32):             "",
		testTraceID + "0":                   "",
		"4bf92f3577b34da6a3ce929d0e0e473\n": "",
	} {
		if got := validTraceID(id); got != want {
			t.Errorf("validTraceID(%q) = %q, want %q", id, got, want)
		}
	}
}

func TestTraceIDOnRecords(t *testing.T) {
	for name, tc := range map[string]struct {
		in   string
		want string
	}{"set": {testTraceID, testTraceID}, "empty": {"", ""}, "invalid": {"not-a-trace-id", ""}} {
		t.Run(name, func(t *testing.T) {
			c := &aiv1alpha1.Chain{}
			c.Status.TraceID = tc.in
			if got := runRecord(c, false).TraceID; got != tc.want {
				t.Errorf("runRecord TraceID = %q, want %q", got, tc.want)
			}
			if got := chainScheduleRun(c).TraceID; got != tc.want {
				t.Errorf("chainScheduleRun TraceID = %q, want %q", got, tc.want)
			}
			m := &aiv1alpha1.Mission{}
			m.Status.TraceID = tc.in
			if got := (&Service{}).missionView(t.Context(), m, false).TraceID; got != tc.want {
				t.Errorf("missionView TraceID = %q, want %q", got, tc.want)
			}
			b, err := json.Marshal(runRecord(c, false))
			if err != nil {
				t.Fatal(err)
			}
			if has := strings.Contains(string(b), `"traceId"`); has != (tc.want != "") {
				t.Errorf("JSON %s: traceId present = %v", b, has)
			}
		})
	}
}
