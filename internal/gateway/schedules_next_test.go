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
	"errors"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Thursday before the 2026 European DST change (Sunday 29 March).
var nextTestNow = time.Date(2026, 3, 26, 12, 0, 0, 0, time.UTC)

func fireTimes(r *ScheduleNextRuns) []string {
	out := make([]string, 0, len(r.Times))
	for _, ft := range r.Times {
		out = append(out, ft.Time.Format(time.RFC3339))
	}
	return out
}

func TestNextFireTimesBerlinAcrossDST(t *testing.T) {
	r, err := nextFireTimes("0 7 * * 1-5", "Europe/Berlin", 4, nextTestNow)
	if err != nil {
		t.Fatal(err)
	}
	// 07:00 local is 06:00Z in CET and 05:00Z in CEST.
	want := []string{"2026-03-27T06:00:00Z", "2026-03-30T05:00:00Z", "2026-03-31T05:00:00Z", "2026-04-01T05:00:00Z"}
	if got := fireTimes(r); !slices.Equal(got, want) {
		t.Fatalf("times = %v, want %v", got, want)
	}
	if got := r.Times[0].Local; got != "2026-03-27T07:00:00+01:00" {
		t.Errorf("local[0] = %s", got)
	}
	if got := r.Times[1].Local; got != "2026-03-30T07:00:00+02:00" {
		t.Errorf("local[1] = %s", got)
	}
	if r.TimeZone != "Europe/Berlin" {
		t.Errorf("timeZone = %s", r.TimeZone)
	}
}

func TestNextFireTimesDefaultsAndStrictlyAfterNow(t *testing.T) {
	now := time.Date(2026, 3, 26, 7, 0, 0, 0, time.UTC)
	r, err := nextFireTimes("0 7 * * *", "", 2, now)
	if err != nil {
		t.Fatal(err)
	}
	if r.TimeZone != "UTC" {
		t.Errorf("timeZone = %s", r.TimeZone)
	}
	want := []string{"2026-03-27T07:00:00Z", "2026-03-28T07:00:00Z"}
	if got := fireTimes(r); !slices.Equal(got, want) {
		t.Fatalf("times = %v, want %v", got, want)
	}
}

func TestNextFireTimesInvalid(t *testing.T) {
	cases := []struct {
		name, expr, tz string
		count          int
		target         error
	}{
		{"bad cron", "not a cron", "", 5, ErrUnprocessable},
		{"empty cron", "", "", 5, ErrUnprocessable},
		{"bad zone", "0 7 * * *", "Mars/Base", 5, ErrUnprocessable},
		{"Local zone", "0 7 * * *", "Local", 5, ErrUnprocessable},
		{"TZ prefix", "TZ=UTC 0 7 * * *", "", 5, ErrUnprocessable},
		{"count zero", "0 7 * * *", "", 0, ErrInvalid},
		{"count too big", "0 7 * * *", "", maxNextCount + 1, ErrInvalid},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := nextFireTimes(c.expr, c.tz, c.count, nextTestNow)
			if !errors.Is(err, c.target) {
				t.Fatalf("err = %v, want %v", err, c.target)
			}
		})
	}
	if r, err := nextFireTimes("0 7 * * *", "", maxNextCount, nextTestNow); err != nil || len(r.Times) != maxNextCount {
		t.Fatalf("max count: %v, %v", r, err)
	}
}

func TestScheduleNextEndpoints(t *testing.T) {
	e := newScheduleEnv(t, true)
	e.svc.now = func() time.Time { return nextTestNow }

	resp, body := e.do(t, "GET", "/v1/schedules/"+testNS+"/morning/next?count=3", testToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d: %v", resp.StatusCode, body)
	}
	times := body["times"].([]any)
	if len(times) != 3 || times[1].(map[string]any)["time"] != "2026-03-30T05:00:00Z" ||
		times[1].(map[string]any)["local"] != "2026-03-30T07:00:00+02:00" {
		t.Errorf("times = %v", times)
	}
	if body["timeZone"] != "Europe/Berlin" || body["name"] != "morning" {
		t.Errorf("body = %v", body)
	}
	if _, ok := body["suspended"]; ok {
		t.Errorf("morning is not suspended: %v", body)
	}

	// Default count, and a suspended schedule still answers.
	resp, body = e.do(t, "GET", "/v1/schedules/"+testNS+"/nightly/next", testToken, nil)
	if resp.StatusCode != http.StatusOK || len(body["times"].([]any)) != defaultNextCount || body["suspended"] != true {
		t.Errorf("nightly: %d %v", resp.StatusCode, body)
	}

	for path, want := range map[string]int{
		"/v1/schedules/" + testNS + "/nope/next":                  http.StatusNotFound,
		"/v1/schedules/" + testNS + "/hook/next":                  http.StatusNotFound,
		"/v1/schedules/" + testNS + "/morning/next?count=0":       http.StatusBadRequest,
		"/v1/schedules/" + testNS + "/morning/next?count=51":      http.StatusBadRequest,
		"/v1/schedules/" + testNS + "/morning/next?count=x":       http.StatusBadRequest,
		"/v1/schedules/preview?cron=0+7+*+*+*&timeZone=Mars/Base": http.StatusUnprocessableEntity,
		"/v1/schedules/preview?cron=nope":                         http.StatusUnprocessableEntity,
		"/v1/schedules/preview":                                   http.StatusUnprocessableEntity,
		"/v1/schedules/preview?cron=0+7+*+*+*&count=51":           http.StatusBadRequest,
	} {
		if resp, body := e.do(t, "GET", path, testToken, nil); resp.StatusCode != want {
			t.Errorf("%s = %d, want %d (%v)", path, resp.StatusCode, want, body)
		}
	}

	resp, body = e.do(t, "GET", "/v1/schedules/preview?cron=0+7+*+*+1-5&timeZone=Europe/Berlin&count=2", testToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("preview status = %d: %v", resp.StatusCode, body)
	}
	times = body["times"].([]any)
	if len(times) != 2 || times[0].(map[string]any)["time"] != "2026-03-27T06:00:00Z" {
		t.Errorf("preview times = %v", times)
	}
}

func TestMCPScheduleNextRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	e := newScheduleEnv(t, true)
	e.svc.now = func() time.Time { return nextTestNow }
	cs := connectMCP(ctx, t, e.server.URL)

	call := func(args map[string]any) *mcp.CallToolResult {
		t.Helper()
		res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "schedule_next_runs", Arguments: args})
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	if res := call(map[string]any{"namespace": testNS, "name": "morning", "count": 2}); res.IsError {
		t.Errorf("by name: %v", res.Content)
	}
	if res := call(map[string]any{"cron": "0 7 * * *", "timeZone": "Europe/Berlin"}); res.IsError {
		t.Errorf("by cron: %v", res.Content)
	}
	for _, args := range []map[string]any{
		{},
		{"name": "morning", "cron": "0 7 * * *"},
		{"cron": "nope"},
		{"name": "nope"},
	} {
		if res := call(args); !res.IsError {
			t.Errorf("%v: want a tool error", args)
		}
	}
}
