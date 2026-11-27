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
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func connectMCP(ctx context.Context, t *testing.T, url string) *mcp.ClientSession {
	t.Helper()
	c := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "v0"}, nil)
	cs, err := c.Connect(ctx, &mcp.StreamableClientTransport{
		Endpoint:   url + "/mcp",
		HTTPClient: &http.Client{Transport: bearer{http.DefaultTransport}},
		MaxRetries: -1,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

func mcpToolNames(ctx context.Context, t *testing.T, cs *mcp.ClientSession) map[string]bool {
	t.Helper()
	tools, err := cs.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, tl := range tools.Tools {
		names[tl.Name] = true
	}
	return names
}

func TestMCPScheduleToolsListedOnlyWhenEnabled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	schedTools := []string{"list_schedules", "get_schedule", "suspend_schedule", "resume_schedule", "run_schedule"}
	runTools := []string{"list_schedule_runs", "get_schedule_run"}

	off := mcpToolNames(ctx, t, connectMCP(ctx, t, newActionEnv(t, false).server.URL))
	for _, n := range append(append([]string{}, schedTools...), runTools...) {
		if off[n] {
			t.Errorf("%s listed with schedules disabled", n)
		}
	}

	e := newActionEnv(t, true)
	on := mcpToolNames(ctx, t, connectMCP(ctx, t, e.server.URL))
	for _, n := range schedTools {
		if !on[n] {
			t.Errorf("%s not listed", n)
		}
	}
	for _, n := range runTools {
		if on[n] {
			t.Errorf("%s listed without the run-history reader", n)
		}
	}
	on = mcpToolNames(ctx, t, connectMCP(ctx, t, newRunsEnv(t).server.URL))
	for _, n := range runTools {
		if !on[n] {
			t.Errorf("%s not listed with the run-history reader", n)
		}
	}
}

func TestMCPSchedules(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	e := newActionEnv(t, true)
	cs := connectMCP(ctx, t, e.server.URL)

	call := func(name string, args map[string]any) (*mcp.CallToolResult, map[string]any) {
		t.Helper()
		res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var sc map[string]any
		b, _ := json.Marshal(res.StructuredContent)
		_ = json.Unmarshal(b, &sc)
		return res, sc
	}

	res, sc := call("list_schedules", map[string]any{})
	if res.IsError || len(sc["schedules"].([]any)) < 2 {
		t.Fatalf("list_schedules: %+v %v", res, sc)
	}
	res, sc = call("get_schedule", map[string]any{"name": "git-digest"})
	if res.IsError || sc["name"] != "git-digest" {
		t.Fatalf("get_schedule: %+v %v", res, sc)
	}
	res, sc = call("suspend_schedule", map[string]any{"name": "git-digest"})
	if res.IsError || sc["suspended"] != true {
		t.Fatalf("suspend_schedule: %+v %v", res, sc)
	}
	if !e.trigger(t, "git-digest").Suspended() {
		t.Fatal("schedule not suspended")
	}
	res, sc = call("resume_schedule", map[string]any{"name": "git-digest"})
	if res.IsError || sc["suspended"] != false {
		t.Fatalf("resume_schedule: %+v %v", res, sc)
	}
	if res, _ = call("suspend_schedule", map[string]any{"name": "nope"}); !res.IsError {
		t.Fatal("suspending a missing schedule should be a tool error")
	}

	go e.playTrigger(t, "git-digest", "")
	res, sc = call("run_schedule", map[string]any{"name": "git-digest"})
	if res.IsError || sc["state"] != ScheduleRunStarted {
		t.Fatalf("run_schedule: %+v %v", res, sc)
	}
}
