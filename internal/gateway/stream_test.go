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
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nats-io/nats.go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

type sseEvent struct {
	Name, ID, Data string
	Comment        bool
}

// sseStream is an open event stream; Events delivers parsed events and is
// closed when the stream ends.
type sseStream struct {
	resp   *http.Response
	Events chan sseEvent
}

func openSSE(t *testing.T, url string, header map[string]string) *sseStream {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	s := &sseStream{resp: resp, Events: make(chan sseEvent, 256)}
	go func() {
		defer close(s.Events)
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		var cur sseEvent
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "":
				if cur.Name != "" || cur.Comment {
					s.Events <- cur
				}
				cur = sseEvent{}
			case strings.HasPrefix(line, ":"):
				cur.Comment = true
			case strings.HasPrefix(line, "event: "):
				cur.Name = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "id: "):
				cur.ID = strings.TrimPrefix(line, "id: ")
			case strings.HasPrefix(line, "data: "):
				cur.Data = strings.TrimPrefix(line, "data: ")
			}
		}
	}()
	return s
}

// next returns the next event, failing the test on timeout or end of stream.
func (s *sseStream) next(t *testing.T) sseEvent {
	t.Helper()
	select {
	case ev, ok := <-s.Events:
		if !ok {
			t.Fatal("stream ended")
		}
		return ev
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for an event")
	}
	return sseEvent{}
}

// nextNamed skips comments and returns the next named event.
func (s *sseStream) nextNamed(t *testing.T) sseEvent {
	t.Helper()
	for {
		if ev := s.next(t); !ev.Comment {
			return ev
		}
	}
}

// expectEnd waits for the stream to close.
func (s *sseStream) expectEnd(t *testing.T) {
	t.Helper()
	for {
		select {
		case _, ok := <-s.Events:
			if !ok {
				return
			}
		case <-time.After(10 * time.Second):
			t.Fatal("stream did not end")
		}
	}
}

func publishActivity(t *testing.T, nc *nats.Conn, taskID string, seq int64, typ natspkg.ActivityType, tool string) {
	t.Helper()
	data, _ := json.Marshal(natspkg.ActivityEvent{V: 1, Type: typ, Knight: "operator", TaskID: taskID, Seq: seq, Tool: tool})
	if err := nc.Publish(natspkg.ActivitySubject(prefix, "operator", taskID), data); err != nil {
		t.Fatal(err)
	}
	_ = nc.Flush()
}

func startHeldTask(t *testing.T, e *env) string {
	t.Helper()
	resp, out := e.do(t, http.MethodPost, "/v1/knights/operator/invoke", testToken,
		map[string]any{"task": "hold on", "timeoutSeconds": 0})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("invoke: %d %v", resp.StatusCode, out)
	}
	return out["taskId"].(string)
}

func TestTaskEventsStreamActivityThenResult(t *testing.T) {
	e := newEnv(t, Options{StreamHeartbeat: 50 * time.Millisecond})
	taskID := startHeldTask(t, e)

	s := openSSE(t, e.server.URL+"/v1/tasks/"+taskID+"/events", nil)
	if ct := s.resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type %q", ct)
	}
	// The heartbeat comment shows the stream is open and subscribed.
	if ev := s.next(t); !ev.Comment {
		t.Fatalf("expected a heartbeat comment first, got %+v", ev)
	}

	publishActivity(t, e.nc, taskID, 1, natspkg.ActivityTaskStarted, "")
	publishActivity(t, e.nc, taskID, 2, natspkg.ActivityToolStarted, "Bash")
	publishActivity(t, e.nc, taskID, 3, natspkg.ActivityToolFinished, "Bash")
	for i, want := range []natspkg.ActivityType{natspkg.ActivityTaskStarted, natspkg.ActivityToolStarted, natspkg.ActivityToolFinished} {
		ev := s.nextNamed(t)
		var a natspkg.ActivityEvent
		if ev.Name != "activity" || json.Unmarshal([]byte(ev.Data), &a) != nil || a.Type != want || a.Seq != int64(i+1) {
			t.Fatalf("event %d: %+v (parsed %+v), want activity %s seq %d", i, ev, a, want, i+1)
		}
	}

	close(e.knight.release)
	ev := s.nextNamed(t)
	var rec TaskRecord
	if ev.Name != "result" || ev.ID != "result" || json.Unmarshal([]byte(ev.Data), &rec) != nil {
		t.Fatalf("expected the result event, got %+v", ev)
	}
	if rec.TaskID != taskID || rec.Status != TaskSucceeded || !strings.Contains(rec.Output, "hold on") {
		t.Fatalf("result record: %+v", rec)
	}
	for ev := range s.Events { // only heartbeats may follow, then the stream closes
		if !ev.Comment {
			t.Fatalf("event after the result: %+v", ev)
		}
	}
	// The result was recorded like a poll would have.
	if got, err := e.svc.GetTask(context.Background(), taskID); err != nil || got.Status != TaskSucceeded {
		t.Fatalf("recorded task: %+v, %v", got, err)
	}
}

func TestTaskEventsFinishedTaskReturnsResultAtOnce(t *testing.T) {
	e := newEnv(t, Options{})
	resp, out := e.do(t, http.MethodPost, "/v1/knights/operator/invoke", testToken, map[string]any{"task": "hello"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("invoke: %d %v", resp.StatusCode, out)
	}
	taskID := out["taskId"].(string)

	s := openSSE(t, e.server.URL+"/v1/tasks/"+taskID+"/events", nil)
	ev := s.nextNamed(t)
	if ev.Name != "result" || !strings.Contains(ev.Data, `"status":"succeeded"`) {
		t.Fatalf("got %+v", ev)
	}
	s.expectEnd(t)

	// A client that already saw the result is told not to reconnect.
	r := openSSE(t, e.server.URL+"/v1/tasks/"+taskID+"/events", map[string]string{"Last-Event-ID": "result"})
	if r.resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status %d, want 204", r.resp.StatusCode)
	}
}

func TestTaskEventsUnknownTask(t *testing.T) {
	e := newEnv(t, Options{})
	resp, _ := e.do(t, http.MethodGet, "/v1/tasks/nope/events", testToken, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status %d", resp.StatusCode)
	}
	resp, _ = e.do(t, http.MethodGet, "/v1/tasks/nope/events", "", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status %d", resp.StatusCode)
	}
}

func TestTaskEventsMaxStreamsAndShutdown(t *testing.T) {
	e := newEnv(t, Options{MaxStreams: 1, StreamHeartbeat: 50 * time.Millisecond})
	taskID := startHeldTask(t, e)

	s := openSSE(t, e.server.URL+"/v1/tasks/"+taskID+"/events", nil)
	s.next(t) // heartbeat: the stream holds its slot

	second := openSSE(t, e.server.URL+"/v1/tasks/"+taskID+"/events", nil)
	if second.resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second stream status %d, want 429", second.resp.StatusCode)
	}

	e.svc.CloseStreams()
	s.expectEnd(t)
	third := openSSE(t, e.server.URL+"/v1/tasks/"+taskID+"/events", nil)
	if third.resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("stream after shutdown: status %d, want 503", third.resp.StatusCode)
	}
	close(e.knight.release)
}

func TestRunEventsStreamsStepChangesAndResult(t *testing.T) {
	e := newRunEnv(t, Options{StreamHeartbeat: 50 * time.Millisecond}, true)
	ns := runNATS(t)
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	bus, err := NewNATSBus(context.Background(), nc, "gateway_tasks_runs_test", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	e.svc.Bus = bus

	resp, out := e.do(t, http.MethodPost, "/v1/chains/digest/runs", testToken, map[string]any{})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("start: %d %v", resp.StatusCode, out)
	}
	runName := out["run"].(string)
	s := openSSE(t, e.server.URL+"/v1/runs/"+testNS+"/"+runName+"/events", nil)
	if s.resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", s.resp.StatusCode)
	}

	update := func(phase aiv1alpha1.ChainPhase, steps ...aiv1alpha1.ChainStepStatus) {
		t.Helper()
		c := &e.runs(t)[0]
		now := metav1.Now()
		c.Status.Phase, c.Status.RunID = phase, "run-123"
		c.Status.LastRunTrigger = c.Annotations[aiv1alpha1.AnnotationChainRun]
		c.Status.StartedAt = &now
		if phase != aiv1alpha1.ChainPhaseRunning {
			c.Status.CompletedAt = &now
		}
		c.Status.StepStatuses = steps
		if err := e.k8s.Status().Update(context.Background(), c); err != nil {
			t.Fatal(err)
		}
	}
	expectStep := func(name, phase string) {
		t.Helper()
		ev := s.nextNamed(t)
		var st stepEvent
		if ev.Name != "step" || json.Unmarshal([]byte(ev.Data), &st) != nil || st.Name != name || st.Phase != phase {
			t.Fatalf("got %+v, want step %s %s", ev, name, phase)
		}
	}

	update(aiv1alpha1.ChainPhaseRunning,
		aiv1alpha1.ChainStepStatus{Name: "fetch", Phase: aiv1alpha1.ChainStepPhaseRunning, TaskID: "chain-fetch-1"})
	expectStep("fetch", "Running")

	// Activity of the running step is forwarded.
	deadline := time.After(10 * time.Second)
	var ev sseEvent
	for ev.Name != "activity" {
		publishActivity(t, nc, "chain-fetch-1", 1, natspkg.ActivityToolStarted, "WebFetch")
		select {
		case got, ok := <-s.Events:
			if !ok {
				t.Fatal("stream ended")
			}
			ev = got
		case <-deadline:
			t.Fatal("no activity event")
		}
	}
	if !strings.Contains(ev.Data, `"tool":"WebFetch"`) {
		t.Fatalf("activity: %s", ev.Data)
	}

	update(aiv1alpha1.ChainPhaseRunning,
		aiv1alpha1.ChainStepStatus{Name: "fetch", Phase: aiv1alpha1.ChainStepPhaseSucceeded, TaskID: "chain-fetch-1", Output: "fetched"},
		aiv1alpha1.ChainStepStatus{Name: "summarise", Phase: aiv1alpha1.ChainStepPhaseRunning})
	var got []string
	for len(got) < 2 {
		ev := s.nextNamed(t)
		if ev.Name == "step" {
			var st stepEvent
			_ = json.Unmarshal([]byte(ev.Data), &st)
			got = append(got, st.Name+"="+st.Phase)
			if st.Name == "fetch" && st.Output != "fetched" {
				t.Fatalf("step output %q", st.Output)
			}
		}
	}
	if strings.Join(got, ",") != "fetch=Succeeded,summarise=Running" {
		t.Fatalf("step changes: %v", got)
	}

	update(aiv1alpha1.ChainPhaseSucceeded,
		aiv1alpha1.ChainStepStatus{Name: "fetch", Phase: aiv1alpha1.ChainStepPhaseSucceeded, Output: "fetched"},
		aiv1alpha1.ChainStepStatus{Name: "summarise", Phase: aiv1alpha1.ChainStepPhaseSucceeded, Output: "the summary"})
	expectStep("summarise", "Succeeded")
	ev = s.nextNamed(t)
	var rec RunRecord
	if ev.Name != "result" || json.Unmarshal([]byte(ev.Data), &rec) != nil {
		t.Fatalf("expected the result event, got %+v", ev)
	}
	if !rec.Done || rec.Phase != "Succeeded" || rec.Output != "the summary" {
		t.Fatalf("result record: %+v", rec)
	}
	s.expectEnd(t)
}

func TestRunEventsFinishedRunAndErrors(t *testing.T) {
	e := newRunEnv(t, Options{}, true)
	resp, _ := e.do(t, http.MethodPost, "/v1/chains/digest/runs", testToken, map[string]any{})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("start: %d", resp.StatusCode)
	}
	e.finish(t, aiv1alpha1.ChainPhaseSucceeded, map[string]string{"summarise": "done"})
	name := e.runs(t)[0].Name

	s := openSSE(t, e.server.URL+"/v1/runs/"+testNS+"/"+name+"/events", nil)
	var names []string
	for ev := range s.Events {
		if !ev.Comment {
			names = append(names, ev.Name)
		}
	}
	if strings.Join(names, ",") != "step,step,result" {
		t.Fatalf("events: %v", names)
	}

	// Not a gateway run, and runs disabled.
	if r, _ := e.do(t, http.MethodGet, "/v1/runs/"+testNS+"/digest/events", testToken, nil); r.StatusCode != http.StatusNotFound {
		t.Fatalf("template chain: %d", r.StatusCode)
	}
	off := newRunEnv(t, Options{}, false)
	if r, _ := off.do(t, http.MethodGet, "/v1/runs/"+testNS+"/x/events", testToken, nil); r.StatusCode != http.StatusNotFound {
		t.Fatalf("runs disabled: %d", r.StatusCode)
	}
}

func TestMCPInvokeKnightReportsProgress(t *testing.T) {
	e := newEnv(t, Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var mu sync.Mutex
	var msgs []string
	c := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "v0"}, &mcp.ClientOptions{
		ProgressNotificationHandler: func(_ context.Context, req *mcp.ProgressNotificationClientRequest) {
			mu.Lock()
			msgs = append(msgs, req.Params.Message)
			mu.Unlock()
		},
	})
	cs, err := c.Connect(ctx, &mcp.StreamableClientTransport{
		Endpoint:   e.server.URL + "/mcp",
		HTTPClient: &http.Client{Transport: bearer{http.DefaultTransport}},
		MaxRetries: -1,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cs.Close() }()

	// Publish activity until the call returns, as a running knight would.
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
				for _, id := range heldTaskIDs(e) {
					publishActivity(t, e.nc, id, 1, natspkg.ActivityToolStarted, "Bash")
				}
			}
		}
	}()
	go func() {
		time.Sleep(700 * time.Millisecond)
		close(e.knight.release)
	}()

	params := &mcp.CallToolParams{Name: "invoke_knight", Arguments: map[string]any{"knight": "operator", "task": "hold on"}}
	params.SetProgressToken("tok-1")
	res, err := cs.CallTool(ctx, params)
	close(stop)
	<-done
	if err != nil || res.IsError {
		t.Fatalf("call: %v %+v", err, res)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(msgs) == 0 || !strings.Contains(msgs[0], "tool Bash") {
		t.Fatalf("progress messages: %q", msgs)
	}
}

func heldTaskIDs(e *env) []string {
	recs, _ := e.svc.ListTasks(context.Background(), TaskFilter{Status: TaskPending})
	ids := make([]string, 0, len(recs))
	for _, r := range recs {
		ids = append(ids, r.TaskID)
	}
	return ids
}
