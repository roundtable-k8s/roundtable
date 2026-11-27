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
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/roundtable-k8s/roundtable/internal/deadletter"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// seedDeadLetters creates the dead-letter stream and state bucket the way the
// operator does and returns a function adding one entry.
func seedDeadLetters(t *testing.T, e *env) func(entry deadletter.Entry, state deadletter.State) {
	t.Helper()
	ctx := context.Background()
	js, _ := jetstream.New(e.nc)
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "rt_dev_tasks_DLQ", Subjects: []string{prefix + ".dlq.>"}}); err != nil {
		t.Fatal(err)
	}
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: deadletter.KVBucket, History: 5})
	if err != nil {
		t.Fatal(err)
	}
	return func(entry deadletter.Entry, state deadletter.State) {
		t.Helper()
		if entry.Table == "" {
			entry.Table = "dev"
		}
		if entry.Attempt == 0 {
			entry.Attempt = 1
		}
		if entry.DeadLetteredAt.IsZero() {
			entry.DeadLetteredAt = time.Now().UTC()
		}
		subject := deadletter.Subject(prefix, entry.Knight, entry.TaskID)
		body, _ := json.Marshal(entry)
		if _, err := js.Publish(ctx, subject, body); err != nil {
			t.Fatal(err)
		}
		rec, _ := json.Marshal(deadletter.Record{State: state, Reason: entry.Reason, Knight: entry.Knight,
			Attempt: entry.Attempt, Subject: subject, UpdatedAt: entry.DeadLetteredAt, DeadLetteredAt: entry.DeadLetteredAt})
		if _, err := kv.Put(ctx, deadletter.Key(entry.Table, entry.TaskID), rec); err != nil {
			t.Fatal(err)
		}
	}
}

// countTasks counts the tasks with this id the fake knight received (one per
// subject, so it is 0 or 1; use it to wait for delivery).
func countTasks(t *testing.T, e *env, id string) int {
	t.Helper()
	e.knight.mu.Lock()
	defer e.knight.mu.Unlock()
	n := 0
	for _, p := range e.knight.seen {
		if p.TaskID == id {
			n++
		}
	}
	return n
}

func TestDeadLettersListAndRetry(t *testing.T) {
	e := newEnv(t, Options{})
	add := seedDeadLetters(t, e)
	add(deadletter.Entry{TaskID: "t-lost", Knight: "reviewer", Reason: deadletter.ReasonLost, Error: "knight went away",
		Task: &natspkg.TaskPayload{TaskID: "t-lost", Task: "review it", RunID: "run-1", Model: "opus"}}, deadletter.StateOpen)
	add(deadletter.Entry{TaskID: "t-failed", Knight: "operator", Reason: deadletter.ReasonFailed, Error: "boom",
		Task: &natspkg.TaskPayload{TaskID: "t-failed", Task: "do it"}}, deadletter.StateResolved)
	add(deadletter.Entry{TaskID: "t-chain", Knight: "reviewer", Reason: deadletter.ReasonFailed, ChainName: "c1",
		Task: &natspkg.TaskPayload{TaskID: "t-chain", ChainName: "c1", Task: "step"}}, deadletter.StateOpen)
	add(deadletter.Entry{TaskID: "t-nopayload", Knight: "reviewer", Reason: deadletter.ReasonLost}, deadletter.StateOpen)

	list := func(q string) []any {
		t.Helper()
		resp, body := e.do(t, "GET", "/v1/dead-letters"+q, testToken, nil)
		if resp.StatusCode != 200 {
			t.Fatalf("list%s: %d %v", q, resp.StatusCode, body)
		}
		return body["deadLetters"].([]any)
	}
	if n := len(list("")); n != 4 {
		t.Fatalf("all: %d", n)
	}
	if n := len(list("?state=open")); n != 3 {
		t.Fatalf("state=open: %d", n)
	}
	if n := len(list("?knight=operator")); n != 1 {
		t.Fatalf("knight: %d", n)
	}
	if n := len(list("?reason=lost&state=open")); n != 2 {
		t.Fatalf("reason: %d", n)
	}
	if n := len(list("?limit=2")); n != 2 {
		t.Fatalf("limit: %d", n)
	}

	resp, body := e.do(t, "GET", "/v1/dead-letters/t-lost", testToken, nil)
	if resp.StatusCode != 200 || body["state"] != "open" || body["task"] == nil || body["error"] != "knight went away" {
		t.Fatalf("get: %d %v", resp.StatusCode, body)
	}
	if resp, _ := e.do(t, "GET", "/v1/dead-letters/nope", testToken, nil); resp.StatusCode != 404 {
		t.Fatalf("get missing: %d", resp.StatusCode)
	}

	// Retry publishes exactly one task with attempt 2.
	resp, body = e.do(t, "POST", "/v1/dead-letters/t-lost/retry", testToken, nil)
	if resp.StatusCode != http.StatusAccepted || body["taskId"] != "t-lost-r2" {
		t.Fatalf("retry: %d %v", resp.StatusCode, body)
	}
	deadline := time.Now().Add(5 * time.Second)
	for countTasks(t, e, "t-lost-r2") == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	p := e.knight.lastPayload()
	if p.TaskID != "t-lost-r2" || p.Attempt != 2 || p.RetryOf != "t-lost" || p.PreviousFailure != "knight went away" ||
		p.Task != "review it" || p.RunID != "run-1" || p.Model != "opus" {
		t.Fatalf("retried payload: %+v", p)
	}
	if resp, rec := e.do(t, "GET", "/v1/tasks/t-lost-r2", testToken, nil); resp.StatusCode != 200 || rec["knight"] != "reviewer" {
		t.Fatalf("task record: %d %v", resp.StatusCode, rec)
	}
	if _, body := e.do(t, "GET", "/v1/dead-letters/t-lost", testToken, nil); body["state"] != "retried" || body["retriedAs"] != "t-lost-r2" {
		t.Fatalf("after retry: %v", body)
	}

	// Second retry, and a resolved entry: 409 with the state.
	resp, body = e.do(t, "POST", "/v1/dead-letters/t-lost/retry", testToken, nil)
	if resp.StatusCode != 409 || body["state"] != "retried" {
		t.Fatalf("second retry: %d %v", resp.StatusCode, body)
	}
	resp, body = e.do(t, "POST", "/v1/dead-letters/t-failed/retry", testToken, nil)
	if resp.StatusCode != 409 || body["state"] != "resolved" {
		t.Fatalf("resolved retry: %d %v", resp.StatusCode, body)
	}
	if n := countTasks(t, e, "t-lost-r2"); n != 1 {
		t.Fatalf("published %d times", n)
	}

	// Chain-owned: 409 without force, retried with force.
	resp, body = e.do(t, "POST", "/v1/dead-letters/t-chain/retry", testToken, nil)
	if resp.StatusCode != 409 || !strings.Contains(body["error"].(string), "rerun the chain") {
		t.Fatalf("chain retry: %d %v", resp.StatusCode, body)
	}
	resp, body = e.do(t, "POST", "/v1/dead-letters/t-chain/retry", testToken, map[string]any{"force": true})
	if resp.StatusCode != http.StatusAccepted || body["taskId"] != "t-chain-r2" {
		t.Fatalf("forced chain retry: %d %v", resp.StatusCode, body)
	}

	// No payload: 422.
	if resp, body = e.do(t, "POST", "/v1/dead-letters/t-nopayload/retry", testToken, nil); resp.StatusCode != 422 {
		t.Fatalf("no payload: %d %v", resp.StatusCode, body)
	}
	if resp, _ = e.do(t, "POST", "/v1/dead-letters/missing/retry", testToken, nil); resp.StatusCode != 404 {
		t.Fatalf("missing: %d", resp.StatusCode)
	}
}

func TestDeadLetterConcurrentRetryPublishesOnce(t *testing.T) {
	e := newEnv(t, Options{})
	add := seedDeadLetters(t, e)
	add(deadletter.Entry{TaskID: "t-race", Knight: "reviewer", Reason: deadletter.ReasonLost,
		Task: &natspkg.TaskPayload{TaskID: "t-race", Task: "x"}}, deadletter.StateOpen)

	var wg sync.WaitGroup
	var mu sync.Mutex
	codes := map[int]int{}
	for range 8 {
		wg.Go(func() {
			resp, _ := e.do(t, "POST", "/v1/dead-letters/t-race/retry", testToken, nil)
			mu.Lock()
			codes[resp.StatusCode]++
			mu.Unlock()
		})
	}
	wg.Wait()
	if codes[202] != 1 || codes[409] != 7 {
		t.Fatalf("status codes: %v", codes)
	}
}

func TestDeadLettersTotalBeyondLimit(t *testing.T) {
	e := newEnv(t, Options{})
	add := seedDeadLetters(t, e)
	for i := range 137 {
		id := fmt.Sprintf("t-%03d", i)
		add(deadletter.Entry{TaskID: id, Knight: "reviewer", Reason: deadletter.ReasonLost}, deadletter.StateOpen)
	}
	add(deadletter.Entry{TaskID: "t-done", Knight: "reviewer", Reason: deadletter.ReasonLost}, deadletter.StateResolved)

	resp, body := e.do(t, "GET", "/v1/dead-letters?state=open", testToken, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("list: %d %v", resp.StatusCode, body)
	}
	if n := len(body["deadLetters"].([]any)); n != 100 {
		t.Fatalf("items: %d", n)
	}
	if body["total"] != float64(137) {
		t.Fatalf("total: %v", body["total"])
	}
	_, body = e.do(t, "GET", "/v1/dead-letters?state=open&limit=5", testToken, nil)
	if len(body["deadLetters"].([]any)) != 5 || body["total"] != float64(137) {
		t.Fatalf("limit=5: %v", body["total"])
	}
}

func TestDeadLetterEmptyWithoutBucket(t *testing.T) {
	e := newEnv(t, Options{})
	resp, body := e.do(t, "GET", "/v1/dead-letters", testToken, nil)
	if resp.StatusCode != 200 || len(body["deadLetters"].([]any)) != 0 || body["total"] != float64(0) {
		t.Fatalf("%d %v", resp.StatusCode, body)
	}
}

func TestTruncateUTF8(t *testing.T) {
	if got := truncateUTF8("héllo", 2); got != "h" {
		t.Fatalf("%q", got)
	}
	if got := truncateUTF8("abc", 10); got != "abc" {
		t.Fatalf("%q", got)
	}
}

func TestMCPDeadLetters(t *testing.T) {
	e := newEnv(t, Options{})
	add := seedDeadLetters(t, e)
	add(deadletter.Entry{TaskID: "t-mcp", Knight: "reviewer", Reason: deadletter.ReasonFailed, Error: "oops",
		Task: &natspkg.TaskPayload{TaskID: "t-mcp", Task: "x"}}, deadletter.StateOpen)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "v0"}, nil)
	cs, err := c.Connect(ctx, &mcp.StreamableClientTransport{
		Endpoint:   e.server.URL + "/mcp",
		HTTPClient: &http.Client{Transport: bearer{http.DefaultTransport}},
		MaxRetries: -1,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cs.Close() }()

	res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "list_dead_letters", Arguments: map[string]any{"state": "open"}})
	if err != nil || res.IsError {
		t.Fatalf("list_dead_letters: %v %+v", err, res)
	}
	b, _ := json.Marshal(res.StructuredContent)
	if !strings.Contains(string(b), "t-mcp") {
		t.Fatalf("list: %s", b)
	}
	res, err = cs.CallTool(ctx, &mcp.CallToolParams{Name: "retry_task", Arguments: map[string]any{"taskId": "t-mcp"}})
	if err != nil || res.IsError {
		t.Fatalf("retry_task: %v %+v", err, res)
	}
	b, _ = json.Marshal(res.StructuredContent)
	if !strings.Contains(string(b), "t-mcp-r2") {
		t.Fatalf("retry: %s", b)
	}
	res, err = cs.CallTool(ctx, &mcp.CallToolParams{Name: "retry_task", Arguments: map[string]any{"taskId": "t-mcp"}})
	if err == nil && !res.IsError {
		t.Fatalf("second retry should fail: %+v", res)
	}
}

func TestDeadLetterRetryOfRetryID(t *testing.T) {
	e := newEnv(t, Options{})
	add := seedDeadLetters(t, e)
	add(deadletter.Entry{TaskID: "t-r2", Knight: "reviewer", Reason: deadletter.ReasonFailed, Attempt: 2,
		Task: &natspkg.TaskPayload{TaskID: "t-r2", Task: "x"}}, deadletter.StateOpen)
	add(deadletter.Entry{TaskID: "u-r2", Knight: "reviewer", Reason: deadletter.ReasonFailed, Attempt: 2, RetryOf: "u",
		Task: &natspkg.TaskPayload{TaskID: "u-r2", Task: "x"}}, deadletter.StateOpen)
	for in, want := range map[string]string{"t-r2": "t-r3", "u-r2": "u-r3"} {
		resp, body := e.do(t, "POST", "/v1/dead-letters/"+in+"/retry", testToken, nil)
		if resp.StatusCode != http.StatusAccepted || body["taskId"] != want {
			t.Fatalf("retry %s: %d %v", in, resp.StatusCode, body)
		}
	}
}

func TestDeadLetterRetryLostHasPreviousFailure(t *testing.T) {
	e := newEnv(t, Options{})
	add := seedDeadLetters(t, e)
	add(deadletter.Entry{TaskID: "t-lost", Knight: "reviewer", Reason: deadletter.ReasonLost,
		Task: &natspkg.TaskPayload{TaskID: "t-lost", Task: "x"}}, deadletter.StateOpen)
	if resp, body := e.do(t, "POST", "/v1/dead-letters/t-lost/retry", testToken, nil); resp.StatusCode != http.StatusAccepted {
		t.Fatalf("retry: %d %v", resp.StatusCode, body)
	}
	deadline := time.Now().Add(5 * time.Second)
	for countTasks(t, e, "t-lost-r2") == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if pf := e.knight.lastPayload().PreviousFailure; !strings.Contains(pf, "lost") {
		t.Fatalf("previousFailure: %q", pf)
	}
}

func TestDeadLetterRetryRefusesOtherTable(t *testing.T) {
	e := newEnv(t, Options{})
	add := seedDeadLetters(t, e)
	add(deadletter.Entry{TaskID: "t-other", Table: "other", Knight: "reviewer", Reason: deadletter.ReasonFailed,
		Task: &natspkg.TaskPayload{TaskID: "t-other", Task: "x"}}, deadletter.StateOpen)
	resp, body := e.do(t, "POST", "/v1/dead-letters/t-other/retry", testToken, nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("other table: %d %v", resp.StatusCode, body)
	}
	if n := countTasks(t, e, "t-other-r2"); n != 0 {
		t.Fatalf("published %d", n)
	}
}

func TestDeadLetterListNewestFirstByDeadLetteredAt(t *testing.T) {
	e := newEnv(t, Options{})
	add := seedDeadLetters(t, e)
	now := time.Now().UTC()
	add(deadletter.Entry{TaskID: "t-old", Knight: "reviewer", Reason: deadletter.ReasonLost, DeadLetteredAt: now.Add(-time.Hour)}, deadletter.StateOpen)
	add(deadletter.Entry{TaskID: "t-new", Knight: "reviewer", Reason: deadletter.ReasonLost, DeadLetteredAt: now}, deadletter.StateOpen)
	// Resolving the old one moves its UpdatedAt but must not reorder the list.
	js, _ := jetstream.New(e.nc)
	kv, _ := js.KeyValue(context.Background(), deadletter.KVBucket)
	ent, err := kv.Get(context.Background(), deadletter.Key("dev", "t-old"))
	if err != nil {
		t.Fatal(err)
	}
	var rec deadletter.Record
	_ = json.Unmarshal(ent.Value(), &rec)
	rec.State, rec.UpdatedAt = deadletter.StateResolved, now.Add(time.Hour)
	b, _ := json.Marshal(rec)
	if _, err := kv.Put(context.Background(), deadletter.Key("dev", "t-old"), b); err != nil {
		t.Fatal(err)
	}
	_, body := e.do(t, "GET", "/v1/dead-letters", testToken, nil)
	l := body["deadLetters"].([]any)
	if len(l) != 2 || l[0].(map[string]any)["taskId"] != "t-new" {
		t.Fatalf("order: %v", l)
	}
}
