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

package deadletter

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/go-logr/logr"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus/testutil"

	rtmetrics "github.com/roundtable-k8s/roundtable/pkg/metrics"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

const (
	testPrefix  = "rt-dev"
	testTasks   = "rt_dev_tasks"
	testResults = "rt_dev_results"
	testTable   = "dev"
)

type env struct {
	t   *testing.T
	nc  *nats.Conn
	js  jetstream.JetStream
	m   *Manager
	cfg TableConfig
}

func runNATS(t *testing.T) *natsserver.Server {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	s := natstest.RunServer(&opts)
	t.Cleanup(s.Shutdown)
	return s
}

// newEnv starts JetStream with the table's WorkQueue tasks and results
// streams (as ensureStreams creates them) and a running dead-letter watcher.
func newEnv(t *testing.T) *env {
	t.Helper()
	return newEnvGrace(t, time.Second)
}

// newEnvGrace is newEnv with the given grace period before a lost task's
// original is deleted from the tasks stream.
func newEnvGrace(t *testing.T, grace time.Duration) *env {
	t.Helper()
	ns := runNATS(t)
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	for name, subj := range map[string]string{testTasks: testPrefix + ".tasks.>", testResults: testPrefix + ".results.>"} {
		if _, err := js.CreateStream(context.Background(), jetstream.StreamConfig{Name: name, Subjects: []string{subj}, Retention: jetstream.WorkQueuePolicy}); err != nil {
			t.Fatal(err)
		}
	}
	e := &env{t: t, nc: nc, js: js, m: NewManager(logr.Discard()),
		cfg: TableConfig{Table: testTable, Prefix: testPrefix, TasksStream: testTasks, LostGrace: grace}}
	if err := e.m.Ensure(context.Background(), nc, "roundtable/"+testTable, e.cfg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.m.StopAll)
	return e
}

func (e *env) streamInfo(name string) (*jetstream.StreamInfo, error) {
	st, err := e.js.Stream(context.Background(), name)
	if err != nil {
		return nil, err
	}
	return st.Info(context.Background())
}

func (e *env) consumerInfo(stream, name string) (*jetstream.ConsumerInfo, error) {
	c, err := e.js.Consumer(context.Background(), stream, name)
	if err != nil {
		return nil, err
	}
	return c.Info(context.Background())
}

// entries returns every message in the dead-letter stream.
func (e *env) entries() []Entry {
	e.t.Helper()
	info, err := e.streamInfo(StreamName(testTasks))
	if err != nil {
		e.t.Fatal(err)
	}
	dlq, err := e.js.Stream(context.Background(), StreamName(testTasks))
	if err != nil {
		e.t.Fatal(err)
	}
	var out []Entry
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq && info.State.Msgs > 0; seq++ {
		m, err := dlq.GetMsg(context.Background(), seq)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			continue
		}
		if err != nil {
			e.t.Fatal(err)
		}
		var en Entry
		if err := json.Unmarshal(m.Data, &en); err != nil {
			e.t.Fatal(err)
		}
		out = append(out, en)
	}
	return out
}

func (e *env) record(taskID string) (Record, bool) {
	e.t.Helper()
	kv, err := e.js.KeyValue(context.Background(), KVBucket)
	if err != nil {
		e.t.Fatal(err)
	}
	ent, err := kv.Get(context.Background(), Key(testTable, taskID))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return Record{}, false
	}
	if err != nil {
		e.t.Fatal(err)
	}
	var r Record
	if err := json.Unmarshal(ent.Value(), &r); err != nil {
		e.t.Fatal(err)
	}
	return r, true
}

func (e *env) publishResult(v any) {
	e.t.Helper()
	data, _ := json.Marshal(v)
	m := map[string]any{}
	_ = json.Unmarshal(data, &m)
	id, _ := m["task_id"].(string)
	if id == "" {
		id, _ = m["taskId"].(string)
	}
	if _, err := e.js.Publish(context.Background(), natspkg.ResultSubject(testPrefix, id), data); err != nil {
		e.t.Fatal(err)
	}
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// loseTask publishes task to the knight's subject, delivers it once to a
// max_deliver=1 consumer like the runtimes create, and never acks it: the
// "knight" then goes away. Nobody subscribes to the advisory and nobody pulls
// again; the server emits it when the ack wait expires.
func (e *env) loseTask(knight string, task natspkg.TaskPayload) {
	e.t.Helper()
	e.loseTaskWithHeader(knight, task, nil)
}

func (e *env) loseTaskWithHeader(knight string, task natspkg.TaskPayload, h nats.Header) {
	e.t.Helper()
	durable := natspkg.KnightTaskConsumerName(knight)
	cons, err := e.js.CreateOrUpdateConsumer(context.Background(), testTasks, jetstream.ConsumerConfig{
		Durable:       durable,
		FilterSubject: natspkg.TaskSubject(testPrefix, "dev", knight),
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       500 * time.Millisecond,
		MaxDeliver:    1,
	})
	if err != nil {
		e.t.Fatal(err)
	}
	data, _ := json.Marshal(task)
	msg := &nats.Msg{Subject: natspkg.TaskSubject(testPrefix, "dev", knight), Data: data, Header: h}
	if _, err := e.js.PublishMsg(context.Background(), msg); err != nil {
		e.t.Fatal(err)
	}
	if _, err := cons.Next(jetstream.FetchMaxWait(2 * time.Second)); err != nil {
		e.t.Fatalf("fetch task: %v", err)
	}
	// Do not ack: the knight dies.
}

func TestLostTaskDeadLetteredWithoutKnightListening(t *testing.T) {
	e := newEnv(t)
	task := natspkg.TaskPayload{TaskID: "chain-x-step-1", ChainName: "x", StepName: "step", RunID: "r1", Task: "do it"}
	before := testutil.ToFloat64(rtmetrics.TasksDeadLetteredTotal.WithLabelValues(testTable, "galahad", "lost"))

	e.loseTask("galahad", task)

	eventually(t, 10*time.Second, "lost entry", func() bool { return len(e.entries()) == 1 })
	got := e.entries()[0]
	if got.Reason != ReasonLost || got.Knight != "galahad" || got.TaskID != task.TaskID || got.Table != testTable {
		t.Fatalf("entry = %+v", got)
	}
	if got.Task == nil || got.Task.Task != "do it" || got.ChainName != "x" || got.StepName != "step" || got.RunID != "r1" {
		t.Fatalf("entry lost the original payload: %+v task=%+v", got, got.Task)
	}
	if got.Attempt != 1 || got.StreamSeq == 0 || got.DeadLetteredAt.IsZero() {
		t.Fatalf("entry attempt/seq/time = %d/%d/%v", got.Attempt, got.StreamSeq, got.DeadLetteredAt)
	}
	eventually(t, 15*time.Second, "original removed from tasks stream", func() bool {
		info, err := e.streamInfo(testTasks)
		return err == nil && info.State.Msgs == 0
	})
	if rec, ok := e.record(task.TaskID); !ok || rec.State != StateOpen || rec.Reason != ReasonLost {
		t.Fatalf("record = %+v, %v", rec, ok)
	}
	if d := testutil.ToFloat64(rtmetrics.TasksDeadLetteredTotal.WithLabelValues(testTable, "galahad", "lost")) - before; d != 1 {
		t.Fatalf("dead-lettered counter delta = %v, want 1", d)
	}
	eventually(t, 5*time.Second, "open gauge", func() bool {
		return testutil.ToFloat64(rtmetrics.DeadLettersOpen.WithLabelValues(testTable, "galahad")) == 1
	})
	// The advisory was acked: nothing pending on the operator's consumer.
	eventually(t, 5*time.Second, "advisory acked", func() bool {
		ci, err := e.consumerInfo(AdvisoriesStreamName(testTasks), ConsumerName)
		return err == nil && ci.NumAckPending == 0 && ci.NumPending == 0
	})
	// Still exactly one entry.
	time.Sleep(300 * time.Millisecond)
	if n := len(e.entries()); n != 1 {
		t.Fatalf("entries = %d, want 1", n)
	}
}

func TestFailedResultDeadLettered(t *testing.T) {
	e := newEnv(t)
	e.publishResult(map[string]any{
		"task_id": "t-fail",
		"knight":  "Percival",
		"success": false,
		"error":   "tests failed",
		"task":    natspkg.TaskPayload{TaskID: "t-fail", Task: "fix it", ChainName: "c"},
	})
	eventually(t, 5*time.Second, "failed entry", func() bool { return len(e.entries()) == 1 })
	got := e.entries()[0]
	if got.Reason != ReasonFailed || got.Error != "tests failed" || got.Knight != "percival" {
		t.Fatalf("entry = %+v", got)
	}
	if got.Task == nil || got.Task.Task != "fix it" || got.ChainName != "c" {
		t.Fatalf("entry task = %+v", got.Task)
	}
	if rec, ok := e.record("t-fail"); !ok || rec.State != StateOpen {
		t.Fatalf("record = %+v, %v", rec, ok)
	}
}

func TestLostAdvisoryAndKnightLostResultProduceOneEntry(t *testing.T) {
	e := newEnv(t)
	task := natspkg.TaskPayload{TaskID: "t-both", Task: "work"}
	// The knight's own watchLostTasks reports it first...
	e.publishResult(map[string]any{
		"task_id": "t-both", "knight": "Galahad", "success": false, "lost": true,
		"error": "Task was delivered to a knight that stopped before finishing it",
	})
	eventually(t, 5*time.Second, "lost result entry", func() bool { return len(e.entries()) == 1 })
	// ...then the stored advisory is processed.
	e.loseTask("galahad", task)
	eventually(t, 10*time.Second, "original removed", func() bool {
		info, err := e.streamInfo(testTasks)
		return err == nil && info.State.Msgs == 0
	})
	time.Sleep(300 * time.Millisecond)
	entries := e.entries()
	if len(entries) != 1 || entries[0].Reason != ReasonLost {
		t.Fatalf("entries = %+v, want one lost entry", entries)
	}
}

func TestLostAdvisoryFirstThenLostResultDedupedByMsgID(t *testing.T) {
	e := newEnv(t)
	e.loseTask("galahad", natspkg.TaskPayload{TaskID: "t-adv-first", Task: "work"})
	eventually(t, 10*time.Second, "advisory entry", func() bool { return len(e.entries()) == 1 })
	e.publishResult(map[string]any{"task_id": "t-adv-first", "knight": "Galahad", "success": false, "lost": true, "error": "lost"})
	time.Sleep(500 * time.Millisecond)
	if n := len(e.entries()); n != 1 {
		t.Fatalf("entries = %d, want 1", n)
	}
	if e.entries()[0].Task == nil {
		t.Fatal("advisory entry should keep the original payload")
	}
}

func TestLaterSuccessResolvesEntry(t *testing.T) {
	e := newEnv(t)
	e.publishResult(map[string]any{"task_id": "t-84", "knight": "Galahad", "success": false, "lost": true, "error": "lost"})
	eventually(t, 5*time.Second, "open entry", func() bool {
		r, ok := e.record("t-84")
		return ok && r.State == StateOpen
	})
	eventually(t, 5*time.Second, "open gauge", func() bool {
		return testutil.ToFloat64(rtmetrics.DeadLettersOpen.WithLabelValues(testTable, "galahad")) == 1
	})
	e.publishResult(map[string]any{"task_id": "t-84", "knight": "Galahad", "success": true, "result": "done"})
	eventually(t, 5*time.Second, "resolved entry", func() bool {
		r, ok := e.record("t-84")
		return ok && r.State == StateResolved
	})
	eventually(t, 5*time.Second, "gauge back to 0", func() bool {
		return testutil.ToFloat64(rtmetrics.DeadLettersOpen.WithLabelValues(testTable, "galahad")) == 0
	})
	if n := len(e.entries()); n != 1 {
		t.Fatalf("entries = %d, want 1 (resolving keeps the entry)", n)
	}
}

// TestResolveByCaller covers #591: the issue pickup resolves a merge step's
// timeout entry once its PRs are queued, whether the timeout result was
// captured before or arrives after.
func TestResolveByCaller(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.publishResult(map[string]any{"task_id": "t-merge", "knight": "Galahad", "success": false, "error": "Task timed out after 3600s"})
	eventually(t, 5*time.Second, "open timeout entry", func() bool {
		r, ok := e.record("t-merge")
		return ok && r.State == StateOpen && r.Reason == ReasonTimeout
	})
	if err := e.m.Resolve(ctx, "roundtable/"+testTable, "t-merge"); err != nil {
		t.Fatal(err)
	}
	if r, _ := e.record("t-merge"); r.State != StateResolved {
		t.Fatalf("state = %s, want resolved", r.State)
	}

	// Resolved before the failure result arrives: captured resolved.
	if err := e.m.Resolve(ctx, "roundtable/"+testTable, "t-late"); err != nil {
		t.Fatal(err)
	}
	e.publishResult(map[string]any{"task_id": "t-late", "knight": "Galahad", "success": false, "error": "timeout"})
	eventually(t, 5*time.Second, "late entry", func() bool {
		r, ok := e.record("t-late")
		return ok && r.State == StateResolved
	})

	// Unknown table and nil manager are no-ops.
	if err := e.m.Resolve(ctx, "roundtable/other", "t-x"); err != nil {
		t.Fatal(err)
	}
	var nilM *Manager
	if err := nilM.Resolve(ctx, "roundtable/"+testTable, "t-x"); err != nil {
		t.Fatal(err)
	}
}

func TestSuccessResultsAreNotDeadLettered(t *testing.T) {
	e := newEnv(t)
	e.publishResult(map[string]any{"task_id": "t-ok", "knight": "Galahad", "success": true, "result": "ok"})
	e.publishResult(map[string]any{"taskId": "t-ok2", "output": "ok"})
	time.Sleep(300 * time.Millisecond)
	if n := len(e.entries()); n != 0 {
		t.Fatalf("entries = %d, want 0", n)
	}
}

func TestLostAdvisoryForVanishedTask(t *testing.T) {
	e := newEnv(t)
	w := e.m.watchers["roundtable/"+testTable]
	adv, _ := json.Marshal(advisory{Stream: testTasks, Consumer: "Tristan-consumer", StreamSeq: 42, Deliveries: 1})
	if err := w.handleAdvisory(context.Background(), adv); err != nil {
		t.Fatal(err)
	}
	entries := e.entries()
	if len(entries) != 1 {
		t.Fatalf("entries = %d", len(entries))
	}
	got := entries[0]
	if got.Reason != ReasonLost || got.Knight != "tristan" || got.StreamSeq != 42 || got.TaskID != "seq-42" || got.Task != nil {
		t.Fatalf("entry = %+v", got)
	}
}

func TestEnsureIsIdempotentAndUpdatesMaxAge(t *testing.T) {
	e := newEnv(t)
	w := e.m.watchers["roundtable/"+testTable]
	if err := e.m.Ensure(context.Background(), e.nc, "roundtable/"+testTable, e.cfg); err != nil {
		t.Fatal(err)
	}
	if e.m.watchers["roundtable/"+testTable] != w {
		t.Fatal("unchanged Ensure restarted the watcher")
	}
	cfg := e.cfg
	cfg.MaxAge = 5 * time.Minute
	if err := e.m.Ensure(context.Background(), e.nc, "roundtable/"+testTable, cfg); err != nil {
		t.Fatal(err)
	}
	info, err := e.streamInfo(StreamName(testTasks))
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.MaxAge != 5*time.Minute || info.Config.Duplicates != 5*time.Minute || info.Config.Retention != jetstream.LimitsPolicy {
		t.Fatalf("DLQ config = %+v", info.Config)
	}
	adv, err := e.streamInfo(AdvisoriesStreamName(testTasks))
	if err != nil {
		t.Fatal(err)
	}
	if adv.Config.MaxAge != AdvisoryMaxAge || adv.Config.Subjects[0] != "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.rt_dev_tasks.*" {
		t.Fatalf("advisories config = %+v", adv.Config)
	}
}

func TestResultClassification(t *testing.T) {
	f := false
	cases := []struct {
		r    result
		want Reason
	}{
		{result{TaskResult: natspkg.TaskResult{Success: &f, Error: "boom"}}, ReasonFailed},
		{result{TaskResult: natspkg.TaskResult{Success: &f, Lost: true}}, ReasonLost},
		{result{TaskResult: natspkg.TaskResult{Success: &f, UsageLimited: true}}, ReasonUsageLimited},
		{result{TaskResult: natspkg.TaskResult{Success: &f}, Refused: json.RawMessage(`"quota"`)}, ReasonUsageLimited},
		{result{TaskResult: natspkg.TaskResult{Success: &f}, Refused: json.RawMessage(`"credits"`)}, ReasonRefused},
		{result{TaskResult: natspkg.TaskResult{Success: &f}, Refused: json.RawMessage(`"policy"`)}, ReasonRefused},
		{result{TaskResult: natspkg.TaskResult{Success: &f}, Refused: json.RawMessage(`false`)}, ReasonFailed},
		{result{TaskResult: natspkg.TaskResult{Success: &f, Error: "Task timed out after 120s"}}, ReasonTimeout},
	}
	for i, c := range cases {
		if got := c.r.reason(); got != c.want {
			t.Errorf("case %d: reason = %s, want %s", i, got, c.want)
		}
	}
}

func TestNamesAndTokens(t *testing.T) {
	if got := Subject("p", "Galahad", "a b.c..d"); got != "p.dlq.Galahad.a_b.c._.d" {
		t.Errorf("Subject = %q", got)
	}
	if got := MsgID("t1", 0); got != "dlq-t1-1" {
		t.Errorf("MsgID = %q", got)
	}
	if got := Key("dev", "t*1"); got != "dev.t_1" {
		t.Errorf("Key = %q", got)
	}
	if knightFromConsumer("Galahad-consumer") != "galahad" || knightFromSubject("p.tasks.d.galahad") != "galahad" {
		t.Error("knight name mapping")
	}
}

func (e *env) tasksMsgs() uint64 {
	e.t.Helper()
	info, err := e.streamInfo(testTasks)
	if err != nil {
		e.t.Fatal(err)
	}
	return info.State.Msgs
}

// The original must stay readable so the knights' own lost report (which
// loads it on the same advisory) still works.
func TestLostTaskKeptDuringGraceThenDeleted(t *testing.T) {
	e := newEnvGrace(t, 3*time.Second)
	e.loseTask("tristan", natspkg.TaskPayload{TaskID: "t-grace", Task: "work"})
	eventually(t, 10*time.Second, "entry", func() bool { return len(e.entries()) == 1 })
	if n := e.tasksMsgs(); n != 1 {
		t.Fatalf("original deleted immediately (msgs=%d); knights could not report it lost", n)
	}
	eventually(t, 15*time.Second, "original removed after grace", func() bool { return e.tasksMsgs() == 0 })
	if n := len(e.entries()); n != 1 {
		t.Fatalf("entries = %d, want 1", n)
	}
}

func TestLostTaskDeletedOnceKnightReportedLost(t *testing.T) {
	e := newEnvGrace(t, time.Hour)
	e.loseTask("galahad", natspkg.TaskPayload{TaskID: "t-reported", Task: "work"})
	eventually(t, 10*time.Second, "entry", func() bool { return len(e.entries()) == 1 })
	time.Sleep(300 * time.Millisecond)
	if n := e.tasksMsgs(); n != 1 {
		t.Fatalf("original deleted before the knight reported (msgs=%d)", n)
	}
	e.publishResult(map[string]any{"task_id": "t-reported", "knight": "Galahad", "success": false, "lost": true, "error": "lost"})
	eventually(t, 15*time.Second, "original removed after lost report", func() bool { return e.tasksMsgs() == 0 })
	if n := len(e.entries()); n != 1 {
		t.Fatalf("entries = %d, want 1", n)
	}
}

func TestStopAllContextNoWatchers(t *testing.T) {
	m := NewManager(logr.Discard())
	if err := m.StopAllContext(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStopAllContextTimesOut(t *testing.T) {
	m := NewManager(logr.Discard())
	// A watcher whose loop ignores cancellation: stop blocks on wg.Wait.
	w := &watcher{cancel: func() {}}
	release := make(chan struct{})
	w.wg.Add(1)
	go func() { defer w.wg.Done(); <-release }()
	t.Cleanup(func() { close(release) })
	m.watchers["k"] = w
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := m.StopAllContext(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("StopAllContext took %s, want ~50ms", d)
	}
}

func TestTraceIDFromHeaders(t *testing.T) {
	const tid = "4bf92f3577b34da6a3ce929d0e0e4736"
	for name, tc := range map[string]struct{ tp, want string }{
		"valid":        {"00-" + tid + "-00f067aa0ba902b7-01", tid},
		"absent":       {"", ""},
		"malformed":    {"garbage", ""},
		"zero trace":   {"00-00000000000000000000000000000000-00f067aa0ba902b7-01", ""},
		"version ff":   {"ff-" + tid + "-00f067aa0ba902b7-01", ""},
		"version 01":   {"01-" + tid + "-00f067aa0ba902b7-01", ""},
		"short trace":  {"00-4bf92f35-00f067aa0ba902b7-01", ""},
		"non hex":      {"00-zbf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", ""},
		"uppercase":    {"00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01", ""},
		"missing flag": {"00-" + tid + "-00f067aa0ba902b7", ""},
	} {
		h := nats.Header{}
		if tc.tp != "" {
			h.Set("traceparent", tc.tp)
		}
		if got := TraceIDFromHeaders(h); got != tc.want {
			t.Errorf("%s: TraceIDFromHeaders = %q, want %q", name, got, tc.want)
		}
	}
	if got := TraceIDFromHeaders(nil); got != "" {
		t.Errorf("nil header = %q", got)
	}
}

func TestLostTaskCapturesTraceID(t *testing.T) {
	const tid = "4bf92f3577b34da6a3ce929d0e0e4736"
	for name, tc := range map[string]struct{ tp, want string }{
		"valid":     {"00-" + tid + "-00f067aa0ba902b7-01", tid},
		"malformed": {"not-a-traceparent", ""},
		"absent":    {"", ""},
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			h := nats.Header{}
			if tc.tp != "" {
				h.Set("traceparent", tc.tp)
			}
			e.loseTaskWithHeader("galahad", natspkg.TaskPayload{TaskID: "t-trace", Task: "x"}, h)
			eventually(t, 10*time.Second, "entry", func() bool { return len(e.entries()) == 1 })
			if got := e.entries()[0]; got.TraceID != tc.want || got.Reason != ReasonLost {
				t.Fatalf("entry traceID = %q reason = %s, want %q", got.TraceID, got.Reason, tc.want)
			}
		})
	}
}
