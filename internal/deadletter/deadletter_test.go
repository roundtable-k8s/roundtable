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
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/go-logr/logr"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
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
	js  nats.JetStreamContext
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
	ns := runNATS(t)
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	for name, subj := range map[string]string{testTasks: testPrefix + ".tasks.>", testResults: testPrefix + ".results.>"} {
		if _, err := js.AddStream(&nats.StreamConfig{Name: name, Subjects: []string{subj}, Retention: nats.WorkQueuePolicy}); err != nil {
			t.Fatal(err)
		}
	}
	e := &env{t: t, nc: nc, js: js, m: NewManager(logr.Discard()),
		cfg: TableConfig{Table: testTable, Prefix: testPrefix, TasksStream: testTasks}}
	if err := e.m.Ensure(nc, "roundtable/"+testTable, e.cfg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.m.StopAll)
	return e
}

// entries returns every message in the dead-letter stream.
func (e *env) entries() []Entry {
	e.t.Helper()
	info, err := e.js.StreamInfo(StreamName(testTasks))
	if err != nil {
		e.t.Fatal(err)
	}
	var out []Entry
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq && info.State.Msgs > 0; seq++ {
		m, err := e.js.GetMsg(StreamName(testTasks), seq)
		if errors.Is(err, nats.ErrMsgNotFound) {
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
	kv, err := e.js.KeyValue(KVBucket)
	if err != nil {
		e.t.Fatal(err)
	}
	ent, err := kv.Get(Key(testTable, taskID))
	if errors.Is(err, nats.ErrKeyNotFound) {
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
	if _, err := e.js.Publish(natspkg.ResultSubject(testPrefix, id), data); err != nil {
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
	durable := natspkg.KnightTaskConsumerName(knight)
	if _, err := e.js.AddConsumer(testTasks, &nats.ConsumerConfig{
		Durable:       durable,
		FilterSubject: natspkg.TaskSubject(testPrefix, "dev", knight),
		AckPolicy:     nats.AckExplicitPolicy,
		AckWait:       500 * time.Millisecond,
		MaxDeliver:    1,
	}); err != nil {
		e.t.Fatal(err)
	}
	data, _ := json.Marshal(task)
	if _, err := e.js.Publish(natspkg.TaskSubject(testPrefix, "dev", knight), data); err != nil {
		e.t.Fatal(err)
	}
	sub, err := e.js.PullSubscribe(natspkg.TaskSubject(testPrefix, "dev", knight), durable, nats.Bind(testTasks, durable))
	if err != nil {
		e.t.Fatal(err)
	}
	msgs, err := sub.Fetch(1, nats.MaxWait(2*time.Second))
	if err != nil || len(msgs) != 1 {
		e.t.Fatalf("fetch task: %v (%d msgs)", err, len(msgs))
	}
	// Do not ack: the knight dies.
	_ = sub.Unsubscribe()
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
	eventually(t, 5*time.Second, "original removed from tasks stream", func() bool {
		info, err := e.js.StreamInfo(testTasks)
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
		ci, err := e.js.ConsumerInfo(AdvisoriesStreamName(testTasks), ConsumerName)
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
		info, err := e.js.StreamInfo(testTasks)
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
	if err := w.handleAdvisory(adv); err != nil {
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
	if err := e.m.Ensure(e.nc, "roundtable/"+testTable, e.cfg); err != nil {
		t.Fatal(err)
	}
	if e.m.watchers["roundtable/"+testTable] != w {
		t.Fatal("unchanged Ensure restarted the watcher")
	}
	cfg := e.cfg
	cfg.MaxAge = 5 * time.Minute
	if err := e.m.Ensure(e.nc, "roundtable/"+testTable, cfg); err != nil {
		t.Fatal(err)
	}
	info, err := e.js.StreamInfo(StreamName(testTasks))
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.MaxAge != 5*time.Minute || info.Config.Duplicates != 5*time.Minute || info.Config.Retention != nats.LimitsPolicy {
		t.Fatalf("DLQ config = %+v", info.Config)
	}
	adv, err := e.js.StreamInfo(AdvisoriesStreamName(testTasks))
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
		{result{TaskResult: natspkg.TaskResult{Success: &f}, Lost: true}, ReasonLost},
		{result{TaskResult: natspkg.TaskResult{Success: &f, UsageLimited: true}}, ReasonUsageLimited},
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
