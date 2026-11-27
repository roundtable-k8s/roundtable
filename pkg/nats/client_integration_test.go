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

package nats

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go/jetstream"
)

// connectTestClient returns a connected JetStreamClient on an in-process
// JetStream server.
func connectTestClient(t *testing.T) *JetStreamClient {
	t.Helper()
	s := runJS(t)
	c := NewClient(Config{URL: s.ClientURL()}, logr.Discard()).(*JetStreamClient)
	if err := c.Connect(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestStreamAndConsumerLifecycle(t *testing.T) {
	ctx := context.Background()
	c := connectTestClient(t)

	cfg := StreamConfig{
		Name: "rt_tasks", Subjects: []string{"rt.tasks.>"},
		Retention: RetentionWorkQueue, Storage: StorageMemory,
		MaxAge: time.Hour, MaxMsgs: 100, Discard: DiscardNew,
	}
	if err := c.CreateStream(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	// A second create leaves the existing stream alone.
	if err := c.CreateStream(ctx, StreamConfig{Name: "rt_tasks", Subjects: []string{"other.>"}}); err != nil {
		t.Fatal(err)
	}
	info, err := c.StreamInfo(ctx, "rt_tasks")
	if err != nil {
		t.Fatal(err)
	}
	got := info.Config
	if len(got.Subjects) != 1 || got.Subjects[0] != "rt.tasks.>" ||
		got.Retention != jetstream.WorkQueuePolicy || got.Storage != jetstream.MemoryStorage ||
		got.MaxAge != time.Hour || got.MaxMsgs != 100 || got.Discard != jetstream.DiscardNew {
		t.Fatalf("unexpected stream config: %+v", got)
	}

	if _, err := c.EnsureConsumer(ctx, "rt_tasks", jetstream.ConsumerConfig{
		Durable: "knight-a", FilterSubject: "rt.tasks.a.>", AckPolicy: jetstream.AckExplicitPolicy,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PublishJSONSeq("rt.tasks.a.t1", TaskPayload{TaskID: "t1", Task: "x"}); err != nil {
		t.Fatal(err)
	}
	if n, err := c.ConsumerPending(ctx, "rt_tasks", "knight-a"); err != nil || n != 1 {
		t.Fatalf("ConsumerPending = %d, %v; want 1", n, err)
	}
	if seq, err := c.ConsumerDeliveredSeq(ctx, "rt_tasks", "knight-a"); err != nil || seq != 0 {
		t.Fatalf("ConsumerDeliveredSeq = %d, %v; want 0", seq, err)
	}

	// Deliver and ack through the consumer; the delivered sequence
	// follows.
	cons, err := c.js.Consumer(ctx, "rt_tasks", "knight-a")
	if err != nil {
		t.Fatal(err)
	}
	msg, err := cons.Next(jetstream.FetchMaxWait(2 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := msg.DoubleAck(ctx); err != nil {
		t.Fatal(err)
	}
	if seq, err := c.ConsumerDeliveredSeq(ctx, "rt_tasks", "knight-a"); err != nil || seq != 1 {
		t.Fatalf("ConsumerDeliveredSeq = %d, %v; want 1", seq, err)
	}

	if err := c.DeleteConsumer(ctx, "rt_tasks", "knight-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ConsumerDeliveredSeq(ctx, "rt_tasks", "knight-a"); err == nil {
		t.Fatal("consumer still there after DeleteConsumer")
	}
	// Deleting a missing consumer is best effort.
	if err := c.DeleteConsumer(ctx, "rt_tasks", "knight-a"); err != nil {
		t.Fatalf("DeleteConsumer of a missing consumer: %v", err)
	}

	if err := c.DeleteStream(ctx, "rt_tasks"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.StreamInfo(ctx, "rt_tasks"); err == nil {
		t.Fatal("stream still there after DeleteStream")
	}
}

func TestKVRoundTrip(t *testing.T) {
	ctx := context.Background()
	c := connectTestClient(t)

	if err := c.KVPut(ctx, "chain-outputs", "ns.chain.step", []byte("out")); err != nil {
		t.Fatal(err)
	}
	v, err := c.KVGet(ctx, "chain-outputs", "ns.chain.step")
	if err != nil || string(v) != "out" {
		t.Fatalf("KVGet = %q, %v", v, err)
	}
	keys, err := c.KVKeys(ctx, "chain-outputs")
	if err != nil || len(keys) != 1 || keys[0] != "ns.chain.step" {
		t.Fatalf("KVKeys = %v, %v", keys, err)
	}
	if err := c.KVDelete(ctx, "chain-outputs", "ns.chain.step"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.KVGet(ctx, "chain-outputs", "ns.chain.step"); err == nil {
		t.Fatal("KVGet after delete succeeded")
	}
	keys, err = c.KVKeys(ctx, "chain-outputs")
	if err != nil || len(keys) != 0 {
		t.Fatalf("KVKeys after delete = %v, %v", keys, err)
	}

	kv, err := c.js.KeyValue(ctx, "chain-outputs")
	if err != nil {
		t.Fatal(err)
	}
	st, err := kv.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.History() != 3 || st.TTL() != 30*24*time.Hour {
		t.Fatalf("bucket history=%d ttl=%s", st.History(), st.TTL())
	}
}

// TestFetchOneResult covers the result poll of the chain controller and the
// planner: a result published before the first poll and one published after
// it are both returned, each exactly once, and acked (removed from the
// WorkQueue results stream).
func TestFetchOneResult(t *testing.T) {
	ctx := context.Background()
	c := connectTestClient(t)
	if err := c.CreateStream(ctx, StreamConfig{
		Name: "rt_results", Subjects: []string{"rt.results.>"},
		Retention: RetentionWorkQueue, Storage: StorageMemory,
	}); err != nil {
		t.Fatal(err)
	}

	poll := func(stream, consumer, taskID string) *TaskResult {
		t.Helper()
		msg, err := c.FetchOne(ctx, stream,
			ResultConsumerConfig(consumer, ResultSubject("rt", taskID)), 200*time.Millisecond)
		_ = c.DeleteConsumer(ctx, "rt_results", consumer) // as pollResult does
		if err != nil {
			t.Fatal(err)
		}
		if msg == nil {
			return nil
		}
		var r TaskResult
		if err := json.Unmarshal(msg.Data(), &r); err != nil {
			t.Fatal(err)
		}
		return &r
	}
	stored := func() uint64 {
		t.Helper()
		info, err := c.StreamInfo(ctx, "rt_results")
		if err != nil {
			t.Fatal(err)
		}
		return info.State.Msgs
	}

	// Published before the first poll.
	if err := c.PublishJSON(ResultSubject("rt", "t1"), TaskResult{TaskID: "t1", Output: "one"}); err != nil {
		t.Fatal(err)
	}
	if r := poll("rt_results", "chain-c-s1", "t1"); r == nil || r.GetOutput() != "one" {
		t.Fatalf("first poll = %+v", r)
	}
	if n := stored(); n != 0 {
		t.Fatalf("result not acked: %d message(s) left in the stream", n)
	}
	if r := poll("rt_results", "chain-c-s1", "t1"); r != nil {
		t.Fatalf("result delivered twice: %+v", r)
	}

	// Published after the first (empty) poll.
	if r := poll("rt_results", "chain-c-s2", "t2"); r != nil {
		t.Fatalf("poll before publish = %+v", r)
	}
	if err := c.PublishJSON(ResultSubject("rt", "t2"), TaskResult{TaskID: "t2", Output: "two"}); err != nil {
		t.Fatal(err)
	}
	if r := poll("rt_results", "chain-c-s2", "t2"); r == nil || r.GetOutput() != "two" {
		t.Fatalf("poll after publish = %+v", r)
	}
	if r := poll("rt_results", "chain-c-s2", "t2"); r != nil {
		t.Fatalf("result delivered twice: %+v", r)
	}

	// A wrong stream name falls back to the stream that captures the subject.
	if err := c.PublishJSON(ResultSubject("rt", "t3"), TaskResult{TaskID: "t3", Output: "three"}); err != nil {
		t.Fatal(err)
	}
	if r := poll("no_such_stream", "mission-planner-m", "t3"); r == nil || r.GetOutput() != "three" {
		t.Fatalf("poll with fallback = %+v", r)
	}
	if n := stored(); n != 0 {
		t.Fatalf("%d message(s) left in the stream", n)
	}
}

// TestFetchOneNoWait pins wait <= 0: an empty stream returns (nil, nil)
// immediately, and a queued message is returned and acked.
func TestFetchOneNoWait(t *testing.T) {
	ctx := context.Background()
	c := connectTestClient(t)
	if err := c.CreateStream(ctx, StreamConfig{
		Name: "rt_results", Subjects: []string{"rt.results.>"},
		Retention: RetentionWorkQueue, Storage: StorageMemory,
	}); err != nil {
		t.Fatal(err)
	}
	cfg := ResultConsumerConfig("chain-c-s", ResultSubject("rt", "t1"))

	start := time.Now()
	msg, err := c.FetchOne(ctx, "rt_results", cfg, 0)
	if err != nil || msg != nil {
		t.Fatalf("empty fetch = %v, %v", msg, err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("no-wait fetch took %s", d)
	}

	if err := c.PublishJSON(ResultSubject("rt", "t1"), TaskResult{TaskID: "t1", Output: "one"}); err != nil {
		t.Fatal(err)
	}
	msg, err = c.FetchOne(ctx, "rt_results", cfg, 0)
	if err != nil || msg == nil {
		t.Fatalf("queued fetch = %v, %v", msg, err)
	}
	var r TaskResult
	if err := json.Unmarshal(msg.Data(), &r); err != nil || r.GetOutput() != "one" {
		t.Fatalf("result = %+v, %v", r, err)
	}
	info, err := c.StreamInfo(ctx, "rt_results")
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 0 {
		t.Fatalf("message not acked: %d left", info.State.Msgs)
	}
}
