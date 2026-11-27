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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/roundtable-k8s/roundtable/internal/deadletter"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// A result that reaches JetStream just after Discard starts must still be
// removed: Discard retries its lookup instead of giving up on the first miss.
func TestNATSBusDiscardRemovesLateResult(t *testing.T) {
	ctx := context.Background()
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
	st, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "wq_late", Subjects: []string{"t.late.>"}, Retention: jetstream.WorkQueuePolicy})
	if err != nil {
		t.Fatal(err)
	}
	bus, err := NewNATSBus(ctx, nc, "bus_late_test", time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	// Widen the lookup window well past the publish delay so -race slowness
	// cannot make the test flaky.
	oldAttempts := discardLookupAttempts
	discardLookupAttempts = 100
	t.Cleanup(func() { discardLookupAttempts = oldAttempts })

	published := make(chan error, 1)
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, err := js.Publish(ctx, "t.late.res", []byte("result"))
		published <- err
	}()
	if err := bus.Discard(ctx, "wq_late", "t.late.res"); err != nil {
		t.Fatal(err)
	}
	if err := <-published; err != nil {
		t.Fatal(err)
	}

	info, err := st.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 0 {
		t.Fatalf("late result not discarded: stream holds %d messages", info.State.Msgs)
	}
}

func TestNATSBusRoundTrip(t *testing.T) {
	ctx := context.Background()
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
	for name, cfg := range map[string]jetstream.StreamConfig{
		"wq":     {Name: "wq", Subjects: []string{"t.wq.>"}, Retention: jetstream.WorkQueuePolicy},
		"limits": {Name: "limits", Subjects: []string{"t.limits.>"}, Retention: jetstream.LimitsPolicy},
	} {
		if _, err := js.CreateStream(ctx, cfg); err != nil {
			t.Fatalf("stream %s: %v", name, err)
		}
	}

	bus, err := NewNATSBus(ctx, nc, "bus_test", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	// Binding to an existing bucket works too.
	if _, err := NewNATSBus(ctx, nc, "bus_test", time.Hour); err != nil {
		t.Fatalf("rebind: %v", err)
	}

	// Publish fails when no stream captures the subject, succeeds otherwise.
	if err := bus.Publish(ctx, "nostream.x", natspkg.TaskPayload{TaskID: "a"}, "rt"); err == nil {
		t.Fatal("publish without stream should fail")
	}
	if err := bus.Publish(ctx, "t.wq.task1", natspkg.TaskPayload{TaskID: "task1", Task: "hi"}, "rt"); err != nil {
		t.Fatal(err)
	}

	// LastResult: nil on empty stream/empty name, data otherwise; no consume.
	if data, err := bus.LastResult(ctx, "", "t.wq.x"); err != nil || data != nil {
		t.Fatalf("empty stream name: %q %v", data, err)
	}
	if data, err := bus.LastResult(ctx, "wq", "t.wq.missing"); err != nil || data != nil {
		t.Fatalf("missing subject: %q %v", data, err)
	}
	for _, subj := range []string{"t.wq.res", "t.limits.res"} {
		if _, err := js.Publish(ctx, subj, []byte("result")); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []struct{ stream, subj string }{{"wq", "t.wq.res"}, {"limits", "t.limits.res"}} {
		data, err := bus.LastResult(ctx, c.stream, c.subj)
		if err != nil || string(data) != "result" {
			t.Fatalf("LastResult %s: %q %v", c.stream, data, err)
		}
	}
	if _, err := bus.LastResult(ctx, "nope", "t.wq.res"); err == nil {
		t.Fatal("unknown stream should error")
	}

	// Discard removes from WorkQueue streams only, and is idempotent.
	if err := bus.Discard(ctx, "wq", "t.wq.res"); err != nil {
		t.Fatal(err)
	}
	if data, _ := bus.LastResult(ctx, "wq", "t.wq.res"); data != nil {
		t.Fatalf("workqueue result not discarded: %q", data)
	}
	if err := bus.Discard(ctx, "wq", "t.wq.res"); err != nil {
		t.Fatalf("second discard: %v", err)
	}
	if err := bus.Discard(ctx, "limits", "t.limits.res"); err != nil {
		t.Fatal(err)
	}
	if data, _ := bus.LastResult(ctx, "limits", "t.limits.res"); string(data) != "result" {
		t.Fatalf("limits result must be kept, got %q", data)
	}

	// Records.
	if recs, err := bus.ListRecords(ctx); err != nil || len(recs) != 0 {
		t.Fatalf("empty list: %v %v", recs, err)
	}
	if _, err := bus.GetRecord(ctx, "absent"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetRecord absent = %v, want ErrNotFound", err)
	}
	for _, id := range []string{"r1", "r2"} {
		if err := bus.PutRecord(ctx, &TaskRecord{TaskID: id, Knight: "k", Status: TaskPending}); err != nil {
			t.Fatal(err)
		}
	}
	rec, err := bus.GetRecord(ctx, "r1")
	if err != nil || rec.TaskID != "r1" || rec.Knight != "k" || rec.Status != TaskPending {
		t.Fatalf("GetRecord = %+v %v", rec, err)
	}
	recs, err := bus.ListRecords(ctx)
	if err != nil || len(recs) != 2 {
		t.Fatalf("ListRecords = %d %v", len(recs), err)
	}
}

func TestNATSBusDeadLetters(t *testing.T) {
	ctx := context.Background()
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
	bus, err := NewNATSBus(ctx, nc, "bus_dl_test", time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	// No bucket yet: empty, and updates report not found.
	if recs, err := bus.DeadLetterRecords(ctx); err != nil || len(recs) != 0 {
		t.Fatalf("no bucket: %v %v", recs, err)
	}
	if err := bus.UpdateDeadLetterRecord(ctx, "dev.t1", deadletter.Record{}, 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("update without bucket = %v, want ErrNotFound", err)
	}

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: deadletter.KVBucket, History: 5})
	if err != nil {
		t.Fatal(err)
	}
	if recs, err := bus.DeadLetterRecords(ctx); err != nil || len(recs) != 0 {
		t.Fatalf("empty bucket: %v %v", recs, err)
	}
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "dlq", Subjects: []string{"t.dlq.>"}}); err != nil {
		t.Fatal(err)
	}
	if e, err := bus.DeadLetterEntry(ctx, "t.other.x"); e != nil || err != nil {
		t.Fatalf("entry without stream = %v %v", e, err)
	}
	if e, err := bus.DeadLetterEntry(ctx, "t.dlq.t1"); e != nil || err != nil {
		t.Fatalf("missing entry = %v %v", e, err)
	}
	body, _ := json.Marshal(deadletter.Entry{TaskID: "t1", Knight: "k"})
	if _, err := js.Publish(ctx, "t.dlq.t1", body); err != nil {
		t.Fatal(err)
	}
	if e, err := bus.DeadLetterEntry(ctx, "t.dlq.t1"); err != nil || e == nil || e.TaskID != "t1" {
		t.Fatalf("entry = %+v %v", e, err)
	}

	rec, _ := json.Marshal(deadletter.Record{State: deadletter.StateOpen, Knight: "k", Subject: "t.dlq.t1"})
	if _, err := kv.Put(ctx, "dev.t1", rec); err != nil {
		t.Fatal(err)
	}
	recs, err := bus.DeadLetterRecords(ctx)
	if err != nil || len(recs) != 1 || recs[0].Key != "dev.t1" || recs[0].Revision == 0 {
		t.Fatalf("records = %+v %v", recs, err)
	}

	// Compare-and-set: the first update wins, a repeat at the old revision
	// is a stale-revision conflict (the retry endpoint's 409).
	upd := recs[0].Record
	upd.State = deadletter.StateRetried
	if err := bus.UpdateDeadLetterRecord(ctx, "dev.t1", upd, recs[0].Revision); err != nil {
		t.Fatalf("first update: %v", err)
	}
	if err := bus.UpdateDeadLetterRecord(ctx, "dev.t1", upd, recs[0].Revision); !errors.Is(err, errStaleRevision) {
		t.Fatalf("stale update = %v, want errStaleRevision", err)
	}
	// A deleted key is skipped.
	if err := kv.Delete(ctx, "dev.t1"); err != nil {
		t.Fatal(err)
	}
	if recs, err := bus.DeadLetterRecords(ctx); err != nil || len(recs) != 0 {
		t.Fatalf("after delete: %v %v", recs, err)
	}
}

func newListTestBus(t *testing.T, bucket string) (*NATSBus, *nats.Conn) {
	t.Helper()
	ns := runNATS(t)
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	bus, err := NewNATSBus(context.Background(), nc, bucket, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return bus, nc
}

func TestNATSBusListRecords(t *testing.T) {
	ctx := context.Background()
	bus, _ := newListTestBus(t, "bus_list_test")

	got, err := bus.ListRecords(ctx)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty bucket: got %d records, err %v", len(got), err)
	}

	for i := range 200 {
		if err := bus.PutRecord(ctx, &TaskRecord{TaskID: fmt.Sprintf("t%03d", i)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := bus.CreateIdempotency(ctx, idempotencyPrefix+"k", IdempotencyEntry{}); err != nil {
		t.Fatal(err)
	}
	if _, err := bus.kv.Put(ctx, "garbage", []byte("not json")); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"t000", "t199"} {
		if err := bus.kv.Delete(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	got, err = bus.ListRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 198 {
		t.Fatalf("got %d records, want 198", len(got))
	}
	for _, r := range got {
		if r.TaskID == "t000" || r.TaskID == "t199" {
			t.Fatalf("deleted record %s listed", r.TaskID)
		}
	}
}

// A record that expires from the bucket is gone from the next listing.
func TestNATSBusListRecordsExpiry(t *testing.T) {
	ctx := context.Background()
	ns := runNATS(t)
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	bus, err := NewNATSBus(ctx, nc, "bus_ttl_test", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := bus.PutRecord(ctx, &TaskRecord{TaskID: "short"}); err != nil {
		t.Fatal(err)
	}
	if got, _ := bus.ListRecords(ctx); len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		got, err := bus.ListRecords(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("expired record still listed")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// Listing costs a constant number of client requests, however many records
// the bucket holds.
func TestNATSBusListRecordsRequestCount(t *testing.T) {
	ctx := context.Background()
	bus, nc := newListTestBus(t, "bus_count_test")
	measure := func() uint64 {
		before := nc.Stats().OutMsgs
		if _, err := bus.ListRecords(ctx); err != nil {
			t.Fatal(err)
		}
		return nc.Stats().OutMsgs - before
	}
	put := func(from, to int) {
		for i := from; i < to; i++ {
			if err := bus.PutRecord(ctx, &TaskRecord{TaskID: fmt.Sprintf("t%03d", i)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	put(0, 10)
	small := measure()
	put(10, 200)
	large := measure()
	if large > small+5 || large > 20 {
		t.Fatalf("requests grew with records: %d for 10, %d for 200", small, large)
	}
}

func TestNATSBusDeadLetterRecordsWatch(t *testing.T) {
	ctx := context.Background()
	bus, _ := newListTestBus(t, "bus_dlw_test")
	got, err := bus.DeadLetterRecords(ctx)
	if err != nil || len(got) != 0 {
		t.Fatalf("no bucket: %v %v", got, err)
	}
	kv, err := bus.js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: deadletter.KVBucket, History: 1})
	if err != nil {
		t.Fatal(err)
	}
	got, err = bus.DeadLetterRecords(ctx)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty bucket: %v %v", got, err)
	}
	for _, k := range []string{"a", "b", "c"} {
		if _, err := kv.Put(ctx, k, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	_ = kv.Delete(ctx, "b")
	_ = kv.Purge(ctx, "c")
	got, err = bus.DeadLetterRecords(ctx)
	if err != nil || len(got) != 1 || got[0].Key != "a" || got[0].Revision == 0 {
		t.Fatalf("got %+v err %v", got, err)
	}
}

func TestNATSBusStreamCacheDroppedWhenStreamGone(t *testing.T) {
	ctx := context.Background()
	bus, _ := newListTestBus(t, "bus_cache_test")
	if _, err := bus.js.CreateStream(ctx, jetstream.StreamConfig{Name: "cache_s", Subjects: []string{"cache.>"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := bus.LastResult(ctx, "cache_s", "cache.x"); err != nil {
		t.Fatal(err)
	}
	if len(bus.streams) != 1 {
		t.Fatalf("stream not cached")
	}
	if err := bus.js.DeleteStream(ctx, "cache_s"); err != nil {
		t.Fatal(err)
	}
	if _, err := bus.LastResult(ctx, "cache_s", "cache.x"); err == nil {
		t.Fatal("want error for deleted stream")
	}
	if len(bus.streams) != 0 {
		t.Fatal("stale stream handle kept")
	}
}
