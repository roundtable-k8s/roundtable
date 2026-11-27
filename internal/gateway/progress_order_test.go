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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// hookBus wraps a Bus: it counts open watches and can run a hook after, or
// fail, Publish.
type hookBus struct {
	Bus
	open         atomic.Int64
	afterPublish func(subject string, p natspkg.TaskPayload)
	publishErr   error
}

type countedWatch struct {
	ResultWatch
	once sync.Once
	b    *hookBus
}

func (w *countedWatch) Close() {
	w.once.Do(func() { w.b.open.Add(-1) })
	w.ResultWatch.Close()
}

func (b *hookBus) Watch(subject string) (ResultWatch, error) {
	w, err := b.Bus.Watch(subject)
	if err != nil {
		return nil, err
	}
	b.open.Add(1)
	return &countedWatch{ResultWatch: w, b: b}, nil
}

func (b *hookBus) Publish(ctx context.Context, subject string, p natspkg.TaskPayload, table string) error {
	if b.publishErr != nil {
		return b.publishErr
	}
	if err := b.Bus.Publish(ctx, subject, p, table); err != nil {
		return err
	}
	if b.afterPublish != nil {
		b.afterPublish(subject, p)
	}
	return nil
}

func TestInvokeProgressSeesActivityPublishedRightAfterPublish(t *testing.T) {
	e := newEnv(t, Options{})
	bus := &hookBus{Bus: e.svc.Bus}
	bus.afterPublish = func(_ string, p natspkg.TaskPayload) {
		publishActivity(t, e.nc, p.TaskID, 1, natspkg.ActivityToolStarted, "FirstTool")
	}
	e.svc.Bus = bus

	var mu sync.Mutex
	var msgs []string
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx = withProgress(ctx, func(msg string, _ bool) {
		mu.Lock()
		msgs = append(msgs, msg)
		mu.Unlock()
	})
	go func() {
		time.Sleep(700 * time.Millisecond)
		close(e.knight.release)
	}()
	if _, err := e.svc.Invoke(ctx, "operator", InvokeRequest{Task: "hold on"}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(msgs) == 0 || !strings.Contains(msgs[0], "tool FirstTool") {
		t.Fatalf("progress messages: %q, want the first event", msgs)
	}
	if n := bus.open.Load(); n != 0 {
		t.Errorf("%d subscriptions left open", n)
	}
}

func TestInvokeFailedPublishClosesProgressPump(t *testing.T) {
	e := newEnv(t, Options{})
	bus := &hookBus{Bus: e.svc.Bus, publishErr: errors.New("no stream")}
	e.svc.Bus = bus
	ctx := withProgress(context.Background(), func(string, bool) {})
	if _, err := e.svc.Invoke(ctx, "operator", InvokeRequest{Task: "x"}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
	if n := bus.open.Load(); n != 0 {
		t.Fatalf("%d subscriptions left open after a failed publish", n)
	}
}

func TestFollowStepActivityAlreadyFollowedSkipsReads(t *testing.T) {
	e := newEnv(t, Options{})
	var gets atomic.Int64
	base := e.svc.Resolver.Client.(client.WithWatch)
	e.svc.Resolver = &Resolver{Client: interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			gets.Add(1)
			return c.Get(ctx, key, obj, opts...)
		},
	})}

	ctx := t.Context()
	pump := e.svc.newActivityPump(ctx)
	defer pump.close()

	e.svc.followStepActivity(ctx, testNS, "operator", "task-1", pump)
	if !pump.has("task-1") {
		t.Fatal("step not followed")
	}
	first := gets.Load()
	if first == 0 {
		t.Fatal("expected the first follow to read the knight")
	}
	e.svc.followStepActivity(ctx, testNS, "operator", "task-1", pump)
	if got := gets.Load(); got != first {
		t.Fatalf("already followed step cost %d extra reads", got-first)
	}
}

func TestInvokeReplayWaitsForResult(t *testing.T) {
	e := newEnv(t, Options{})
	zero, wait := 0, 10
	first, err := e.svc.Invoke(context.Background(), "operator", InvokeRequest{Task: "hold", TimeoutSeconds: &zero, IdempotencyKey: "w"})
	if err != nil || first.Done() {
		t.Fatalf("first: %+v %v", first, err)
	}
	go func() {
		time.Sleep(500 * time.Millisecond)
		close(e.knight.release)
	}()
	rec, err := e.svc.Invoke(context.Background(), "operator", InvokeRequest{Task: "hold", TimeoutSeconds: &wait, IdempotencyKey: "w"})
	if err != nil {
		t.Fatal(err)
	}
	if !rec.Replayed || rec.TaskID != first.TaskID || !rec.Done() || rec.Status != TaskSucceeded {
		t.Fatalf("replay = %+v, want the finished original task", rec)
	}
}

func TestInvokeStaleOrphanKeyIsReplaced(t *testing.T) {
	e := newEnv(t, Options{})
	req := InvokeRequest{Task: "hello", IdempotencyKey: "o"}
	key := idempotencyPrefix + idempotencyHash(testNS, "operator", "o")
	old := IdempotencyEntry{TaskID: "operator-ghost", Fingerprint: invokeFingerprint(req), CreatedAt: time.Now().Add(-time.Minute)}
	if prior, err := e.svc.Bus.CreateIdempotency(context.Background(), key, old); err != nil || prior != nil {
		t.Fatal(prior, err)
	}
	rec, err := e.svc.Invoke(context.Background(), "operator", req)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Replayed || rec.TaskID == "operator-ghost" || e.knight.received() != 1 {
		t.Fatalf("rec = %+v published=%d, want a new task", rec, e.knight.received())
	}

	// A fresh orphan still answers 503.
	req2 := InvokeRequest{Task: "hello", IdempotencyKey: "o2"}
	key2 := idempotencyPrefix + idempotencyHash(testNS, "operator", "o2")
	fresh := IdempotencyEntry{TaskID: "operator-ghost", Fingerprint: invokeFingerprint(req2), CreatedAt: time.Now()}
	if prior, err := e.svc.Bus.CreateIdempotency(context.Background(), key2, fresh); err != nil || prior != nil {
		t.Fatal(prior, err)
	}
	if _, err := e.svc.Invoke(context.Background(), "operator", req2); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
}

func TestInvokePublishFailureReleasesKey(t *testing.T) {
	e := newEnv(t, Options{})
	bus := &hookBus{Bus: e.svc.Bus, publishErr: errors.New("no stream")}
	e.svc.Bus = bus
	req := InvokeRequest{Task: "hello", IdempotencyKey: "r"}
	if _, err := e.svc.Invoke(context.Background(), "operator", req); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
	bus.publishErr = nil
	rec, err := e.svc.Invoke(context.Background(), "operator", req)
	if err != nil || rec.Replayed || rec.Status != TaskSucceeded {
		t.Fatalf("retry = %+v %v, want a new run", rec, err)
	}
	if n := e.knight.received(); n != 1 {
		t.Fatalf("published %d, want 1", n)
	}
}

func TestInvokeReplayWaitCountsAgainstMaxInFlight(t *testing.T) {
	e := newEnv(t, Options{MaxInFlight: 1})
	zero, wait := 0, 10
	first, err := e.svc.Invoke(context.Background(), "operator", InvokeRequest{Task: "hold", TimeoutSeconds: &zero, IdempotencyKey: "m"})
	if err != nil || first.Done() {
		t.Fatalf("first: %+v %v", first, err)
	}
	release, err := e.svc.acquireSlot()
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	_, err = e.svc.Invoke(context.Background(), "operator", InvokeRequest{Task: "hold", TimeoutSeconds: &wait, IdempotencyKey: "m"})
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("err = %v, want ErrBusy", err)
	}
}

func TestStaleIdempotencyDeleteKeepsNewerEntry(t *testing.T) {
	e := newEnv(t, Options{})
	ctx := context.Background()
	req := InvokeRequest{Task: "hello", IdempotencyKey: "s"}
	key := idempotencyPrefix + idempotencyHash(testNS, "operator", "s")
	old := IdempotencyEntry{TaskID: "operator-ghost", Fingerprint: invokeFingerprint(req), CreatedAt: time.Now().Add(-time.Minute)}
	if prior, err := e.svc.Bus.CreateIdempotency(ctx, key, old); err != nil || prior != nil {
		t.Fatal(prior, err)
	}
	stale, err := e.svc.Bus.GetIdempotency(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	// Another retry replaces the orphan before this one deletes it.
	if err := e.svc.Bus.DeleteIdempotency(ctx, key); err != nil {
		t.Fatal(err)
	}
	newer := IdempotencyEntry{TaskID: "operator-new", Fingerprint: invokeFingerprint(req), CreatedAt: time.Now()}
	if prior, err := e.svc.Bus.CreateIdempotency(ctx, key, newer); err != nil || prior != nil {
		t.Fatal(prior, err)
	}
	deleted, err := e.svc.Bus.DeleteIdempotencyAt(ctx, key, stale.Revision)
	if err != nil || deleted {
		t.Fatalf("deleted = %v, %v; want false", deleted, err)
	}
	got, err := e.svc.Bus.GetIdempotency(ctx, key)
	if err != nil || got.TaskID != "operator-new" {
		t.Fatalf("entry = %+v %v, want the newer entry kept", got, err)
	}
	// The replay with the stale read re-reads rather than starting a new call.
	_, isStale, err := e.svc.replayInvoke(ctx, key, stale, invokeFingerprint(req), 0, false)
	if isStale || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("stale = %v err = %v, want the newer fresh orphan to answer 503", isStale, err)
	}
	if e.knight.received() != 0 {
		t.Fatal("published a duplicate task")
	}
}
