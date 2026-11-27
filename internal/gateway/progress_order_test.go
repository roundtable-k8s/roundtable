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
