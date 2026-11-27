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

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/pkg/metrics"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// ackingKVNATS serves each queued result once (FetchOne acks it off the
// stream) and keeps the chain-outputs KV in memory.
type ackingKVNATS struct {
	*fakeNATSClient
	results map[string]natspkg.TaskResult
	kv      map[string][]byte
}

func (f *ackingKVNATS) FetchOne(_ context.Context, _ string, cfg jetstream.ConsumerConfig, _ time.Duration) (jetstream.Msg, error) {
	res, ok := f.results[cfg.FilterSubject]
	if !ok {
		return nil, nil
	}
	delete(f.results, cfg.FilterSubject)
	data, _ := json.Marshal(res)
	return &fakeMsg{subject: cfg.FilterSubject, data: data}, nil
}

func (f *ackingKVNATS) KVPut(_ context.Context, _, key string, v []byte) error {
	f.kv[key] = v
	return nil
}

func (f *ackingKVNATS) KVGet(_ context.Context, _, key string) ([]byte, error) {
	if v, ok := f.kv[key]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("not found")
}

func (f *ackingKVNATS) PublishJSONSeq(string, any) (uint64, error) { return 1, nil }

func (f *ackingKVNATS) ConsumerDeliveredSeq(context.Context, string, string) (uint64, error) {
	return 1, nil
}

func newResultsFixture() (*ChainReconciler, *ackingKVNATS, *aiv1alpha1.Chain, *chainRun) {
	fake := &ackingKVNATS{fakeNATSClient: newFakeNATSClient(), results: map[string]natspkg.TaskResult{}, kv: map[string][]byte{}}
	r := &ChainReconciler{Recorder: record.NewFakeRecorder(100), NATS: natspkg.NewProviderWithClient(fake, logr.Discard())}
	started := metav1.NewTime(time.Now().Add(-time.Minute))
	chain := &aiv1alpha1.Chain{
		ObjectMeta: metav1.ObjectMeta{Name: "c"},
		Spec: aiv1alpha1.ChainSpec{Steps: []aiv1alpha1.ChainStep{
			{Name: "build", KnightRef: corev1.LocalObjectReference{Name: "k"}, Task: "t", TimeoutSeconds: 600},
		}},
		Status: aiv1alpha1.ChainStatus{RunID: "run-1", StepStatuses: []aiv1alpha1.ChainStepStatus{
			{Name: "build", Phase: aiv1alpha1.ChainStepPhaseRunning, TaskID: "task-1", StartedAt: &started},
		}},
	}
	nc := natsConfig{SubjectPrefix: "p", ResultsStream: "res", TasksStream: "tasks"}
	return r, fake, chain, newChainRun(nc, chain)
}

func (f *ackingKVNATS) queue(res natspkg.TaskResult) {
	f.results[natspkg.ResultSubject("p", "task-1")] = res
}

func putKV(f *ackingKVNATS, runID, output, errStr string, storedAt time.Time) {
	b, _ := json.Marshal(map[string]any{"runId": runID, "output": output, "error": errStr, "storedAt": storedAt.UTC().Format(time.RFC3339)})
	f.kv["c.build"] = b
}

// A result acked off the stream whose status write conflicted is completed
// from KV on the next reconcile, not left to time out.
func TestCollectStepResult_RecoversAckedSuccessAfterLostStatusWrite(t *testing.T) {
	ctx := context.Background()
	r, fake, chain, run := newResultsFixture()
	persisted := chain.DeepCopy() // what the API server holds: the status write conflicts

	fake.queue(natspkg.TaskResult{TaskID: "task-1", Output: "built ok"})
	r.collectStepResult(ctx, chain, run, &chain.Status.StepStatuses[0])
	if got := chain.Status.StepStatuses[0]; got.Phase != aiv1alpha1.ChainStepPhaseSucceeded {
		t.Fatalf("first reconcile: phase %s", got.Phase)
	}
	if len(fake.results) != 0 {
		t.Fatalf("result still on the stream")
	}

	// Conflict: in-memory change discarded; the next reconcile starts from persisted.
	next := persisted
	r.collectStepResult(ctx, next, newChainRun(run.nc, next), &next.Status.StepStatuses[0])
	got := next.Status.StepStatuses[0]
	if got.Phase != aiv1alpha1.ChainStepPhaseSucceeded || got.Output != "built ok" {
		t.Fatalf("want Succeeded with output, got phase=%s output=%q error=%q", got.Phase, got.Output, got.Error)
	}
}

func TestCollectStepResult_RecoversAckedFailureAfterLostStatusWrite(t *testing.T) {
	ctx := context.Background()
	r, fake, chain, run := newResultsFixture()
	persisted := chain.DeepCopy()

	fake.queue(natspkg.TaskResult{TaskID: "task-1", Error: "compile failed"})
	r.collectStepResult(ctx, chain, run, &chain.Status.StepStatuses[0])
	if chain.Status.StepStatuses[0].Phase != aiv1alpha1.ChainStepPhaseFailed {
		t.Fatalf("first reconcile: phase %s", chain.Status.StepStatuses[0].Phase)
	}

	next := persisted
	r.collectStepResult(ctx, next, newChainRun(run.nc, next), &next.Status.StepStatuses[0])
	got := next.Status.StepStatuses[0]
	if got.Phase != aiv1alpha1.ChainStepPhaseFailed || got.Error != "compile failed" {
		t.Fatalf("want Failed with the knight's error, got phase=%s error=%q", got.Phase, got.Error)
	}
}

func TestCollectStepResult_IgnoresStaleKVEntries(t *testing.T) {
	ctx := context.Background()
	cases := map[string]struct {
		runID    string
		storedAt func(started time.Time) time.Time
	}{
		"different run":     {"run-0", func(s time.Time) time.Time { return s.Add(time.Second) }},
		"older than start":  {"run-1", func(s time.Time) time.Time { return s.Add(-time.Hour) }},
		"entry without run": {"", func(s time.Time) time.Time { return s.Add(time.Second) }},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			r, fake, chain, run := newResultsFixture()
			ss := &chain.Status.StepStatuses[0]
			putKV(fake, tc.runID, "stale output", "", tc.storedAt(ss.StartedAt.Time))
			r.collectStepResult(ctx, chain, run, ss)
			if ss.Phase != aiv1alpha1.ChainStepPhaseRunning {
				t.Fatalf("stale KV entry completed the step: phase=%s", ss.Phase)
			}
		})
	}
}

func TestCollectStepResult_NormalPathUnchanged(t *testing.T) {
	ctx := context.Background()
	r, fake, chain, run := newResultsFixture()
	ss := &chain.Status.StepStatuses[0]

	// No result and no KV entry: still running.
	r.collectStepResult(ctx, chain, run, ss)
	if ss.Phase != aiv1alpha1.ChainStepPhaseRunning {
		t.Fatalf("phase %s, want Running", ss.Phase)
	}

	// A result on the stream wins over a (stale) KV entry.
	putKV(fake, "run-1", "old", "", time.Now().Add(time.Second))
	fake.queue(natspkg.TaskResult{TaskID: "task-1", Output: "fresh"})
	r.collectStepResult(ctx, chain, run, ss)
	if ss.Phase != aiv1alpha1.ChainStepPhaseSucceeded || ss.Output != "fresh" {
		t.Fatalf("phase=%s output=%q", ss.Phase, ss.Output)
	}
}

// A recovered result keeps the PRs and verdict the knight reported, in the
// step status path and in the KV entry applyStepSuccess rewrites.
func TestCollectStepResult_RecoveryPreservesPRsAndVerdict(t *testing.T) {
	ctx := context.Background()
	r, fake, chain, run := newResultsFixture()
	persisted := chain.DeepCopy()

	// Output text deliberately parses to a different verdict than the reported one.
	fake.queue(natspkg.TaskResult{
		TaskID:  "task-1",
		Output:  "Verdict: needs_changes (quoted from the PR)",
		Verdict: natspkg.VerdictShipIt,
		PRs:     []natspkg.PRRef{{Repo: "o/r", Number: 7, URL: "https://github.com/o/r/pull/7", Action: "opened"}},
	})
	r.collectStepResult(ctx, chain, run, &chain.Status.StepStatuses[0])

	next := persisted
	r.collectStepResult(ctx, next, newChainRun(run.nc, next), &next.Status.StepStatuses[0])
	if got := next.Status.StepStatuses[0]; got.Phase != aiv1alpha1.ChainStepPhaseSucceeded {
		t.Fatalf("phase %s", got.Phase)
	}

	var kv kvRecord
	if err := json.Unmarshal(fake.kv["c.build"], &kv); err != nil {
		t.Fatal(err)
	}
	if kv.Verdict != natspkg.VerdictShipIt {
		t.Fatalf("KV verdict = %q, want %q", kv.Verdict, natspkg.VerdictShipIt)
	}
	if len(kv.PRs) != 1 || kv.PRs[0].Number != 7 {
		t.Fatalf("KV PRs = %+v", kv.PRs)
	}
}

// lockedWaitNATS serves queued results under a lock and records the wait each
// FetchOne was called with, so concurrent reconciles can share it.
type lockedWaitNATS struct {
	*fakeNATSClient
	mu    sync.Mutex
	waits []time.Duration
}

func (f *lockedWaitNATS) FetchOne(_ context.Context, _ string, _ jetstream.ConsumerConfig, wait time.Duration) (jetstream.Msg, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.waits = append(f.waits, wait)
	return nil, nil
}

func (f *lockedWaitNATS) ConsumerDeliveredSeq(context.Context, string, string) (uint64, error) {
	return 1, nil
}

func (f *lockedWaitNATS) PublishJSONSeq(string, any) (uint64, error) { return 1, nil }

// The result poll must not block: no wait, and a step with no result yet
// returns quickly.
func TestPollResult_DoesNotBlock(t *testing.T) {
	fake := &lockedWaitNATS{fakeNATSClient: newFakeNATSClient()}
	r := &ChainReconciler{NATS: natspkg.NewProviderWithClient(fake, logr.Discard())}
	nc := natsConfig{SubjectPrefix: "p", ResultsStream: "res"}

	start := time.Now()
	res, err := r.pollResult(context.Background(), nc, "c", "build", "task-1")
	if err != nil || res != nil {
		t.Fatalf("pollResult = %v, %v", res, err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("pollResult took %s", d)
	}
	if len(fake.waits) != 1 || fake.waits[0] > 0 {
		t.Fatalf("FetchOne waits = %v, want [0]", fake.waits)
	}
}

// Two chains poll and schedule through one reconciler at once; run with -race.
func TestChainReconciler_ConcurrentChains(t *testing.T) {
	fake := &lockedWaitNATS{fakeNATSClient: newFakeNATSClient()}
	r := &ChainReconciler{NATS: natspkg.NewProviderWithClient(fake, logr.Discard())}
	t.Cleanup(func() {
		if r.cron != nil {
			<-r.cron.Stop().Done()
		}
	})
	nc := natsConfig{SubjectPrefix: "p", ResultsStream: "res"}

	var wg sync.WaitGroup
	for i := range 2 {
		name := fmt.Sprintf("chain-%d", i)
		wg.Go(func() {
			for range 20 {
				if _, err := r.pollResult(context.Background(), nc, name, "build", "task-"+name); err != nil {
					t.Error(err)
				}
				ch := &aiv1alpha1.Chain{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name},
					Spec: aiv1alpha1.ChainSpec{Schedule: "0 0 * * *"}}
				r.reconcileSchedule(context.Background(), ch)
				r.removeCronEntry(types.NamespacedName{Namespace: "ns", Name: name})
			}
		})
	}
	wg.Wait()
	if len(fake.waits) != 40 {
		t.Fatalf("got %d polls, want 40", len(fake.waits))
	}
}

// erroringFetchNATS fails every stream read; the KV still works.
type erroringFetchNATS struct{ *ackingKVNATS }

func (f *erroringFetchNATS) FetchOne(context.Context, string, jetstream.ConsumerConfig, time.Duration) (jetstream.Msg, error) {
	return nil, fmt.Errorf("stream read failed")
}

// A failed stream read must not hide a result already stored in KV (#908), and
// the recovered step keeps the model, tokens and cost the entry holds.
func TestCollectStepResult_RecoversFromKVAfterStreamReadError(t *testing.T) {
	t.Cleanup(metrics.ChainStepCostUSDTotal.Reset) // keep the global series count of the usage tests stable
	ctx := context.Background()
	r, fake, chain, run := newResultsFixture()
	r.NATS = natspkg.NewProviderWithClient(&erroringFetchNATS{fake}, logr.Discard())
	ss := &chain.Status.StepStatuses[0]
	b, _ := json.Marshal(map[string]any{
		"runId": "run-1", "output": "built ok", "storedAt": time.Now().Add(time.Second).UTC().Format(time.RFC3339),
		"model": "claude-sonnet-4", "cost": 0.25, "tokens": map[string]any{"input": 10, "output": 5},
	})
	fake.kv["c.build"] = b

	r.collectStepResult(ctx, chain, run, ss)

	if ss.Phase != aiv1alpha1.ChainStepPhaseSucceeded || ss.Output != "built ok" {
		t.Fatalf("phase=%s output=%q error=%q", ss.Phase, ss.Output, ss.Error)
	}
	if ss.Model != "claude-sonnet-4" || ss.CostUSD == "" || ss.Tokens.Input != 10 || ss.Tokens.Output != 5 {
		t.Fatalf("usage not carried: model=%q cost=%q tokens=%+v", ss.Model, ss.CostUSD, ss.Tokens)
	}
}

// Old KV entries without model/tokens/cost still recover.
func TestCollectStepResult_RecoversOldKVEntryWithoutUsage(t *testing.T) {
	ctx := context.Background()
	r, fake, chain, run := newResultsFixture()
	ss := &chain.Status.StepStatuses[0]
	putKV(fake, "run-1", "old entry", "", time.Now().Add(time.Second))
	r.collectStepResult(ctx, chain, run, ss)
	if ss.Phase != aiv1alpha1.ChainStepPhaseSucceeded || ss.Model != "" || ss.CostUSD != "" {
		t.Fatalf("%+v", ss)
	}
}

// The KV entry written on success carries the result's model, tokens and cost.
func TestApplyStepSuccessStoresUsageInKV(t *testing.T) {
	t.Cleanup(metrics.ChainStepCostUSDTotal.Reset) // keep the global series count of the usage tests stable
	ctx := context.Background()
	r, fake, chain, run := newResultsFixture()
	ss := &chain.Status.StepStatuses[0]
	fake.queue(natspkg.TaskResult{TaskID: "task-1", Output: "ok", Model: "m1", Cost: 0.5, Tokens: &natspkg.TokenUsage{Input: 3}})
	r.collectStepResult(ctx, chain, run, ss)
	var got map[string]any
	if err := json.Unmarshal(fake.kv["c.build"], &got); err != nil {
		t.Fatal(err)
	}
	if got["model"] != "m1" || got["cost"] != 0.5 || got["tokens"] == nil {
		t.Fatalf("kv entry: %v", got)
	}
}

// headerFetchNATS serves one result message with the given NATS headers.
type headerFetchNATS struct {
	*fakeNATSClient
	headers nats.Header
}

type headerMsg struct {
	*fakeMsg
	headers nats.Header
}

func (m *headerMsg) Headers() nats.Header { return m.headers }

func (f *headerFetchNATS) FetchOne(_ context.Context, _ string, cfg jetstream.ConsumerConfig, _ time.Duration) (jetstream.Msg, error) {
	data, _ := json.Marshal(natspkg.TaskResult{TaskID: "task-1", Output: "ok"})
	return &headerMsg{fakeMsg: &fakeMsg{subject: cfg.FilterSubject, data: data}, headers: f.headers}, nil
}

func (f *headerFetchNATS) PublishJSONSeq(string, any) (uint64, error) { return 1, nil }

func (f *headerFetchNATS) ConsumerDeliveredSeq(context.Context, string, string) (uint64, error) {
	return 1, nil
}

// The result's consume span is parented on the message's traceparent, and on
// the run span from ctx when the message carries none.
func TestPollResult_ParentsConsumeSpanOnResultTraceparent(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTracerProvider(prevTP); otel.SetTextMapPropagator(prevProp) })

	tid, _ := trace.TraceIDFromHex("0af7651916cd43dd8448eb211c80319c")
	fromMsg, _ := trace.SpanIDFromHex("b7ad6b7169203331")
	fromRun, _ := trace.SpanIDFromHex("1111111111111111")
	runCtx := trace.ContextWithRemoteSpanContext(context.Background(), trace.NewSpanContext(
		trace.SpanContextConfig{TraceID: tid, SpanID: fromRun, TraceFlags: trace.FlagsSampled, Remote: true}))
	nc := natsConfig{SubjectPrefix: "p", ResultsStream: "res"}

	for name, tc := range map[string]struct {
		headers nats.Header
		want    trace.SpanID
	}{
		"header wins":     {nats.Header{"traceparent": {"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}}, fromMsg},
		"fallback to ctx": {nil, fromRun},
	} {
		exp.Reset()
		fake := &headerFetchNATS{fakeNATSClient: newFakeNATSClient(), headers: tc.headers}
		r := &ChainReconciler{NATS: natspkg.NewProviderWithClient(fake, logr.Discard())}
		if res, err := r.pollResult(runCtx, nc, "c", "build", "task-1"); err != nil || res == nil {
			t.Fatalf("%s: pollResult = %v, %v", name, res, err)
		}
		spans := exp.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("%s: got %d spans", name, len(spans))
		}
		if got := spans[0].Parent.SpanID(); got != tc.want || spans[0].SpanContext.TraceID() != tid {
			t.Fatalf("%s: parent = %s, want %s (trace %s)", name, got, tc.want, spans[0].SpanContext.TraceID())
		}
	}
}
