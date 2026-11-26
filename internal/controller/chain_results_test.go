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
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go/jetstream"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
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

func (f *ackingKVNATS) ConsumerAckFloor(context.Context, string, string) (uint64, error) {
	return 1, nil
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
