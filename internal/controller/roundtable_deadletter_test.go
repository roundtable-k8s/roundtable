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
	"testing"
	"time"

	"github.com/go-logr/logr"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go/jetstream"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/deadletter"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// ensureStreams creates the tasks, results, advisories and dead-letter
// streams and starts the table's dead-letter watcher (#396).
func TestEnsureStreamsCreatesDeadLetterStreams(t *testing.T) {
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	ctx := context.Background()
	ns := natstest.RunServer(&opts)
	defer ns.Shutdown()

	cli := natspkg.NewClient(natspkg.Config{URL: ns.ClientURL()}, logr.Discard())
	if err := cli.Connect(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()
	dl := deadletter.NewManager(logr.Discard())
	defer dl.StopAll()
	r := &RoundTableReconciler{NATS: natspkg.NewProviderWithClient(cli, logr.Discard()), DeadLetters: dl}

	rt := &aiv1alpha1.RoundTable{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "roundtable"},
		Spec: aiv1alpha1.RoundTableSpec{NATS: aiv1alpha1.RoundTableNATS{
			SubjectPrefix:    "rt-dev",
			TasksStream:      "rt_dev_tasks",
			ResultsStream:    "rt_dev_results",
			CreateStreams:    true,
			DeadLetterMaxAge: &metav1.Duration{Duration: 72 * time.Hour},
		}},
	}
	if err := r.ensureStreams(ctx, rt); err != nil {
		t.Fatal(err)
	}
	// A second reconcile is a no-op.
	if err := r.ensureStreams(ctx, rt); err != nil {
		t.Fatal(err)
	}

	dlq, err := cli.StreamInfo(ctx, "rt_dev_tasks_DLQ")
	if err != nil {
		t.Fatal(err)
	}
	if dlq.Config.MaxAge != 72*time.Hour || dlq.Config.Retention != jetstream.LimitsPolicy ||
		dlq.Config.Duplicates != deadletter.DuplicateWindow || dlq.Config.Subjects[0] != "rt-dev.dlq.>" {
		t.Fatalf("DLQ stream config = %+v", dlq.Config)
	}
	events, err := cli.StreamInfo(ctx, "rt_dev_tasks_EVENTS")
	if err != nil {
		t.Fatal(err)
	}
	if events.Config.Retention != jetstream.LimitsPolicy || events.Config.MaxAge != aiv1alpha1.DefaultEventsMaxAge ||
		events.Config.Subjects[0] != "rt-dev.events.>" {
		t.Fatalf("events stream config = %+v", events.Config)
	}
	if _, err := cli.StreamInfo(ctx, "rt_dev_tasks_ADVISORIES"); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.ConsumerDeliveredSeq(ctx, "rt_dev_tasks_ADVISORIES", deadletter.ConsumerName); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureStreamsSkipsEventsStreamForEphemeralTables(t *testing.T) {
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	ctx := context.Background()
	ns := natstest.RunServer(&opts)
	defer ns.Shutdown()

	cli := natspkg.NewClient(natspkg.Config{URL: ns.ClientURL()}, logr.Discard())
	if err := cli.Connect(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()
	r := &RoundTableReconciler{NATS: natspkg.NewProviderWithClient(cli, logr.Discard())}

	rt := &aiv1alpha1.RoundTable{
		ObjectMeta: metav1.ObjectMeta{Name: "msn", Namespace: "roundtable"},
		Spec: aiv1alpha1.RoundTableSpec{
			Ephemeral: true,
			NATS: aiv1alpha1.RoundTableNATS{
				SubjectPrefix: "msn", TasksStream: "msn_tasks", ResultsStream: "msn_results", CreateStreams: true,
			},
		},
	}
	if err := r.ensureStreams(ctx, rt); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.StreamInfo(ctx, "msn_tasks"); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.StreamInfo(ctx, "msn_tasks_EVENTS"); err == nil {
		t.Fatal("ephemeral table must not get an events stream")
	}
}

func TestEnsureStreamsResultsMaxAge(t *testing.T) {
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	ctx := context.Background()
	ns := natstest.RunServer(&opts)
	defer ns.Shutdown()

	cli := natspkg.NewClient(natspkg.Config{URL: ns.ClientURL()}, logr.Discard())
	if err := cli.Connect(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()
	r := &RoundTableReconciler{NATS: natspkg.NewProviderWithClient(cli, logr.Discard())}

	rt := &aiv1alpha1.RoundTable{
		ObjectMeta: metav1.ObjectMeta{Name: "ra", Namespace: "roundtable"},
		Spec: aiv1alpha1.RoundTableSpec{NATS: aiv1alpha1.RoundTableNATS{
			SubjectPrefix: "ra", TasksStream: "ra_tasks", ResultsStream: "ra_results", CreateStreams: true,
		}},
	}
	check := func(want time.Duration) {
		t.Helper()
		if err := r.ensureStreams(ctx, rt); err != nil {
			t.Fatal(err)
		}
		info, err := cli.StreamInfo(ctx, "ra_results")
		if err != nil {
			t.Fatal(err)
		}
		if info.Config.MaxAge != want {
			t.Fatalf("results MaxAge = %v, want %v", info.Config.MaxAge, want)
		}
		if info.Config.Retention != jetstream.WorkQueuePolicy {
			t.Fatalf("retention changed: %v", info.Config.Retention)
		}
		tasks, err := cli.StreamInfo(ctx, "ra_tasks")
		if err != nil {
			t.Fatal(err)
		}
		if tasks.Config.MaxAge != 0 {
			t.Fatalf("tasks stream must not expire, MaxAge = %v", tasks.Config.MaxAge)
		}
	}

	// Unset: created without MaxAge, and no update is issued.
	check(0)
	if updated, err := cli.EnsureStream(ctx, natspkg.StreamConfig{Name: "ra_results"}); err != nil || updated {
		t.Fatalf("unset must not update: updated=%v err=%v", updated, err)
	}
	// Set on an existing stream: applied.
	rt.Spec.NATS.ResultsMaxAge = &metav1.Duration{Duration: 168 * time.Hour}
	check(168 * time.Hour)
	// Changed.
	rt.Spec.NATS.ResultsMaxAge = &metav1.Duration{Duration: 24 * time.Hour}
	check(24 * time.Hour)
	// Unset after set clears it.
	rt.Spec.NATS.ResultsMaxAge = nil
	check(0)
}

func TestEnsureStreamsCreatesResultsWithMaxAge(t *testing.T) {
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	ctx := context.Background()
	ns := natstest.RunServer(&opts)
	defer ns.Shutdown()

	cli := natspkg.NewClient(natspkg.Config{URL: ns.ClientURL()}, logr.Discard())
	if err := cli.Connect(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Close() }()
	r := &RoundTableReconciler{NATS: natspkg.NewProviderWithClient(cli, logr.Discard())}
	rt := &aiv1alpha1.RoundTable{
		ObjectMeta: metav1.ObjectMeta{Name: "rb", Namespace: "roundtable"},
		Spec: aiv1alpha1.RoundTableSpec{NATS: aiv1alpha1.RoundTableNATS{
			SubjectPrefix: "rb", TasksStream: "rb_tasks", ResultsStream: "rb_results", CreateStreams: true,
			ResultsMaxAge: &metav1.Duration{Duration: 168 * time.Hour},
		}},
	}
	if err := r.ensureStreams(ctx, rt); err != nil {
		t.Fatal(err)
	}
	info, err := cli.StreamInfo(ctx, "rb_results")
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.MaxAge != 168*time.Hour {
		t.Fatalf("MaxAge = %v", info.Config.MaxAge)
	}
}
