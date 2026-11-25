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
	if _, err := cli.ConsumerAckFloor(ctx, "rt_dev_tasks_ADVISORIES", deadletter.ConsumerName); err != nil {
		t.Fatal(err)
	}
}
