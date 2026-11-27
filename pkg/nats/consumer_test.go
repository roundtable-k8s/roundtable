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
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestKnightTaskConsumerConfig(t *testing.T) {
	got := KnightTaskConsumerConfig("galahad", []string{" rt.tasks.security.> ", "", "rt.tasks.all.>"})
	want := jetstream.ConsumerConfig{
		Durable:        "Galahad-consumer",
		FilterSubjects: []string{"rt.tasks.security.>", "rt.tasks.all.>"},
		AckPolicy:      jetstream.AckExplicitPolicy,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
		MaxDeliver:     1,
		AckWait:        30 * time.Second,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("KnightTaskConsumerConfig = %+v, want %+v", got, want)
	}
}

// TestKnightTaskConsumerMatchesFixture builds the consumer from the knights'
// golden fixture's filter subjects and compares every setting with the fixture.
// The durable name is compared case-insensitively: the fixture was generated
// from the lowercase knight name "galahad", while the operator (like the pod's
// KNIGHT_NAME) uses the capitalized one, so only the case of the name part can
// differ.
func TestKnightTaskConsumerMatchesFixture(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(contractDir, "testdata", "consumer.knight-tasks.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fx struct {
		DurableName    string                  `json:"durable_name"`
		FilterSubjects []string                `json:"filter_subjects"`
		AckPolicy      jetstream.AckPolicy     `json:"ack_policy"`
		DeliverPolicy  jetstream.DeliverPolicy `json:"deliver_policy"`
		MaxDeliver     int                     `json:"max_deliver"`
		AckWait        int64                   `json:"ack_wait"`
	}
	if err := json.Unmarshal(data, &fx); err != nil {
		t.Fatal(err)
	}
	const knight = "galahad"
	got := KnightTaskConsumerConfig(knight, fx.FilterSubjects)

	if !strings.EqualFold(got.Durable, fx.DurableName) {
		t.Errorf("durable name = %q, fixture %q", got.Durable, fx.DurableName)
	}
	if !slices.Equal(got.FilterSubjects, fx.FilterSubjects) {
		t.Errorf("filter subjects = %v, fixture %v", got.FilterSubjects, fx.FilterSubjects)
	}
	if got.AckPolicy != fx.AckPolicy {
		t.Errorf("ack policy = %v, fixture %v", got.AckPolicy, fx.AckPolicy)
	}
	if got.DeliverPolicy != fx.DeliverPolicy {
		t.Errorf("deliver policy = %v, fixture %v", got.DeliverPolicy, fx.DeliverPolicy)
	}
	if got.MaxDeliver != fx.MaxDeliver {
		t.Errorf("max deliver = %d, fixture %d", got.MaxDeliver, fx.MaxDeliver)
	}
	if got.AckWait != time.Duration(fx.AckWait) {
		t.Errorf("ack wait = %v, fixture %v", got.AckWait, time.Duration(fx.AckWait))
	}
}

// tasksStream creates the work-queue tasks stream the knights consume from.
func tasksStream(t *testing.T, c *JetStreamClient) {
	t.Helper()
	if err := c.CreateStream(context.Background(), StreamConfig{
		Name: "rt_tasks", Subjects: []string{"rt.tasks.>"},
		Retention: RetentionWorkQueue, Storage: StorageMemory,
	}); err != nil {
		t.Fatal(err)
	}
}

// createLikeRuntime creates the consumer the way the knight runtimes do
// (packages/core subscribe(): jsm.consumers.add with filter_subjects).
func createLikeRuntime(t *testing.T, c *JetStreamClient, durable string, filters []string, ackWait time.Duration) {
	t.Helper()
	req, _ := json.Marshal(map[string]any{
		"stream_name": "rt_tasks",
		"config": map[string]any{
			"durable_name":    durable,
			"filter_subjects": filters,
			"ack_policy":      "explicit",
			"deliver_policy":  "all",
			"max_deliver":     1,
			"ack_wait":        ackWait.Nanoseconds(),
		},
	})
	msg, err := c.nc.Request("$JS.API.CONSUMER.CREATE.rt_tasks."+durable, req, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var resp struct {
		Error *struct{ Description string } `json:"error"`
	}
	if err := json.Unmarshal(msg.Data, &resp); err != nil || resp.Error != nil {
		t.Fatalf("runtime-style consumer create: %s (%v)", msg.Data, err)
	}
}

// takeOneTask publishes a task and fetches it without acking, so the
// consumer has one ack-pending message (a task in flight).
func takeOneTask(t *testing.T, c *JetStreamClient, durable string) {
	t.Helper()
	ctx := context.Background()
	if _, err := c.PublishJSONSeq("rt.tasks.code.t1", TaskPayload{TaskID: "t1", Task: "x"}); err != nil {
		t.Fatal(err)
	}
	cons, err := c.js.Consumer(ctx, "rt_tasks", durable)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cons.Next(jetstream.FetchMaxWait(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureConsumerCreatesThenLeavesAlone(t *testing.T) {
	ctx := context.Background()
	c := connectTestClient(t)
	tasksStream(t, c)

	cfg := KnightTaskConsumerConfig("lancelot", []string{"rt.tasks.code.>"})
	act, err := c.EnsureConsumer(ctx, "rt_tasks", cfg)
	if err != nil || act != ConsumerCreated {
		t.Fatalf("first EnsureConsumer = %q, %v; want Created", act, err)
	}
	info, err := c.consumerInfo(ctx, "rt_tasks", "Lancelot-consumer")
	if err != nil {
		t.Fatal(err)
	}
	got := info.Config
	if got.AckPolicy != jetstream.AckExplicitPolicy || got.DeliverPolicy != jetstream.DeliverAllPolicy ||
		got.MaxDeliver != 1 || got.AckWait != 30*time.Second ||
		!reflect.DeepEqual(consumerFilters(got), []string{"rt.tasks.code.>"}) {
		t.Fatalf("unexpected consumer config: %+v", got)
	}

	act, err = c.EnsureConsumer(ctx, "rt_tasks", cfg)
	if err != nil || act != ConsumerUnchanged {
		t.Fatalf("second EnsureConsumer = %q, %v; want Unchanged", act, err)
	}
}

func TestEnsureConsumerAdoptsRuntimeConsumer(t *testing.T) {
	ctx := context.Background()
	c := connectTestClient(t)
	tasksStream(t, c)

	createLikeRuntime(t, c, "Lancelot-consumer", []string{"rt.tasks.code.>"}, 30*time.Second)
	takeOneTask(t, c, "Lancelot-consumer")
	before, err := c.consumerInfo(ctx, "rt_tasks", "Lancelot-consumer")
	if err != nil {
		t.Fatal(err)
	}

	act, err := c.EnsureConsumer(ctx, "rt_tasks", KnightTaskConsumerConfig("lancelot", []string{"rt.tasks.code.>"}))
	if err != nil || act != ConsumerUnchanged {
		t.Fatalf("EnsureConsumer = %q, %v; want Unchanged (adopted)", act, err)
	}
	after, err := c.consumerInfo(ctx, "rt_tasks", "Lancelot-consumer")
	if err != nil {
		t.Fatal(err)
	}
	if !after.Created.Equal(before.Created) || after.NumAckPending != 1 || after.Delivered.Stream != 1 {
		t.Fatalf("consumer was recreated or lost state: before %+v, after %+v", before, after)
	}
}

func TestEnsureConsumerUpdatesMutableFieldsInPlace(t *testing.T) {
	ctx := context.Background()
	c := connectTestClient(t)
	tasksStream(t, c)

	// A runtime started with a test ack_wait and an old subject list.
	createLikeRuntime(t, c, "Lancelot-consumer", []string{"rt.tasks.old.>", "rt.tasks.code.>"}, 5*time.Second)
	takeOneTask(t, c, "Lancelot-consumer")
	before, err := c.consumerInfo(ctx, "rt_tasks", "Lancelot-consumer")
	if err != nil {
		t.Fatal(err)
	}

	act, err := c.EnsureConsumer(ctx, "rt_tasks", KnightTaskConsumerConfig("lancelot", []string{"rt.tasks.code.>"}))
	if err != nil || act != ConsumerUpdated {
		t.Fatalf("EnsureConsumer = %q, %v; want Updated", act, err)
	}
	after, err := c.consumerInfo(ctx, "rt_tasks", "Lancelot-consumer")
	if err != nil {
		t.Fatal(err)
	}
	if !after.Created.Equal(before.Created) || after.NumAckPending != 1 {
		t.Fatalf("update recreated the consumer: before %+v, after %+v", before, after)
	}
	if after.Config.AckWait != 30*time.Second ||
		!reflect.DeepEqual(consumerFilters(after.Config), []string{"rt.tasks.code.>"}) {
		t.Fatalf("consumer not updated: %+v", after.Config)
	}
}

func TestEnsureConsumerRecreatesOnImmutableChange(t *testing.T) {
	ctx := context.Background()
	c := connectTestClient(t)
	// A limits stream: a work-queue stream only takes deliver-all consumers.
	if err := c.CreateStream(ctx, StreamConfig{
		Name: "rt_lim", Subjects: []string{"rt.lim.>"}, Retention: RetentionLimits, Storage: StorageMemory,
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := c.js.CreateConsumer(ctx, "rt_lim", jetstream.ConsumerConfig{
		Durable: "Lancelot-consumer", FilterSubject: "rt.lim.code.>",
		AckPolicy: jetstream.AckExplicitPolicy, DeliverPolicy: jetstream.DeliverNewPolicy,
	}); err != nil {
		t.Fatal(err)
	}
	act, err := c.EnsureConsumer(ctx, "rt_lim", KnightTaskConsumerConfig("lancelot", []string{"rt.lim.code.>"}))
	if err != nil || act != ConsumerRecreated {
		t.Fatalf("EnsureConsumer = %q, %v; want Recreated", act, err)
	}
	info, err := c.consumerInfo(ctx, "rt_lim", "Lancelot-consumer")
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.DeliverPolicy != jetstream.DeliverAllPolicy {
		t.Fatalf("deliver policy = %v, want all", info.Config.DeliverPolicy)
	}
}

func TestEnsureConsumerMissingStream(t *testing.T) {
	c := connectTestClient(t)
	_, err := c.EnsureConsumer(context.Background(), "nope", KnightTaskConsumerConfig("a", []string{"x.>"}))
	if err == nil {
		t.Fatal("EnsureConsumer on a missing stream succeeded")
	}
}

func TestRemoveConsumer(t *testing.T) {
	ctx := context.Background()
	c := connectTestClient(t)
	tasksStream(t, c)

	if _, err := c.EnsureConsumer(ctx, "rt_tasks", KnightTaskConsumerConfig("a", []string{"rt.tasks.a.>"})); err != nil {
		t.Fatal(err)
	}
	if err := c.RemoveConsumer(ctx, "rt_tasks", "A-consumer"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.consumerInfo(ctx, "rt_tasks", "A-consumer"); err == nil {
		t.Fatal("consumer still there after RemoveConsumer")
	}
	// Already gone, and a missing stream, are not errors.
	if err := c.RemoveConsumer(ctx, "rt_tasks", "A-consumer"); err != nil {
		t.Fatalf("RemoveConsumer of a missing consumer: %v", err)
	}
	if err := c.RemoveConsumer(ctx, "nope", "A-consumer"); err != nil {
		t.Fatalf("RemoveConsumer on a missing stream: %v", err)
	}
}
