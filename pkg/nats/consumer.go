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
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// The knight task consumer settings. The source of truth is the knights'
// golden fixture pkg/nats/contract/testdata/consumer.knight-tasks.json (synced
// from roundtable-k8s/knights packages/core/contract/fixtures by
// `make contract-sync`); TestKnightTaskConsumerMatchesFixture fails if these
// values drift from it. Until the runtimes only bind to the consumer, both
// sides create it, so these values must stay identical to theirs or the two
// would keep rewriting each other's consumer.
const (
	// KnightTaskAckWait is the heartbeat window (ACK_WAIT_MS), not the task
	// timeout: a running task sends an in-progress ack well within it.
	KnightTaskAckWait = 30 * time.Second

	// KnightTaskMaxDeliver is 1: a task that crashed mid-run is reported as a
	// failure, never re-run silently.
	KnightTaskMaxDeliver = 1
)

// KnightTaskConsumerConfig is the durable pull consumer a knight reads its
// tasks from: KnightTaskConsumerName(knightName), explicit ack, deliver all,
// max_deliver 1, 30 s ack_wait, filtered on the knight's subjects. KEDA
// scales the knight on this consumer's lag, so its name and config must not
// change. Subjects are trimmed and empty ones dropped, as the runtimes do with
// SUBSCRIBE_TOPICS.
func KnightTaskConsumerConfig(knightName string, subjects []string) jetstream.ConsumerConfig {
	filters := make([]string, 0, len(subjects))
	for _, s := range subjects {
		if s = strings.TrimSpace(s); s != "" {
			filters = append(filters, s)
		}
	}
	return jetstream.ConsumerConfig{
		Durable:        KnightTaskConsumerName(knightName),
		FilterSubjects: filters,
		AckPolicy:      jetstream.AckExplicitPolicy,
		DeliverPolicy:  jetstream.DeliverAllPolicy,
		MaxDeliver:     KnightTaskMaxDeliver,
		AckWait:        KnightTaskAckWait,
	}
}

// ConsumerAction is what EnsureConsumer did to reach the wanted config.
type ConsumerAction string

const (
	// ConsumerCreated means the consumer did not exist and was created.
	ConsumerCreated ConsumerAction = "Created"
	// ConsumerUnchanged means the consumer already matched and was left
	// alone (adopted as it is).
	ConsumerUnchanged ConsumerAction = "Unchanged"
	// ConsumerUpdated means mutable fields (filters, ack_wait, max_deliver)
	// differed and were updated in place, keeping the consumer's state.
	ConsumerUpdated ConsumerAction = "Updated"
	// ConsumerRecreated means a field NATS cannot update differed, so the
	// consumer was deleted and created again.
	ConsumerRecreated ConsumerAction = "Recreated"
)

// EnsureConsumer makes the durable consumer cfg.Durable on stream match cfg.
// It never writes a consumer that already matches, updates mutable fields in
// place and deletes and recreates the consumer only when a field NATS cannot
// update differs (see consumerNeedsRecreate), because recreating a
// deliver-all consumer loses its delivery state.
func (c *JetStreamClient) EnsureConsumer(
	ctx context.Context, stream string, cfg jetstream.ConsumerConfig,
) (ConsumerAction, error) {
	js, err := c.jetStream()
	if err != nil {
		return "", err
	}
	if cfg.Durable == "" {
		return "", errors.New("EnsureConsumer: durable name is required")
	}

	existing, err := c.consumerInfo(ctx, stream, cfg.Durable)
	switch {
	case errors.Is(err, jetstream.ErrConsumerNotFound):
		if _, err := js.CreateConsumer(ctx, stream, cfg); err != nil {
			return "", fmt.Errorf("create consumer %s on stream %s: %w", cfg.Durable, stream, err)
		}
		return ConsumerCreated, nil
	case err != nil:
		return "", err
	}

	if reason := consumerNeedsRecreate(existing.Config, cfg); reason != "" {
		c.log.Info("Recreating consumer: a field NATS cannot update differs",
			"stream", stream, "consumer", cfg.Durable, "field", reason)
		if err := js.DeleteConsumer(ctx, stream, cfg.Durable); err != nil &&
			!errors.Is(err, jetstream.ErrConsumerNotFound) {
			return "", fmt.Errorf("delete consumer %s on stream %s: %w", cfg.Durable, stream, err)
		}
		if _, err := js.CreateConsumer(ctx, stream, cfg); err != nil {
			return "", fmt.Errorf("recreate consumer %s on stream %s: %w", cfg.Durable, stream, err)
		}
		return ConsumerRecreated, nil
	}

	if consumerMatches(existing.Config, cfg) {
		return ConsumerUnchanged, nil
	}
	if _, err := js.UpdateConsumer(ctx, stream, cfg); err != nil {
		return "", fmt.Errorf("update consumer %s on stream %s: %w", cfg.Durable, stream, err)
	}
	return ConsumerUpdated, nil
}

// RemoveConsumer deletes a consumer and returns any error, unlike the best
// effort DeleteConsumer. A consumer or stream that is already gone is not an
// error.
func (c *JetStreamClient) RemoveConsumer(ctx context.Context, stream, consumer string) error {
	js, err := c.jetStream()
	if err != nil {
		return err
	}
	err = js.DeleteConsumer(ctx, stream, consumer)
	if err == nil || errors.Is(err, jetstream.ErrConsumerNotFound) || errors.Is(err, jetstream.ErrStreamNotFound) {
		return nil
	}
	return fmt.Errorf("delete consumer %s on stream %s: %w", consumer, stream, err)
}

// consumerNeedsRecreate names the first field NATS refuses to update
// (nats-server checkNewConsumerConfig) that differs between the existing
// consumer and the wanted one, or returns "" when an in-place update will do.
// Only fields the wanted config sets are compared; the rest keep the server
// defaults on both sides.
func consumerNeedsRecreate(have, want jetstream.ConsumerConfig) string {
	switch {
	case have.DeliverPolicy != want.DeliverPolicy:
		return "deliver_policy"
	case have.AckPolicy != want.AckPolicy:
		return "ack_policy"
	case have.ReplayPolicy != want.ReplayPolicy:
		return "replay_policy"
	case have.OptStartSeq != want.OptStartSeq:
		return "opt_start_seq"
	case (have.OptStartTime == nil) != (want.OptStartTime == nil):
		return "opt_start_time"
	case (have.DeliverSubject == "") != (want.DeliverSubject == ""):
		return "deliver_subject" // push <-> pull
	case have.IdleHeartbeat != want.IdleHeartbeat:
		return "idle_heartbeat"
	case have.FlowControl != want.FlowControl:
		return "flow_control"
	}
	return ""
}

// consumerMatches reports whether the mutable fields the knight consumer sets
// already have the wanted values.
func consumerMatches(have, want jetstream.ConsumerConfig) bool {
	return slices.Equal(consumerFilters(have), consumerFilters(want)) &&
		have.AckWait == want.AckWait &&
		have.MaxDeliver == want.MaxDeliver
}

// consumerFilters returns the consumer's filter subjects, sorted. A single
// filter_subject and a one-element filter_subjects are the same filter.
func consumerFilters(cfg jetstream.ConsumerConfig) []string {
	var out []string
	if cfg.FilterSubject != "" {
		out = append(out, cfg.FilterSubject)
	}
	out = append(out, cfg.FilterSubjects...)
	slices.Sort(out)
	return out
}
