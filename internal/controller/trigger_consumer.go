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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	triggerFetchBatch = 10
	triggerFetchWait  = 5 * time.Second
	triggerRetryDelay = 5 * time.Second
	// triggerAckWait is how long a delivered event may take to be handled
	// before JetStream redelivers it.
	triggerAckWait = 2 * time.Minute
)

// triggerConsumerConfig is the durable consumer a NATS-source Trigger reads.
// DeliverNew: a new Trigger fires on events published after it was created,
// not on the stream's backlog; afterwards the durable consumer remembers
// where it was, so events published while the operator is down still fire.
func triggerConsumerConfig(durable, subject string) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable:       durable,
		Description:   "Round Table Trigger",
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverNewPolicy,
		AckWait:       triggerAckWait,
	}
}

// ensureTriggerConsumer creates the durable consumer or updates its subject.
func ensureTriggerConsumer(ctx context.Context, nc *nats.Conn, stream, durable, subject string) error {
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	if _, err := js.CreateOrUpdateConsumer(ctx, stream, triggerConsumerConfig(durable, subject)); err != nil {
		return fmt.Errorf("consumer %s on stream %s: %w", durable, stream, err)
	}
	return nil
}

// deleteTriggerConsumer deletes the durable consumer. A missing consumer or
// stream is not an error.
func deleteTriggerConsumer(ctx context.Context, nc *nats.Conn, stream, durable string) error {
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	err = js.DeleteConsumer(ctx, stream, durable)
	if err == nil || errors.Is(err, jetstream.ErrConsumerNotFound) || errors.Is(err, jetstream.ErrStreamNotFound) {
		return nil
	}
	return fmt.Errorf("delete consumer %s on stream %s: %w", durable, stream, err)
}

// triggerMsgHandler handles one event; it must ack, nak or term msg.
type triggerMsgHandler func(ctx context.Context, msg jetstream.Msg)

// triggerConsumers runs one fetch loop per NATS-source Trigger. Loops are
// started from Reconcile, so they only run in the elected leader.
type triggerConsumers struct {
	log   logr.Logger
	mu    sync.Mutex
	loops map[string]*triggerLoop
}

type triggerLoopConfig struct {
	stream, durable, subject string
}

type triggerLoop struct {
	cfg    triggerLoopConfig
	nc     *nats.Conn
	cancel context.CancelFunc
	done   chan struct{}
}

func newTriggerConsumers(log logr.Logger) *triggerConsumers {
	return &triggerConsumers{log: log, loops: map[string]*triggerLoop{}}
}

// ensure starts the fetch loop for key unless it already runs on nc with cfg.
func (t *triggerConsumers) ensure(nc *nats.Conn, key string, cfg triggerLoopConfig, handle triggerMsgHandler) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if l, ok := t.loops[key]; ok {
		select {
		case <-l.done:
			// The loop ended (connection closed): start a new one.
		default:
			if l.nc == nc && !nc.IsClosed() && l.cfg == cfg {
				return nil
			}
		}
		l.stop()
		delete(t.loops, key)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	cons, err := js.Consumer(ctx, cfg.stream, cfg.durable)
	if err != nil {
		cancel()
		return fmt.Errorf("consumer %s on stream %s: %w", cfg.durable, cfg.stream, err)
	}
	l := &triggerLoop{cfg: cfg, nc: nc, cancel: cancel, done: make(chan struct{})}
	go l.run(logr.NewContext(ctx, t.log.WithValues("trigger", key)), cons, handle)
	t.loops[key] = l
	return nil
}

// stop ends key's fetch loop (the durable consumer is kept) and waits for it.
func (t *triggerConsumers) stop(key string) {
	t.mu.Lock()
	l, ok := t.loops[key]
	delete(t.loops, key)
	t.mu.Unlock()
	if ok {
		l.stop()
	}
}

// stopAll ends every fetch loop.
func (t *triggerConsumers) stopAll() {
	t.mu.Lock()
	loops := t.loops
	t.loops = map[string]*triggerLoop{}
	t.mu.Unlock()
	for _, l := range loops {
		l.stop()
	}
}

func (l *triggerLoop) stop() {
	l.cancel()
	<-l.done
}

// run fetches and handles events one at a time until ctx is cancelled, so a
// Trigger's concurrency and dedupe checks never race with each other.
func (l *triggerLoop) run(ctx context.Context, cons jetstream.Consumer, handle triggerMsgHandler) {
	defer close(l.done)
	log := logr.FromContextOrDiscard(ctx)
	log.Info("Trigger consumer started", "stream", l.cfg.stream, "consumer", l.cfg.durable, "subject", l.cfg.subject)
	for ctx.Err() == nil {
		batch, err := cons.Fetch(triggerFetchBatch, jetstream.FetchMaxWait(triggerFetchWait))
		if err == nil {
			for msg := range batch.Messages() {
				if ctx.Err() != nil {
					// Not handled: Nak so JetStream redelivers it right away.
					_ = msg.Nak()
					continue
				}
				handle(ctx, msg)
			}
			err = batch.Error()
		}
		if err == nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) {
			continue
		}
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, nats.ErrConnectionClosed) || errors.Is(err, jetstream.ErrConsumerDeleted) ||
			errors.Is(err, jetstream.ErrConsumerNotFound) {
			log.Info("Trigger consumer stopped", "error", err.Error())
			return
		}
		log.Error(err, "Trigger consumer fetch failed")
		select {
		case <-ctx.Done():
			return
		case <-time.After(triggerRetryDelay):
		}
	}
}
