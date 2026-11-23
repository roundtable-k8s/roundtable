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
	"time"

	"github.com/nats-io/nats.go"

	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// Bus is the gateway's NATS side.
type Bus interface {
	// Watch subscribes to a result subject. Call it before publishing the
	// task so a fast result can't be missed.
	Watch(subject string) (ResultWatch, error)
	// Publish publishes a task through JetStream, so it fails when no
	// stream captures the subject.
	Publish(subject string, payload natspkg.TaskPayload) error
	// LastResult returns the newest message on subject in stream, or nil
	// when there is none. It never consumes or acknowledges the message.
	LastResult(stream, subject string) ([]byte, error)
	// Discard removes a result that has been recorded from a WorkQueue
	// stream, standing in for the ack nobody else will send. Results in
	// Limits/Interest streams are left to the stream's own retention.
	Discard(stream, subject string) error

	// PutRecord stores a task record.
	PutRecord(rec *TaskRecord) error
	// GetRecord loads a task record; ErrNotFound when it doesn't exist.
	GetRecord(taskID string) (*TaskRecord, error)
	// ListRecords returns every stored task record.
	ListRecords() ([]*TaskRecord, error)
}

// ResultWatch waits for messages on one subject.
type ResultWatch interface {
	// Next returns the next message's data or the context's error.
	Next(ctx context.Context) ([]byte, error)
	// Close stops the watch.
	Close()
}

// NATSBus implements Bus on a NATS connection with JetStream. Task records
// live in a JetStream KV bucket so every gateway replica sees them and they
// survive restarts.
type NATSBus struct {
	nc *nats.Conn
	js nats.JetStreamContext
	kv nats.KeyValue
}

// NewNATSBus binds to (or creates) the task-record KV bucket. ttl bounds how
// long records are kept.
func NewNATSBus(nc *nats.Conn, bucket string, ttl time.Duration) (*NATSBus, error) {
	js, err := nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("JetStream context: %w", err)
	}
	kv, err := js.KeyValue(bucket)
	if errors.Is(err, nats.ErrBucketNotFound) {
		kv, err = js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket:      bucket,
			Description: "Round Table gateway task records",
			History:     1,
			TTL:         ttl,
		})
	}
	if err != nil {
		return nil, fmt.Errorf("KV bucket %s: %w", bucket, err)
	}
	return &NATSBus{nc: nc, js: js, kv: kv}, nil
}

// Watch implements Bus.
func (b *NATSBus) Watch(subject string) (ResultWatch, error) {
	// A core subscription sees the knight's publish whether or not (and
	// however) a stream also stores it, and creates no JetStream consumer,
	// so it can't clash with WorkQueue streams' one-consumer-per-subject rule.
	sub, err := b.nc.SubscribeSync(subject)
	if err != nil {
		return nil, fmt.Errorf("subscribe %s: %w", subject, err)
	}
	// Make sure the server has registered the interest before we publish.
	if err := b.nc.Flush(); err != nil {
		_ = sub.Unsubscribe()
		return nil, fmt.Errorf("flush: %w", err)
	}
	return &natsWatch{sub: sub}, nil
}

type natsWatch struct{ sub *nats.Subscription }

func (w *natsWatch) Next(ctx context.Context) ([]byte, error) {
	msg, err := w.sub.NextMsgWithContext(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	return msg.Data, nil
}

func (w *natsWatch) Close() { _ = w.sub.Unsubscribe() }

// Publish implements Bus.
func (b *NATSBus) Publish(subject string, payload natspkg.TaskPayload) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if _, err := b.js.Publish(subject, data); err != nil {
		return fmt.Errorf("publish %s: %w", subject, err)
	}
	return nil
}

// LastResult implements Bus.
func (b *NATSBus) LastResult(stream, subject string) ([]byte, error) {
	if stream == "" {
		return nil, nil
	}
	msg, err := b.js.GetLastMsg(stream, subject)
	if err != nil {
		if errors.Is(err, nats.ErrMsgNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("read %s from stream %s: %w", subject, stream, err)
	}
	return msg.Data, nil
}

// Discard implements Bus.
func (b *NATSBus) Discard(stream, subject string) error {
	if stream == "" {
		return nil
	}
	info, err := b.js.StreamInfo(stream)
	if err != nil {
		return fmt.Errorf("stream info %s: %w", stream, err)
	}
	if info.Config.Retention != nats.WorkQueuePolicy {
		return nil
	}
	msg, err := b.js.GetLastMsg(stream, subject)
	if err != nil {
		if errors.Is(err, nats.ErrMsgNotFound) {
			return nil
		}
		return err
	}
	if err := b.js.DeleteMsg(stream, msg.Sequence); err != nil && !errors.Is(err, nats.ErrMsgNotFound) {
		return fmt.Errorf("delete %s seq %d: %w", stream, msg.Sequence, err)
	}
	return nil
}

// PutRecord implements Bus.
func (b *NATSBus) PutRecord(rec *TaskRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = b.kv.Put(rec.TaskID, data)
	return err
}

// GetRecord implements Bus.
func (b *NATSBus) GetRecord(taskID string) (*TaskRecord, error) {
	entry, err := b.kv.Get(taskID)
	if err != nil {
		if errors.Is(err, nats.ErrKeyNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	var rec TaskRecord
	if err := json.Unmarshal(entry.Value(), &rec); err != nil {
		return nil, fmt.Errorf("decode task %s: %w", taskID, err)
	}
	return &rec, nil
}

// ListRecords implements Bus.
func (b *NATSBus) ListRecords() ([]*TaskRecord, error) {
	keys, err := b.kv.Keys()
	if err != nil {
		if errors.Is(err, nats.ErrNoKeysFound) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]*TaskRecord, 0, len(keys))
	for _, k := range keys {
		rec, err := b.GetRecord(k)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue // expired or deleted between Keys and Get
			}
			return nil, err
		}
		out = append(out, rec)
	}
	return out, nil
}
