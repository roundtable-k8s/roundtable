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

	"github.com/roundtable-k8s/roundtable/internal/deadletter"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// Bus is the gateway's NATS side.
type Bus interface {
	// Watch subscribes to a result subject. Call it before publishing the
	// task so a fast result can't be missed.
	Watch(subject string) (ResultWatch, error)
	// Publish publishes a task through JetStream, so it fails when no
	// stream captures the subject.
	Publish(ctx context.Context, subject string, payload natspkg.TaskPayload, table string) error
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

	// DeadLetterRecords returns the state of every dead-letter entry.
	DeadLetterRecords() ([]DeadLetterRecord, error)
	// DeadLetterEntry returns the entry body stored at a record's subject,
	// or nil when it has aged out of the dead-letter stream.
	DeadLetterEntry(subject string) (*deadletter.Entry, error)
	// UpdateDeadLetterRecord stores rec if the key is still at revision;
	// errStaleRevision when somebody else changed it first.
	UpdateDeadLetterRecord(key string, rec deadletter.Record, revision uint64) error
}

// DeadLetterRecord is a dead-letter entry's KV state with its key and revision.
type DeadLetterRecord struct {
	Key      string
	Revision uint64
	deadletter.Record
}

// errStaleRevision is a lost compare-and-set on a dead-letter record.
var errStaleRevision = errors.New("dead-letter record changed")

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
func (b *NATSBus) Publish(ctx context.Context, subject string, payload natspkg.TaskPayload, table string) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if _, err := natspkg.PublishTraced(ctx, b.js, subject, data, payload.TaskID, natspkg.WithTable(table)); err != nil {
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

// deadLetterKV binds the operator's dead-letter state bucket; nil when the
// operator hasn't created it yet (no table has dead-lettering).
func (b *NATSBus) deadLetterKV() (nats.KeyValue, error) {
	kv, err := b.js.KeyValue(deadletter.KVBucket)
	if errors.Is(err, nats.ErrBucketNotFound) {
		return nil, nil
	}
	return kv, err
}

// DeadLetterRecords implements Bus.
func (b *NATSBus) DeadLetterRecords() ([]DeadLetterRecord, error) {
	kv, err := b.deadLetterKV()
	if err != nil || kv == nil {
		return nil, err
	}
	keys, err := kv.Keys()
	if err != nil {
		if errors.Is(err, nats.ErrNoKeysFound) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]DeadLetterRecord, 0, len(keys))
	for _, k := range keys {
		ent, err := kv.Get(k)
		if err != nil {
			if errors.Is(err, nats.ErrKeyNotFound) {
				continue
			}
			return nil, err
		}
		if op := ent.Operation(); op == nats.KeyValueDelete || op == nats.KeyValuePurge {
			continue
		}
		rec := DeadLetterRecord{Key: k, Revision: ent.Revision()}
		if err := json.Unmarshal(ent.Value(), &rec.Record); err != nil {
			continue // not a record we understand
		}
		out = append(out, rec)
	}
	return out, nil
}

// DeadLetterEntry implements Bus.
func (b *NATSBus) DeadLetterEntry(subject string) (*deadletter.Entry, error) {
	stream, err := b.js.StreamNameBySubject(subject)
	if err != nil {
		if errors.Is(err, nats.ErrStreamNotFound) {
			return nil, nil
		}
		return nil, err
	}
	msg, err := b.js.GetLastMsg(stream, subject)
	if err != nil {
		if errors.Is(err, nats.ErrMsgNotFound) {
			return nil, nil
		}
		return nil, err
	}
	var e deadletter.Entry
	if err := json.Unmarshal(msg.Data, &e); err != nil {
		return nil, fmt.Errorf("decode dead-letter entry %s: %w", subject, err)
	}
	return &e, nil
}

// UpdateDeadLetterRecord implements Bus.
func (b *NATSBus) UpdateDeadLetterRecord(key string, rec deadletter.Record, revision uint64) error {
	kv, err := b.deadLetterKV()
	if err != nil {
		return err
	}
	if kv == nil {
		return ErrNotFound
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err := kv.Update(key, data, revision); err != nil {
		var api *nats.APIError
		if errors.Is(err, nats.ErrKeyExists) || (errors.As(err, &api) && api.ErrorCode == nats.JSErrCodeStreamWrongLastSequence) {
			return errStaleRevision
		}
		return err
	}
	return nil
}
