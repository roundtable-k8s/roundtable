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
	"github.com/nats-io/nats.go/jetstream"

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
	// PublishEvent publishes an event to subject through JetStream, expecting
	// it to land in stream. A non-empty msgID becomes Nats-Msg-Id, so a
	// repeat within the stream's duplicate window is dropped: duplicate is
	// then true and seq is the original's sequence.
	PublishEvent(ctx context.Context, stream, subject string, data []byte, header nats.Header, msgID string) (seq uint64, duplicate bool, err error)
	// LastResult returns the newest message on subject in stream, or nil
	// when there is none. It never consumes or acknowledges the message.
	LastResult(ctx context.Context, stream, subject string) ([]byte, error)
	// Discard removes a result that has been recorded from a WorkQueue
	// stream, standing in for the ack nobody else will send. Results in
	// Limits/Interest streams are left to the stream's own retention.
	Discard(ctx context.Context, stream, subject string) error

	// PutRecord stores a task record.
	PutRecord(ctx context.Context, rec *TaskRecord) error
	// GetRecord loads a task record; ErrNotFound when it doesn't exist.
	GetRecord(ctx context.Context, taskID string) (*TaskRecord, error)
	// ListRecords returns every stored task record.
	ListRecords(ctx context.Context) ([]*TaskRecord, error)

	// DeadLetterRecords returns the state of every dead-letter entry.
	DeadLetterRecords(ctx context.Context) ([]DeadLetterRecord, error)
	// DeadLetterEntry returns the entry body stored at a record's subject,
	// or nil when it has aged out of the dead-letter stream.
	DeadLetterEntry(ctx context.Context, subject string) (*deadletter.Entry, error)
	// UpdateDeadLetterRecord stores rec if the key is still at revision;
	// errStaleRevision when somebody else changed it first.
	UpdateDeadLetterRecord(ctx context.Context, key string, rec deadletter.Record, revision uint64) error
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
	js jetstream.JetStream
	kv jetstream.KeyValue
}

// NewNATSBus binds to (or creates) the task-record KV bucket. ttl bounds how
// long records are kept.
func NewNATSBus(ctx context.Context, nc *nats.Conn, bucket string, ttl time.Duration) (*NATSBus, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("JetStream context: %w", err)
	}
	kv, err := js.KeyValue(ctx, bucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		kv, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
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

// PublishEvent implements Bus.
func (b *NATSBus) PublishEvent(ctx context.Context, stream, subject string, data []byte, header nats.Header, msgID string) (uint64, bool, error) {
	msg := &nats.Msg{Subject: subject, Data: data, Header: header}
	opts := []jetstream.PublishOpt{jetstream.WithExpectStream(stream)}
	if msgID != "" {
		opts = append(opts, jetstream.WithMsgID(msgID))
	}
	ack, err := b.js.PublishMsg(ctx, msg, opts...)
	if err != nil {
		return 0, false, fmt.Errorf("publish %s: %w", subject, err)
	}
	return ack.Sequence, ack.Duplicate, nil
}

// LastResult implements Bus.
func (b *NATSBus) LastResult(ctx context.Context, stream, subject string) ([]byte, error) {
	if stream == "" {
		return nil, nil
	}
	st, err := b.js.Stream(ctx, stream)
	if err != nil {
		return nil, fmt.Errorf("read %s from stream %s: %w", subject, stream, err)
	}
	msg, err := st.GetLastMsgForSubject(ctx, subject)
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("read %s from stream %s: %w", subject, stream, err)
	}
	return msg.Data, nil
}

// Discard implements Bus.
func (b *NATSBus) Discard(ctx context.Context, stream, subject string) error {
	if stream == "" {
		return nil
	}
	st, err := b.js.Stream(ctx, stream)
	if err != nil {
		return fmt.Errorf("stream info %s: %w", stream, err)
	}
	if st.CachedInfo().Config.Retention != jetstream.WorkQueuePolicy {
		return nil
	}
	msg, err := st.GetLastMsgForSubject(ctx, subject)
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return nil
		}
		return err
	}
	if err := st.DeleteMsg(ctx, msg.Sequence); err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
		return fmt.Errorf("delete %s seq %d: %w", stream, msg.Sequence, err)
	}
	return nil
}

// PutRecord implements Bus.
func (b *NATSBus) PutRecord(ctx context.Context, rec *TaskRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = b.kv.Put(ctx, rec.TaskID, data)
	return err
}

// GetRecord implements Bus.
func (b *NATSBus) GetRecord(ctx context.Context, taskID string) (*TaskRecord, error) {
	entry, err := b.kv.Get(ctx, taskID)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
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
func (b *NATSBus) ListRecords(ctx context.Context) ([]*TaskRecord, error) {
	lister, err := b.kv.ListKeys(ctx)
	if err != nil {
		return nil, err
	}
	// The lister closes its channel once every key is read, so there is
	// nothing to Stop after a full drain.
	var keys []string
	for k := range lister.Keys() {
		keys = append(keys, k)
	}
	out := make([]*TaskRecord, 0, len(keys))
	for _, k := range keys {
		rec, err := b.GetRecord(ctx, k)
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
func (b *NATSBus) deadLetterKV(ctx context.Context) (jetstream.KeyValue, error) {
	kv, err := b.js.KeyValue(ctx, deadletter.KVBucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		return nil, nil
	}
	return kv, err
}

// DeadLetterRecords implements Bus.
func (b *NATSBus) DeadLetterRecords(ctx context.Context) ([]DeadLetterRecord, error) {
	kv, err := b.deadLetterKV(ctx)
	if err != nil || kv == nil {
		return nil, err
	}
	lister, err := kv.ListKeys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}
		return nil, err
	}
	var keys []string
	for k := range lister.Keys() {
		keys = append(keys, k)
	}
	out := make([]DeadLetterRecord, 0, len(keys))
	for _, k := range keys {
		ent, err := kv.Get(ctx, k)
		if err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				continue
			}
			return nil, err
		}
		if op := ent.Operation(); op == jetstream.KeyValueDelete || op == jetstream.KeyValuePurge {
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
func (b *NATSBus) DeadLetterEntry(ctx context.Context, subject string) (*deadletter.Entry, error) {
	stream, err := b.js.StreamNameBySubject(ctx, subject)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil, nil
		}
		return nil, err
	}
	st, err := b.js.Stream(ctx, stream)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil, nil
		}
		return nil, err
	}
	msg, err := st.GetLastMsgForSubject(ctx, subject)
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
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
func (b *NATSBus) UpdateDeadLetterRecord(ctx context.Context, key string, rec deadletter.Record, revision uint64) error {
	kv, err := b.deadLetterKV(ctx)
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
	if _, err := kv.Update(ctx, key, data, revision); err != nil {
		var api *jetstream.APIError
		if errors.Is(err, jetstream.ErrKeyExists) || (errors.As(err, &api) && api.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence) {
			return errStaleRevision
		}
		return err
	}
	return nil
}
