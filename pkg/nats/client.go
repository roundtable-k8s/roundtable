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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Client is the interface for NATS operations in the Round Table operator.
type Client interface {
	// Connect establishes a connection to NATS (if not already connected).
	Connect() error

	// Close closes the NATS connection.
	Close() error

	// IsConnected returns true if the client is connected to NATS.
	IsConnected() bool

	// Publish publishes raw bytes to a subject.
	Publish(subject string, data []byte) error

	// PublishJSON publishes a JSON-encoded value to a subject.
	PublishJSON(subject string, v any) error

	// PublishJSONSeq publishes a JSON-encoded value and returns the stream
	// sequence number JetStream assigned to it.
	PublishJSONSeq(subject string, v any) (uint64, error)

	// PublishJSONCtx is PublishJSON that injects the trace context carried by
	// ctx into the message headers and records a producer span.
	PublishJSONCtx(ctx context.Context, subject string, v any, opts ...PublishOption) error

	// PublishJSONSeqCtx is PublishJSONSeq with trace-context propagation.
	PublishJSONSeqCtx(ctx context.Context, subject string, v any, opts ...PublishOption) (uint64, error)

	// ConsumerDeliveredSeq returns the highest stream sequence the named
	// consumer has delivered to a client.
	ConsumerDeliveredSeq(ctx context.Context, stream, consumer string) (uint64, error)

	// CreateStream creates a JetStream stream with the given configuration.
	// An existing stream is left as it is.
	CreateStream(ctx context.Context, config StreamConfig) error

	// EnsureStream creates the stream if missing, otherwise makes its MaxAge
	// equal config.MaxAge (0 = unlimited), with a single stream lookup. It
	// reports whether an existing stream was updated.
	EnsureStream(ctx context.Context, config StreamConfig) (bool, error)

	// DeleteStream deletes a JetStream stream.
	DeleteStream(ctx context.Context, name string) error

	// StreamInfo returns information about a stream.
	StreamInfo(ctx context.Context, name string) (*jetstream.StreamInfo, error)

	// EnsureConsumer makes the durable consumer config.Durable on stream
	// match config: it creates it, updates mutable fields in place, or
	// recreates it only when a field NATS cannot update differs. A consumer
	// that already matches is not written.
	EnsureConsumer(ctx context.Context, stream string, config jetstream.ConsumerConfig) (ConsumerAction, error)

	// DeleteConsumer deletes a JetStream consumer (best effort).
	DeleteConsumer(ctx context.Context, stream, consumer string) error

	// RemoveConsumer deletes a JetStream consumer and returns the error, if
	// any. A consumer or stream that is already gone is not an error.
	RemoveConsumer(ctx context.Context, stream, consumer string) error

	// FetchOne creates or updates the pull consumer described by config on
	// stream, waits up to wait for one message and acknowledges it before
	// returning it. It returns (nil, nil) when no message arrived in time.
	// When stream does not exist, the stream is looked up by
	// config.FilterSubject instead.
	FetchOne(
		ctx context.Context, stream string, config jetstream.ConsumerConfig, wait time.Duration,
	) (jetstream.Msg, error)

	// KVPut stores a value in a NATS KV bucket (creates bucket if needed).
	KVPut(ctx context.Context, bucket, key string, value []byte) error

	// KVGet retrieves a value from a NATS KV bucket.
	KVGet(ctx context.Context, bucket, key string) ([]byte, error)

	// KVDelete deletes a key from a NATS KV bucket.
	KVDelete(ctx context.Context, bucket, key string) error

	// KVKeys lists all keys in a NATS KV bucket.
	KVKeys(ctx context.Context, bucket string) ([]string, error)
}

// ResultConsumerConfig is the consumer FetchOne uses to read one task result:
// a durable pull consumer named durable that delivers every stored message on
// subject and needs an explicit ack (which removes the message from a
// WorkQueue results stream).
func ResultConsumerConfig(durable, subject string) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable:       durable,
		FilterSubject: subject,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
	}
}

// JetStreamClient implements the Client interface using NATS JetStream.
type JetStreamClient struct {
	config Config
	nc     *nats.Conn
	js     jetstream.JetStream
	mu     sync.Mutex
	// dialMu serialises dialers only; the dial itself runs without mu so a
	// NATS outage does not block IsConnected, Conn, Close or the KV cache.
	dialMu sync.Mutex
	// kvs caches KeyValue handles per bucket for the current connection
	// (guarded by mu). It is dropped whenever the connection changes.
	kvs map[string]jetstream.KeyValue
	// closeSeq counts Close calls (guarded by mu). A Connect whose dial
	// started before a Close sees the change and discards its connection.
	closeSeq uint64
	log      logr.Logger
}

// NewClient creates a new NATS client with the given configuration.
func NewClient(config Config, log logr.Logger) Client {
	return &JetStreamClient{
		config: config,
		log:    log,
	}
}

// Connect establishes a connection to NATS if not already connected.
func (c *JetStreamClient) Connect() error {
	if c.hasLiveConn() {
		return nil // Already connected, or reconnecting by itself
	}

	// Only one caller dials at a time; the others wait here, not on c.mu.
	c.dialMu.Lock()
	defer c.dialMu.Unlock()
	if c.hasLiveConn() {
		return nil // Another caller connected while we waited
	}
	c.mu.Lock()
	startSeq := c.closeSeq
	c.mu.Unlock()

	opts := []nats.Option{}
	if c.config.RetryOnFailedConnect {
		opts = append(opts, nats.RetryOnFailedConnect(true))
	}
	if c.config.MaxReconnects != 0 {
		opts = append(opts, nats.MaxReconnects(c.config.MaxReconnects))
	}
	if c.config.ReconnectWait > 0 {
		opts = append(opts, nats.ReconnectWait(c.config.ReconnectWait))
	}

	nc, err := nats.Connect(c.config.URL, opts...)
	if err != nil {
		return fmt.Errorf("NATS connect to %s failed: %w", c.config.URL, err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return fmt.Errorf("JetStream context failed: %w", err)
	}

	c.mu.Lock()
	if c.closeSeq != startSeq {
		// Close ran while we were dialling: do not bring the connection back.
		c.mu.Unlock()
		nc.Close()
		return errors.New("NATS client closed while connecting")
	}
	old := c.nc
	c.nc = nc
	c.js = js
	c.kvs = nil // handles belong to the previous connection
	c.mu.Unlock()
	if old != nil {
		old.Close()
	}
	c.log.Info("Connected to NATS", "url", c.config.URL)

	return nil
}

// hasLiveConn reports whether Connect has nothing to do: the connection is
// up, or it is reconnecting by itself with infinite reconnects (replacing it
// would drop the connection that watchers and fetch loops already hold).
func (c *JetStreamClient) hasLiveConn() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.nc == nil {
		return false
	}
	switch c.nc.Status() {
	case nats.CONNECTED:
		return true
	case nats.RECONNECTING:
		return c.config.MaxReconnects == -1
	}
	return false
}

// Close closes the NATS connection. A later Connect call opens a new one.
func (c *JetStreamClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeSeq++

	if c.nc != nil {
		c.nc.Close()
		c.nc = nil
		c.js = nil
		c.kvs = nil
		c.log.Info("Closed NATS connection")
	}
	return nil
}

// Conn returns the underlying NATS connection, or nil before Connect. It is
// not part of Client so test fakes need not provide it; callers that need raw
// JetStream access (the dead-letter watcher) type-assert for ConnProvider.
func (c *JetStreamClient) Conn() *nats.Conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nc
}

// ConnProvider is implemented by clients that expose their NATS connection.
type ConnProvider interface {
	Conn() *nats.Conn
}

// IsConnected returns true if the client is connected to NATS.
func (c *JetStreamClient) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nc != nil && c.nc.IsConnected()
}

// jetStream connects if needed and returns the JetStream handle.
func (c *JetStreamClient) jetStream() (jetstream.JetStream, error) {
	if err := c.Connect(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.js == nil {
		return nil, errors.New("JetStream not connected")
	}
	return c.js, nil
}

// Publish publishes raw bytes to a subject.
func (c *JetStreamClient) Publish(subject string, data []byte) error {
	js, err := c.jetStream()
	if err != nil {
		return err
	}
	if _, err := js.Publish(context.Background(), subject, data); err != nil {
		return fmt.Errorf("NATS publish to %s failed: %w", subject, err)
	}
	return nil
}

// PublishJSON publishes a JSON-encoded value to a subject.
func (c *JetStreamClient) PublishJSON(subject string, v any) error {
	return c.PublishJSONCtx(context.Background(), subject, v)
}

// PublishJSONSeq publishes a JSON-encoded value and returns its stream sequence.
func (c *JetStreamClient) PublishJSONSeq(subject string, v any) (uint64, error) {
	return c.PublishJSONSeqCtx(context.Background(), subject, v)
}

// PublishJSONCtx publishes a JSON-encoded value with trace-context headers.
func (c *JetStreamClient) PublishJSONCtx(ctx context.Context, subject string, v any, opts ...PublishOption) error {
	_, err := c.PublishJSONSeqCtx(ctx, subject, v, opts...)
	return err
}

// PublishJSONSeqCtx publishes a JSON-encoded value with trace-context headers
// and returns its stream sequence. A producer span wraps the publish; with no
// tracer provider configured it is a no-op and no headers are added unless ctx
// already carries a span context.
func (c *JetStreamClient) PublishJSONSeqCtx(
	ctx context.Context, subject string, v any, opts ...PublishOption,
) (uint64, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return 0, fmt.Errorf("marshal JSON payload: %w", err)
	}
	js, err := c.jetStream()
	if err != nil {
		return 0, err
	}
	ack, err := PublishTraced(ctx, js, subject, data, taskIDOf(v), opts...)
	if err != nil {
		return 0, err
	}
	return ack.Sequence, nil
}

// consumerInfo returns the named consumer's info.
func (c *JetStreamClient) consumerInfo(ctx context.Context, stream, consumer string) (*jetstream.ConsumerInfo, error) {
	js, err := c.jetStream()
	if err != nil {
		return nil, err
	}
	return consumerInfoOn(ctx, js, stream, consumer)
}

// consumerInfoOn returns the named consumer's info using js.
func consumerInfoOn(
	ctx context.Context, js jetstream.JetStream, stream, consumer string,
) (*jetstream.ConsumerInfo, error) {
	cons, err := js.Consumer(ctx, stream, consumer)
	if err != nil {
		return nil, fmt.Errorf("consumer info %s/%s: %w", stream, consumer, err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("consumer info %s/%s: %w", stream, consumer, err)
	}
	return info, nil
}

// ConsumerDeliveredSeq returns the consumer's delivered stream sequence.
func (c *JetStreamClient) ConsumerDeliveredSeq(ctx context.Context, stream, consumer string) (uint64, error) {
	info, err := c.consumerInfo(ctx, stream, consumer)
	if err != nil {
		return 0, err
	}
	return info.Delivered.Stream, nil
}

// ConsumerPending returns num_pending + num_ack_pending for the consumer:
// tasks queued plus tasks delivered but not yet acknowledged.
func (c *JetStreamClient) ConsumerPending(ctx context.Context, stream, consumer string) (uint64, error) {
	info, err := c.consumerInfo(ctx, stream, consumer)
	if err != nil {
		return 0, err
	}
	return info.NumPending + uint64(info.NumAckPending), nil
}

// ConsumerLoad returns the consumer's queued (num_pending) and delivered but
// unacknowledged (num_ack_pending) task counts. A consumer that does not exist
// yet reports zeros.
func (c *JetStreamClient) ConsumerLoad(
	ctx context.Context, stream, consumer string,
) (pending, ackPending uint64, err error) {
	js, err := c.jetStream()
	if err != nil {
		return 0, 0, err
	}
	return ConsumerLoad(ctx, js, stream, consumer)
}

// ConsumerLoad is JetStreamClient.ConsumerLoad for a caller that holds its own
// jetstream.JetStream handle.
func ConsumerLoad(
	ctx context.Context, js jetstream.JetStream, stream, consumer string,
) (pending, ackPending uint64, err error) {
	info, err := consumerInfoOn(ctx, js, stream, consumer)
	if err != nil {
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			return 0, 0, nil
		}
		return 0, 0, err
	}
	return info.NumPending, uint64(info.NumAckPending), nil
}

// CreateStream creates a JetStream stream with the given configuration. An
// existing stream is left unchanged.
func (c *JetStreamClient) CreateStream(ctx context.Context, config StreamConfig) error {
	js, err := c.jetStream()
	if err != nil {
		return err
	}

	// Check if stream already exists
	if _, err := js.Stream(ctx, config.Name); err == nil {
		return nil
	}
	return c.createStream(ctx, js, config)
}

// EnsureStream creates the stream when missing, otherwise makes its MaxAge
// equal config.MaxAge (0 = unlimited). It looks the stream up once. It
// reports whether an existing stream's MaxAge was updated.
func (c *JetStreamClient) EnsureStream(ctx context.Context, config StreamConfig) (bool, error) {
	js, err := c.jetStream()
	if err != nil {
		return false, err
	}
	stream, err := js.Stream(ctx, config.Name)
	if err != nil {
		if !errors.Is(err, jetstream.ErrStreamNotFound) {
			return false, fmt.Errorf("failed to get stream %s: %w", config.Name, err)
		}
		return false, c.createStream(ctx, js, config)
	}
	return c.applyMaxAge(ctx, js, stream, config.MaxAge)
}

func (c *JetStreamClient) createStream(
	ctx context.Context, js jetstream.JetStream, config StreamConfig,
) error {
	streamConfig := jetstream.StreamConfig{
		Name:      config.Name,
		Subjects:  config.Subjects,
		Retention: config.Retention.ToNATS(),
		Storage:   config.Storage.ToNATS(),
	}
	if config.MaxAge > 0 {
		streamConfig.MaxAge = config.MaxAge
	}
	if config.MaxMsgs > 0 {
		streamConfig.MaxMsgs = config.MaxMsgs
	}
	if config.Discard != "" {
		streamConfig.Discard = config.Discard.ToNATS()
	}

	if _, err := js.CreateStream(ctx, streamConfig); err != nil {
		return fmt.Errorf("failed to create stream %s: %w", config.Name, err)
	}

	c.log.Info("Created JetStream stream", "name", config.Name, "retention", config.Retention)
	return nil
}

func (c *JetStreamClient) applyMaxAge(
	ctx context.Context, js jetstream.JetStream, stream jetstream.Stream, maxAge time.Duration,
) (bool, error) {
	cfg := stream.CachedInfo().Config
	if cfg.MaxAge == maxAge {
		return false, nil
	}
	cfg.MaxAge = maxAge
	if _, err := js.UpdateStream(ctx, cfg); err != nil {
		return false, fmt.Errorf("failed to update stream %s: %w", cfg.Name, err)
	}
	c.log.Info("Updated JetStream stream max age", "name", cfg.Name, "maxAge", maxAge)
	return true, nil
}

// DeleteStream deletes a JetStream stream.
func (c *JetStreamClient) DeleteStream(ctx context.Context, name string) error {
	js, err := c.jetStream()
	if err != nil {
		return err
	}
	if err := js.DeleteStream(ctx, name); err != nil {
		return fmt.Errorf("failed to delete stream %s: %w", name, err)
	}
	c.log.Info("Deleted JetStream stream", "name", name)
	return nil
}

// StreamInfo returns information about a stream.
func (c *JetStreamClient) StreamInfo(ctx context.Context, name string) (*jetstream.StreamInfo, error) {
	js, err := c.jetStream()
	if err != nil {
		return nil, err
	}
	s, err := js.Stream(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("failed to get stream info for %s: %w", name, err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get stream info for %s: %w", name, err)
	}
	return info, nil
}

// DeleteConsumer deletes a JetStream consumer. Errors are logged, not
// returned: deletion is best effort.
func (c *JetStreamClient) DeleteConsumer(ctx context.Context, stream, consumer string) error {
	js, err := c.jetStream()
	if err != nil {
		return err
	}
	if err := js.DeleteConsumer(ctx, stream, consumer); err != nil {
		c.log.Info("Failed to delete consumer (best effort)", "stream", stream, "consumer", consumer, "error", err.Error())
	}
	return nil
}

// FetchOne reads and acknowledges one message through the consumer described
// by config on stream. See Client.FetchOne.
func (c *JetStreamClient) FetchOne(
	ctx context.Context, stream string, config jetstream.ConsumerConfig, wait time.Duration,
) (jetstream.Msg, error) {
	js, err := c.jetStream()
	if err != nil {
		return nil, err
	}

	cons, err := js.CreateOrUpdateConsumer(ctx, stream, config)
	if errors.Is(err, jetstream.ErrStreamNotFound) && config.FilterSubject != "" {
		// The configured stream does not exist: find the one that captures
		// the subject instead.
		c.log.Info("Stream not found, looking it up by subject",
			"stream", stream, "subject", config.FilterSubject)
		found, lerr := js.StreamNameBySubject(ctx, config.FilterSubject)
		if lerr != nil {
			return nil, fmt.Errorf("consumer %s on stream %s: %w (lookup by subject: %v)", config.Durable, stream, err, lerr)
		}
		cons, err = js.CreateOrUpdateConsumer(ctx, found, config)
		if err == nil {
			// Callers delete the consumer on the configured (missing)
			// stream, so remove the one created here ourselves; otherwise
			// the durable would stay behind and block later consumers on
			// the subject.
			defer func() {
				dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
				defer cancel()
				if derr := js.DeleteConsumer(dctx, found, config.Durable); derr != nil {
					c.log.Info("Failed to delete fallback consumer (best effort)",
						"stream", found, "consumer", config.Durable, "error", derr.Error())
				}
			}()
		}
	}
	if err != nil {
		return nil, fmt.Errorf("consumer %s on stream %s: %w", config.Durable, stream, err)
	}

	var batch jetstream.MessageBatch
	if wait <= 0 {
		// No wait: return at once with whatever is queued.
		batch, err = cons.FetchNoWait(1)
	} else {
		batch, err = cons.Fetch(1, jetstream.FetchMaxWait(wait))
	}
	if err != nil {
		return nil, fmt.Errorf("fetch from %s/%s: %w", stream, config.Durable, err)
	}
	var msg jetstream.Msg
	for m := range batch.Messages() {
		if msg == nil {
			msg = m
		}
	}
	if err := batch.Error(); err != nil && msg == nil &&
		!errors.Is(err, nats.ErrTimeout) && !errors.Is(err, context.DeadlineExceeded) {
		return nil, fmt.Errorf("fetch from %s/%s: %w", stream, config.Durable, err)
	}
	if msg == nil {
		return nil, nil // No message available, not an error
	}

	// Acknowledge once and wait for the server to confirm, so a caller that
	// deletes the consumer right after cannot see the message redelivered
	// (with WorkQueue retention the ack is what removes it from the stream).
	if config.AckPolicy == jetstream.AckExplicitPolicy || config.AckPolicy == jetstream.AckAllPolicy {
		if err := msg.DoubleAck(ctx); err != nil {
			c.log.Error(err, "Failed to ack message", "subject", msg.Subject())
		}
	}
	return msg, nil
}

// kvBucket returns a KV bucket, creating it if it doesn't exist.
func (c *JetStreamClient) kvBucket(ctx context.Context, bucket string) (jetstream.KeyValue, error) {
	c.mu.Lock()
	js := c.js
	kv, ok := c.kvs[bucket]
	c.mu.Unlock()
	if js == nil {
		return nil, errors.New("JetStream not connected")
	}
	if ok {
		return kv, nil
	}
	kv, err := c.openKVBucket(ctx, js, bucket)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	// Cache only if the connection did not change while we were opening.
	if c.js == js {
		if c.kvs == nil {
			c.kvs = map[string]jetstream.KeyValue{}
		}
		c.kvs[bucket] = kv
	}
	c.mu.Unlock()
	return kv, nil
}

// dropKV forgets the cached handle for bucket after the bucket went away.
func (c *JetStreamClient) dropKV(bucket string, err error) {
	if errors.Is(err, jetstream.ErrBucketNotFound) || errors.Is(err, jetstream.ErrStreamNotFound) ||
		errors.Is(err, nats.ErrNoResponders) || errors.Is(err, nats.ErrNoStreamResponse) {
		c.mu.Lock()
		delete(c.kvs, bucket)
		c.mu.Unlock()
	}
}

// openKVBucket looks the bucket up (creating it if missing) and, for the
// results bucket, aligns its TTL. kvBucket caches the result.
func (c *JetStreamClient) openKVBucket(
	ctx context.Context, js jetstream.JetStream, bucket string,
) (jetstream.KeyValue, error) {
	ttl := c.config.kvTTL(bucket)
	kv, err := js.KeyValue(ctx, bucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		kv, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
			Bucket:      bucket,
			Description: fmt.Sprintf("Round Table %s store", bucket),
			History:     3,
			TTL:         ttl,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create KV bucket %s: %w", bucket, err)
		}
		c.log.Info("Created NATS KV bucket", "bucket", bucket, "ttl", ttl.String())
	} else if err != nil {
		return nil, fmt.Errorf("failed to access KV bucket %s: %w", bucket, err)
	} else if bucket == ResultsBucket {
		// The results TTL is operator configuration: bring an existing bucket
		// in line when it differs.
		if status, serr := kv.Status(ctx); serr == nil && status.TTL() != ttl {
			cfg := status.Config()
			cfg.TTL = ttl
			if kv, err = js.UpdateKeyValue(ctx, cfg); err != nil {
				return nil, fmt.Errorf("failed to update TTL of KV bucket %s: %w", bucket, err)
			}
			c.log.Info("Updated NATS KV bucket TTL", "bucket", bucket, "ttl", ttl.String())
		}
	}
	return kv, nil
}

// KVPut stores a value in a NATS KV bucket (creates bucket if needed).
func (c *JetStreamClient) KVPut(ctx context.Context, bucket, key string, value []byte) error {
	kv, err := c.kvBucket(ctx, bucket)
	if err != nil {
		return err
	}
	if _, err := kv.Put(ctx, key, value); err != nil {
		c.dropKV(bucket, err)
		return fmt.Errorf("failed to put key %s in bucket %s: %w", key, bucket, err)
	}
	return nil
}

// KVGet retrieves a value from a NATS KV bucket.
func (c *JetStreamClient) KVGet(ctx context.Context, bucket, key string) ([]byte, error) {
	kv, err := c.kvBucket(ctx, bucket)
	if err != nil {
		return nil, err
	}
	entry, err := kv.Get(ctx, key)
	if err != nil {
		c.dropKV(bucket, err)
		return nil, fmt.Errorf("failed to get key %s from bucket %s: %w", key, bucket, err)
	}
	return entry.Value(), nil
}

// KVDelete deletes a key from a NATS KV bucket.
func (c *JetStreamClient) KVDelete(ctx context.Context, bucket, key string) error {
	kv, err := c.kvBucket(ctx, bucket)
	if err != nil {
		return err
	}
	err = kv.Delete(ctx, key)
	c.dropKV(bucket, err)
	return err
}

// KVKeys lists all keys in a NATS KV bucket.
func (c *JetStreamClient) KVKeys(ctx context.Context, bucket string) ([]string, error) {
	kv, err := c.kvBucket(ctx, bucket)
	if err != nil {
		return nil, err
	}
	keys, err := kv.Keys(ctx)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return []string{}, nil
	}
	if err != nil {
		c.dropKV(bucket, err)
		return nil, fmt.Errorf("failed to list keys in bucket %s: %w", bucket, err)
	}
	return keys, nil
}
