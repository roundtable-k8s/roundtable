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
	"fmt"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/roundtable-k8s/roundtable/pkg/nats"

// Span attribute keys. The messaging.* names follow OTel messaging semconv;
// roundtable.* names are Round Table specific.
const (
	attrMessagingSystem        = "messaging.system"
	attrMessagingDestination   = "messaging.destination.name"
	attrMessagingOperationType = "messaging.operation.type"
	attrDeliveryCount          = "messaging.nats.delivery_count"
	attrTable                  = "roundtable.table"
	attrTaskID                 = "roundtable.task.id"
)

// headerCarrier adapts nats.Header to propagation.TextMapCarrier.
type headerCarrier nats.Header

var _ propagation.TextMapCarrier = headerCarrier(nil)

// Get returns the first value for key, or "".
func (h headerCarrier) Get(key string) string {
	return nats.Header(h).Get(key)
}

// Set stores value under key, replacing any existing values.
func (h headerCarrier) Set(key, value string) {
	nats.Header(h).Set(key, value)
}

// Keys lists the header names that are set.
func (h headerCarrier) Keys() []string {
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	return keys
}

// InjectContext writes the trace context in ctx into msg's headers using the
// global propagator. Headers are left untouched (and not allocated) when ctx
// carries no valid span context.
func InjectContext(ctx context.Context, msg *nats.Msg) {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return
	}
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
	otel.GetTextMapPropagator().Inject(ctx, headerCarrier(msg.Header))
}

// ExtractContext returns ctx carrying the remote trace context found in msg's
// headers. It returns ctx unchanged when msg has no headers.
func ExtractContext(ctx context.Context, msg *nats.Msg) context.Context {
	if msg == nil || len(msg.Header) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, headerCarrier(msg.Header))
}

// PublishOption configures the producer span of a publish.
type PublishOption func(*publishOptions)

type publishOptions struct {
	table  string
	taskID string
}

// WithTable sets the roundtable.table span attribute (the publishing
// resource's RoundTable). Empty values are omitted.
func WithTable(table string) PublishOption {
	return func(o *publishOptions) { o.table = table }
}

// WithTaskID overrides the roundtable.task.id span attribute, which is
// otherwise taken from a TaskPayload value.
func WithTaskID(id string) PublishOption {
	return func(o *publishOptions) { o.taskID = id }
}

func taskIDOf(v any) string {
	switch p := v.(type) {
	case TaskPayload:
		return p.TaskID
	case *TaskPayload:
		if p != nil {
			return p.TaskID
		}
	}
	return ""
}

func startPublishSpan(
	ctx context.Context, subject, taskID string, opts ...PublishOption,
) (context.Context, trace.Span) {
	o := publishOptions{taskID: taskID}
	for _, opt := range opts {
		opt(&o)
	}
	attrs := []attribute.KeyValue{
		attribute.String(attrMessagingSystem, "nats"),
		attribute.String(attrMessagingDestination, subject),
		attribute.String(attrMessagingOperationType, "publish"),
	}
	if o.table != "" {
		attrs = append(attrs, attribute.String(attrTable, o.table))
	}
	if o.taskID != "" {
		attrs = append(attrs, attribute.String(attrTaskID, o.taskID))
	}
	return otel.Tracer(tracerName).Start(ctx, "publish "+subject,
		trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(attrs...))
}

// PublishTraced publishes data to subject through js inside a producer span,
// injecting the span's trace context into the message headers. With no tracer
// provider and no span in ctx the message carries no trace headers.
func PublishTraced(
	ctx context.Context, js nats.JetStreamContext, subject string, data []byte, taskID string, opts ...PublishOption,
) (*nats.PubAck, error) {
	ctx, span := startPublishSpan(ctx, subject, taskID, opts...)
	defer span.End()

	msg := &nats.Msg{Subject: subject, Data: data}
	InjectContext(ctx, msg)
	ack, err := js.PublishMsg(msg)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("NATS publish to %s failed: %w", subject, err)
	}
	return ack, nil
}

// StartConsumeSpan starts a consumer span for a received message. A first
// delivery makes the span a child of the producer's context carried in the
// headers. A redelivery (delivery count > 1) instead starts under ctx and
// links to the original context, so retries do not create a second parent.
// The delivery count is recorded when JetStream metadata is available.
// Callers must End the returned span.
func StartConsumeSpan(ctx context.Context, msg *nats.Msg, opts ...PublishOption) (context.Context, trace.Span) {
	o := publishOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	attrs := []attribute.KeyValue{
		attribute.String(attrMessagingSystem, "nats"),
		attribute.String(attrMessagingDestination, msg.Subject),
		attribute.String(attrMessagingOperationType, "process"),
	}
	if o.table != "" {
		attrs = append(attrs, attribute.String(attrTable, o.table))
	}
	if o.taskID != "" {
		attrs = append(attrs, attribute.String(attrTaskID, o.taskID))
	}
	startOpts := []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindConsumer)}

	parent := ExtractContext(ctx, msg)
	if md, err := msg.Metadata(); err == nil && md != nil {
		attrs = append(attrs, attribute.Int64(attrDeliveryCount, int64(md.NumDelivered)))
		if md.NumDelivered > 1 {
			if sc := trace.SpanContextFromContext(parent); sc.IsValid() && !sc.Equal(trace.SpanContextFromContext(ctx)) {
				startOpts = append(startOpts, trace.WithLinks(trace.Link{SpanContext: sc}))
			}
			parent = ctx
		}
	}
	startOpts = append(startOpts, trace.WithAttributes(attrs...))
	return otel.Tracer(tracerName).Start(parent, "process "+msg.Subject, startOpts...)
}
