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
	"sort"
	"testing"
	"time"

	"github.com/go-logr/logr"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// setupTracing installs an in-memory tracer provider and the W3C propagator
// for the duration of the test.
func setupTracing(t *testing.T) (*tracetest.InMemoryExporter, trace.Tracer) {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})
	return exp, tp.Tracer("test")
}

func runJS(t *testing.T) *natsserver.Server {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	s := natstest.RunServer(&opts)
	t.Cleanup(s.Shutdown)
	return s
}

func TestHeaderCarrierKeys(t *testing.T) {
	h := nats.Header{}
	c := headerCarrier(h)
	c.Set("Traceparent", "a")
	c.Set("Tracestate", "b")
	keys := c.Keys()
	sort.Strings(keys)
	if len(keys) != 2 || keys[0] != "Traceparent" || keys[1] != "Tracestate" {
		t.Fatalf("keys = %v", keys)
	}
	if c.Get("traceparent") != "" && c.Get("Traceparent") != "a" {
		t.Fatalf("get mismatch")
	}
}

func TestInjectExtractRoundTrip(t *testing.T) {
	_, tracer := setupTracing(t)
	ctx, span := tracer.Start(context.Background(), "parent")
	defer span.End()

	msg := &nats.Msg{Subject: "x"}
	InjectContext(ctx, msg)
	if msg.Header.Get("traceparent") == "" {
		t.Fatal("traceparent header not set")
	}
	got := trace.SpanContextFromContext(ExtractContext(context.Background(), msg))
	want := span.SpanContext()
	if got.TraceID() != want.TraceID() || got.SpanID() != want.SpanID() || !got.IsRemote() {
		t.Fatalf("round trip: got %v want %v", got, want)
	}
}

func TestExtractWithoutHeadersReturnsInput(t *testing.T) {
	setupTracing(t)
	type key struct{}
	ctx := context.WithValue(context.Background(), key{}, "v")
	if got := ExtractContext(ctx, &nats.Msg{Subject: "x"}); got != ctx {
		t.Fatal("context changed without headers")
	}
	if got := ExtractContext(ctx, nil); got != ctx {
		t.Fatal("context changed for nil msg")
	}
}

func TestPublishJSONSeqCtxPropagatesAndRecordsSpan(t *testing.T) {
	exp, tracer := setupTracing(t)
	s := runJS(t)
	c := NewClient(Config{URL: s.ClientURL()}, logr.Discard())
	defer func() { _ = c.Close() }()
	mustStream(t, c)

	ctx, parent := tracer.Start(context.Background(), "parent")
	if _, err := c.PublishJSONSeqCtx(ctx, "t.a", TaskPayload{TaskID: "id1", Task: "x"}, WithTable("tbl")); err != nil {
		t.Fatal(err)
	}
	parent.End()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, _ := nc.JetStream()
	sub, err := js.SubscribeSync("t.a", nats.DeliverAll())
	if err != nil {
		t.Fatal(err)
	}
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	tp := msg.Header.Get("traceparent")
	if tp == "" {
		t.Fatal("no traceparent header on published message")
	}
	got := trace.SpanContextFromContext(ExtractContext(context.Background(), msg))
	if got.TraceID() != parent.SpanContext().TraceID() {
		t.Fatalf("trace id %s != %s", got.TraceID(), parent.SpanContext().TraceID())
	}

	var found bool
	for _, sp := range exp.GetSpans() {
		if sp.Name != "publish t.a" {
			continue
		}
		found = true
		if sp.SpanKind != trace.SpanKindProducer || sp.SpanContext.SpanID() != got.SpanID() {
			t.Fatalf("unexpected producer span: kind=%v", sp.SpanKind)
		}
		attrs := map[string]string{}
		for _, a := range sp.Attributes {
			attrs[string(a.Key)] = a.Value.String()
		}
		for k, v := range map[string]string{
			"messaging.system": "nats", "messaging.destination.name": "t.a",
			"messaging.operation.type": "publish", "roundtable.table": "tbl", "roundtable.task.id": "id1",
		} {
			if attrs[k] != v {
				t.Errorf("attr %s = %q want %q", k, attrs[k], v)
			}
		}
		if sp.Parent.SpanID() != parent.SpanContext().SpanID() {
			t.Error("producer span not a child of parent")
		}
	}
	if !found {
		t.Fatal("producer span not recorded")
	}

	// Consumer span: first delivery is a child of the producer.
	_, cs := StartConsumeSpan(context.Background(), msg)
	cs.End()
	if cs.SpanContext().TraceID() != parent.SpanContext().TraceID() {
		t.Error("consumer span not in producer trace")
	}
}

func TestPublishWithoutTracerHasNoTraceparent(t *testing.T) {
	// Emulate an unconfigured process: noop provider, no span in ctx.
	prevTP := otel.GetTracerProvider()
	otel.SetTracerProvider(noop.NewTracerProvider())
	t.Cleanup(func() { otel.SetTracerProvider(prevTP) })
	s := runJS(t)
	c := NewClient(Config{URL: s.ClientURL()}, logr.Discard())
	defer func() { _ = c.Close() }()
	mustStream(t, c)
	if err := c.PublishJSONCtx(context.Background(), "t.b", map[string]string{"a": "b"}); err != nil {
		t.Fatal(err)
	}
	nc, _ := nats.Connect(s.ClientURL())
	defer nc.Close()
	js, _ := nc.JetStream()
	sub, _ := js.SubscribeSync("t.b", nats.DeliverAll())
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Header.Get("traceparent") != "" {
		t.Fatal("unexpected traceparent header")
	}
}

func TestConsumeSpanRedeliveryLinks(t *testing.T) {
	exp, tracer := setupTracing(t)
	s := runJS(t)
	c := NewClient(Config{URL: s.ClientURL()}, logr.Discard())
	defer func() { _ = c.Close() }()
	mustStream(t, c)
	ctx, producer := tracer.Start(context.Background(), "producer")
	defer producer.End()
	if err := c.PublishJSONCtx(ctx, "t.c", map[string]string{"a": "b"}); err != nil {
		t.Fatal(err)
	}
	nc, _ := nats.Connect(s.ClientURL())
	defer nc.Close()
	js, _ := nc.JetStream()
	sub, err := js.SubscribeSync("t.c", nats.DeliverAll(), nats.AckExplicit(), nats.AckWait(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.NextMsg(2 * time.Second); err != nil { // first delivery, not acked
		t.Fatal(err)
	}
	msg, err := sub.NextMsg(3 * time.Second) // redelivery
	if err != nil {
		t.Fatal(err)
	}
	other, otherSpan := tracer.Start(context.Background(), "poller")
	otherSpan.End()
	_, cs := StartConsumeSpan(other, msg)
	cs.End()

	for _, sp := range exp.GetSpans() {
		if sp.Name != "process "+msg.Subject {
			continue
		}
		if sp.Parent.SpanID() == producer.SpanContext().SpanID() {
			t.Error("redelivered span must not parent to producer")
		}
		if len(sp.Links) != 1 || sp.Links[0].SpanContext.SpanID().String() == "" {
			t.Errorf("expected one link, got %v", sp.Links)
		}
		for _, a := range sp.Attributes {
			if a.Key == "messaging.nats.delivery_count" && a.Value.AsInt64() < 2 {
				t.Errorf("delivery_count = %d", a.Value.AsInt64())
			}
		}
		return
	}
	t.Fatal("consumer span not recorded")
}

func mustStream(t *testing.T, c Client) {
	t.Helper()
	cfg := StreamConfig{Name: "T", Subjects: []string{"t.>"}, Retention: RetentionLimits, Storage: StorageMemory}
	if err := c.CreateStream(cfg); err != nil {
		t.Fatal(err)
	}
}
