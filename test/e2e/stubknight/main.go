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

// Command stubknight is a knight runtime for the e2e tests: no LLM. It reads
// the env the operator gives every knight pod, consumes its tasks over NATS,
// continues the incoming trace context and emits the spans a knight runtime
// owes under docs/design/telemetry-conventions.md (an `invoke_agent <knight>`
// span and one `chat <model>` span with token, cost and redacted content
// attributes), then publishes the result inside a traced publish.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

const (
	serviceName = "e2e-stub-knight"
	tracerName  = "github.com/roundtable-k8s/roundtable/test/e2e/stubknight"
	// costPerToken prices the stub's fake usage so cost attributes are non-zero.
	costPerToken = 0.000002
	systemPrompt = "You are a stub knight. Never call a real model."
)

type config struct {
	knight         string // Knight resource name
	table          string
	model          string
	natsURL        string
	tasksStream    string
	resultsPrefix  string
	subjects       []string
	captureContent bool
}

func loadConfig() config {
	c := config{
		// KNIGHT_NAME is the capitalized resource name; the resource name is
		// what the conventions use.
		knight:         strings.ToLower(os.Getenv("KNIGHT_NAME")),
		table:          os.Getenv("ROUNDTABLE_TABLE"),
		model:          os.Getenv("KNIGHT_MODEL"),
		natsURL:        os.Getenv("NATS_URL"),
		tasksStream:    os.Getenv("NATS_TASKS_STREAM"),
		resultsPrefix:  os.Getenv("NATS_RESULTS_PREFIX"),
		captureContent: !strings.EqualFold(strings.TrimSpace(os.Getenv("ROUNDTABLE_TELEMETRY_CAPTURE_CONTENT")), "false"),
	}
	if c.table == "" {
		c.table = "none"
	}
	if c.model == "" {
		c.model = "stub-model"
	}
	for s := range strings.SplitSeq(os.Getenv("SUBSCRIBE_TOPICS"), ",") {
		if s = strings.TrimSpace(s); s != "" {
			c.subjects = append(c.subjects, s)
		}
	}
	return c
}

func main() {
	cfg := loadConfig()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	tp, err := setupTracing(ctx)
	if err != nil {
		log.Fatalf("tracing: %v", err)
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tp.Shutdown(sctx)
	}()

	var ready atomic.Bool
	go serveProbes(&ready)

	nc, err := nats.Connect(cfg.natsURL, nats.MaxReconnects(-1))
	if err != nil {
		log.Fatalf("connect %s: %v", cfg.natsURL, err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}

	cons, err := createConsumer(ctx, js, cfg)
	if err != nil {
		log.Fatalf("consumer: %v", err)
	}
	k := &knight{cfg: cfg, js: js, tp: tp, tracer: tp.Tracer(tracerName)}
	cc, err := cons.Consume(func(msg jetstream.Msg) { k.handle(ctx, msg) })
	if err != nil {
		log.Fatalf("consume: %v", err)
	}
	defer cc.Stop()
	ready.Store(true)
	log.Printf("stub knight %s (table %s) consuming %v, captureContent=%t",
		cfg.knight, cfg.table, cfg.subjects, cfg.captureContent)
	<-ctx.Done()
}

// setupTracing builds a tracer provider from the standard OTEL_* env the
// operator injects (endpoint, sampler, resource attributes with the table).
func setupTracing(ctx context.Context) (*sdktrace.TracerProvider, error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	res, err := resource.New(ctx,
		resource.WithAttributes(attribute.String("service.name", serviceName)),
		resource.WithFromEnv(),
	)
	if err != nil {
		return nil, err
	}
	opts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res)}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" && !strings.EqualFold(os.Getenv("OTEL_SDK_DISABLED"), "true") {
		exp, err := otlptracegrpc.New(ctx)
		if err != nil {
			return nil, err
		}
		opts = append(opts, sdktrace.WithBatcher(exp))
	}
	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	return tp, nil
}

func serveProbes(ready *atomic.Bool) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{Addr: ":3000", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("probes: %v", err)
	}
}

// createConsumer waits for the table's tasks stream (the operator creates
// it) and binds the knight's durable consumer.
func createConsumer(ctx context.Context, js jetstream.JetStream, cfg config) (jetstream.Consumer, error) {
	ccfg := jetstream.ConsumerConfig{
		Durable:        natspkg.KnightTaskConsumerName(cfg.knight),
		FilterSubjects: cfg.subjects,
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        time.Minute,
		MaxAckPending:  1,
	}
	for {
		cons, err := js.CreateOrUpdateConsumer(ctx, cfg.tasksStream, ccfg)
		if err == nil {
			return cons, nil
		}
		log.Printf("waiting for stream %s: %v", cfg.tasksStream, err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

type knight struct {
	cfg    config
	js     jetstream.JetStream
	tp     *sdktrace.TracerProvider
	tracer trace.Tracer
}

// message is one GenAI semconv chat message with text parts.
type message struct {
	Role  string `json:"role"`
	Parts []part `json:"parts"`
}

type part struct {
	Type    string `json:"type"`
	Content string `json:"content"`
}

func messagesJSON(role, text string) string {
	b, _ := json.Marshal([]message{{Role: role, Parts: []part{{Type: "text", Content: text}}}})
	return string(b)
}

func (k *knight) handle(ctx context.Context, msg jetstream.Msg) {
	var task natspkg.TaskPayload
	if err := json.Unmarshal(msg.Data(), &task); err != nil {
		log.Printf("bad task on %s: %v", msg.Subject(), err)
		_ = msg.Term()
		return
	}

	common := []attribute.KeyValue{
		attribute.String("roundtable.table", k.cfg.table),
		attribute.String("roundtable.knight", k.cfg.knight),
		attribute.String("roundtable.task.id", task.TaskID),
	}
	for key, v := range map[string]string{
		"roundtable.chain": task.ChainName, "roundtable.chain.step": task.StepName, "roundtable.run.id": task.RunID,
	} {
		if v != "" {
			common = append(common, attribute.String(key, v))
		}
	}

	// The task's publish span is the parent (trace context in the headers).
	parent := natspkg.ExtractContext(ctx, msg.Headers())
	agentCtx, agent := k.tracer.Start(parent, "invoke_agent "+k.cfg.knight,
		trace.WithAttributes(common...),
		trace.WithAttributes(
			attribute.String("gen_ai.operation.name", "invoke_agent"),
			attribute.String("gen_ai.agent.name", k.cfg.knight),
		))

	model := k.cfg.model
	if task.Model != "" {
		model = task.Model
	}
	prompt := redactSecrets(task.Task)
	output := fmt.Sprintf("stub knight %s handled step %q: %s", k.cfg.knight, task.StepName, prompt)
	inTokens, outTokens := int64(len(task.Task)/4+1), int64(len(output)/4+1)
	cost := float64(inTokens+outTokens) * costPerToken

	_, chat := k.tracer.Start(agentCtx, "chat "+model, trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(common...),
		trace.WithAttributes(
			attribute.String("gen_ai.operation.name", "chat"),
			attribute.String("gen_ai.provider.name", "e2e-stub"),
			attribute.String("gen_ai.request.model", model),
			attribute.String("gen_ai.response.model", model),
			attribute.Int64("gen_ai.usage.input_tokens", inTokens),
			attribute.Int64("gen_ai.usage.output_tokens", outTokens),
			attribute.Float64("roundtable.cost.usd", cost),
		))
	if k.cfg.captureContent {
		chat.SetAttributes(
			attribute.String("gen_ai.system_instructions", messagesJSON("system", redactSecrets(systemPrompt))),
			attribute.String("gen_ai.input.messages", messagesJSON("user", prompt)),
			attribute.String("gen_ai.output.messages", messagesJSON("assistant", redactSecrets(output))),
		)
	}
	chat.End()
	agent.SetAttributes(attribute.Float64("roundtable.cost.usd", cost))

	result, _ := json.Marshal(natspkg.TaskResult{TaskID: task.TaskID, Output: output, Model: model})
	subject := k.cfg.resultsPrefix + "." + task.TaskID
	_, err := natspkg.PublishTraced(agentCtx, k.js, subject, result, task.TaskID, natspkg.WithTable(k.cfg.table))
	if err != nil {
		log.Printf("publish result %s: %v", subject, err)
		agent.End()
		_ = msg.Nak()
		return
	}
	agent.End()
	_ = msg.Ack()

	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = k.tp.ForceFlush(fctx)
	log.Printf("handled task %s", task.TaskID)
}
