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
	"strings"
	"sync"
	"testing"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/telemetry"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

func TestDeriveSpanIDIsStablePerTraceAndKey(t *testing.T) {
	a, b := trace.TraceID{1}, trace.TraceID{2}
	if first, again := deriveSpanID(a, "step/x/1"), deriveSpanID(a, "step/x/1"); first != again {
		t.Fatal("same trace and key must give the same span ID")
	}
	if deriveSpanID(a, "step/x/1") == deriveSpanID(a, "step/x/2") || deriveSpanID(a, "k") == deriveSpanID(b, "k") {
		t.Fatal("different trace or key must give a different span ID")
	}
	at := metav1.Now()
	persisted := metav1.NewTime(at.Truncate(1e9)) // what a status round-trip keeps
	if groupStepSpanKey("s", &at) != groupStepSpanKey("s", &persisted) {
		t.Fatal("group step key must survive the status round-trip")
	}
}

// traceFakeNATS records the span context each task was published under.
type traceFakeNATS struct {
	*loopFakeNATS
	published map[string]trace.SpanContext // taskID → publish ctx span
}

func (f *traceFakeNATS) PublishJSONSeqCtx(ctx context.Context, subject string, v any, _ ...natspkg.PublishOption) (uint64, error) {
	if p, ok := v.(natspkg.TaskPayload); ok {
		f.published[p.TaskID] = trace.SpanContextFromContext(ctx)
	}
	return f.PublishJSONSeq(subject, v)
}

var _ = Describe("Chain run tracing", func() {
	const (
		namespace = "default"
		rtName    = "trace-table"
		knight    = "trace-knight"
		chainName = "trace-chain"
		prefix    = "tr"
	)
	ctx := context.Background()
	nn := types.NamespacedName{Name: chainName, Namespace: namespace}

	var (
		fake *traceFakeNATS
		rec  *tracetest.SpanRecorder
		tp   trace.TracerProvider
	)

	// A fresh reconciler per reconcile: no span state may live in memory
	// (the operator can restart mid-run).
	reconcileN := func(n int) {
		for range n {
			r := &ChainReconciler{
				Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: record.NewFakeRecorder(1000),
				NATS: natspkg.NewProviderWithClient(fake, logr.Discard()), TracerProvider: tp,
			}
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())
		}
	}
	get := func() *aiv1alpha1.Chain {
		c := &aiv1alpha1.Chain{}
		Expect(k8sClient.Get(ctx, nn, c)).To(Succeed())
		return c
	}
	start := func(steps []aiv1alpha1.ChainStep) {
		Expect(k8sClient.Create(ctx, &aiv1alpha1.Chain{
			ObjectMeta: metav1.ObjectMeta{Name: chainName, Namespace: namespace},
			Spec:       aiv1alpha1.ChainSpec{RoundTableRef: aiv1alpha1.LocalRef(rtName), Steps: steps, TimeoutSeconds: 86400, Input: "x"},
		})).To(Succeed())
		reconcileN(3)
		c := get()
		c.Annotations = map[string]string{aiv1alpha1.AnnotationChainRun: "go"}
		Expect(k8sClient.Update(ctx, c)).To(Succeed())
		reconcileN(1)
		Expect(get().Status.Phase).To(Equal(aiv1alpha1.ChainPhaseRunning))
	}
	tasksFor := func(step string) []natspkg.TaskPayload {
		var out []natspkg.TaskPayload
		for _, t := range fake.tasks {
			if t.StepName == step {
				out = append(out, t)
			}
		}
		return out
	}
	finish := func(t natspkg.TaskPayload, output, errMsg string) {
		fake.results[natspkg.ResultSubject(prefix, t.TaskID)] = natspkg.TaskResult{TaskID: t.TaskID, Output: output, Error: errMsg}
	}
	spansNamed := func(name string) []sdktrace.ReadOnlySpan {
		var out []sdktrace.ReadOnlySpan
		for _, s := range rec.Ended() {
			if s.Name() == name {
				out = append(out, s)
			}
		}
		return out
	}
	attr := func(s sdktrace.ReadOnlySpan, key string) attribute.Value {
		for _, kv := range s.Attributes() {
			if string(kv.Key) == key {
				return kv.Value
			}
		}
		return attribute.Value{}
	}
	task := func(name string) aiv1alpha1.ChainStep {
		return aiv1alpha1.ChainStep{Name: name, KnightRef: corev1.LocalObjectReference{Name: knight}, Task: name + " it", TimeoutSeconds: 120}
	}

	BeforeEach(func() {
		fake = &traceFakeNATS{
			loopFakeNATS: &loopFakeNATS{
				fanoutFakeNATS: &fanoutFakeNATS{fakeNATSClient: newFakeNATSClient(), results: map[string]natspkg.TaskResult{}},
				kv:             map[string][]byte{},
			},
			published: map[string]trace.SpanContext{},
		}
		rec = tracetest.NewSpanRecorder()
		tp = sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec), sdktrace.WithIDGenerator(telemetry.NewIDGenerator()))
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: rtName, Namespace: namespace}, &aiv1alpha1.RoundTable{}); apierrors.IsNotFound(err) {
			Expect(k8sClient.Create(ctx, &aiv1alpha1.RoundTable{
				ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: namespace},
				Spec: aiv1alpha1.RoundTableSpec{NATS: aiv1alpha1.RoundTableNATS{
					URL: "nats://localhost:4222", SubjectPrefix: prefix, TasksStream: "tr_tasks", ResultsStream: "tr_results",
				}},
			})).To(Succeed())
		}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: knight, Namespace: namespace}, &aiv1alpha1.Knight{}); apierrors.IsNotFound(err) {
			Expect(k8sClient.Create(ctx, &aiv1alpha1.Knight{
				ObjectMeta: metav1.ObjectMeta{Name: knight, Namespace: namespace},
				Spec: aiv1alpha1.KnightSpec{
					Domain: "ops", Model: "claude-sonnet-4", Skills: []string{"ops"},
					NATS:        aiv1alpha1.KnightNATS{URL: "nats://x:4222", Subjects: []string{"tr.tasks.ops.>"}, Stream: "tr_tasks", ResultsStream: "tr_results"},
					Concurrency: 2, TaskTimeoutSeconds: 120,
				},
			})).To(Succeed())
		}
	})

	AfterEach(func() {
		c := &aiv1alpha1.Chain{}
		if err := k8sClient.Get(ctx, nn, c); err == nil {
			c.Finalizers = nil
			_ = k8sClient.Update(ctx, c)
			_ = k8sClient.Delete(ctx, c)
		}
	})

	It("puts the run's trace_id and table on operator log lines", func() {
		var mu sync.Mutex
		var lines []string
		sink := funcr.New(func(prefix, args string) {
			mu.Lock()
			defer mu.Unlock()
			lines = append(lines, args)
		}, funcr.Options{Verbosity: 2})
		logCtx := logf.IntoContext(ctx, sink)

		start([]aiv1alpha1.ChainStep{task("a")})
		r := &ChainReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: record.NewFakeRecorder(1000),
			NATS: natspkg.NewProviderWithClient(fake, logr.Discard()), TracerProvider: tp,
		}
		for range 3 {
			_, err := r.Reconcile(logCtx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())
		}
		traceID := get().Status.TraceID
		Expect(traceID).To(HaveLen(32))

		mu.Lock()
		defer mu.Unlock()
		var withTrace int
		for _, l := range lines {
			if strings.Contains(l, `"trace_id"="`+traceID+`"`) {
				withTrace++
				Expect(l).To(ContainSubstring(`"table"="` + rtName + `"`))
			}
		}
		Expect(withTrace).To(BeNumerically(">", 0), "lines: %v", lines)
	})

	It("makes a two-step run one trace: a root span with a span per step", func() {
		b := task("b")
		b.DependsOn = []string{"a"}
		start([]aiv1alpha1.ChainStep{task("a"), b})

		reconcileN(1)
		c := get()
		Expect(c.Status.TraceContext).NotTo(BeNil())
		Expect(c.Status.TraceContext.Traceparent).To(MatchRegexp(`^00-[0-9a-f]{32}-[0-9a-f]{16}-01$`))
		Expect(c.Status.TraceID).To(HaveLen(32))
		Expect(rec.Ended()).To(BeEmpty(), "nothing has finished yet")

		finish(tasksFor("a")[0], "done a", "")
		reconcileN(1)
		Expect(spansNamed("step a")).To(HaveLen(1))
		finish(tasksFor("b")[0], "done b", "")
		reconcileN(2)
		c = get()
		Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhaseSucceeded))

		roots := spansNamed("chain " + chainName)
		Expect(roots).To(HaveLen(1), "the root is emitted once")
		root := roots[0]
		Expect(root.Parent().IsValid()).To(BeFalse())
		Expect(root.SpanContext().TraceID().String()).To(Equal(c.Status.TraceID))
		Expect(c.Status.TraceContext.Traceparent).To(ContainSubstring(root.SpanContext().SpanID().String()))
		Expect(root.Status().Code).To(Equal(codes.Unset))
		Expect(attr(root, attrTable).AsString()).To(Equal(rtName))
		Expect(attr(root, attrChain).AsString()).To(Equal(chainName))
		Expect(attr(root, attrRunID).AsString()).To(Equal(c.Status.RunID))
		Expect(root.EndTime().Before(root.StartTime())).To(BeFalse())

		for _, name := range []string{"a", "b"} {
			steps := spansNamed("step " + name)
			Expect(steps).To(HaveLen(1))
			s := steps[0]
			Expect(s.SpanContext().TraceID()).To(Equal(root.SpanContext().TraceID()))
			Expect(s.Parent().SpanID()).To(Equal(root.SpanContext().SpanID()))
			Expect(attr(s, attrChainStep).AsString()).To(Equal(name))
			Expect(attr(s, attrChainStepType).AsString()).To(Equal(stepTypeTask))
			Expect(attr(s, attrAttempt).AsInt64()).To(BeEquivalentTo(1))
			Expect(attr(s, attrTable).AsString()).To(Equal(rtName))
			// The task was published under the step span, so knight spans
			// join the trace beneath it.
			Expect(fake.published[tasksFor(name)[0].TaskID].SpanID()).To(Equal(s.SpanContext().SpanID()))
		}
		Expect(rec.Ended()).To(HaveLen(3))

		// Further reconciles of the finished run emit nothing more.
		reconcileN(2)
		Expect(rec.Ended()).To(HaveLen(3))
	})

	It("marks a failed run and a retried attempt as errors", func() {
		a := task("a")
		a.Retry = &aiv1alpha1.StepRetry{MaxAttempts: 1}
		start([]aiv1alpha1.ChainStep{a})
		reconcileN(1)
		finish(tasksFor("a")[0], "", "boom")
		reconcileN(2) // fail → retry dispatched
		Expect(tasksFor("a")).To(HaveLen(2))
		finish(tasksFor("a")[1], "", "boom again")
		reconcileN(2)
		Expect(get().Status.Phase).To(Equal(aiv1alpha1.ChainPhaseFailed))

		steps := spansNamed("step a")
		Expect(steps).To(HaveLen(2), "one span per attempt")
		attempts := map[int64]sdktrace.ReadOnlySpan{}
		for _, s := range steps {
			Expect(s.Status().Code).To(Equal(codes.Error))
			attempts[attr(s, attrAttempt).AsInt64()] = s
		}
		Expect(attempts).To(HaveKey(int64(1)))
		Expect(attempts).To(HaveKey(int64(2)))
		Expect(attempts[2].Status().Description).To(Equal("boom again"))
		root := spansNamed("chain " + chainName)
		Expect(root).To(HaveLen(1))
		Expect(root[0].Status().Code).To(Equal(codes.Error))
	})

	It("gives each fan-out item and loop round a span under its step", func() {
		start([]aiv1alpha1.ChainStep{
			task("plan"),
			{Name: "impl", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "impl {{ .Item }}", TimeoutSeconds: 120,
				DependsOn: []string{"plan"}, ForEach: &aiv1alpha1.ChainForEach{From: "steps.plan", Path: ".items"}},
			{Name: "fixloop", DependsOn: []string{"impl"}, Loop: &aiv1alpha1.ChainLoop{
				Until: `last.output.contains("ship it")`, MaxIterations: 3,
				Steps: []aiv1alpha1.ChainLoopStep{{Name: "fix", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "fix"}}}},
		})
		reconcileN(1)
		finish(tasksFor("plan")[0], "```json\n{\"items\":[1,2]}\n```", "")
		reconcileN(1)
		items := tasksFor("impl")
		Expect(items).To(HaveLen(2))
		for _, it := range items {
			finish(it, "ok", "")
		}
		reconcileN(2)
		Expect(tasksFor("fixloop.fix")).To(HaveLen(1))
		finish(tasksFor("fixloop.fix")[0], "not yet", "")
		reconcileN(2)
		Expect(tasksFor("fixloop.fix")).To(HaveLen(2))
		finish(tasksFor("fixloop.fix")[1], "ship it", "")
		reconcileN(2)
		c := get()
		Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhaseSucceeded))

		root := spansNamed("chain " + chainName)
		Expect(root).To(HaveLen(1))
		tid := root[0].SpanContext().TraceID()
		for _, s := range rec.Ended() {
			Expect(s.SpanContext().TraceID()).To(Equal(tid), s.Name())
		}

		impl := spansNamed("step impl")
		Expect(impl).To(HaveLen(1))
		Expect(attr(impl[0], attrChainStepType).AsString()).To(Equal(stepTypeForEach))
		for i, it := range items {
			s := spansNamed("item impl[" + string(rune('0'+i)) + "]")
			Expect(s).To(HaveLen(1))
			Expect(s[0].Parent().SpanID()).To(Equal(impl[0].SpanContext().SpanID()))
			Expect(attr(s[0], attrTaskID).AsString()).To(Equal(it.TaskID))
			Expect(fake.published[it.TaskID].SpanID()).To(Equal(s[0].SpanContext().SpanID()))
		}

		loop := spansNamed("step fixloop")
		Expect(loop).To(HaveLen(1))
		Expect(attr(loop[0], attrChainStepType).AsString()).To(Equal(stepTypeLoop))
		for n, t := range tasksFor("fixloop.fix") {
			s := spansNamed("iteration fixloop " + string(rune('1'+n)))
			Expect(s).To(HaveLen(1))
			Expect(s[0].Parent().SpanID()).To(Equal(loop[0].SpanContext().SpanID()))
			Expect(attr(s[0], attrChainIteration).AsInt64()).To(BeEquivalentTo(n + 1))
			Expect(fake.published[t.TaskID].SpanID()).To(Equal(s[0].SpanContext().SpanID()))
		}
		Expect(spansNamed("iteration fixloop 3")).To(BeEmpty())
	})

	It("stores nothing and changes nothing without a tracer provider", func() {
		tp = noop.NewTracerProvider()
		b := task("b")
		b.DependsOn = []string{"a"}
		start([]aiv1alpha1.ChainStep{task("a"), b})
		reconcileN(1)
		finish(tasksFor("a")[0], "done a", "")
		reconcileN(1)
		finish(tasksFor("b")[0], "done b", "")
		reconcileN(2)
		c := get()
		Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhaseSucceeded))
		Expect(c.Status.TraceContext).To(BeNil())
		Expect(c.Status.TraceID).To(BeEmpty())
		for _, sc := range fake.published {
			Expect(sc.IsValid()).To(BeFalse())
		}
	})

	It("starts a new trace for a new run", func() {
		start([]aiv1alpha1.ChainStep{task("a")})
		reconcileN(1)
		finish(tasksFor("a")[0], "ok", "")
		reconcileN(2)
		first := get().Status.TraceID
		Expect(first).NotTo(BeEmpty())

		c := get()
		c.Annotations[aiv1alpha1.AnnotationChainRun] = "again"
		Expect(k8sClient.Update(ctx, c)).To(Succeed())
		reconcileN(2)
		c = get()
		Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhaseRunning))
		Expect(c.Status.TraceID).NotTo(BeEmpty())
		Expect(c.Status.TraceID).NotTo(Equal(first))
	})
})
