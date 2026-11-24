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
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// ---- unit tests ----

func TestParseJSONDocument(t *testing.T) {
	cases := map[string]struct {
		in   string
		want string
		ok   bool
	}{
		"plain array":  {`[1, "a", {"n": 2}]`, `[1,"a",{"n":2}]`, true},
		"object":       {` {"issues":[216,217]} `, `{"issues":[216,217]}`, true},
		"fenced":       {"Here you go:\n```json\n[\"x\", \"y\"]\n```\nThanks", `["x","y"]`, true},
		"bare fence":   {"```\n[3]\n```", `[3]`, true},
		"prose only":   {"no json here", "", false},
		"trailing":     {`[1] [2]`, "", false},
		"big integers": {`[12345678901234]`, `[12345678901234]`, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			v, err := parseJSONDocument(tc.in)
			if !tc.ok {
				if err == nil {
					t.Fatalf("expected error, got %v", v)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got, _ := json.Marshal(v)
			if string(got) != tc.want {
				t.Fatalf("got %s, want %s", got, tc.want)
			}
		})
	}
}

func TestExtractItems(t *testing.T) {
	doc, err := parseJSONDocument(`{"issues":[{"number":1,"t":"a"},{"number":2,"t":"b"}],"one":{"number":9}}`)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]struct {
		path string
		want string
		ok   bool
	}{
		"list field":       {".issues", `[{"number":1,"t":"a"},{"number":2,"t":"b"}]`, true},
		"braced":           {"{.issues}", `[{"number":1,"t":"a"},{"number":2,"t":"b"}]`, true},
		"projection":       {".issues[*].number", `[1,2]`, true},
		"no leading dot":   {"issues[*].t", `["a","b"]`, true},
		"single object":    {".one", `[{"number":9}]`, true},
		"missing key":      {".nope", "", false},
		"doc not an array": {"", "", false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			items, err := extractItems(doc, tc.path)
			if !tc.ok {
				if err == nil {
					t.Fatalf("expected error, got %v", items)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got, _ := json.Marshal(items)
			if string(got) != tc.want {
				t.Fatalf("got %s, want %s", got, tc.want)
			}
		})
	}
}

func TestUniqueItems(t *testing.T) {
	doc, err := parseJSONDocument(`[{"number":206,"title":"Queues page"},{"number":207,"title":"b"},{"number":206,"title":"Queues page (again)"},{"number":206.0}]`)
	if err != nil {
		t.Fatal(err)
	}
	items := doc.([]any)
	got, err := uniqueItems(items, "number")
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := json.Marshal(got); string(b) != `[{"number":206,"title":"Queues page"},{"number":207,"title":"b"}]` {
		t.Fatalf("got %s", b)
	}
	if got, _ := uniqueItems(items, ""); len(got) != len(items) {
		t.Fatalf("empty key dropped items: %v", got)
	}
	if _, err := uniqueItems(items, "id"); err == nil || !strings.Contains(err.Error(), `item 0 has no key "id"`) {
		t.Fatalf("missing key: %v", err)
	}
	if _, err := uniqueItems([]any{"a"}, "number"); err == nil {
		t.Fatal("expected an error for a non-object item")
	}
}

func TestValidateForEach(t *testing.T) {
	mk := func(steps ...aiv1alpha1.ChainStep) *aiv1alpha1.Chain {
		return &aiv1alpha1.Chain{Spec: aiv1alpha1.ChainSpec{Steps: steps}}
	}
	plan := aiv1alpha1.ChainStep{Name: "plan", KnightRef: corev1.LocalObjectReference{Name: "k"}, Task: "plan"}
	fe := func(from string, deps ...string) aiv1alpha1.ChainStep {
		return aiv1alpha1.ChainStep{Name: "impl", KnightRef: corev1.LocalObjectReference{Name: "k"}, Task: "do {{ .Item }}", DependsOn: deps,
			ForEach: &aiv1alpha1.ChainForEach{From: from}}
	}
	cases := map[string]struct {
		chain *aiv1alpha1.Chain
		ok    bool
	}{
		"input":                   {mk(fe("input")), true},
		"prior step in dependsOn": {mk(plan, fe("steps.plan", "plan")), true},
		"prior step not in deps":  {mk(plan, fe("steps.plan")), false},
		"unknown step":            {mk(fe("steps.ghost", "ghost")), false},
		"self":                    {mk(fe("steps.impl", "impl")), false},
		"bad from":                {mk(fe("output")), false},
		"maxParallel w/o forEach": {mk(aiv1alpha1.ChainStep{Name: "a", Task: "x", MaxParallel: 2}), false},
		"policy w/o forEach":      {mk(aiv1alpha1.ChainStep{Name: "a", Task: "x", FailurePolicy: aiv1alpha1.ChainFailurePolicyContinue}), false},
		"task ignores item": {mk(aiv1alpha1.ChainStep{Name: "a", Task: "same for all",
			ForEach: &aiv1alpha1.ChainForEach{From: "input"}}), false},
		"bad path": {mk(aiv1alpha1.ChainStep{Name: "a", Task: "{{ .Item }}",
			ForEach: &aiv1alpha1.ChainForEach{From: "input", Path: "{.issues[}"}}), false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := validateForEach(tc.chain)
			if tc.ok && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !tc.ok && err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestCompactItemDetailsIsBounded(t *testing.T) {
	const total = 300
	fo := &aiv1alpha1.ChainFanOutStatus{Total: total}
	phases := []byte(strings.Repeat("S", total))
	base := time.Now().Add(-time.Hour)
	for i := range total {
		at := metav1.NewTime(base.Add(time.Duration(i) * time.Second))
		it := aiv1alpha1.ChainItemStatus{Index: int32(i), Phase: aiv1alpha1.ChainStepPhaseSucceeded, CompletedAt: &at, Output: "ok"}
		switch {
		case i == 3:
			it.Phase = aiv1alpha1.ChainStepPhaseFailed
			phases[i] = 'F'
		case i >= total-4:
			it.Phase = aiv1alpha1.ChainStepPhaseRunning
			it.CompletedAt = nil
			phases[i] = 'R'
		}
		fo.Items = append(fo.Items, it)
	}
	fo.ItemPhases = string(phases)
	summariseFanOut(fo)
	compactItemDetails(fo)

	if len(fo.Items) != maxItemDetails {
		t.Fatalf("kept %d details, want %d", len(fo.Items), maxItemDetails)
	}
	if fo.OmittedItems != int32(total-4-(maxItemDetails-4)) {
		t.Fatalf("omittedItems = %d", fo.OmittedItems)
	}
	kept := map[int32]bool{}
	for _, it := range fo.Items {
		kept[it.Index] = true
	}
	for i := total - 4; i < total; i++ {
		if !kept[int32(i)] {
			t.Fatalf("running item %d dropped", i)
		}
	}
	if !kept[3] {
		t.Fatal("failed item dropped before succeeded ones")
	}
	if kept[0] {
		t.Fatal("oldest succeeded item kept over newer ones")
	}
	if len(fo.ItemPhases) != total || fo.Succeeded != total-5 || fo.Failed != 1 || fo.Running != 4 {
		t.Fatalf("summary lost: %+v", fo)
	}
}

func TestResetFailedItemsKeepsFinished(t *testing.T) {
	fo := &aiv1alpha1.ChainFanOutStatus{Total: 4, ItemPhases: "SFKS", Items: []aiv1alpha1.ChainItemStatus{
		{Index: 0, Phase: aiv1alpha1.ChainStepPhaseSucceeded, Output: "a"},
		{Index: 1, Phase: aiv1alpha1.ChainStepPhaseFailed, Error: "x", Retries: 2},
	}}
	if n := resetFailedItems(fo); n != 2 {
		t.Fatalf("reset %d items, want 2", n)
	}
	if fo.ItemPhases != "SPPS" || len(fo.Items) != 1 || fo.Items[0].Index != 0 || fo.Pending != 2 || fo.Succeeded != 2 {
		t.Fatalf("unexpected state: %+v", fo)
	}
}

// ---- envtest ----

// fanoutFakeNATS records every published task and serves results by subject.
type fanoutFakeNATS struct {
	*fakeNATSClient
	seq     uint64
	tasks   []natspkg.TaskPayload
	results map[string]natspkg.TaskResult
}

func (f *fanoutFakeNATS) PublishJSONSeq(subject string, v any) (uint64, error) {
	if err := f.PublishJSON(subject, v); err != nil {
		return 0, err
	}
	if p, ok := v.(natspkg.TaskPayload); ok {
		f.tasks = append(f.tasks, p)
	}
	f.seq++
	return f.seq, nil
}
func (f *fanoutFakeNATS) ConsumerAckFloor(string, string) (uint64, error) { return f.seq, nil }
func (f *fanoutFakeNATS) PollMessage(subject string, _ time.Duration, _ ...natspkg.SubscribeOption) (*nats.Msg, error) {
	res, ok := f.results[subject]
	if !ok {
		return nil, nil
	}
	data, _ := json.Marshal(res)
	return &nats.Msg{Subject: subject, Data: data}, nil
}

var _ = Describe("Chain forEach fan-out", func() {
	const (
		namespace = "default"
		rtName    = "fanout-table"
		knight    = "fanout-knight"
		chainName = "fanout-chain"
		prefix    = "fan"
	)
	ctx := context.Background()
	nn := types.NamespacedName{Name: chainName, Namespace: namespace}

	var (
		fake *fanoutFakeNATS
		r    *ChainReconciler
	)

	reconcileN := func(n int) {
		for range n {
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())
		}
	}
	get := func() *aiv1alpha1.Chain {
		c := &aiv1alpha1.Chain{}
		Expect(k8sClient.Get(ctx, nn, c)).To(Succeed())
		return c
	}
	annotate := func(key, val string) {
		c := get()
		if c.Annotations == nil {
			c.Annotations = map[string]string{}
		}
		c.Annotations[key] = val
		Expect(k8sClient.Update(ctx, c)).To(Succeed())
	}
	start := func(steps []aiv1alpha1.ChainStep, input string) {
		Expect(k8sClient.Create(ctx, &aiv1alpha1.Chain{
			ObjectMeta: metav1.ObjectMeta{Name: chainName, Namespace: namespace},
			Spec:       aiv1alpha1.ChainSpec{RoundTableRef: aiv1alpha1.LocalRef(rtName), Steps: steps, TimeoutSeconds: 86400, Input: input},
		})).To(Succeed())
		reconcileN(3)
		Expect(get().Status.Phase).To(Equal(aiv1alpha1.ChainPhaseIdle))
		annotate(aiv1alpha1.AnnotationChainRun, "go")
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
	// impl is the fan-out step in every spec below.
	implStatus := func(c *aiv1alpha1.Chain) aiv1alpha1.ChainStepStatus {
		for _, ss := range c.Status.StepStatuses {
			if ss.Name == "impl" {
				return ss
			}
		}
		Fail("no status for step impl")
		return aiv1alpha1.ChainStepStatus{}
	}

	BeforeEach(func() {
		fake = &fanoutFakeNATS{fakeNATSClient: newFakeNATSClient(), results: map[string]natspkg.TaskResult{}}
		r = &ChainReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: record.NewFakeRecorder(1000),
			NATS:     natspkg.NewProviderWithClient(fake, logr.Discard()),
		}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: rtName, Namespace: namespace}, &aiv1alpha1.RoundTable{}); apierrors.IsNotFound(err) {
			Expect(k8sClient.Create(ctx, &aiv1alpha1.RoundTable{
				ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: namespace},
				Spec: aiv1alpha1.RoundTableSpec{NATS: aiv1alpha1.RoundTableNATS{
					URL: "nats://localhost:4222", SubjectPrefix: prefix, TasksStream: "fan_tasks", ResultsStream: "fan_results",
				}},
			})).To(Succeed())
		}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: knight, Namespace: namespace}, &aiv1alpha1.Knight{}); apierrors.IsNotFound(err) {
			Expect(k8sClient.Create(ctx, &aiv1alpha1.Knight{
				ObjectMeta: metav1.ObjectMeta{Name: knight, Namespace: namespace},
				Spec: aiv1alpha1.KnightSpec{
					Domain: "ops", Model: "claude-sonnet-4", Skills: []string{"ops"},
					NATS:        aiv1alpha1.KnightNATS{URL: "nats://x:4222", Subjects: []string{"fan.tasks.ops.>"}, Stream: "fan_tasks", ResultsStream: "fan_results"},
					Concurrency: 1, TaskTimeoutSeconds: 120,
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

	It("fans out over a prior step's output with maxParallel, then joins into the next step", func() {
		start([]aiv1alpha1.ChainStep{
			{Name: "plan", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "plan {{ .Input }}", TimeoutSeconds: 120},
			{Name: "impl", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "implement #{{ .Item.number }} ({{ .ItemIndex }}): {{ .item.title }}", TimeoutSeconds: 120,
				DependsOn: []string{"plan"}, MaxParallel: 2,
				ForEach: &aiv1alpha1.ChainForEach{From: "steps.plan", Path: ".issues"}},
			{Name: "summary", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "summarise {{ .Steps.impl.Output }}", TimeoutSeconds: 120, DependsOn: []string{"impl"}},
		}, "epic-1")

		reconcileN(1)
		Expect(tasksFor("plan")).To(HaveLen(1))
		finish(tasksFor("plan")[0], "Plan:\n```json\n{\"issues\":[{\"number\":11,\"title\":\"a\"},{\"number\":12,\"title\":\"b\"},{\"number\":13,\"title\":\"c\"}]}\n```", "")

		// plan succeeds → impl fans out, but only 2 items go in flight.
		reconcileN(1)
		c := get()
		impl := implStatus(c)
		Expect(impl.Phase).To(Equal(aiv1alpha1.ChainStepPhaseRunning))
		Expect(impl.FanOut).NotTo(BeNil())
		Expect(impl.FanOut.Total).To(BeEquivalentTo(3))
		Expect(impl.FanOut.ItemPhases).To(Equal("RRP"))
		items := tasksFor("impl")
		Expect(items).To(HaveLen(2))
		Expect(items[0].Task).To(Equal("implement #11 (0): a"))
		Expect(items[1].Task).To(Equal("implement #12 (1): b"))
		// Distinct task and run identities per item, all derived from the run.
		Expect(items[0].TaskID).NotTo(Equal(items[1].TaskID))
		Expect(items[0].RunID).To(Equal(c.Status.RunID + "-i0"))
		Expect(items[1].RunID).To(Equal(c.Status.RunID + "-i1"))

		reconcileN(1)
		Expect(tasksFor("impl")).To(HaveLen(2), "no third item while two are in flight")

		finish(items[0], "done 11", "")
		reconcileN(1)
		items = tasksFor("impl")
		Expect(items).To(HaveLen(3))
		Expect(items[2].Task).To(Equal("implement #13 (2): c"))
		Expect(implStatus(get()).FanOut.ItemPhases).To(Equal("SRR"))

		finish(items[1], "done 12", "")
		finish(items[2], "done 13", "")
		reconcileN(1)
		c = get()
		impl = implStatus(c)
		Expect(impl.Phase).To(Equal(aiv1alpha1.ChainStepPhaseSucceeded))
		Expect(impl.FanOut.Succeeded).To(BeEquivalentTo(3))

		var joined []map[string]any
		Expect(json.Unmarshal([]byte(impl.Output), &joined)).To(Succeed())
		Expect(joined).To(HaveLen(3))
		Expect(joined[1]["output"]).To(Equal("done 12"))
		Expect(joined[1]["phase"]).To(Equal("Succeeded"))
		Expect(joined[2]["item"]).To(HaveKeyWithValue("number", BeNumerically("==", 13)))

		// The join is handed to the next step.
		Expect(tasksFor("summary")).To(HaveLen(1))
		Expect(tasksFor("summary")[0].Task).To(ContainSubstring(`"output":"done 13"`))
		finish(tasksFor("summary")[0], "all good", "")
		reconcileN(1)
		Expect(get().Status.Phase).To(Equal(aiv1alpha1.ChainPhaseSucceeded))
	})

	It("failFast stops dispatching, and retry-failed resumes without redoing finished items", func() {
		start([]aiv1alpha1.ChainStep{
			{Name: "impl", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "do {{ .Item }}", TimeoutSeconds: 120, MaxParallel: 1,
				ForEach: &aiv1alpha1.ChainForEach{From: "input"}},
		}, `["a","b","c"]`)

		reconcileN(1)
		finish(tasksFor("impl")[0], "ok a", "")
		reconcileN(1)
		Expect(tasksFor("impl")).To(HaveLen(2))
		finish(tasksFor("impl")[1], "", "boom")
		reconcileN(1)

		c := get()
		impl := implStatus(c)
		Expect(impl.Phase).To(Equal(aiv1alpha1.ChainStepPhaseFailed))
		Expect(impl.FanOut.ItemPhases).To(Equal("SFK"))
		Expect(impl.Error).To(ContainSubstring("1/3 item(s) failed, 1 skipped"))
		Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhaseFailed))
		Expect(tasksFor("impl")).To(HaveLen(2), "item c never dispatched")

		annotate(aiv1alpha1.AnnotationChainRetryFailed, "r1")
		reconcileN(1)
		c = get()
		Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhaseRunning))
		Expect(implStatus(c).FanOut.ItemPhases).To(Equal("SPP"))

		reconcileN(1)
		items := tasksFor("impl")
		Expect(items).To(HaveLen(3))
		Expect(items[2].Task).To(Equal("do b"), "resumes at the failed item, not a")
		finish(items[2], "ok b", "")
		reconcileN(1)
		items = tasksFor("impl")
		Expect(items).To(HaveLen(4))
		Expect(items[3].Task).To(Equal("do c"))
		finish(items[3], "ok c", "")
		reconcileN(1)

		c = get()
		Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhaseSucceeded))
		var joined []map[string]any
		Expect(json.Unmarshal([]byte(implStatus(c).Output), &joined)).To(Succeed())
		Expect(joined[0]["output"]).To(Equal("ok a"))
		Expect(joined[1]["output"]).To(Equal("ok b"))
	})

	It("continue runs every item and ends the chain PartiallySucceeded", func() {
		start([]aiv1alpha1.ChainStep{
			{Name: "impl", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "do {{ .ItemJSON }}", TimeoutSeconds: 120, MaxParallel: 3,
				FailurePolicy: aiv1alpha1.ChainFailurePolicyContinue,
				ForEach:       &aiv1alpha1.ChainForEach{From: "input", Path: "{.xs}"}},
		}, `{"xs":[1,2,3]}`)

		reconcileN(1)
		items := tasksFor("impl")
		Expect(items).To(HaveLen(3))
		Expect(items[0].Task).To(Equal("do 1"))
		finish(items[0], "ok", "")
		finish(items[1], "", "nope")
		finish(items[2], "ok", "")
		reconcileN(1)

		c := get()
		impl := implStatus(c)
		Expect(impl.Phase).To(Equal(aiv1alpha1.ChainStepPhaseSucceeded))
		Expect(impl.FanOut.Failed).To(BeEquivalentTo(1))
		Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhasePartiallySucceeded))
		var joined []map[string]any
		Expect(json.Unmarshal([]byte(impl.Output), &joined)).To(Succeed())
		Expect(joined[1]["phase"]).To(Equal("Failed"))
		Expect(joined[1]["error"]).To(Equal("nope"))
	})

	It("retries a failed item per the step retry policy before failing it", func() {
		start([]aiv1alpha1.ChainStep{
			{Name: "impl", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "do {{ .Item }}", TimeoutSeconds: 120,
				Retry:   &aiv1alpha1.StepRetry{MaxAttempts: 1, BackoffSeconds: 1},
				ForEach: &aiv1alpha1.ChainForEach{From: "input"}},
		}, `["x"]`)
		reconcileN(1)
		finish(tasksFor("impl")[0], "", "flaky")
		reconcileN(1)
		impl := implStatus(get())
		Expect(impl.Phase).To(Equal(aiv1alpha1.ChainStepPhaseRunning))
		Expect(impl.FanOut.ItemPhases).To(Equal("P"))
		Expect(impl.FanOut.Items[0].Retries).To(BeEquivalentTo(1))

		time.Sleep(1100 * time.Millisecond) // backoff
		reconcileN(1)
		Expect(tasksFor("impl")).To(HaveLen(2))
		finish(tasksFor("impl")[1], "fine", "")
		reconcileN(1)
		Expect(get().Status.Phase).To(Equal(aiv1alpha1.ChainPhaseSucceeded))
	})

	It("passes the step model to every item and rejects a model the knight cannot run", func() {
		r.DefaultKnightImage = "ghcr.io/roundtable-k8s/claude-knight:v1"
		start([]aiv1alpha1.ChainStep{
			{Name: "impl", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "do {{ .Item }}", TimeoutSeconds: 120, MaxParallel: 2, Model: "opus",
				ForEach: &aiv1alpha1.ChainForEach{From: "input"}},
		}, `["a","b"]`)
		reconcileN(1)
		items := tasksFor("impl")
		Expect(items).To(HaveLen(2))
		for _, it := range items {
			Expect(it.Model).To(Equal("opus"))
		}
		Expect(implStatus(get()).Model).To(Equal("opus"))

		// Same chain, but a provider/model the claude runtime can't run.
		c := get()
		c.Spec.Steps[0].Model = "openrouter/z-ai/glm-5.3"
		Expect(k8sClient.Update(ctx, c)).To(Succeed())
		finish(items[0], "ok", "")
		finish(items[1], "ok", "")
		reconcileN(2) // finish the old run, then the generation drift resets to Idle
		annotate(aiv1alpha1.AnnotationChainRun, "go-2")
		reconcileN(2)
		c = get()
		Expect(implStatus(c).Phase).To(Equal(aiv1alpha1.ChainStepPhaseFailed))
		Expect(implStatus(c).Error).To(ContainSubstring("invalid model"))
		Expect(tasksFor("impl")).To(HaveLen(2), "nothing dispatched with a rejected model")
	})

	It("forEach.uniqueBy fans out once per distinct value", func() {
		start([]aiv1alpha1.ChainStep{
			{Name: "impl", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "do #{{ .Item.number }}", TimeoutSeconds: 120, MaxParallel: 2,
				ForEach: &aiv1alpha1.ChainForEach{From: "input", UniqueBy: "number"}},
		}, `[{"number":1},{"number":2},{"number":1,"title":"dup"}]`)
		reconcileN(1)
		Expect(implStatus(get()).FanOut.Total).To(BeEquivalentTo(2))
		got := make([]string, 0, 2)
		for _, t := range tasksFor("impl") {
			got = append(got, t.Task)
		}
		Expect(got).To(Equal([]string{"do #1", "do #2"}))
	})

	// #313: a split must give every sub-issue exactly one implement task, on a
	// branch named after the sub-issue, which updates an existing open PR for
	// it instead of opening another (#209 and #210 were both opened for #206).
	It("the PR pipeline's split runs one task per sub-issue that reuses its open PR", func() {
		flat := func(s string) string { return strings.Join(strings.Fields(s), " ") }
		example, err := readPRPipelineExample()
		Expect(err).NotTo(HaveOccurred())
		var steps []aiv1alpha1.ChainStep
		for _, s := range example.Spec.Steps {
			if s.Name == "plan" || s.Name == "split_rt_operator" {
				s.KnightRef.Name, s.Model = knight, "" // the fake knight has no aliases
				if s.ForEach != nil {
					s.Name = "impl"
				}
				steps = append(steps, s)
			}
		}
		Expect(steps).To(HaveLen(2))
		Expect(steps[1].ForEach.UniqueBy).To(Equal("number"))
		start(steps, `{"repo": "roundtable-k8s/roundtable-ui", "issue": 185}`)

		reconcileN(1)
		Expect(tasksFor("plan")).To(HaveLen(1))
		Expect(flat(tasksFor("plan")[0].Task)).To(ContainSubstring("lists each sub-issue exactly once and never the parent issue"))
		// The planner repeats sub-issue #206 (the #209/#210 case).
		finish(tasksFor("plan")[0], "```json\n"+`{"coder": "rt-operator", "model": "deep", "split": true, "breaking": false, "base": "",`+
			` "issues": [{"number": 206, "title": "Queues page"}, {"number": 207, "title": "Queue metrics"},`+
			` {"number": 206, "title": "Queues page"}], "reason": "r"}`+"\n```", "")
		reconcileN(1)
		Expect(implStatus(get()).FanOut.Total).To(BeEquivalentTo(2))
		items := tasksFor("impl")
		Expect(items).To(HaveLen(2))
		for i, n := range []string{"206", "207"} {
			task := flat(items[i].Task)
			Expect(task).To(HavePrefix("Implement sub-issue #" + n + " "))
			Expect(task).To(ContainSubstring("branch named after the sub-issue, rt/" + n + "-<short-slug>"))
			Expect(task).To(ContainSubstring(`--search "Closes #` + n + ` in:body"`))
			Expect(task).To(ContainSubstring(`branch starts with "rt/` + n + `-"`))
			Expect(task).To(ContainSubstring("update that PR instead of opening a new one"))
			Expect(task).To(ContainSubstring(`body ends "Closes #` + n + `"`))
		}

		finish(items[0], "https://github.com/roundtable-k8s/roundtable-ui/pull/209", "")
		finish(items[1], "https://github.com/roundtable-k8s/roundtable-ui/pull/211", "")
		reconcileN(1)
		c := get()
		Expect(implStatus(c).Phase).To(Equal(aiv1alpha1.ChainStepPhaseSucceeded))
		Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhaseSucceeded))
		Expect(tasksFor("impl")).To(HaveLen(2), "no second task (and PR) for #206")
	})

	It("an empty list joins immediately as []", func() {
		start([]aiv1alpha1.ChainStep{
			{Name: "impl", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "do {{ .Item }}", TimeoutSeconds: 120, ForEach: &aiv1alpha1.ChainForEach{From: "input"}},
		}, `[]`)
		reconcileN(1)
		c := get()
		Expect(implStatus(c).Output).To(Equal("[]"))
		Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhaseSucceeded))
	})

	It("fails the step when the list exceeds maxItems or is not a list", func() {
		start([]aiv1alpha1.ChainStep{
			{Name: "impl", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "do {{ .Item }}", TimeoutSeconds: 120,
				ForEach: &aiv1alpha1.ChainForEach{From: "input", MaxItems: 2}},
		}, `[1,2,3]`)
		reconcileN(1)
		c := get()
		Expect(implStatus(c).Phase).To(Equal(aiv1alpha1.ChainStepPhaseFailed))
		Expect(implStatus(c).Error).To(ContainSubstring("maxItems=2"))
		Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhaseFailed))
		Expect(fake.tasks).To(BeEmpty())
	})

	It("marks the chain invalid when forEach.from is not in dependsOn", func() {
		Expect(k8sClient.Create(ctx, &aiv1alpha1.Chain{
			ObjectMeta: metav1.ObjectMeta{Name: chainName, Namespace: namespace},
			Spec: aiv1alpha1.ChainSpec{RoundTableRef: aiv1alpha1.LocalRef(rtName), TimeoutSeconds: 600, Steps: []aiv1alpha1.ChainStep{
				{Name: "plan", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "plan", TimeoutSeconds: 120},
				{Name: "impl", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "do {{ .Item }}", TimeoutSeconds: 120,
					ForEach: &aiv1alpha1.ChainForEach{From: "steps.plan"}},
			}},
		})).To(Succeed())
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(err).To(HaveOccurred())
		cond := meta.FindStatusCondition(get().Status.Conditions, aiv1alpha1.ConditionChainValid)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Reason).To(Equal(aiv1alpha1.ReasonInvalidForEach))
		Expect(cond.Message).To(ContainSubstring(fmt.Sprintf("%q must be listed in dependsOn", "plan")))
	})
})
