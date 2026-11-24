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
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/go-logr/logr"
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

func TestRecordLoopHistory(t *testing.T) {
	ss := &aiv1alpha1.ChainStepStatus{Loop: &aiv1alpha1.ChainLoopStatus{MaxIterations: 3}}
	for round := int32(1); round <= 5; round++ {
		for _, name := range []string{"review", "fix"} {
			bs := &aiv1alpha1.ChainLoopStepStatus{Name: name, Iteration: round, TaskID: fmt.Sprintf("%s-%d", name, round)}
			if name == "fix" && round%2 == 0 {
				bs.Error = "boom"
				recordLoopHistory(ss, bs, aiv1alpha1.ChainStepPhaseFailed, "")
				continue
			}
			recordLoopHistory(ss, bs, aiv1alpha1.ChainStepPhaseSucceeded, strings.Repeat("x", 5000))
		}
		if round == 3 && len(ss.Loop.History) != 6 {
			t.Fatalf("3 rounds should leave 6 entries, got %d", len(ss.Loop.History))
		}
	}
	h := ss.Loop.History
	if len(h) != 6 || h[0].Round != 3 || h[len(h)-1].Round != 5 {
		t.Fatalf("history not capped to last 3 rounds: %+v", h)
	}
	if h[1].Step != "fix" || h[1].Phase != aiv1alpha1.ChainStepPhaseSucceeded || !strings.HasPrefix(h[1].OutputSummary, strings.Repeat("x", loopHistorySummaryBytes)) || len(h[1].OutputSummary) > loopHistorySummaryBytes+32 {
		t.Fatalf("unexpected entry: %+v", h[1])
	}
	if h[3].Phase != aiv1alpha1.ChainStepPhaseFailed || h[3].Error != "boom" || h[3].OutputSummary != "" {
		t.Fatalf("unexpected failed entry: %+v", h[3])
	}
	// The cap never exceeds 20 rounds even for a larger maxIterations.
	ss = &aiv1alpha1.ChainStepStatus{Loop: &aiv1alpha1.ChainLoopStatus{MaxIterations: 100}}
	for round := int32(1); round <= 30; round++ {
		recordLoopHistory(ss, &aiv1alpha1.ChainLoopStepStatus{Name: "a", Iteration: round}, aiv1alpha1.ChainStepPhaseSucceeded, "ok")
	}
	if len(ss.Loop.History) != loopHistoryMaxRounds || ss.Loop.History[0].Round != 11 {
		t.Fatalf("expected last %d rounds, got %d starting at %d", loopHistoryMaxRounds, len(ss.Loop.History), ss.Loop.History[0].Round)
	}
}

func TestEvalCondition(t *testing.T) {
	vars := func() map[string]any {
		return map[string]any{
			"input": "epic",
			"steps": map[string]any{
				"recon":  map[string]any{"phase": "Succeeded", "output": "22/tcp open\n80/tcp closed", "outcome": "", "iterations": int64(0)},
				"api":    map[string]any{"phase": "Succeeded", "output": "Result:\n```json\n{\"vulnerable\": true, \"score\": 7}\n```"},
				"failed": map[string]any{"phase": "Failed", "output": "", "error": "boom"},
			},
		}
	}
	cases := map[string]struct {
		expr string
		want bool
		err  bool
	}{
		"contains":            {`steps.recon.output.contains("22/tcp open")`, true, false},
		"not contains":        {`steps.recon.output.contains("21/tcp open")`, false, false},
		"and/or/not":          {`steps.recon.output.contains("22/tcp") && !steps.recon.output.contains("80/tcp open") || false`, true, false},
		"regex":               {`steps.recon.output.matches("(?m)^80/tcp (open|closed)$")`, true, false},
		"phase":               {`steps.failed.phase == "Failed" && steps.failed.error == "boom"`, true, false},
		"json fenced":         {`json(steps.api.output).vulnerable == true && json(steps.api.output).score > 5`, true, false},
		"ext strings":         {`steps.recon.output.lowerAscii().split("\n").size() == 2`, true, false},
		"input":               {`input == "epic"`, true, false},
		"exists over steps":   {`steps.exists(k, steps[k].phase == "Failed")`, true, false},
		"defaults for last":   {`!has(last.output)`, true, false},
		"loop var defaults":   {`iteration == 0 && maxIterations == 0 && size(body) == 0`, true, false},
		"unknown step errors": {`steps.ghost.output == ""`, false, true},
		"json of prose":       {`json(steps.recon.output).x == 1`, false, true},
		"non-bool":            {`steps.recon.output`, false, true},
		"syntax":              {`steps.recon.output.contains(`, false, true},
		"undeclared var":      {`foo == 1`, false, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := evalCondition(tc.expr, vars())
			if tc.err {
				if err == nil {
					t.Fatalf("expected an error, got %v", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestEvalConditionCostLimit(t *testing.T) {
	// Nested comprehensions over a large list must hit the cost limit rather
	// than hog the reconciler.
	expr := `[1,2,3,4,5,6,7,8,9,10].all(a, [1,2,3,4,5,6,7,8,9,10].all(b, [1,2,3,4,5,6,7,8,9,10].all(c, [1,2,3,4,5,6,7,8,9,10].all(d, [1,2,3,4,5,6,7,8,9,10].all(e, [1,2,3,4,5,6,7,8,9,10].all(f, a+b+c+d+e+f > 0))))))`
	if _, err := evalCondition(expr, map[string]any{}); err == nil || !strings.Contains(err.Error(), "cost") {
		t.Fatalf("expected a cost limit error, got %v", err)
	}
}

func TestValidateConditionsAndLoops(t *testing.T) {
	mk := func(steps ...aiv1alpha1.ChainStep) *aiv1alpha1.Chain {
		return &aiv1alpha1.Chain{Spec: aiv1alpha1.ChainSpec{Steps: steps}}
	}
	review := aiv1alpha1.ChainStep{Name: "review", KnightRef: corev1.LocalObjectReference{Name: "k"}, Task: "review"}
	body := []aiv1alpha1.ChainLoopStep{{Name: "fix", KnightRef: corev1.LocalObjectReference{Name: "k"}, Task: "fix"}, {Name: "review", KnightRef: corev1.LocalObjectReference{Name: "k"}, Task: "re-review"}}
	loop := func(mut func(*aiv1alpha1.ChainStep)) aiv1alpha1.ChainStep {
		s := aiv1alpha1.ChainStep{Name: "fixloop", DependsOn: []string{"review"},
			Loop: &aiv1alpha1.ChainLoop{From: "review", Until: `last.output.contains("ship it")`, MaxIterations: 3, Steps: body}}
		if mut != nil {
			mut(&s)
		}
		return s
	}
	cases := map[string]struct {
		chain  *aiv1alpha1.Chain
		reason string
	}{
		"valid loop":   {mk(review, loop(nil)), ""},
		"valid when":   {mk(review, aiv1alpha1.ChainStep{Name: "b", KnightRef: corev1.LocalObjectReference{Name: "k"}, Task: "t", When: `steps.review.phase == "Succeeded"`}), ""},
		"bad when":     {mk(aiv1alpha1.ChainStep{Name: "b", KnightRef: corev1.LocalObjectReference{Name: "k"}, Task: "t", When: `steps.x.output.contains(`}), aiv1alpha1.ReasonInvalidCondition},
		"non-bool":     {mk(aiv1alpha1.ChainStep{Name: "b", KnightRef: corev1.LocalObjectReference{Name: "k"}, Task: "t", When: `input`}), aiv1alpha1.ReasonInvalidCondition},
		"bad until":    {mk(review, loop(func(s *aiv1alpha1.ChainStep) { s.Loop.Until = "1 +" })), aiv1alpha1.ReasonInvalidCondition},
		"no knightRef": {mk(aiv1alpha1.ChainStep{Name: "b", Task: "t"}), aiv1alpha1.ReasonInvalidLoop},
		"loop with knightRef and model": {mk(review, loop(func(s *aiv1alpha1.ChainStep) { s.KnightRef.Name = "k"; s.Model = "opus" })),
			aiv1alpha1.ReasonInvalidLoop},
		"loop with forEach": {mk(review, loop(func(s *aiv1alpha1.ChainStep) { s.ForEach = &aiv1alpha1.ChainForEach{From: "input"} })),
			aiv1alpha1.ReasonInvalidLoop},
		"from not in deps": {mk(review, loop(func(s *aiv1alpha1.ChainStep) { s.DependsOn = nil })), aiv1alpha1.ReasonInvalidLoop},
		"from unknown":     {mk(review, loop(func(s *aiv1alpha1.ChainStep) { s.Loop.From = "ghost" })), aiv1alpha1.ReasonInvalidLoop},
		"no from is fine":  {mk(review, loop(func(s *aiv1alpha1.ChainStep) { s.Loop.From = "" })), ""},
		"duplicate body": {mk(review, loop(func(s *aiv1alpha1.ChainStep) {
			s.Loop.Steps = []aiv1alpha1.ChainLoopStep{body[0], body[0]}
		})), aiv1alpha1.ReasonInvalidLoop},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			reason, err := validateConditionsAndLoops(tc.chain)
			if tc.reason == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || reason != tc.reason {
				t.Fatalf("got reason %q err %v, want reason %q", reason, err, tc.reason)
			}
		})
	}
}

func TestResetLoopForRetry(t *testing.T) {
	spec := &aiv1alpha1.ChainStep{Name: "l", Loop: &aiv1alpha1.ChainLoop{MaxIterations: 2}}
	exhausted := &aiv1alpha1.ChainLoopStatus{Iteration: 2, MaxIterations: 2, Outcome: aiv1alpha1.ChainLoopExhausted,
		Steps: []aiv1alpha1.ChainLoopStepStatus{{Name: "a", Iteration: 2, Phase: aiv1alpha1.ChainStepPhaseSucceeded}}}
	got := resetLoopForRetry(exhausted, spec)
	if got == nil || got.MaxIterations != 4 || got.Outcome != "" || got.Steps[0].Phase != aiv1alpha1.ChainStepPhaseSucceeded {
		t.Fatalf("exhausted loop not extended: %+v", got)
	}
	failed := &aiv1alpha1.ChainLoopStatus{Iteration: 1, MaxIterations: 2, Outcome: aiv1alpha1.ChainLoopFailed,
		Steps: []aiv1alpha1.ChainLoopStepStatus{
			{Name: "a", Iteration: 1, Phase: aiv1alpha1.ChainStepPhaseSucceeded},
			{Name: "b", Iteration: 1, Phase: aiv1alpha1.ChainStepPhaseFailed, Error: "x", Retries: 2},
		}}
	got = resetLoopForRetry(failed, spec)
	if got.MaxIterations != 2 || got.Steps[1].Phase != aiv1alpha1.ChainStepPhasePending || got.Steps[1].Retries != 0 || nextBodyIndex(got) != 1 {
		t.Fatalf("failed loop not resumed at the failed body step: %+v", got)
	}
	if resetLoopForRetry(&aiv1alpha1.ChainLoopStatus{}, spec) != nil {
		t.Fatal("a loop that never started should restart from scratch")
	}
}

// ---- envtest ----

// loopFakeNATS adds a working KV store to the fan-out fake.
type loopFakeNATS struct {
	*fanoutFakeNATS
	kvMu sync.Mutex
	kv   map[string][]byte
}

func (f *loopFakeNATS) KVPut(_ string, key string, data []byte) error {
	f.kvMu.Lock()
	defer f.kvMu.Unlock()
	f.kv[key] = data
	return nil
}

func (f *loopFakeNATS) KVGet(_ string, key string) ([]byte, error) {
	f.kvMu.Lock()
	defer f.kvMu.Unlock()
	if d, ok := f.kv[key]; ok {
		return d, nil
	}
	return nil, fmt.Errorf("key %q not found", key)
}

var _ = Describe("Chain when conditions and loops", func() {
	const (
		namespace = "default"
		rtName    = "loop-table"
		knight    = "loop-knight"
		chainName = "loop-chain"
		prefix    = "lp"
	)
	ctx := context.Background()
	nn := types.NamespacedName{Name: chainName, Namespace: namespace}

	var (
		fake *loopFakeNATS
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
	start := func(steps []aiv1alpha1.ChainStep) {
		Expect(k8sClient.Create(ctx, &aiv1alpha1.Chain{
			ObjectMeta: metav1.ObjectMeta{Name: chainName, Namespace: namespace},
			Spec:       aiv1alpha1.ChainSpec{RoundTableRef: aiv1alpha1.LocalRef(rtName), Steps: steps, TimeoutSeconds: 86400, Input: "issue #56"},
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
	finishLast := func(step, output string) {
		ts := tasksFor(step)
		ExpectWithOffset(1, ts).NotTo(BeEmpty(), "no task for %s", step)
		finish(ts[len(ts)-1], output, "")
	}
	status := func(c *aiv1alpha1.Chain, name string) aiv1alpha1.ChainStepStatus {
		for _, ss := range c.Status.StepStatuses {
			if ss.Name == name {
				return ss
			}
		}
		Fail("no status for step " + name)
		return aiv1alpha1.ChainStepStatus{}
	}

	// pipeline is implement → review → fixloop → merge | escalate.
	pipeline := func(maxIter int32, onExhausted aiv1alpha1.ChainLoopExhaustedPolicy) []aiv1alpha1.ChainStep {
		return []aiv1alpha1.ChainStep{
			{Name: "implement", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "implement {{ .Input }}", TimeoutSeconds: 120},
			{Name: "review", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "review {{ .Steps.implement.Output }}", TimeoutSeconds: 120, DependsOn: []string{"implement"}},
			{Name: "fixloop", DependsOn: []string{"review"}, Loop: &aiv1alpha1.ChainLoop{
				From: "review", Until: `last.output.contains("Verdict: ship it")`, MaxIterations: maxIter, OnExhausted: onExhausted,
				Steps: []aiv1alpha1.ChainLoopStep{
					{Name: "fix", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "round {{ .Loop.Iteration }}/{{ .Loop.MaxIterations }} fix: {{ .Loop.Last.Output }}", Model: "opus"},
					{Name: "rereview", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "re-review after: {{ .Loop.Last.Output }}"},
				}}},
			{Name: "merge", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "merge {{ .Steps.fixloop.Output }}", TimeoutSeconds: 120, DependsOn: []string{"fixloop"},
				When: `steps.fixloop.outcome == "Converged"`},
			{Name: "escalate", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "label needs-human after {{ .Steps.fixloop.Iterations }} rounds", TimeoutSeconds: 120,
				DependsOn: []string{"fixloop"}, When: `steps.fixloop.outcome == "Exhausted"`},
		}
	}

	BeforeEach(func() {
		fake = &loopFakeNATS{
			fanoutFakeNATS: &fanoutFakeNATS{fakeNATSClient: newFakeNATSClient(), results: map[string]natspkg.TaskResult{}},
			kv:             map[string][]byte{},
		}
		r = &ChainReconciler{
			Client:             k8sClient,
			Scheme:             k8sClient.Scheme(),
			Recorder:           record.NewFakeRecorder(1000),
			NATS:               natspkg.NewProviderWithClient(fake, logr.Discard()),
			DefaultKnightImage: "ghcr.io/roundtable-k8s/claude-knight:v1",
		}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: rtName, Namespace: namespace}, &aiv1alpha1.RoundTable{}); apierrors.IsNotFound(err) {
			Expect(k8sClient.Create(ctx, &aiv1alpha1.RoundTable{
				ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: namespace},
				Spec: aiv1alpha1.RoundTableSpec{NATS: aiv1alpha1.RoundTableNATS{
					URL: "nats://localhost:4222", SubjectPrefix: prefix, TasksStream: "lp_tasks", ResultsStream: "lp_results",
				}},
			})).To(Succeed())
		}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: knight, Namespace: namespace}, &aiv1alpha1.Knight{}); apierrors.IsNotFound(err) {
			Expect(k8sClient.Create(ctx, &aiv1alpha1.Knight{
				ObjectMeta: metav1.ObjectMeta{Name: knight, Namespace: namespace},
				Spec: aiv1alpha1.KnightSpec{
					Domain: "ops", Model: "claude-sonnet-4", Skills: []string{"ops"},
					NATS:        aiv1alpha1.KnightNATS{URL: "nats://x:4222", Subjects: []string{"lp.tasks.ops.>"}, Stream: "lp_tasks", ResultsStream: "lp_results"},
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

	It("skips steps whose when is false without dispatching, and their dependents still run", func() {
		start([]aiv1alpha1.ChainStep{
			{Name: "recon", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "scan", TimeoutSeconds: 120},
			{Name: "ssh", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "ssh", TimeoutSeconds: 120, DependsOn: []string{"recon"},
				When: `steps.recon.output.contains("22/tcp open")`},
			{Name: "web", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "web", TimeoutSeconds: 120, DependsOn: []string{"recon"},
				When: `steps.recon.output.contains("80/tcp open")`},
			{Name: "report", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "report ssh={{ .Steps.ssh.Phase }} web={{ .Steps.web.Phase }}/{{ .Steps.web.Reason }}",
				TimeoutSeconds: 120, DependsOn: []string{"ssh", "web"}},
		})
		reconcileN(1)
		// The marker sits past the 4000-char status truncation: when must see the full output.
		finishLast("recon", strings.Repeat(".", 5000)+"\n22/tcp open")
		reconcileN(1)

		c := get()
		Expect(status(c, "recon").Output).To(ContainSubstring("[truncated"))
		Expect(status(c, "ssh").Phase).To(Equal(aiv1alpha1.ChainStepPhaseRunning))
		Expect(status(c, "web").Phase).To(Equal(aiv1alpha1.ChainStepPhaseSkipped))
		Expect(status(c, "web").Reason).To(Equal("WhenFalse"))
		Expect(tasksFor("web")).To(BeEmpty())

		finishLast("ssh", "ssh weak")
		reconcileN(1)
		Expect(tasksFor("report")).To(HaveLen(1))
		Expect(tasksFor("report")[0].Task).To(Equal("report ssh=Succeeded web=Skipped/WhenFalse"))
		finishLast("report", "done")
		reconcileN(1)
		Expect(get().Status.Phase).To(Equal(aiv1alpha1.ChainPhaseSucceeded))
	})

	It("fails a step whose when cannot be evaluated", func() {
		start([]aiv1alpha1.ChainStep{
			{Name: "a", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "a", TimeoutSeconds: 120},
			{Name: "b", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "b", TimeoutSeconds: 120, DependsOn: []string{"a"}, When: `json(steps.a.output).ok`},
		})
		reconcileN(1)
		finishLast("a", `{"other": true}`) // valid JSON, but no "ok" key
		reconcileN(1)
		c := get()
		Expect(status(c, "b").Phase).To(Equal(aiv1alpha1.ChainStepPhaseFailed))
		Expect(status(c, "b").Error).To(HavePrefix("when: "))
		Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhaseFailed))
		Expect(tasksFor("b")).To(BeEmpty())
	})

	// #314: a plan whose output later steps parse as JSON is re-run once with
	// a reminder when its reply has no parseable JSON block.
	planSteps := func() []aiv1alpha1.ChainStep {
		return []aiv1alpha1.ChainStep{
			{Name: "plan", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "plan {{ .Input }}", TimeoutSeconds: 120},
			{Name: "implement", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "implement {{ .Steps.plan.Output }}", TimeoutSeconds: 120,
				DependsOn: []string{"plan"}, When: `!json(steps.plan.output).split`},
			{Name: "split", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "sub-issue {{ .Item.number }}", TimeoutSeconds: 120,
				DependsOn: []string{"plan"}, When: `json(steps["plan"].output).split`,
				ForEach: &aiv1alpha1.ChainForEach{From: "steps.plan", Path: ".issues"}},
		}
	}

	It("retries a plan with no parseable JSON once, with a reminder, then continues", func() {
		start(planSteps())
		reconcileN(1)
		Expect(tasksFor("plan")).To(HaveLen(1))
		finishLast("plan", "Here is the plan:\n```json\n`{\"split\": false}`\n```")
		reconcileN(1)

		c := get()
		Expect(status(c, "plan").Phase).To(Equal(aiv1alpha1.ChainStepPhaseRunning))
		Expect(status(c, "plan").Reason).To(Equal("JSONRetry"))
		Expect(status(c, "plan").Retries).To(BeZero()) // not charged to the retry policy
		Expect(tasksFor("implement")).To(BeEmpty())
		plans := tasksFor("plan")
		Expect(plans).To(HaveLen(2))
		Expect(plans[1].Task).To(HavePrefix(plans[0].Task))
		Expect(plans[1].Task).To(ContainSubstring("no parseable JSON block (not valid JSON"))
		Expect(plans[1].Task).To(ContainSubstring("reply with ONLY the JSON block"))

		finishLast("plan", "```json\n{\"split\": false, \"issues\": [{\"number\": 1}]}\n```")
		reconcileN(1)
		c = get()
		Expect(status(c, "plan").Phase).To(Equal(aiv1alpha1.ChainStepPhaseSucceeded))
		Expect(status(c, "plan").Reason).To(BeEmpty())
		Expect(status(c, "implement").Phase).To(Equal(aiv1alpha1.ChainStepPhaseRunning))
		Expect(status(c, "split").Phase).To(Equal(aiv1alpha1.ChainStepPhaseSkipped))
		finishLast("implement", "https://github.com/acme/widgets/pull/1")
		reconcileN(1)
		Expect(get().Status.Phase).To(Equal(aiv1alpha1.ChainPhaseSucceeded))
	})

	It("fails the plan, not every branch, when the retry still has no JSON", func() {
		start(planSteps())
		reconcileN(1)
		finishLast("plan", "I could not decide.")
		reconcileN(1)
		Expect(tasksFor("plan")).To(HaveLen(2))
		finishLast("plan", "Still prose.")
		reconcileN(1)

		c := get()
		Expect(tasksFor("plan")).To(HaveLen(2)) // only one retry
		Expect(status(c, "plan").Phase).To(Equal(aiv1alpha1.ChainStepPhaseFailed))
		Expect(status(c, "plan").Error).To(HavePrefix("output has no parseable JSON block after one retry: "))
		Expect(status(c, "implement").Phase).To(Equal(aiv1alpha1.ChainStepPhaseSkipped))
		Expect(status(c, "split").Phase).To(Equal(aiv1alpha1.ChainStepPhaseSkipped))
		Expect(tasksFor("implement")).To(BeEmpty())
		Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhaseFailed))
	})

	It("does not retry a step whose output no later step reads as JSON", func() {
		start([]aiv1alpha1.ChainStep{
			{Name: "a", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "a", TimeoutSeconds: 120},
			{Name: "b", KnightRef: corev1.LocalObjectReference{Name: knight}, Task: "b {{ .Steps.a.Output }}", TimeoutSeconds: 120,
				DependsOn: []string{"a"}, When: `steps.a.output.contains("go")`},
		})
		reconcileN(1)
		finishLast("a", "go, not json")
		reconcileN(1)
		Expect(tasksFor("a")).To(HaveLen(1))
		Expect(status(get(), "a").Phase).To(Equal(aiv1alpha1.ChainStepPhaseSucceeded))
		Expect(tasksFor("b")).To(HaveLen(1))
	})

	It("runs the review/fix loop in the coder's session until the review passes, then merges", func() {
		start(pipeline(3, ""))
		reconcileN(1)
		finishLast("implement", "PR https://example/pr/1")
		reconcileN(1)
		finishLast("review", "Verdict: needs changes — add tests")
		reconcileN(1)

		c := get()
		loop := status(c, "fixloop")
		Expect(loop.Phase).To(Equal(aiv1alpha1.ChainStepPhaseRunning))
		Expect(loop.Loop.Iteration).To(BeEquivalentTo(1))
		fix := tasksFor("fixloop.fix")
		Expect(fix).To(HaveLen(1))
		Expect(fix[0].Task).To(Equal("round 1/3 fix: Verdict: needs changes — add tests"))
		Expect(fix[0].RunID).To(Equal(c.Status.RunID), "same runId as implement → same agent session")
		Expect(fix[0].RunID).To(Equal(tasksFor("implement")[0].RunID))
		Expect(fix[0].Model).To(Equal("opus"))

		finishLast("fixloop.fix", "pushed tests")
		reconcileN(1)
		Expect(tasksFor("fixloop.rereview")).To(HaveLen(1))
		Expect(tasksFor("fixloop.rereview")[0].Task).To(Equal("re-review after: pushed tests"))
		finishLast("fixloop.rereview", "Verdict: needs changes — tests flaky")
		reconcileN(1)

		// Round 2: the fix sees the latest re-review, not the first review.
		fix = tasksFor("fixloop.fix")
		Expect(fix).To(HaveLen(2))
		Expect(fix[1].Task).To(Equal("round 2/3 fix: Verdict: needs changes — tests flaky"))
		finishLast("fixloop.fix", "deflaked")
		reconcileN(1)
		// The verdict sits past the status truncation; until reads the full output.
		finishLast("fixloop.rereview", strings.Repeat("LGTM ", 1000)+"\nVerdict: ship it")
		reconcileN(1)

		c = get()
		loop = status(c, "fixloop")
		Expect(loop.Phase).To(Equal(aiv1alpha1.ChainStepPhaseSucceeded))
		Expect(loop.Loop.Outcome).To(Equal(aiv1alpha1.ChainLoopConverged))
		Expect(loop.Loop.Iteration).To(BeEquivalentTo(2))
		Expect(loop.Loop.Steps).To(HaveLen(2), "status keeps one entry per body step")
		Expect(len(loop.Loop.Steps[1].Output)).To(BeNumerically("<", 4200))
		Expect(status(c, "escalate").Phase).To(Equal(aiv1alpha1.ChainStepPhaseSkipped))
		Expect(tasksFor("merge")).To(HaveLen(1))
		Expect(tasksFor("escalate")).To(BeEmpty())

		finishLast("merge", "queued")
		reconcileN(1)
		Expect(get().Status.Phase).To(Equal(aiv1alpha1.ChainPhaseSucceeded))
	})

	It("runs zero rounds when the first review already passes", func() {
		start(pipeline(3, ""))
		reconcileN(1)
		finishLast("implement", "PR 2")
		reconcileN(1)
		finishLast("review", "Verdict: ship it")
		reconcileN(1)
		c := get()
		loop := status(c, "fixloop")
		Expect(loop.Phase).To(Equal(aiv1alpha1.ChainStepPhaseSucceeded))
		Expect(loop.Loop.Outcome).To(Equal(aiv1alpha1.ChainLoopConverged))
		Expect(loop.Loop.Iteration).To(BeEquivalentTo(0))
		Expect(loop.Output).To(Equal("Verdict: ship it"))
		Expect(tasksFor("fixloop.fix")).To(BeEmpty())
		Expect(tasksFor("merge")).To(HaveLen(1))
	})

	It("onExhausted=continue branches to escalation and ends PartiallySucceeded", func() {
		start(pipeline(1, aiv1alpha1.ChainLoopExhaustedContinue))
		reconcileN(1)
		finishLast("implement", "PR 3")
		reconcileN(1)
		finishLast("review", "needs changes")
		reconcileN(1)
		finishLast("fixloop.fix", "tried")
		reconcileN(1)
		finishLast("fixloop.rereview", "still needs changes")
		reconcileN(1)

		c := get()
		loop := status(c, "fixloop")
		Expect(loop.Phase).To(Equal(aiv1alpha1.ChainStepPhaseSucceeded))
		Expect(loop.Loop.Outcome).To(Equal(aiv1alpha1.ChainLoopExhausted))
		Expect(loop.Output).To(Equal("still needs changes"))
		Expect(tasksFor("fixloop.fix")).To(HaveLen(1), "maxIterations bounds the rounds")
		Expect(status(c, "merge").Phase).To(Equal(aiv1alpha1.ChainStepPhaseSkipped))
		Expect(tasksFor("escalate")).To(HaveLen(1))
		Expect(tasksFor("escalate")[0].Task).To(Equal("label needs-human after 1 rounds"))
		finishLast("escalate", "labelled")
		reconcileN(1)
		Expect(get().Status.Phase).To(Equal(aiv1alpha1.ChainPhasePartiallySucceeded))
	})

	It("onExhausted=fail fails the chain; retry-failed grants more rounds and continues", func() {
		start(pipeline(1, ""))
		reconcileN(1)
		finishLast("implement", "PR 4")
		reconcileN(1)
		finishLast("review", "needs changes")
		reconcileN(1)
		finishLast("fixloop.fix", "tried")
		reconcileN(1)
		finishLast("fixloop.rereview", "still needs changes")
		reconcileN(1)

		c := get()
		Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhaseFailed))
		loop := status(c, "fixloop")
		Expect(loop.Phase).To(Equal(aiv1alpha1.ChainStepPhaseFailed))
		Expect(loop.Loop.Outcome).To(Equal(aiv1alpha1.ChainLoopExhausted))
		Expect(loop.Error).To(ContainSubstring("exhausted"))
		Expect(status(c, "merge").Reason).To(Equal("ChainFailed"))

		annotate(aiv1alpha1.AnnotationChainRetryFailed, "r1")
		reconcileN(2)
		c = get()
		loop = status(c, "fixloop")
		Expect(loop.Loop.MaxIterations).To(BeEquivalentTo(2))
		Expect(loop.Loop.Iteration).To(BeEquivalentTo(2))
		fix := tasksFor("fixloop.fix")
		Expect(fix).To(HaveLen(2))
		Expect(fix[1].Task).To(Equal("round 2/2 fix: still needs changes"), "continues from the last review")
		finishLast("fixloop.fix", "fixed")
		reconcileN(1)
		finishLast("fixloop.rereview", "Verdict: ship it")
		reconcileN(1)
		finishLast("merge", "queued")
		reconcileN(1)
		c = get()
		Expect(status(c, "fixloop").Loop.Outcome).To(Equal(aiv1alpha1.ChainLoopConverged))
		Expect(c.Status.Phase).To(Equal(aiv1alpha1.ChainPhaseSucceeded))
	})

	It("a failed body task fails the loop, and retry-failed re-runs only that body step", func() {
		start(pipeline(3, ""))
		reconcileN(1)
		finishLast("implement", "PR 5")
		reconcileN(1)
		finishLast("review", "needs changes")
		reconcileN(1)
		finishLast("fixloop.fix", "fixed")
		reconcileN(1)
		finish(tasksFor("fixloop.rereview")[0], "", "reviewer crashed")
		reconcileN(1)

		c := get()
		loop := status(c, "fixloop")
		Expect(loop.Phase).To(Equal(aiv1alpha1.ChainStepPhaseFailed))
		Expect(loop.Loop.Outcome).To(Equal(aiv1alpha1.ChainLoopFailed))
		Expect(loop.Error).To(ContainSubstring("rereview failed in round 1: reviewer crashed"))

		annotate(aiv1alpha1.AnnotationChainRetryFailed, "r1")
		reconcileN(2)
		Expect(tasksFor("fixloop.fix")).To(HaveLen(1), "the finished fix is not redone")
		Expect(tasksFor("fixloop.rereview")).To(HaveLen(2))
		Expect(tasksFor("fixloop.rereview")[1].Task).To(Equal("re-review after: fixed"))
	})

	It("marks the chain invalid when a loop step sets knightRef", func() {
		steps := pipeline(2, "")
		steps[2].KnightRef.Name = knight
		Expect(k8sClient.Create(ctx, &aiv1alpha1.Chain{
			ObjectMeta: metav1.ObjectMeta{Name: chainName, Namespace: namespace},
			Spec:       aiv1alpha1.ChainSpec{RoundTableRef: aiv1alpha1.LocalRef(rtName), TimeoutSeconds: 600, Steps: steps},
		})).To(Succeed())
		_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(err).To(HaveOccurred())
		cond := meta.FindStatusCondition(get().Status.Conditions, aiv1alpha1.ConditionChainValid)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Reason).To(Equal(aiv1alpha1.ReasonInvalidLoop))
	})
})

func TestStructuredVarsProseFallbackInConditions(t *testing.T) {
	r := &ChainReconciler{}
	chain := &aiv1alpha1.Chain{}
	prs, verdict := r.structuredVars(chain, "c.review", "PR #245 reviewed.\nVerdict: ship_it", true)
	vars := map[string]any{"input": "", "steps": map[string]any{
		"review": map[string]any{"phase": "Succeeded", "prs": prs, "verdict": verdict},
		"idle":   map[string]any{"phase": "Pending", "prs": []any{}, "verdict": ""},
	}}
	for expr, want := range map[string]bool{
		`steps.review.verdict == "ship_it"`:                                 true,
		`steps.review.prs.size() == 1 && steps.review.prs[0].number == 245`: true,
		`steps.idle.verdict == "" && size(steps.idle.prs) == 0`:             true,
	} {
		got, err := evalCondition(expr, vars)
		if err != nil || got != want {
			t.Errorf("%s: got %v, %v", expr, got, err)
		}
	}
	// Not loaded (step not mentioned/succeeded): empty, no prose parsing.
	if prs, v := r.structuredVars(chain, "c.x", "Verdict: ship_it", false); v != "" || len(prs) != 0 {
		t.Fatalf("unloaded step leaked %v %q", prs, v)
	}
}
