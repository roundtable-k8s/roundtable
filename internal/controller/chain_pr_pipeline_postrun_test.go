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
	"os"
	"slices"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/yaml"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// examples/chain-pr-pipeline-postrun.yaml is the PR pipeline rendered with
// prPipeline.postRun: chain (#538): verify, close_parent and handback steps do
// the post-run work the operator does for builtin tables.

const postRunExample = "../../examples/chain-pr-pipeline-postrun.yaml"

// postRunSteps are the steps prPipeline.postRun: chain adds.
var postRunSteps = []string{"verify", "close_parent", "handback"}

func readPRPipelinePostRunExample() (*aiv1alpha1.Chain, error) {
	data, err := os.ReadFile(postRunExample)
	if err != nil {
		return nil, err
	}
	var chain aiv1alpha1.Chain
	if err := yaml.UnmarshalStrict(data, &chain); err != nil {
		return nil, err
	}
	return &chain, nil
}

func loadPRPipelinePostRunExample(t *testing.T) *aiv1alpha1.Chain {
	t.Helper()
	chain, err := readPRPipelinePostRunExample()
	if err != nil {
		t.Fatal(err)
	}
	return chain
}

var _ = Describe("PR pipeline post-run example", func() {
	It("is accepted by the Chain CRD", func() {
		chain, err := readPRPipelinePostRunExample()
		Expect(err).NotTo(HaveOccurred())
		chain.Namespace = "default"
		chain.Name = "pr-pipeline-postrun-example"
		chain.Spec.Suspend = true // validate only; never dispatch
		Expect(k8sClient.Create(ctx, chain)).To(Succeed())
		got := &aiv1alpha1.Chain{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: chain.Name, Namespace: chain.Namespace}, got)).To(Succeed())
		Expect(got.Spec.Steps).To(HaveLen(len(chain.Spec.Steps)))
		Expect(k8sClient.Delete(ctx, got)).To(Succeed())
	})
})

func TestPRPipelinePostRunExampleIsValid(t *testing.T) {
	chain := loadPRPipelinePostRunExample(t)
	if err := (&ChainReconciler{}).validateDAG(chain); err != nil {
		t.Fatal(err)
	}
	if err := validateForEach(chain); err != nil {
		t.Fatal(err)
	}
	if reason, err := validateConditionsAndLoops(chain); err != nil {
		t.Fatalf("%s: %v", reason, err)
	}
	if err := (&ChainReconciler{}).validateTemplates(chain); err != nil {
		t.Fatal(err)
	}
	last := chain.Spec.Steps[len(chain.Spec.Steps)-1]
	if last.Name != "handback" {
		t.Fatalf("last step is %q, want handback", last.Name)
	}
	for _, s := range chain.Spec.Steps {
		// A failure must reach handback, and a failed handback must fail
		// the run so the operator releases the issue.
		if want := s.Name != "handback"; s.ContinueOnFailure != want {
			t.Errorf("step %q: continueOnFailure = %v, want %v", s.Name, s.ContinueOnFailure, want)
		}
		if !slices.Contains(postRunSteps, s.Name) {
			continue
		}
		if s.Loop == nil || s.Loop.MaxIterations != 1 || s.Loop.OnExhausted == aiv1alpha1.ChainLoopExhaustedContinue {
			t.Errorf("step %q: want a one-round loop that fails when exhausted", s.Name)
		}
	}
}

// TestPRPipelinePostRunScriptCheck: a post-run step only succeeds on its
// script's "ok" line, so a missing script or a failed check fails it.
func TestPRPipelinePostRunScriptCheck(t *testing.T) {
	chain := loadPRPipelinePostRunExample(t)
	byName := map[string]aiv1alpha1.ChainStep{}
	for _, s := range chain.Spec.Steps {
		byName[s.Name] = s
	}
	cases := []struct {
		step, out string
		want      bool
	}{
		{"verify", "rt-verify: ok acme/w#7 queued", true},
		{"verify", "rt-verify: missing script /skills/project/pr-postrun/scripts/rt-verify", false},
		{"verify", "rt-verify: needs-human acme/w#7 is closed", false},
		{"verify", "All PRs are merged.", false},
		{"close_parent", "rt-close-parent: ok closed acme/w#319", true},
		{"close_parent", "rt-close-parent: missing script x", false},
		{"handback", "rt-handback: ok labelled acme/w#314", true},
		{"handback", "rt-handback: missing script x", false},
	}
	for _, tc := range cases {
		s := byName[tc.step]
		if s.Loop == nil {
			t.Fatalf("step %q is not a loop", tc.step)
		}
		got, err := evalCondition(s.Loop.Until, map[string]any{"last": map[string]any{"step": "run", "phase": "Succeeded", "output": tc.out, "error": ""}})
		if err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Errorf("%s until(%q) = %v, want %v", tc.step, tc.out, got, tc.want)
		}
	}
}

// simulatePostRunPipeline walks the example in spec order like
// simulatePRPipeline, but the steps in failed fail (plan with prose output)
// and the post-run loops converge.
func simulatePostRunPipeline(t *testing.T, chain *aiv1alpha1.Chain, planOut string, outcome aiv1alpha1.ChainLoopOutcome, failed ...string) []string {
	t.Helper()
	steps := map[string]any{}
	var ran []string
	for _, s := range chain.Spec.Steps {
		run := true
		if s.When != "" {
			var err error
			run, err = evalCondition(s.When, map[string]any{"input": chain.Spec.Input, "steps": steps})
			if err != nil {
				t.Fatalf("step %q when: %v", s.Name, err)
			}
		}
		entry := map[string]any{"name": s.Name, "phase": "Skipped", "output": "", "error": "",
			"reason": stepReasonWhenFalse, "outcome": "", "iterations": int64(0)}
		if run {
			ran = append(ran, s.Name)
			entry["phase"], entry["reason"], entry["output"] = "Succeeded", "", "PRs: https://github.com/o/r/pull/1\nok"
			if s.Name == "plan" {
				entry["output"] = planOut
			}
			if s.Loop != nil {
				entry["outcome"], entry["iterations"] = string(aiv1alpha1.ChainLoopConverged), int64(1)
				if strings.HasPrefix(s.Name, "fixloop_") {
					entry["outcome"], entry["iterations"] = string(outcome), int64(2)
				}
			}
			if slices.Contains(failed, s.Name) {
				entry["phase"], entry["error"], entry["outcome"] = "Failed", "boom", ""
				if s.Name == "plan" {
					entry["output"] = "no JSON here"
				}
			}
		}
		steps[s.Name] = entry
	}
	return ran
}

func TestPRPipelinePostRunExampleRouting(t *testing.T) {
	chain := loadPRPipelinePostRunExample(t)
	plan := func(split bool) string {
		s := "false"
		if split {
			s = "true"
		}
		return "```json\n{\"coder\": \"rt-ui\", \"model\": \"fast\", \"split\": " + s +
			", \"issues\": [{\"number\": 225, \"title\": \"t\"}], \"reason\": \"r\"}\n```"
	}
	impl := []string{"plan", "implement_rt_ui_fast", "review", "fixloop_rt_ui"}
	split := []string{"plan", "split_rt_ui", "review", "fixloop_rt_ui"}
	with := func(base []string, more ...string) []string { return append(slices.Clone(base), more...) }
	converged, exhausted := aiv1alpha1.ChainLoopConverged, aiv1alpha1.ChainLoopExhausted
	cases := map[string]struct {
		plan    string
		outcome aiv1alpha1.ChainLoopOutcome
		failed  []string
		want    []string
	}{
		"converged → merge → verify":              {plan(false), converged, nil, with(impl, "merge", "verify")},
		"split → merge → verify → close_parent":   {plan(true), converged, nil, with(split, "merge", "verify", "close_parent")},
		"exhausted → escalate, no handback":       {plan(false), exhausted, nil, with(impl, "escalate")},
		"unknown coder → escalate":                {strings.Replace(plan(false), "rt-ui", "rt-nobody", 1), converged, nil, []string{"plan", "escalate"}},
		"plan fails → handback only":              {plan(false), converged, []string{"plan"}, []string{"plan", "handback"}},
		"implement fails → handback, no escalate": {plan(false), converged, []string{"implement_rt_ui_fast"}, []string{"plan", "implement_rt_ui_fast", "handback"}},
		"fix loop fails → handback":               {plan(false), converged, []string{"fixloop_rt_ui"}, with(impl, "handback")},
		"merge fails → handback, no verify":       {plan(false), converged, []string{"merge"}, with(impl, "merge", "handback")},
		"verify fails → handback":                 {plan(false), converged, []string{"verify"}, with(impl, "merge", "verify", "handback")},
		"close_parent fails → handback":           {plan(true), converged, []string{"close_parent"}, with(split, "merge", "verify", "close_parent", "handback")},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := simulatePostRunPipeline(t, chain, tc.plan, tc.outcome, tc.failed...)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("ran %v, want %v", got, tc.want)
			}
		})
	}
}

// TestPRPipelinePostRunHandbackTask renders the handback task: it lists only
// the steps that failed, with their errors.
func TestPRPipelinePostRunHandbackTask(t *testing.T) {
	chain := loadPRPipelinePostRunExample(t)
	var task string
	for _, s := range chain.Spec.Steps {
		if s.Name == "handback" {
			task = s.Loop.Steps[0].Task
		}
	}
	chain.Status.StepStatuses = []aiv1alpha1.ChainStepStatus{
		{Name: "plan", Phase: aiv1alpha1.ChainStepPhaseSucceeded, Output: "{}"},
		{Name: "merge", Phase: aiv1alpha1.ChainStepPhaseSucceeded, Output: "queued"},
		{Name: "verify", Phase: aiv1alpha1.ChainStepPhaseFailed, Error: "loop exhausted"},
		{Name: "close_parent", Phase: aiv1alpha1.ChainStepPhaseSkipped},
	}
	out, err := (&ChainReconciler{}).renderTemplate(chain, task)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "- verify: loop exhausted") {
		t.Errorf("handback task does not name the failed verify step:\n%s", out)
	}
	for _, s := range []string{"- plan:", "- merge:", "- close_parent:"} {
		if strings.Contains(out, s) {
			t.Errorf("handback task names %q, which did not fail:\n%s", s, out)
		}
	}
}
