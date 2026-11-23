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
	"bytes"
	"os"
	"slices"
	"strings"
	"testing"
	"text/template"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/yaml"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
)

// loadPRPipelineExample reads examples/chain-pr-pipeline.yaml strictly, so a
// field the CRD types don't know fails the test.
func loadPRPipelineExample(t *testing.T) *aiv1alpha1.Chain {
	t.Helper()
	chain, err := readPRPipelineExample()
	if err != nil {
		t.Fatal(err)
	}
	return chain
}

func readPRPipelineExample() (*aiv1alpha1.Chain, error) {
	data, err := os.ReadFile("../../examples/chain-pr-pipeline.yaml")
	if err != nil {
		return nil, err
	}
	var chain aiv1alpha1.Chain
	if err := yaml.UnmarshalStrict(data, &chain); err != nil {
		return nil, err
	}
	return &chain, nil
}

// The API server (envtest) accepts the example against the generated Chain
// CRD schema: enums, patterns, limits and loop/forEach shapes.
var _ = Describe("PR pipeline example", func() {
	It("is accepted by the Chain CRD", func() {
		chain, err := readPRPipelineExample()
		Expect(err).NotTo(HaveOccurred())
		chain.Namespace = "default"
		chain.Name = "pr-pipeline-example"
		chain.Spec.Suspend = true // validate only; never dispatch
		Expect(k8sClient.Create(ctx, chain)).To(Succeed())
		got := &aiv1alpha1.Chain{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: chain.Name, Namespace: chain.Namespace}, got)).To(Succeed())
		Expect(got.Spec.Steps).To(HaveLen(len(chain.Spec.Steps)))
		Expect(k8sClient.Delete(ctx, got)).To(Succeed())
	})
})

// TestPRPipelineExampleIsValid runs the example through the same validation
// the controller applies before a run (everything except the knight lookups).
func TestPRPipelineExampleIsValid(t *testing.T) {
	chain := loadPRPipelineExample(t)
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
	for _, s := range chain.Spec.Steps {
		if s.Loop == nil && s.Model == "" && s.KnightRef.Name != "rt-lead" {
			t.Errorf("coder step %q has no model alias", s.Name)
		}
		if s.Loop != nil {
			if s.Loop.MaxIterations != 3 || s.Loop.OnExhausted != aiv1alpha1.ChainLoopExhaustedContinue {
				t.Errorf("loop %q: want maxIterations 3, onExhausted continue", s.Name)
			}
		}
	}
}

// simulatePRPipeline walks the example in spec order (which is topological),
// evaluating every when against the statuses so far. Steps that run succeed;
// plan returns planOut and loops end with loopOutcome. It returns the steps
// that ran.
func simulatePRPipeline(t *testing.T, chain *aiv1alpha1.Chain, planOut string, loopOutcome aiv1alpha1.ChainLoopOutcome) []string {
	t.Helper()
	steps := map[string]any{}
	var ran []string
	for _, s := range chain.Spec.Steps {
		for _, d := range s.DependsOn {
			if _, ok := steps[d]; !ok {
				t.Fatalf("step %q runs before its dependency %q", s.Name, d)
			}
		}
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
				entry["outcome"], entry["iterations"] = string(loopOutcome), int64(2)
			}
		}
		steps[s.Name] = entry
	}
	return ran
}

func TestPRPipelineExampleRouting(t *testing.T) {
	chain := loadPRPipelineExample(t)
	plan := func(coder, model string, split bool) string {
		s := "false"
		if split {
			s = "true"
		}
		return "Plan below.\n```json\n{\"coder\": \"" + coder + "\", \"model\": \"" + model + "\", \"split\": " + s +
			", \"issues\": [{\"number\": 225, \"title\": \"PR pipeline\"}], \"reason\": \"r\"}\n```"
	}
	cases := map[string]struct {
		plan    string
		outcome aiv1alpha1.ChainLoopOutcome
		want    []string
	}{
		"single PR, fast alias, converged → merge": {
			plan: plan("rt-ui", "fast", false), outcome: aiv1alpha1.ChainLoopConverged,
			want: []string{"plan", "implement_rt_ui_fast", "review", "fixloop_rt_ui", "merge"},
		},
		"single PR, deep alias, exhausted → escalate": {
			plan: plan("rt-operator", "deep", false), outcome: aiv1alpha1.ChainLoopExhausted,
			want: []string{"plan", "implement_rt_operator_deep", "review", "fixloop_rt_operator", "escalate"},
		},
		"split into sub-issues → forEach": {
			plan: plan("rt-operator", "deep", true), outcome: aiv1alpha1.ChainLoopConverged,
			want: []string{"plan", "split_rt_operator", "review", "fixloop_rt_operator", "merge"},
		},
		"unknown coder → escalate without implementing": {
			plan: plan("rt-nobody", "deep", false), outcome: aiv1alpha1.ChainLoopConverged,
			want: []string{"plan", "escalate"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := simulatePRPipeline(t, chain, tc.plan, tc.outcome)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("ran %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPRPipelineExampleVerdict(t *testing.T) {
	chain := loadPRPipelineExample(t)
	var until string
	for _, s := range chain.Spec.Steps {
		if s.Loop != nil {
			until = s.Loop.Until
			break
		}
	}
	cases := map[string]bool{
		"PRs: x\nLooks good.\nVerdict: ship it":                                                               true,
		"PRs: x\nLooks good.\n**Verdict: Ship it**\n":                                                         true,
		"PRs: x\nMissing tests.\nVerdict: needs changes":                                                      false,
		"I was asked to end with \"Verdict: ship it\" or \"Verdict: needs changes\".\nVerdict: needs changes": false,
		"PRs: x\nVerdict: ship it once the tests pass":                                                        false,
	}
	for out, want := range cases {
		got, err := evalCondition(until, map[string]any{"last": map[string]any{"step": "review", "phase": "Succeeded", "output": out, "error": ""}})
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("until(%q) = %v, want %v", out, got, want)
		}
	}
}

// TestPRPipelineExampleTemplates renders the merge and escalate tasks the way
// the controller does, checking that only the loop that ran contributes.
func TestPRPipelineExampleTemplates(t *testing.T) {
	chain := loadPRPipelineExample(t)
	steps := map[string]map[string]string{}
	for _, s := range chain.Spec.Steps {
		steps[s.Name] = stepTemplateData(aiv1alpha1.ChainStepStatus{Phase: aiv1alpha1.ChainStepPhaseSkipped})
	}
	steps["review"] = stepTemplateData(aiv1alpha1.ChainStepStatus{Phase: aiv1alpha1.ChainStepPhaseSucceeded})
	steps["fixloop_rt_ui"] = stepTemplateData(aiv1alpha1.ChainStepStatus{
		Phase: aiv1alpha1.ChainStepPhaseSucceeded, Output: "PRs: https://github.com/o/r/pull/7\nVerdict: ship it",
		Loop: &aiv1alpha1.ChainLoopStatus{Iteration: 2, Outcome: aiv1alpha1.ChainLoopConverged},
	})
	render := func(name string) string {
		for _, s := range chain.Spec.Steps {
			if s.Name != name {
				continue
			}
			tmpl := template.Must(template.New(name).Parse(s.Task))
			var buf bytes.Buffer
			if err := tmpl.Execute(&buf, map[string]any{"Steps": steps, "Input": chain.Spec.Input}); err != nil {
				t.Fatal(err)
			}
			return buf.String()
		}
		t.Fatalf("no step %q", name)
		return ""
	}
	merge := render("merge")
	if !strings.Contains(merge, "Fix rounds: 2\n") || !strings.Contains(merge, "pull/7") || strings.Count(merge, "Final review:") != 1 {
		t.Fatalf("merge task:\n%s", merge)
	}
	if esc := render("escalate"); strings.Contains(esc, "fix rounds. Last review") || strings.Contains(esc, "No implementation ran") {
		t.Fatalf("escalate task mentions a loop or plan that didn't apply:\n%s", esc)
	}
}

// TestPRPipelineExampleMixedRuntimes checks the chain on a RoundTable whose lead
// runs pi-knight and whose coders run claude-knight (#244): lead steps set no
// model, so none resolves to a Claude alias on the pi lead (the controller's
// InvalidStepModel), and every coder step's alias resolves on claude-knight.
func TestPRPipelineExampleMixedRuntimes(t *testing.T) {
	chain := loadPRPipelineExample(t)
	aliases := map[string]string{"deep": "opus", "fast": "haiku"}
	const piImage, claudeImage = "ghcr.io/roundtable-k8s/pi-knight:latest", "ghcr.io/roundtable-k8s/claude-knight:latest"

	check := func(name, knight, model string) {
		image, lead := claudeImage, knight == "rt-lead"
		if lead {
			image = piImage
			if model != "" {
				t.Errorf("lead step %q sets model %q, want none (lead default)", name, model)
			}
		} else if model == "" {
			t.Errorf("coder step %q has no model alias", name)
		}
		if _, err := knightpkg.ResolveModel(model, aliases, image); err != nil {
			t.Errorf("step %q (knight %s): %v", name, knight, err)
		}
	}
	leadSteps := 0
	for _, s := range chain.Spec.Steps {
		if s.Loop == nil {
			check(s.Name, s.KnightRef.Name, s.Model)
		} else {
			for _, b := range s.Loop.Steps {
				check(b.Name, b.KnightRef.Name, b.Model)
			}
		}
		if s.KnightRef.Name == "rt-lead" {
			leadSteps++
		}
	}
	if leadSteps == 0 {
		t.Fatal("no lead steps found")
	}
	// The pre-fix behaviour: a lead alias resolving to a Claude model is rejected on pi.
	if _, err := knightpkg.ResolveModel("deep", aliases, piImage); err == nil {
		t.Fatal("expected a Claude alias to be rejected on a pi-knight lead")
	}
}
