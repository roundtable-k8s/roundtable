package mission

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func outputStep(name, task string, deps ...string) aiv1alpha1.ChainStep {
	return aiv1alpha1.ChainStep{Name: name, KnightRef: corev1.LocalObjectReference{Name: "galahad"}, Task: task, DependsOn: deps}
}

func outputPlan(chains ...PlannerChain) *PlannerOutput {
	for i := range chains {
		chains[i].Phase = chainPhaseActive
	}
	return &PlannerOutput{
		PlanVersion: "v1alpha1",
		Metadata:    PlannerMetadata{Objective: "do"},
		Knights:     []PlannerKnight{{Name: "galahad"}},
		Chains:      chains,
	}
}

func TestStepOutputRefs(t *testing.T) {
	task := `Report on {{ .Steps.survey.Output }} and {{index .Steps "needs-detail" "Output"}}.
Ignore .Steps.not_a_template and {{ .Input }}; {{ if .Steps.survey.Error }}x{{ end }}`
	got := stepOutputRefs(task)
	if want := []string{"survey", "needs-detail"}; !slices.Equal(got, want) {
		t.Fatalf("refs = %v, want %v", got, want)
	}
}

// The #531 run: two gathering chains and a report chain that reads their
// output. Chains start together and cannot read each other's output, so the
// plan must be rejected with an error that says how to fix it.
func TestValidatePlanRejectsCrossChainOutputRef(t *testing.T) {
	plan := outputPlan(
		PlannerChain{Name: "milestone-survey", Steps: []aiv1alpha1.ChainStep{outputStep("survey", "list milestones")}},
		PlannerChain{Name: "needs-human-detail", Steps: []aiv1alpha1.ChainStep{outputStep("needs_detail", "list needs-human")}},
		PlannerChain{Name: "write-owner-report", Steps: []aiv1alpha1.ChainStep{
			outputStep("report", "Write from {{ .Steps.survey.Output }} and {{ .Steps.needs_detail.Output }}"),
		}},
	)
	err := existingChainsPlanner(t, nil).validatePlan(context.Background(), existingChainsMission(), plan)
	if err == nil {
		t.Fatal("expected a cross-chain output reference to be rejected")
	}
	for _, want := range []string{`"write-owner-report"`, `"survey"`, `in chain "milestone-survey"`, "same chain"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

func TestValidatePlanRejectsUnreachableOutputRefs(t *testing.T) {
	cases := map[string][]aiv1alpha1.ChainStep{
		"not in dependsOn": {
			outputStep("gather", "gather"),
			outputStep("report", "from {{ .Steps.gather.Output }}"),
		},
		"unknown step": {
			outputStep("report", "from {{ .Steps.nowhere.Output }}"),
		},
		"own output": {
			outputStep("report", "from {{ .Steps.report.Output }}"),
		},
		"index form not in dependsOn": {
			outputStep("gather", "gather"),
			outputStep("report", `from {{ index .Steps "gather" "Output" }}`),
		},
	}
	for name, steps := range cases {
		t.Run(name, func(t *testing.T) {
			plan := outputPlan(PlannerChain{Name: "c", Steps: steps})
			err := existingChainsPlanner(t, nil).validatePlan(context.Background(), existingChainsMission(), plan)
			if err == nil || !strings.Contains(err.Error(), "task reads the output of step") {
				t.Fatalf("expected output reference error, got %v", err)
			}
		})
	}
}

// The fix the prompt asks for: gathering and report in one chain, the report
// depending on the gatherers (directly or transitively, hyphenated names
// included). It validates and applyPlan keeps the references consistent.
func TestValidatePlanAcceptsSameChainOutputRefs(t *testing.T) {
	plan := outputPlan(PlannerChain{Name: "backlog-review", Steps: []aiv1alpha1.ChainStep{
		outputStep("milestone-survey", "list milestones"),
		outputStep("needs_detail", "detail {{ .Steps.milestone_survey.Output }}", "milestone-survey"),
		outputStep("report", "Write from {{ .Steps.milestone-survey.Output }} and {{ .Steps.needs_detail.Output }}", "needs_detail"),
	}})
	ctx := context.Background()
	p := existingChainsPlanner(t, nil)
	m := existingChainsMission()
	if err := p.validatePlan(ctx, m, plan); err != nil {
		t.Fatalf("validatePlan: %v", err)
	}
	if err := p.applyPlan(ctx, m, plan); err != nil {
		t.Fatalf("applyPlan: %v", err)
	}
	steps := m.Spec.GeneratedChains[0].Steps
	report := steps[2]
	if report.Task != "Write from {{ .Steps.milestone_survey.Output }} and {{ .Steps.needs_detail.Output }}" {
		t.Fatalf("report task = %q", report.Task)
	}
	if !slices.Equal(steps[1].DependsOn, []string{"milestone_survey"}) {
		t.Fatalf("needs_detail dependsOn = %v", steps[1].DependsOn)
	}
}

func TestPlanningPromptSaysOutputStaysInChain(t *testing.T) {
	p := existingChainsPlanner(t, nil)
	prompt := p.buildPlanningPrompt(context.Background(), existingChainsMission())
	for _, want := range []string{"{{ .Steps.step_name.Output }}", "ONLY between steps of the SAME chain", "into ONE chain with dependsOn steps", "[truncated"} {
		if !strings.Contains(prompt, want) {
			t.Errorf("prompt does not contain %q", want)
		}
	}
}

// stubSamplePlan is the plan in test/e2e/stubknight/plan_test.go (samplePlan),
// which the e2e stub returns for planning tasks. It must stay a plan the
// planner accepts, including its knights[] entry.
const stubSamplePlan = `{"planVersion":"v1alpha1","metadata":{"objective":"Summarise the release"},` +
	`"knights":[{"name":"stub","ephemeral":false}],` +
	`"chains":[{"name":"summarise","steps":[{"name":"gather","knightRef":{"name":"stub"},"task":"Gather facts."}]}]}`

func TestValidatePlanAcceptsStubSamplePlan(t *testing.T) {
	var plan PlannerOutput
	if err := json.Unmarshal([]byte(stubSamplePlan), &plan); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// The plan's non-ephemeral knight "stub" must exist on the table.
	stub := &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{Name: "stub", Namespace: "default", Labels: map[string]string{"ai.roundtable.io/table": "dev"}},
		Spec:       aiv1alpha1.KnightSpec{Domain: "dev", Model: "sonnet"},
	}
	m := existingChainsMission()
	if err := existingChainsPlanner(t, nil, stub).validatePlan(context.Background(), m, &plan); err != nil {
		t.Fatalf("validatePlan rejected the e2e stub sample plan: %v", err)
	}
}
