package mission

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func tmplChain(name string, labels map[string]string, rt string) *aiv1alpha1.Chain {
	c := &aiv1alpha1.Chain{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: labels},
		Spec: aiv1alpha1.ChainSpec{
			Description: "desc of " + name,
			Input:       "example-" + name,
			Steps: []aiv1alpha1.ChainStep{
				{Name: "s1", KnightRef: corev1.LocalObjectReference{Name: "galahad"}, Task: "t"},
			},
		},
	}
	if rt != "" {
		c.Spec.RoundTableRef = aiv1alpha1.LocalRef(rt)
	}
	return c
}

func existingChainsPlanner(t *testing.T, templates map[string]aiv1alpha1.KnightSpec, objs ...client.Object) *Planner {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = aiv1alpha1.AddToScheme(scheme)
	rt := &aiv1alpha1.RoundTable{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "default"},
		Spec:       aiv1alpha1.RoundTableSpec{Description: "the dev table", KnightTemplates: templates},
	}
	knight := &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{Name: "galahad", Namespace: "default", Labels: map[string]string{"ai.roundtable.io/table": "dev"}},
		Spec:       aiv1alpha1.KnightSpec{Domain: "dev", Model: "sonnet"},
	}
	all := append([]client.Object{rt, knight}, objs...)
	return &Planner{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(all...).Build()}
}

func existingChainsMission() *aiv1alpha1.Mission {
	return &aiv1alpha1.Mission{
		ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "default"},
		Spec: aiv1alpha1.MissionSpec{
			Objective:       "do",
			MetaMission:     true,
			RecruitExisting: true,
			RoundTableRef:   aiv1alpha1.LocalRef("dev"),
			Planner:         &aiv1alpha1.MissionPlanner{},
		},
	}
}

func TestPromptListsTemplateChainsSkipsCopies(t *testing.T) {
	p := existingChainsPlanner(t, map[string]aiv1alpha1.KnightSpec{"base": {}},
		tmplChain("good", nil, "dev"),
		tmplChain("norefs", nil, ""),
		tmplChain("copy", map[string]string{aiv1alpha1.LabelMission: "x"}, "dev"),
		tmplChain("pickup", map[string]string{aiv1alpha1.LabelPickupRoundTable: "dev"}, "dev"),
		tmplChain("gen", map[string]string{"ai.roundtable.io/generated-by": "planner"}, "dev"),
		tmplChain("other", nil, "other-table"),
	)
	out := p.buildPlanningPrompt(context.Background(), existingChainsMission())
	for _, want := range []string{"- good: desc of good", "- norefs", "s1 (knight: galahad)", "example input: example-good",
		"the dev table", "model: sonnet", "only knights of that table", `"existingChains"`} {
		if !strings.Contains(out, want) {
			t.Errorf("prompt missing %q", want)
		}
	}
	for _, bad := range []string{"- copy", "- pickup", "- gen", "- other"} {
		if strings.Contains(out, bad) {
			t.Errorf("prompt should not list %q", bad)
		}
	}
}

func TestPromptChainCapAndTruncation(t *testing.T) {
	objs := make([]client.Object, 0, 35)
	for i := range 35 {
		c := tmplChain("c"+string(rune('a'+i/26))+string(rune('a'+i%26)), nil, "dev")
		c.Spec.Input = strings.Repeat("x", 600)
		objs = append(objs, c)
	}
	p := existingChainsPlanner(t, nil, objs...)
	out := p.buildPlanningPrompt(context.Background(), existingChainsMission())
	if n := strings.Count(out, "  steps: "); n != 30 {
		t.Errorf("listed %d chains, want 30", n)
	}
	if strings.Contains(out, strings.Repeat("x", 501)) {
		t.Error("input example not truncated to 500 chars")
	}
}

func TestEphemeralGuidelineDependsOnBase(t *testing.T) {
	ctx := context.Background()
	withBase := existingChainsPlanner(t, map[string]aiv1alpha1.KnightSpec{"base": {}})
	if !strings.Contains(withBase.buildPlanningPrompt(ctx, existingChainsMission()), `ALWAYS use templateName="base"`) {
		t.Error("expected base guideline")
	}
	other := existingChainsPlanner(t, map[string]aiv1alpha1.KnightSpec{"researcher": {}})
	out := other.buildPlanningPrompt(ctx, existingChainsMission())
	if strings.Contains(out, `ALWAYS use templateName="base"`) || !strings.Contains(out, "one of: researcher") {
		t.Errorf("expected template list guideline, got:\n%s", out)
	}
	none := existingChainsPlanner(t, nil)
	if !strings.Contains(none.buildPlanningPrompt(ctx, existingChainsMission()), "Ephemeral knights are not available") {
		t.Error("expected no-ephemeral guideline")
	}
}

func existingPlan(ecs ...PlannerExistingChain) *PlannerOutput {
	return &PlannerOutput{PlanVersion: "v1alpha1", Metadata: PlannerMetadata{Objective: "do"}, ExistingChains: ecs}
}

func TestValidateAndApplyExistingChains(t *testing.T) {
	ctx := context.Background()
	p := existingChainsPlanner(t, nil, tmplChain("good", nil, "dev"))
	m := existingChainsMission()

	plan := existingPlan(PlannerExistingChain{Name: "good", Input: "in", Phase: "Setup"})
	if err := p.validatePlan(ctx, m, plan); err != nil {
		t.Fatal(err)
	}
	if len(plan.Knights) != 1 || plan.Knights[0].Name != "galahad" || plan.Knights[0].Ephemeral {
		t.Fatalf("expected implicit recruit of galahad, got %+v", plan.Knights)
	}
	if err := p.applyPlan(ctx, m, plan); err != nil {
		t.Fatal(err)
	}
	if len(m.Spec.Chains) != 1 || m.Spec.Chains[0] != (aiv1alpha1.MissionChainRef{Name: "good", InputOverride: "in", Phase: "Setup"}) {
		t.Fatalf("spec.chains = %+v", m.Spec.Chains)
	}
	if len(m.Spec.GeneratedKnights) != 1 || m.Spec.GeneratedKnights[0].Ephemeral {
		t.Fatalf("generatedKnights = %+v", m.Spec.GeneratedKnights)
	}
}

func TestValidateExistingChainsNoDuplicateRecruit(t *testing.T) {
	p := existingChainsPlanner(t, nil, tmplChain("good", nil, "dev"))
	plan := existingPlan(PlannerExistingChain{Name: "good"})
	plan.Knights = []PlannerKnight{{Name: "galahad"}}
	if err := p.validatePlan(context.Background(), existingChainsMission(), plan); err != nil {
		t.Fatal(err)
	}
	if len(plan.Knights) != 1 {
		t.Fatalf("knights = %+v", plan.Knights)
	}
}

func TestValidateExistingChainsRejects(t *testing.T) {
	ctx := context.Background()
	p := existingChainsPlanner(t, nil,
		tmplChain("good", nil, "dev"),
		tmplChain("copy", map[string]string{aiv1alpha1.LabelMission: "x"}, "dev"),
	)
	for name, ec := range map[string]PlannerExistingChain{
		"unknown":  {Name: "nope"},
		"copy":     {Name: "copy"},
		"badphase": {Name: "good", Phase: "Never"},
		"noname":   {},
	} {
		if err := p.validatePlan(ctx, existingChainsMission(), existingPlan(ec)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	// name colliding with a generated chain
	plan := existingPlan(PlannerExistingChain{Name: "good"})
	plan.Knights = []PlannerKnight{{Name: "galahad"}}
	plan.Chains = []PlannerChain{{Name: "good", Steps: []aiv1alpha1.ChainStep{
		{Name: "s", KnightRef: corev1.LocalObjectReference{Name: "galahad"}, Task: "t"}}}}
	if err := p.validatePlan(ctx, existingChainsMission(), plan); err == nil {
		t.Error("expected duplicate chain name error")
	}
}

func TestValidateMaxChainsCountsBothKinds(t *testing.T) {
	p := existingChainsPlanner(t, nil, tmplChain("good", nil, "dev"))
	m := existingChainsMission()
	m.Spec.Planner.MaxChains = 1
	plan := existingPlan(PlannerExistingChain{Name: "good"})
	plan.Knights = []PlannerKnight{{Name: "galahad"}}
	plan.Chains = []PlannerChain{{Name: "gen", Steps: []aiv1alpha1.ChainStep{
		{Name: "s", KnightRef: corev1.LocalObjectReference{Name: "galahad"}, Task: "t"}}}}
	err := p.validatePlan(context.Background(), m, plan)
	if err == nil || !strings.Contains(err.Error(), "too many chains: 2 > 1") {
		t.Fatalf("err = %v", err)
	}
}
