package controller

import (
	"context"
	"strings"
	"testing"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func structuredChain(task string) *aiv1alpha1.Chain {
	return &aiv1alpha1.Chain{
		Spec: aiv1alpha1.ChainSpec{Steps: []aiv1alpha1.ChainStep{
			{Name: "implement", Task: "x"}, {Name: "none", Task: "x"}, {Name: "next", Task: task},
		}},
		Status: aiv1alpha1.ChainStatus{StepStatuses: []aiv1alpha1.ChainStepStatus{
			{Name: "implement", Phase: aiv1alpha1.ChainStepPhaseSucceeded,
				Output: "Done.\nPRs: https://github.com/o/r/pull/7 https://github.com/o/s/pull/9\nVerdict: ship it"},
			{Name: "none", Phase: aiv1alpha1.ChainStepPhaseSucceeded, Output: "nothing to report"},
			{Name: "next", Phase: aiv1alpha1.ChainStepPhasePending},
		}},
	}
}

func TestRenderTemplateStructuredFields(t *testing.T) {
	r := &ChainReconciler{}
	task := `urls={{ prURLs .Steps.implement.PRs }}|step={{ prURLs .Steps.implement }}|verdict={{ .Steps.implement.Verdict }}|` +
		`{{ range .Steps.implement.PRs }}[{{ .Repo }}#{{ .Number }} {{ .URL }}]{{ end }}`
	got, err := r.renderTemplate(context.Background(), structuredChain(task), task)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"urls=https://github.com/o/r/pull/7 https://github.com/o/s/pull/9|",
		"step=https://github.com/o/r/pull/7 https://github.com/o/s/pull/9|",
		"verdict=ship_it|", "[o/r#7 https://github.com/o/r/pull/7][o/s#9 https://github.com/o/s/pull/9]",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in %q", want, got)
		}
	}
}

func TestRenderTemplateStructuredEmpty(t *testing.T) {
	r := &ChainReconciler{}
	task := `[{{ prURLs .Steps.none.PRs }}][{{ .Steps.none.Verdict }}][{{ len .Steps.none.PRs }}][{{ prURLs .Steps.next.PRs }}][{{ .Steps.next.Verdict }}]`
	got, err := r.renderTemplate(context.Background(), structuredChain(task), task)
	if err != nil {
		t.Fatal(err)
	}
	if got != "[][][0][][]" {
		t.Errorf("got %q", got)
	}
}

func TestTemplatePRsMissingFields(t *testing.T) {
	got := templatePRs([]any{
		map[string]any{"number": int64(3)},
		map[string]any{"url": 5, "repo": nil},
		"not a map",
	})
	if len(got) != 3 || got[0] != (templatePR{Number: 3}) || got[1] != (templatePR{}) || got[2] != (templatePR{}) {
		t.Errorf("got %+v", got)
	}
}
