package controller

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// TestDependentStepTaskContainsUpstreamOutput covers #533: a step that
// depends on another step of its chain gets that step's output in its task,
// bounded in size and with a marker when the output was cut.
func TestDependentStepTaskContainsUpstreamOutput(t *testing.T) {
	const task = "Write the owner report from:\n{{ .Steps.survey.Output }}\n---\n{{ .Steps.needs_detail.Output }}"
	r := &ChainReconciler{}
	long := "BEGIN-DETAIL " + strings.Repeat("x", 3*stepOutputStatusLimit) + " END-DETAIL"

	chain := &aiv1alpha1.Chain{
		ObjectMeta: metav1.ObjectMeta{Name: "mission-m-backlog-review", Namespace: "default"},
		Spec: aiv1alpha1.ChainSpec{Steps: []aiv1alpha1.ChainStep{
			{Name: "survey", Task: "survey"},
			{Name: "needs_detail", Task: "detail"},
			{Name: "report", Task: task, DependsOn: []string{"survey", "needs_detail"}},
		}},
		Status: aiv1alpha1.ChainStatus{StepStatuses: []aiv1alpha1.ChainStepStatus{
			// Outputs are stored the way applyStepSuccess stores them.
			{Name: "survey", Phase: aiv1alpha1.ChainStepPhaseSucceeded,
				Output: truncateStatusOutput("M9: 4 open issues", "mission-m-backlog-review.survey")},
			{Name: "needs_detail", Phase: aiv1alpha1.ChainStepPhaseSucceeded,
				Output: truncateStatusOutput(long, "mission-m-backlog-review.needs_detail")},
			{Name: "report", Phase: aiv1alpha1.ChainStepPhasePending},
		}},
	}

	got, err := r.renderTemplate(chain, task)
	if err != nil {
		t.Fatalf("renderTemplate: %v", err)
	}
	if strings.Contains(got, "<no value>") {
		t.Fatalf("rendered task has an empty reference: %q", got)
	}
	if !strings.Contains(got, "M9: 4 open issues") {
		t.Errorf("short upstream output missing from task: %q", got)
	}
	if strings.Contains(got, "M9: 4 open issues\n\n... [truncated") {
		t.Errorf("short output must not carry a truncation marker")
	}
	if !strings.Contains(got, "BEGIN-DETAIL") {
		t.Errorf("start of long upstream output missing from task")
	}
	if strings.Contains(got, "END-DETAIL") {
		t.Errorf("long upstream output was not cut")
	}
	if !strings.Contains(got, "... [truncated — full output in NATS KV bucket 'chain-outputs', key 'mission-m-backlog-review.needs_detail']") {
		t.Errorf("truncation marker missing from task: %q", got[len(got)-200:])
	}
	if limit := len(task) + 2*stepOutputStatusLimit + 400; len(got) > limit {
		t.Errorf("rendered task is %d bytes, want at most %d", len(got), limit)
	}
}
