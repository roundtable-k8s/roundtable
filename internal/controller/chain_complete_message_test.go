package controller

import (
	"context"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"strings"
	"testing"
	"unicode/utf8"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func TestFailedStepsMessage(t *testing.T) {
	step := func(n, e string) aiv1alpha1.ChainStepStatus {
		return aiv1alpha1.ChainStepStatus{Name: n, Error: e}
	}
	if got := failedStepsMessage(nil); got != "" {
		t.Errorf("no failures: got %q", got)
	}
	if got := failedStepsMessage([]aiv1alpha1.ChainStepStatus{step("build", "exit 1")}); got != "step build: exit 1" {
		t.Errorf("one: got %q", got)
	}
	four := []aiv1alpha1.ChainStepStatus{step("a", "x"), step("b", "y"), step("c", "z"), step("d", "w")}
	if got, want := failedStepsMessage(four), "step a: x; step b: y; step c: z; and 1 more"; got != want {
		t.Errorf("four: got %q want %q", got, want)
	}
	if got := failedStepsMessage([]aiv1alpha1.ChainStepStatus{step("a", "")}); got != "step a" {
		t.Errorf("empty error: got %q", got)
	}
	got := failedStepsMessage([]aiv1alpha1.ChainStepStatus{step("a", strings.Repeat("e", 500))})
	if want := "step a: " + strings.Repeat("e", 200) + "…"; got != want {
		t.Errorf("long error not truncated to 200: len=%d", len(got))
	}
	got = failedStepsMessage([]aiv1alpha1.ChainStepStatus{step("a", strings.Repeat("日本", 150))})
	if !utf8.ValidString(got) || utf8.RuneCountInString(got) != len("step a: ")+201 {
		t.Errorf("multi-byte truncation broke: %q", got)
	}
	if got := failedStepsMessage([]aiv1alpha1.ChainStepStatus{step("a", "line1\nline2")}); got != "step a: line1 line2" {
		t.Errorf("newlines: got %q", got)
	}
}

func TestFailedChainsMessage(t *testing.T) {
	s := k8sruntime.NewScheme()
	if err := aiv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	chain := &aiv1alpha1.Chain{ObjectMeta: metav1.ObjectMeta{Name: "m-build", Namespace: "ns"}}
	meta.SetStatusCondition(&chain.Status.Conditions, metav1.Condition{
		Type: aiv1alpha1.ConditionChainComplete, Status: metav1.ConditionTrue,
		Reason: aiv1alpha1.ReasonChainFailed, Message: "step compile: boom",
	})
	r := &MissionReconciler{Client: fake.NewClientBuilder().WithScheme(s).WithObjects(chain).Build()}
	m := &aiv1alpha1.Mission{ObjectMeta: metav1.ObjectMeta{Namespace: "ns"}}

	if got := r.failedChainsMessage(context.Background(), m); got != "One or more mission chains failed" {
		t.Errorf("no failed chains: got %q", got)
	}
	m.Status.ChainStatuses = []aiv1alpha1.MissionChainStatus{
		{Name: "ok", Phase: aiv1alpha1.ChainPhaseSucceeded},
		{Name: "build", ChainCRName: "m-build", Phase: aiv1alpha1.ChainPhaseFailed},
		{Name: "gone", ChainCRName: "missing", Phase: aiv1alpha1.ChainPhaseFailed},
	}
	want := "Mission chains failed: chain build: step compile: boom; chain gone"
	if got := r.failedChainsMessage(context.Background(), m); got != want {
		t.Errorf("got %q want %q", got, want)
	}
}
