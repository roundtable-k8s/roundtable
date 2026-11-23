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
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/github"
)

func ts(min int) *time.Time {
	t := time.Date(2026, 11, 23, 12, min, 0, 0, time.UTC)
	return &t
}

func actionsCheck(id int64, name, conclusion string, started int) github.CheckRun {
	cr := github.CheckRun{ID: id, Name: name, Status: "completed", Conclusion: conclusion, StartedAt: ts(started), HTMLURL: "https://ci/" + name}
	cr.App = &struct {
		Slug string `json:"slug"`
	}{Slug: "github-actions"}
	return cr
}

func mgRun(id int64, pr, sha, conclusion string, created int) github.WorkflowRun {
	return github.WorkflowRun{
		ID: id, Name: "CI", HeadBranch: "gh-readonly-queue/main/pr-" + pr + "-0123abcd", HeadSHA: sha,
		Status: "completed", Conclusion: conclusion, CreatedAt: *ts(created), HTMLURL: "https://ci/run",
	}
}

func TestLatestMergeGroups(t *testing.T) {
	runs := []github.WorkflowRun{
		mgRun(3, "221", "m2", "success", 10),
		mgRun(4, "221", "m2", "failure", 10), // second workflow of the same group
		mgRun(1, "221", "m1", "failure", 5),
		mgRun(2, "7", "m0", "failure", 1),
		{ID: 9, HeadBranch: "main", HeadSHA: "x"},
	}
	g := latestMergeGroups(runs)
	if len(g) != 2 {
		t.Fatalf("groups = %v", g)
	}
	if g[221].sha != "m2" || len(g[221].runs) != 2 {
		t.Fatalf("pr 221 group = %+v", g[221])
	}
	if f := g[221].failed(); len(f) != 1 || f[0].ID != 4 {
		t.Errorf("failed = %+v", f)
	}
	if g[221].passed() {
		t.Error("a group with a failed run did not pass")
	}
}

func TestDetectFailure(t *testing.T) {
	pr := &github.PullRequest{Number: 5}
	pr.Head.SHA = "h1"

	// Red CI on the head commit wins over the merge queue.
	f := detectFailure(pr, []github.CheckRun{
		actionsCheck(1, "lint", "failure", 1),
		actionsCheck(2, "build", "success", 1),
	}, &mergeGroup{sha: "m1", runs: []github.WorkflowRun{mgRun(9, "5", "m1", "failure", 1)}})
	if f == nil || f.kind != failureKindCI || f.key != "ci:h1:lint" || f.flakyOn {
		t.Fatalf("ci failure = %+v", f)
	}

	// A re-run that passed hides the earlier failure.
	if f := detectFailure(pr, []github.CheckRun{actionsCheck(1, "lint", "failure", 1), actionsCheck(2, "lint", "success", 2)}, nil); f != nil {
		t.Fatalf("passing re-run: %+v", f)
	}

	// Passed, then failed on the same commit: flaky.
	f = detectFailure(pr, []github.CheckRun{actionsCheck(1, "test", "success", 1), actionsCheck(2, "test", "failure", 2)}, nil)
	if f == nil || !f.flakyOn {
		t.Fatalf("flaky failure = %+v", f)
	}

	// Merge queue rejection.
	f = detectFailure(pr, []github.CheckRun{actionsCheck(1, "lint", "success", 1)},
		&mergeGroup{sha: "m1", runs: []github.WorkflowRun{mgRun(9, "5", "m1", "failure", 1)}})
	if f == nil || f.kind != failureKindMergeQueue || f.key != "mq:m1" || !slices.Equal(f.checks, []string{"CI"}) {
		t.Fatalf("merge-queue failure = %+v", f)
	}

	// In progress or green: nothing.
	if f := detectFailure(pr, []github.CheckRun{{Name: "lint", Status: "in_progress"}}, &mergeGroup{sha: "m2", runs: []github.WorkflowRun{mgRun(9, "5", "m2", "success", 1)}}); f != nil {
		t.Fatalf("green: %+v", f)
	}
}

func TestFailureCleared(t *testing.T) {
	ci := &aiv1alpha1.PRFeedbackPullRequest{FailureKind: failureKindCI, FailedChecks: []string{"lint"}}
	if failureCleared(ci, []github.CheckRun{actionsCheck(1, "lint", "failure", 1)}, nil) {
		t.Error("still failing")
	}
	if !failureCleared(ci, []github.CheckRun{actionsCheck(1, "lint", "failure", 1), actionsCheck(2, "lint", "success", 2)}, nil) {
		t.Error("re-run passed")
	}
	mq := &aiv1alpha1.PRFeedbackPullRequest{FailureKind: failureKindMergeQueue, LastFailure: "mq:m1", FailedChecks: []string{"CI"}}
	if failureCleared(mq, nil, &mergeGroup{sha: "m1", runs: []github.WorkflowRun{mgRun(1, "5", "m1", "failure", 1)}}) {
		t.Error("same failed group")
	}
	if !failureCleared(mq, nil, &mergeGroup{sha: "m2", runs: []github.WorkflowRun{mgRun(2, "5", "m2", "success", 2)}}) {
		t.Error("a newer group passed")
	}
}

func TestTrimJobLog(t *testing.T) {
	lines := make([]string, 0, 200)
	for i := range 200 {
		lines = append(lines, "2026-11-23T17:44:20.5506830Z noise "+strings.Repeat("x", i%3))
	}
	lines[100] = "2026-11-23T17:44:20.5606733Z ##[error]internal/controller/knight_scaling.go:98:9: undefined: agentRuntimeForImage"
	out := trimJobLog(strings.Join(lines, "\r\n"), 3000)
	if !strings.Contains(out, "##[error]internal/controller/knight_scaling.go:98:9") {
		t.Fatalf("error line missing:\n%s", out)
	}
	if strings.Contains(out, "2026-11-23T") {
		t.Error("timestamps not stripped")
	}
	if n := strings.Count(out, "\n"); n > 20 {
		t.Errorf("kept %d lines, want the error and 15 before it", n)
	}

	// No error marker: the tail.
	out = trimJobLog("a\nb\nlast line", 3000)
	if !strings.HasSuffix(strings.TrimSpace(out), "last line") {
		t.Errorf("tail = %q", out)
	}

	// Capped, keeping the end, on a rune boundary.
	out = trimJobLog(strings.Repeat("é", 5000)+"\n##[error]end", 100)
	if len(out) > 110 || !strings.HasSuffix(strings.TrimSpace(out), "##[error]end") || !strings.HasPrefix(out, "...\n") {
		t.Errorf("capped = %q", out)
	}
}

func TestAuthorInChain(t *testing.T) {
	re := regexp.MustCompile(regexp.QuoteMeta("https://github.com/o/r/pull/5") + `(?:[^0-9]|$)`)
	ch := &aiv1alpha1.Chain{
		Spec: aiv1alpha1.ChainSpec{
			Input: `{"repo": "o/r", "issue": 42}`,
			Steps: []aiv1alpha1.ChainStep{
				{Name: "plan", KnightRef: corev1.LocalObjectReference{Name: "rt-lead"}},
				{Name: "implement", KnightRef: corev1.LocalObjectReference{Name: "rt-operator"}},
				{Name: "review", KnightRef: corev1.LocalObjectReference{Name: "rt-lead"}},
			},
		},
		Status: aiv1alpha1.ChainStatus{RunID: "run-1", StepStatuses: []aiv1alpha1.ChainStepStatus{
			{Name: "plan", Output: "plan mentions https://github.com/o/r/pull/50"},
			{Name: "implement", Output: "https://github.com/o/r/pull/5\nDone."},
			{Name: "review", Output: "PRs: https://github.com/o/r/pull/5"},
		}},
	}
	a, ok := authorInChain(ch, re)
	if !ok || a.knight != "rt-operator" || a.runID != "run-1" || a.issue != 42 {
		t.Fatalf("author = %+v, %v", a, ok)
	}

	// forEach items carry their own runId.
	ch.Spec.Steps[1].ForEach = &aiv1alpha1.ChainForEach{}
	ch.Status.StepStatuses[1].Output = ""
	ch.Status.StepStatuses[1].FanOut = &aiv1alpha1.ChainFanOutStatus{Items: []aiv1alpha1.ChainItemStatus{
		{Index: 0, RunID: "run-1-0", Output: "https://github.com/o/r/pull/4"},
		{Index: 1, RunID: "run-1-1", Output: "https://github.com/o/r/pull/5"},
	}}
	a, ok = authorInChain(ch, re)
	if !ok || a.knight != "rt-operator" || a.runID != "run-1-1" {
		t.Fatalf("fan-out author = %+v, %v", a, ok)
	}

	if _, ok := authorInChain(ch, regexp.MustCompile(regexp.QuoteMeta("https://github.com/o/r/pull/9")+`(?:[^0-9]|$)`)); ok {
		t.Error("unrelated PR matched")
	}
}

func TestResumeRunIDExpression(t *testing.T) {
	got, err := evalString("json(input).runId", map[string]any{"input": `{"runId": "abc"}`})
	if err != nil || got != "abc" {
		t.Fatalf("got %q, %v", got, err)
	}
	if _, err := evalString("json(input).runId", map[string]any{"input": `{"runId": 3}`}); err == nil {
		t.Error("a number is not a runId")
	}
	if _, err := compileStringExpr("1 + 2"); err == nil {
		t.Error("an int expression compiled as a string")
	}
	bad := &aiv1alpha1.Chain{Spec: aiv1alpha1.ChainSpec{Steps: []aiv1alpha1.ChainStep{{
		Name: "l", ResumeRunID: "input", Loop: &aiv1alpha1.ChainLoop{Until: "true", Steps: []aiv1alpha1.ChainLoopStep{{Name: "a", KnightRef: corev1.LocalObjectReference{Name: "k"}, Task: "t"}}},
	}}}}
	if _, err := validateConditionsAndLoops(bad); err == nil || !strings.Contains(err.Error(), "resumeRunId") {
		t.Errorf("loop with resumeRunId: %v", err)
	}
}

func loadPRFeedbackExample(t interface {
	Helper()
	Fatal(args ...any)
}) *aiv1alpha1.Chain {
	t.Helper()
	data, err := os.ReadFile("../../examples/chain-pr-feedback.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var chain aiv1alpha1.Chain
	if err := yaml.UnmarshalStrict(data, &chain); err != nil {
		t.Fatal(err)
	}
	return &chain
}

func TestPRFeedbackExample(t *testing.T) {
	chain := loadPRFeedbackExample(t)
	if err := (&ChainReconciler{}).validateDAG(chain); err != nil {
		t.Fatal(err)
	}
	if reason, err := validateConditionsAndLoops(chain); err != nil {
		t.Fatalf("%s: %v", reason, err)
	}
	if err := (&ChainReconciler{}).validateTemplates(chain); err != nil {
		t.Fatal(err)
	}

	simulate := func(input string, fixPhase aiv1alpha1.ChainStepPhase) (ran []string, runIDs map[string]string) {
		steps := map[string]any{}
		runIDs = map[string]string{}
		for _, s := range chain.Spec.Steps {
			run := true
			if s.When != "" {
				var err error
				run, err = evalCondition(s.When, map[string]any{"input": input, "steps": steps})
				if err != nil {
					t.Fatalf("step %s when: %v", s.Name, err)
				}
			}
			phase := aiv1alpha1.ChainStepPhaseSkipped
			if run {
				ran = append(ran, s.Name)
				phase = aiv1alpha1.ChainStepPhaseSucceeded
				if strings.HasPrefix(s.Name, "fix_") {
					phase = fixPhase
				}
				if s.ResumeRunID != "" {
					id, err := evalString(s.ResumeRunID, map[string]any{"input": input, "steps": steps})
					if err != nil {
						t.Fatalf("step %s resumeRunId: %v", s.Name, err)
					}
					runIDs[s.Name] = id
				}
			}
			steps[s.Name] = map[string]any{"phase": string(phase), "output": "", "outcome": ""}
		}
		return ran, runIDs
	}

	in := `{"repo": "o/r", "pr": 5, "knight": "rt-ui", "runId": "run-1", "kind": "ci", "log": "##[error]x"}`
	ran, ids := simulate(in, aiv1alpha1.ChainStepPhaseSucceeded)
	if !slices.Equal(ran, []string{"fix_rt_ui", "requeue"}) {
		t.Errorf("ran %v", ran)
	}
	if ids["fix_rt_ui"] != "run-1" || ids["requeue"] != "run-1" {
		t.Errorf("runIds %v", ids)
	}

	ran, _ = simulate(in, aiv1alpha1.ChainStepPhaseFailed)
	if !slices.Equal(ran, []string{"fix_rt_ui", "escalate"}) {
		t.Errorf("failed fix: ran %v", ran)
	}

	ran, _ = simulate(`{"repo": "o/r", "pr": 5, "knight": "someone-else", "runId": ""}`, aiv1alpha1.ChainStepPhaseSucceeded)
	if !slices.Equal(ran, []string{"escalate"}) {
		t.Errorf("unknown knight: ran %v", ran)
	}

	for _, s := range chain.Spec.Steps {
		if s.Model == "" {
			t.Errorf("step %q has no model alias", s.Name)
		}
		if strings.HasPrefix(s.Name, "fix_") && !s.ContinueOnFailure {
			t.Errorf("fix step %q must continueOnFailure so escalate runs", s.Name)
		}
	}
}
