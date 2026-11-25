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
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/github"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

func TestTimedOutMergeStep(t *testing.T) {
	ok := aiv1alpha1.ChainStepStatus{Name: "review", Phase: aiv1alpha1.ChainStepPhaseSucceeded}
	mergeTO := aiv1alpha1.ChainStepStatus{Name: "merge", Phase: aiv1alpha1.ChainStepPhaseFailed, Error: "step timed out after 3600s"}
	cases := map[string]struct {
		steps []aiv1alpha1.ChainStepStatus
		want  bool
	}{
		"operator timeout":     {[]aiv1alpha1.ChainStepStatus{ok, mergeTO}, true},
		"knight timeout":       {[]aiv1alpha1.ChainStepStatus{ok, {Name: "merge", Phase: aiv1alpha1.ChainStepPhaseFailed, Error: "Task Timeout (3600s)"}}, true},
		"merge failed":         {[]aiv1alpha1.ChainStepStatus{ok, {Name: "merge", Phase: aiv1alpha1.ChainStepPhaseFailed, Error: "gh: not found"}}, false},
		"merge succeeded":      {[]aiv1alpha1.ChainStepStatus{ok, {Name: "merge", Phase: aiv1alpha1.ChainStepPhaseSucceeded}}, false},
		"another step failed":  {[]aiv1alpha1.ChainStepStatus{{Name: "review", Phase: aiv1alpha1.ChainStepPhaseFailed, Error: "boom"}, mergeTO}, false},
		"other step timed out": {[]aiv1alpha1.ChainStepStatus{{Name: "plan", Phase: aiv1alpha1.ChainStepPhaseFailed, Error: "step timed out after 60s"}}, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c := &aiv1alpha1.Chain{Status: aiv1alpha1.ChainStatus{StepStatuses: tc.steps}}
			if got := timedOutMergeStep(c) != nil; got != tc.want {
				t.Fatalf("timedOutMergeStep = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestQueuedCountsAsDone is the queued-counts-as-done check: GitHub's PR
// states mapped to the merge step's shape, then PRsLanded.
func TestQueuedCountsAsDone(t *testing.T) {
	ref := prRef{repo: "o/r", number: 7}
	queued := github.PullRequest{State: "open", AutoMerge: &struct{}{}}
	cases := map[string]struct {
		prs    []github.PullRequest
		action string
		landed bool
	}{
		"queued":             {[]github.PullRequest{queued}, natspkg.PRActionQueued, true},
		"merged":             {[]github.PullRequest{{State: "closed", Merged: true}}, natspkg.PRActionMerged, true},
		"open, not queued":   {[]github.PullRequest{{State: "open"}}, "open", false},
		"closed unmerged":    {[]github.PullRequest{{State: "closed"}}, "closed", false},
		"draft with auto":    {[]github.PullRequest{{State: "open", Draft: true, AutoMerge: &struct{}{}}}, "open", false},
		"one of two missing": {[]github.PullRequest{queued, {State: "open"}}, natspkg.PRActionQueued, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := make([]natspkg.PRRef, 0, len(tc.prs))
			for i := range tc.prs {
				got = append(got, ghPRState(ref, &tc.prs[i]))
			}
			if got[0].Action != tc.action {
				t.Errorf("action = %q, want %q", got[0].Action, tc.action)
			}
			if got[0].URL != "https://github.com/o/r/pull/7" {
				t.Errorf("url = %q", got[0].URL)
			}
			if l := natspkg.PRsLanded(got); l != tc.landed {
				t.Errorf("PRsLanded = %v, want %v", l, tc.landed)
			}
		})
	}
}

func TestMarkMergeQueued(t *testing.T) {
	prs := []natspkg.PRRef{{URL: "https://github.com/o/r/pull/7", Action: natspkg.PRActionQueued}}
	t.Run("hard failure becomes success", func(t *testing.T) {
		c := &aiv1alpha1.Chain{Status: aiv1alpha1.ChainStatus{
			Phase: aiv1alpha1.ChainPhaseFailed, RunsFailed: 1, RunsCompleted: 3,
			StepStatuses: []aiv1alpha1.ChainStepStatus{
				{Name: "review", Phase: aiv1alpha1.ChainStepPhaseSucceeded},
				{Name: "merge", Phase: aiv1alpha1.ChainStepPhaseFailed, Error: "step timed out after 3600s", TaskID: "t1"},
			},
		}}
		ss := timedOutMergeStep(c)
		markMergeQueued(c, ss, prs)
		m := c.Status.StepStatuses[1]
		if m.Phase != aiv1alpha1.ChainStepPhaseSucceeded || m.Reason != stepReasonQueuedAtTimeout || m.Error != "" {
			t.Fatalf("merge step = %+v", m)
		}
		if !strings.Contains(m.Output, "timed out") || !strings.Contains(m.Output, "https://github.com/o/r/pull/7 queued") {
			t.Errorf("note = %q", m.Output)
		}
		// The note is in the merge step's own reply shape.
		if st := natspkg.ParseStructured(m.Output); !natspkg.PRsLanded(st.PRs) {
			t.Errorf("note does not parse as landed: %+v", st.PRs)
		}
		if c.Status.Phase != aiv1alpha1.ChainPhaseSucceeded || c.Status.RunsFailed != 0 || c.Status.RunsCompleted != 4 {
			t.Errorf("phase/counters = %s %d/%d", c.Status.Phase, c.Status.RunsFailed, c.Status.RunsCompleted)
		}
		cond := meta.FindStatusCondition(c.Status.Conditions, aiv1alpha1.ConditionChainComplete)
		if cond == nil || cond.Reason != aiv1alpha1.ReasonChainSucceeded {
			t.Errorf("condition = %+v", cond)
		}
	})
	t.Run("soft failure (postRun: chain) keeps counters", func(t *testing.T) {
		c := &aiv1alpha1.Chain{Status: aiv1alpha1.ChainStatus{
			Phase: aiv1alpha1.ChainPhasePartiallySucceeded, RunsCompleted: 1,
			StepStatuses: []aiv1alpha1.ChainStepStatus{
				{Name: "merge", Phase: aiv1alpha1.ChainStepPhaseFailed, Error: "step timed out after 3600s"},
				{Name: "verify", Phase: aiv1alpha1.ChainStepPhaseSucceeded},
			},
		}}
		markMergeQueued(c, timedOutMergeStep(c), prs)
		if c.Status.Phase != aiv1alpha1.ChainPhaseSucceeded || c.Status.RunsCompleted != 1 {
			t.Errorf("phase/counters = %s %d", c.Status.Phase, c.Status.RunsCompleted)
		}
	})
	t.Run("exhausted loop stays partially succeeded", func(t *testing.T) {
		c := &aiv1alpha1.Chain{Status: aiv1alpha1.ChainStatus{
			Phase: aiv1alpha1.ChainPhaseFailed, RunsFailed: 1,
			StepStatuses: []aiv1alpha1.ChainStepStatus{
				{Name: "fixloop_x", Phase: aiv1alpha1.ChainStepPhaseSucceeded, Loop: &aiv1alpha1.ChainLoopStatus{Outcome: aiv1alpha1.ChainLoopExhausted}},
				{Name: "merge", Phase: aiv1alpha1.ChainStepPhaseFailed, Error: "timeout"},
			},
		}}
		markMergeQueued(c, timedOutMergeStep(c), prs)
		if c.Status.Phase != aiv1alpha1.ChainPhasePartiallySucceeded || c.Status.RunsFailed != 0 || c.Status.RunsCompleted != 1 {
			t.Errorf("phase/counters = %s %d/%d", c.Status.Phase, c.Status.RunsFailed, c.Status.RunsCompleted)
		}
	})
}

// TestMergeStepLandedCEL: the merge step's structured prs[] can be checked
// in a when/until expression ("queued" counts as done).
func TestMergeStepLandedCEL(t *testing.T) {
	expr := `steps.merge.prs.size() > 0 && steps.merge.prs.all(p, p.action in ["queued", "merged"])`
	for out, want := range map[string]bool{
		"https://github.com/o/r/pull/1 queued (position 2)\nhttps://github.com/o/r/pull/2 merged": true,
		"https://github.com/o/r/pull/1 queued\nhttps://github.com/o/r/pull/2 blocked: ci red":     false,
		"nothing queued": false,
	} {
		parsed := natspkg.ParseStructured(out).PRs
		prs := make([]any, 0, len(parsed))
		for _, p := range parsed {
			prs = append(prs, map[string]any{"repo": p.Repo, "number": int64(p.Number), "url": p.URL, "action": p.Action})
		}
		got, err := evalCondition(expr, map[string]any{"input": "", "steps": map[string]any{"merge": map[string]any{"prs": prs}}})
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("%q: got %v, want %v", out, got, want)
		}
	}
}
