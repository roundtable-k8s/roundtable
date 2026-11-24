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
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/github"
)

// When the planner splits an issue, the sub-PRs close the sub-issues but not
// the parent (#319). The operator tracks the parent through its Chain: once
// every sub-issue of the plan is closed as completed it closes the parent; if
// any sub-issue needs a human, the parent does too. The Chain is kept until
// then.

const (
	planStepName = "plan"
	// stateReasonNotPlanned is the state_reason of an issue closed as not planned.
	stateReasonNotPlanned = "not_planned"
)

// planSubIssues returns the sub-issue numbers of a split plan: the entries of
// the plan's "issues" list, minus the parent. It is nil when the plan did not
// split.
func planSubIssues(c *aiv1alpha1.Chain, parent int) []int {
	for _, s := range c.Status.StepStatuses {
		if s.Name != planStepName {
			continue
		}
		doc, err := parseJSONDocument(s.Output)
		if err != nil {
			return nil
		}
		m, ok := doc.(map[string]any)
		if !ok || m["split"] != true {
			return nil
		}
		list, _ := m["issues"].([]any)
		var out []int
		for _, it := range list {
			im, ok := it.(map[string]any)
			if !ok {
				continue
			}
			num, ok := im["number"].(json.Number)
			if !ok {
				continue
			}
			n, err := strconv.Atoi(num.String())
			if err != nil || n == parent || slices.Contains(out, n) {
				continue
			}
			out = append(out, n)
		}
		return out
	}
	return nil
}

// pickupNeedsSplitClose reports whether a verified, succeeded pickup Chain of
// a split plan still has its parent issue to settle.
func pickupNeedsSplitClose(c *aiv1alpha1.Chain) bool {
	if pickupChainActive(c) || c.Status.Phase != aiv1alpha1.ChainPhaseSucceeded || c.Annotations[aiv1alpha1.AnnotationPickupRepo] == "" {
		return false
	}
	if c.Annotations[aiv1alpha1.AnnotationPickupVerified] != c.Status.LastRunTrigger ||
		c.Annotations[aiv1alpha1.AnnotationPickupParentDone] == c.Status.LastRunTrigger {
		return false
	}
	parent, err := strconv.Atoi(c.Annotations[aiv1alpha1.AnnotationPickupIssue])
	if err != nil {
		return false
	}
	return len(planSubIssues(c, parent)) > 0
}

// closeSplitParent checks the sub-issues of a split run on GitHub. Pending
// sub-issues leave the Chain for a later poll; otherwise the parent is closed
// or handed back and the Chain marked settled. A returned error is retried on
// the next poll.
func (r *IssuePickupReconciler) closeSplitParent(ctx context.Context, gh *github.Client, rt *aiv1alpha1.RoundTable,
	c *aiv1alpha1.Chain, labels pickupLabelSet) error {
	repo := c.Annotations[aiv1alpha1.AnnotationPickupRepo]
	parent, err := strconv.Atoi(c.Annotations[aiv1alpha1.AnnotationPickupIssue])
	if err != nil {
		return fmt.Errorf("bad %s annotation: %w", aiv1alpha1.AnnotationPickupIssue, err)
	}
	pi, err := gh.GetIssue(ctx, repo, parent)
	if err != nil {
		return fmt.Errorf("get %s#%d: %w", repo, parent, err)
	}
	// A parent a human already took over (no in-progress label) is left alone.
	if pi.HasLabel(labels.inProgress) {
		var stuck []string
		pending := false
		for _, n := range planSubIssues(c, parent) {
			si, err := gh.GetIssue(ctx, repo, n)
			if err != nil {
				return fmt.Errorf("get %s#%d: %w", repo, n, err)
			}
			switch {
			case si.State == prStateClosed && si.StateReason != stateReasonNotPlanned:
			case si.State == prStateClosed:
				stuck = append(stuck, fmt.Sprintf("#%d was closed as not planned", n))
			case si.HasLabel(labels.needsHuman):
				stuck = append(stuck, fmt.Sprintf("#%d is labelled `%s`", n, labels.needsHuman))
			default:
				pending = true
			}
		}
		switch {
		case len(stuck) > 0:
			if err := r.handBackSplitParent(ctx, gh, rt, c, repo, parent, stuck, labels); err != nil {
				return err
			}
		case pending:
			return nil
		default:
			if err := r.closeParent(ctx, gh, rt, c, repo, parent, labels); err != nil {
				return err
			}
		}
	}

	orig := c.DeepCopy()
	if c.Annotations == nil {
		c.Annotations = map[string]string{}
	}
	c.Annotations[aiv1alpha1.AnnotationPickupParentDone] = c.Status.LastRunTrigger
	return r.Patch(ctx, c, client.MergeFrom(orig))
}

// closeParent closes the parent as completed, comments the merged PRs and
// removes the in-progress label (last, so a failed step is retried).
func (r *IssuePickupReconciler) closeParent(ctx context.Context, gh *github.Client, rt *aiv1alpha1.RoundTable,
	c *aiv1alpha1.Chain, repo string, parent int, labels pickupLabelSet) error {
	refs := chainPRs(c)
	prs := make([]string, 0, len(refs))
	for _, ref := range refs {
		prs = append(prs, "- "+ref.String())
	}
	body := fmt.Sprintf("All sub-issues of this issue are closed (Chain `%s`, run `%s`).", c.Name, c.Status.LastRunTrigger)
	if len(prs) > 0 {
		body += "\n\nMerged PRs:\n\n" + strings.Join(prs, "\n")
	}
	if err := gh.CloseIssue(ctx, repo, parent); err != nil {
		return err
	}
	if err := gh.Comment(ctx, repo, parent, body); err != nil {
		return err
	}
	if err := gh.RemoveLabel(ctx, repo, parent, labels.inProgress); err != nil {
		return err
	}
	logf.FromContext(ctx).Info("Closed split parent issue", "chain", c.Name, "repo", repo, "issue", parent)
	r.Recorder.Eventf(rt, corev1.EventTypeNormal, "SplitParentClosed", "%s#%d closed: all sub-issues done", repo, parent)
	return nil
}

// handBackSplitParent labels the parent needs-human because a sub-issue is stuck.
func (r *IssuePickupReconciler) handBackSplitParent(ctx context.Context, gh *github.Client, rt *aiv1alpha1.RoundTable,
	c *aiv1alpha1.Chain, repo string, parent int, stuck []string, labels pickupLabelSet) error {
	body := fmt.Sprintf("Chain `%s` split this issue, but not every sub-issue finished:\n\n- %s\n\nLabelled `%s` for a human.",
		c.Name, strings.Join(stuck, "\n- "), labels.needsHuman)
	if err := gh.AddLabels(ctx, repo, parent, labels.needsHuman); err != nil {
		return err
	}
	if err := gh.Comment(ctx, repo, parent, body); err != nil {
		return err
	}
	if err := gh.RemoveLabel(ctx, repo, parent, labels.inProgress); err != nil {
		return err
	}
	r.Recorder.Eventf(rt, corev1.EventTypeWarning, "SplitParentNeedsHuman", "%s#%d: %s", repo, parent, strings.Join(stuck, "; "))
	return nil
}
