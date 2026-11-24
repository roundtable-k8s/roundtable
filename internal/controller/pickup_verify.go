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
	"fmt"
	"regexp"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/github"
)

// The merge step of the PR pipeline is a knight's say-so: it can reply "queued"
// without having queued anything (#301). After a pickup Chain succeeds, the
// operator therefore checks GitHub itself: every PR must be merged or queued
// (auto-merge on). An open PR without auto-merge is queued by the operator; a
// closed, draft, held or un-queueable PR gets the needs-human label.

const mergeStepName = "merge"

var (
	prURLRe   = regexp.MustCompile(`https://github\.com/([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)/pull/(\d+)`)
	prsLineRe = regexp.MustCompile(`(?mi)^\W*PRs:\s*(.*)$`)
)

// pickupNeedsVerify reports whether a finished pickup Chain still has to have
// its merge step checked on GitHub.
func pickupNeedsVerify(c *aiv1alpha1.Chain) bool {
	if pickupChainActive(c) || c.Status.Phase != aiv1alpha1.ChainPhaseSucceeded || c.Annotations[aiv1alpha1.AnnotationPickupRepo] == "" {
		return false
	}
	if c.Annotations[aiv1alpha1.AnnotationPickupVerified] == c.Status.LastRunTrigger {
		return false
	}
	for _, s := range c.Status.StepStatuses {
		if s.Name == mergeStepName {
			return s.Phase == aiv1alpha1.ChainStepPhaseSucceeded
		}
	}
	return false
}

type prRef struct {
	repo   string
	number int
}

func (p prRef) String() string { return fmt.Sprintf("%s#%d", p.repo, p.number) }

func parsePRs(text string) []prRef {
	var out []prRef
	seen := map[prRef]bool{}
	for _, m := range prURLRe.FindAllStringSubmatch(text, -1) {
		n, _ := strconv.Atoi(m[2])
		ref := prRef{m[1], n}
		if !seen[ref] {
			seen[ref] = true
			out = append(out, ref)
		}
	}
	return out
}

// lastPRsLine returns the text of the last "PRs:" line in output.
func lastPRsLine(output string) string {
	all := prsLineRe.FindAllStringSubmatch(output, -1)
	if len(all) == 0 {
		return ""
	}
	return all[len(all)-1][1]
}

// chainPRs finds the PRs the run produced: the "PRs:" line of the converged
// fix loop's final review, else of the first review, else any PR URL in the
// merge step's output.
func chainPRs(c *aiv1alpha1.Chain) []prRef {
	var review, merge string
	for _, s := range c.Status.StepStatuses {
		switch {
		case strings.HasPrefix(s.Name, "fixloop_") && s.Loop != nil && s.Loop.Outcome == aiv1alpha1.ChainLoopConverged:
			if prs := parsePRs(lastPRsLine(s.Output)); len(prs) > 0 {
				return prs
			}
		case s.Name == "review":
			review = s.Output
		case s.Name == mergeStepName:
			merge = s.Output
		}
	}
	if prs := parsePRs(lastPRsLine(review)); len(prs) > 0 {
		return prs
	}
	return parsePRs(merge)
}

// verifyMerge checks each PR of a succeeded run on GitHub and records the
// result on the Chain. A returned error is retried on the next poll.
func (r *IssuePickupReconciler) verifyMerge(ctx context.Context, gh *github.Client, rt *aiv1alpha1.RoundTable,
	c *aiv1alpha1.Chain, labels pickupLabelSet) error {
	repo := c.Annotations[aiv1alpha1.AnnotationPickupRepo]
	issue, err := strconv.Atoi(c.Annotations[aiv1alpha1.AnnotationPickupIssue])
	if err != nil {
		return fmt.Errorf("bad %s annotation: %w", aiv1alpha1.AnnotationPickupIssue, err)
	}
	prs := chainPRs(c)
	var problems, queued []string
	if len(prs) == 0 {
		problems = append(problems, "no PR URL found in the review or merge output")
	}
	for _, ref := range prs {
		pr, err := gh.GetPull(ctx, ref.repo, ref.number)
		if err != nil {
			return fmt.Errorf("get %s: %w", ref, err)
		}
		switch {
		case pr.Merged:
		case pr.State == "closed":
			problems = append(problems, fmt.Sprintf("%s is closed without being merged", ref))
		case pr.Draft:
			problems = append(problems, fmt.Sprintf("%s is a draft", ref))
		case pr.HasLabel(labels.needsHuman):
			problems = append(problems, fmt.Sprintf("%s is labelled `%s`", ref, labels.needsHuman))
		case pr.AutoMerge != nil:
		default:
			if err := gh.EnableAutoMerge(ctx, pr.NodeID); err != nil {
				problems = append(problems, fmt.Sprintf("%s is open and could not be queued: %v", ref, err))
			} else {
				queued = append(queued, ref.String())
			}
		}
	}

	if len(queued) > 0 {
		logf.FromContext(ctx).Info("Queued PRs the merge step left open", "chain", c.Name, "prs", queued)
		r.Recorder.Eventf(c, corev1.EventTypeWarning, "MergeStepUnverified",
			"merge step did not queue %s; the operator enabled auto-merge", strings.Join(queued, ", "))
	}
	if len(problems) > 0 {
		body := fmt.Sprintf("The merge step of Chain `%s` finished, but GitHub does not show the PR(s) merged or queued:\n\n- %s\n\n"+
			"Labelled `%s` for a human.", c.Name, strings.Join(problems, "\n- "), labels.needsHuman)
		if err := gh.AddLabels(ctx, repo, issue, labels.needsHuman); err != nil {
			return err
		}
		if err := gh.Comment(ctx, repo, issue, body); err != nil {
			return err
		}
		if err := gh.RemoveLabel(ctx, repo, issue, labels.inProgress); err != nil {
			return err
		}
		r.Recorder.Eventf(rt, corev1.EventTypeWarning, "MergeNeedsHuman", "%s#%d: %s", repo, issue, strings.Join(problems, "; "))
	}

	orig := c.DeepCopy()
	if c.Annotations == nil {
		c.Annotations = map[string]string{}
	}
	c.Annotations[aiv1alpha1.AnnotationPickupVerified] = c.Status.LastRunTrigger
	return r.Patch(ctx, c, client.MergeFrom(orig))
}
