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
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/github"
)

// With spec.issuePickup.postRun: chain (#538) the chain's own steps verify
// the PRs, post a missing review, hand a failed run back and close a split
// parent. The operator parses no step output for such a table: it only makes
// sure a finished run never leaves its issue in progress.

// pickupPostRun returns the table's post-run mode, builtin unless chain is set.
func pickupPostRun(spec *aiv1alpha1.IssuePickupSpec) aiv1alpha1.IssuePickupPostRun {
	if spec != nil && spec.PostRun == aiv1alpha1.IssuePickupPostRunChain {
		return aiv1alpha1.IssuePickupPostRunChain
	}
	return aiv1alpha1.IssuePickupPostRunBuiltin
}

// pickupPostRunPending reports whether a pickup Chain still has post-run work
// for the operator, so it is not garbage-collected yet.
func pickupPostRunPending(c *aiv1alpha1.Chain, mode aiv1alpha1.IssuePickupPostRun) bool {
	if mode == aiv1alpha1.IssuePickupPostRunChain {
		return pickupNeedsRelease(c)
	}
	return pickupNeedsVerify(c) || pickupNeedsHandBack(c) || pickupNeedsSplitClose(c)
}

// pickupNeedsRelease reports whether a finished pickup Chain of a
// postRun: chain table has not had its issue checked for release yet.
func pickupNeedsRelease(c *aiv1alpha1.Chain) bool {
	if pickupChainActive(c) || c.Annotations[aiv1alpha1.AnnotationPickupRepo] == "" || c.Status.LastRunTrigger == "" {
		return false
	}
	switch c.Status.Phase {
	case aiv1alpha1.ChainPhaseSucceeded, aiv1alpha1.ChainPhaseFailed, aiv1alpha1.ChainPhasePartiallySucceeded:
	default:
		return false
	}
	return c.Annotations[aiv1alpha1.AnnotationPickupVerified] != c.Status.LastRunTrigger
}

// releaseIssue frees the issue of a finished postRun: chain run. Only an
// issue that is still open with the in-progress label is touched:
//   - already labelled needs-human (by the handback, verify or escalate step):
//     the in-progress label is removed;
//   - the run did not succeed (a step failed, the handback or verify step
//     included): needs-human, a comment naming the failed steps, and the
//     in-progress label removed;
//   - the run succeeded: left alone, the chain's steps own it (a queued PR
//     closes it on merge, the split branch closes a parent). A run whose
//     only failure is a merge step timeout with every PR merged or queued
//     is first recorded as succeeded (#591).
//
// The run is then marked handled. A returned error is retried on the next poll.
func (r *IssuePickupReconciler) releaseIssue(ctx context.Context, gh *github.Client, rt *aiv1alpha1.RoundTable,
	c *aiv1alpha1.Chain, labels pickupLabelSet) error {
	repo := c.Annotations[aiv1alpha1.AnnotationPickupRepo]
	issue, err := strconv.Atoi(c.Annotations[aiv1alpha1.AnnotationPickupIssue])
	if err != nil {
		return fmt.Errorf("bad %s annotation: %w", aiv1alpha1.AnnotationPickupIssue, err)
	}
	// #591: a merge step that timed out with its PRs queued is done; the
	// recovered run is succeeded and leaves the issue to the merge queue.
	if _, err := r.recoverQueuedMerge(ctx, gh, rt, c); err != nil {
		return err
	}
	gi, err := gh.GetIssue(ctx, repo, issue)
	if err != nil {
		return fmt.Errorf("get %s#%d: %w", repo, issue, err)
	}
	if gi.State == issueStateOpen && gi.HasLabel(labels.inProgress) {
		switch {
		case gi.HasLabel(labels.needsHuman):
			if err := gh.RemoveLabel(ctx, repo, issue, labels.inProgress); err != nil {
				return err
			}
			logf.FromContext(ctx).Info("Released issue already handed to a human", "chain", c.Name, "repo", repo, "issue", issue)
			r.Recorder.Eventf(rt, corev1.EventTypeNormal, "PickupIssueReleased", "%s#%d: removed %s after chain %s",
				repo, issue, labels.inProgress, c.Name)
		case c.Status.Phase != aiv1alpha1.ChainPhaseSucceeded:
			if err := r.labelFailedIssue(ctx, gh, rt, c, repo, issue, labels); err != nil {
				return err
			}
		}
	}

	orig := c.DeepCopy()
	if c.Annotations == nil {
		c.Annotations = map[string]string{}
	}
	c.Annotations[aiv1alpha1.AnnotationPickupVerified] = c.Status.LastRunTrigger
	return r.Patch(ctx, c, client.MergeFrom(orig))
}
