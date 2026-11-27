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
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/github"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// A PR in the merge queue is a finished merge step (#591). The merge step
// queues the PRs and returns; when it still runs out of time (a slow knight,
// a stuck tool call) while GitHub shows every PR merged or queued, the work
// is done: the operator records the step as succeeded with a note, resolves
// the step's timeout dead-letter entry and does not hand the issue back.

// stepReasonQueuedAtTimeout marks a merge step that timed out with its PRs
// already merged or queued.
const stepReasonQueuedAtTimeout = "QueuedAtTimeout"

// isTimeoutError reports whether a step error is a timeout: the operator's
// "step timed out after Ns" or a knight's own time limit. It matches the
// dead-letter stream's timeout classification. Keep in step with the verify
// step's CEL `when` in charts/roundtable-operator/templates/pr-pipeline.yaml,
// which matches the same two substrings.
func isTimeoutError(err string) bool {
	e := strings.ToLower(err)
	return strings.Contains(e, "timed out") || strings.Contains(e, "timeout")
}

// timedOutMergeStep returns the merge step's status when it is the only
// failed step of the run and it failed on a timeout; nil otherwise.
func timedOutMergeStep(c *aiv1alpha1.Chain) *aiv1alpha1.ChainStepStatus {
	var merge *aiv1alpha1.ChainStepStatus
	for i := range c.Status.StepStatuses {
		ss := &c.Status.StepStatuses[i]
		if ss.Phase != aiv1alpha1.ChainStepPhaseFailed {
			continue
		}
		if ss.Name != mergeStepName || !isTimeoutError(ss.Error) {
			return nil
		}
		merge = ss
	}
	return merge
}

// ghPRState maps a PR as GitHub shows it to the merge step's structured
// shape: merged, queued (open with auto-merge on or in a merge queue, not a draft) or its state.
func ghPRState(ref prRef, pr *github.PullRequest) natspkg.PRRef {
	out := natspkg.PRRef{Repo: ref.repo, Number: ref.number, URL: pr.HTMLURL, Action: pr.State}
	if out.URL == "" {
		out.URL = fmt.Sprintf("https://github.com/%s/pull/%d", ref.repo, ref.number)
	}
	switch {
	case pr.Merged:
		out.Action = natspkg.PRActionMerged
	case pr.State != prStateClosed && !pr.Draft && pr.Queued():
		out.Action = natspkg.PRActionQueued
	}
	return out
}

// settledPhase is the phase completeRun gives a run whose steps all ended
// without a failed step: partially succeeded when a fan-out lost items or a
// loop was exhausted, succeeded otherwise.
func settledPhase(c *aiv1alpha1.Chain) aiv1alpha1.ChainPhase {
	for _, ss := range c.Status.StepStatuses {
		if ss.Phase == aiv1alpha1.ChainStepPhaseFailed {
			return aiv1alpha1.ChainPhaseFailed
		}
		if ss.Phase == aiv1alpha1.ChainStepPhaseSucceeded &&
			((ss.FanOut != nil && ss.FanOut.Failed > 0) || (ss.Loop != nil && ss.Loop.Outcome == aiv1alpha1.ChainLoopExhausted)) {
			return aiv1alpha1.ChainPhasePartiallySucceeded
		}
	}
	return aiv1alpha1.ChainPhaseSucceeded
}

// markMergeQueued records a timed-out merge step as succeeded with a note
// listing the PRs' GitHub states (in the "<url> queued|merged" form the merge
// step replies with) and recomputes the run's phase and counters.
func markMergeQueued(c *aiv1alpha1.Chain, ss *aiv1alpha1.ChainStepStatus, prs []natspkg.PRRef) {
	lines := make([]string, 0, len(prs))
	for _, p := range prs {
		lines = append(lines, p.URL+" "+p.Action)
	}
	ss.Output = fmt.Sprintf("The merge step timed out (%s), but GitHub shows every PR merged or queued, so the step counts as done:\n%s",
		strings.TrimSpace(ss.Error), strings.Join(lines, "\n"))
	ss.Phase = aiv1alpha1.ChainStepPhaseSucceeded
	ss.Reason = stepReasonQueuedAtTimeout
	ss.Error = ""

	was := c.Status.Phase
	c.Status.Phase = settledPhase(c)
	if was == aiv1alpha1.ChainPhaseFailed && c.Status.Phase != aiv1alpha1.ChainPhaseFailed {
		if c.Status.RunsFailed > 0 {
			c.Status.RunsFailed--
		}
		c.Status.RunsCompleted++
	}
	reason, msg := aiv1alpha1.ReasonChainSucceeded, "All steps completed successfully (merge step timed out with its PRs queued)"
	if c.Status.Phase == aiv1alpha1.ChainPhasePartiallySucceeded {
		reason, msg = aiv1alpha1.ReasonChainPartiallySucceeded, "Steps completed with soft failures (merge step timed out with its PRs queued)"
	}
	meta.SetStatusCondition(&c.Status.Conditions, metav1.Condition{
		Type:               aiv1alpha1.ConditionChainComplete,
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: c.Generation,
	})
}

// recoverQueuedMerge checks a finished run whose only failure is a merge
// step timeout: when GitHub shows every PR of the run merged or queued, the
// step is recorded as succeeded and its dead-letter entry resolved. It
// reports whether the run was recovered; a returned error is retried on the
// next poll.
func (r *IssuePickupReconciler) recoverQueuedMerge(ctx context.Context, gh *github.Client, rt *aiv1alpha1.RoundTable,
	c *aiv1alpha1.Chain) (bool, error) {
	if timedOutMergeStep(c) == nil {
		return false, nil
	}
	refs := chainPRs(c)
	if len(refs) == 0 {
		return false, nil
	}
	prs := make([]natspkg.PRRef, 0, len(refs))
	for _, ref := range refs {
		pr, err := gh.GetPull(ctx, ref.repo, ref.number)
		if err != nil {
			return false, fmt.Errorf("get %s: %w", ref, err)
		}
		if err := gh.LoadMergeQueueState(ctx, pr); err != nil {
			return false, fmt.Errorf("merge queue state of %s: %w", ref, err)
		}
		prs = append(prs, ghPRState(ref, pr))
	}
	if !natspkg.PRsLanded(prs) {
		return false, nil
	}

	orig := c.DeepCopy()
	ss := timedOutMergeStep(c)
	taskID := ss.TaskID
	markMergeQueued(c, ss, prs)
	if err := r.Status().Patch(ctx, c, client.MergeFrom(orig)); err != nil {
		return false, fmt.Errorf("record merge step of %s as queued: %w", c.Name, err)
	}
	if err := r.DeadLetters.Resolve(ctx, client.ObjectKeyFromObject(rt).String(), taskID); err != nil {
		logf.FromContext(ctx).Error(err, "Resolve dead-letter entry of timed-out merge step", "chain", c.Name, "taskId", taskID)
	}
	logf.FromContext(ctx).Info("Merge step timed out with its PRs queued; recorded as succeeded", "chain", c.Name, "taskId", taskID)
	r.Recorder.Eventf(c, corev1.EventTypeNormal, "MergeQueuedAtTimeout",
		"merge step timed out, but every PR is merged or queued on GitHub; recorded as succeeded")
	return true, nil
}
