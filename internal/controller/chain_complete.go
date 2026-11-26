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
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/pkg/metrics"
)

// finishRun completes the run once every step is terminal: it sets the chain
// phase, counters and ChainComplete condition. Otherwise it requeues to poll
// for results.
func (r *ChainReconciler) finishRun(ctx context.Context, chain *aiv1alpha1.Chain, run *chainRun) (ctrl.Result, error) {
	if r.settleSteps(ctx, chain, run.specMap) {
		r.completeRun(ctx, chain, run.specMap)
		r.warnNoStepsExecuted(ctx, chain)
		chain.Status.ObservedGeneration = chain.Generation
		return r.updateStatus(ctx, chain, 0)
	}

	chain.Status.ObservedGeneration = chain.Generation

	// Requeue to poll for results
	return r.updateStatus(ctx, chain, RequeueDefault)
}

// settleSteps reports whether every step is terminal. Once a step failed
// without continueOnFailure it skips the remaining pending steps, stops
// loops and fan-outs from starting more work, and the run is terminal as soon
// as nothing is running any more.
func (r *ChainReconciler) settleSteps(ctx context.Context, chain *aiv1alpha1.Chain, specMap map[string]*aiv1alpha1.ChainStep) bool {
	// Check if all steps are terminal
	allTerminal := true
	anyFailed := false
	for _, ss := range chain.Status.StepStatuses {
		switch ss.Phase {
		case aiv1alpha1.ChainStepPhaseSucceeded, aiv1alpha1.ChainStepPhaseFailed, aiv1alpha1.ChainStepPhaseSkipped:
			if ss.Phase == aiv1alpha1.ChainStepPhaseFailed {
				// Check if continueOnFailure — if not, the chain fails
				spec := specMap[ss.Name]
				if spec == nil || !spec.ContinueOnFailure {
					anyFailed = true
				}
			}
		default:
			allTerminal = false
		}
	}
	if !anyFailed {
		return allTerminal
	}

	// If a step failed without continueOnFailure, skip remaining pending steps and fail chain
	for i := range chain.Status.StepStatuses {
		ss := &chain.Status.StepStatuses[i]
		if ss.Phase == aiv1alpha1.ChainStepPhasePending {
			skipStep(ss, stepReasonChainFailed)
		}
		// A running loop starts no further rounds; it stops as soon as
		// its in-flight body task (if any) is done.
		if ss.Phase == aiv1alpha1.ChainStepPhaseRunning && ss.Loop != nil && !loopInFlight(ss.Loop) {
			skipStep(ss, stepReasonChainFailed)
		}
		// A running fan-out dispatches no further items once the chain
		// is failing; it joins as soon as its in-flight items finish.
		if spec := specMap[ss.Name]; ss.Phase == aiv1alpha1.ChainStepPhaseRunning && ss.FanOut != nil && spec != nil {
			skipPendingItems(ss.FanOut)
			r.finalizeFanOutIfDone(ctx, chain, spec, ss)
			compactItemDetails(ss.FanOut)
		}
	}
	// Check if still have running steps
	for _, ss := range chain.Status.StepStatuses {
		if ss.Phase == aiv1alpha1.ChainStepPhaseRunning {
			return false
		}
	}
	return true
}

// completeRun sets the completed run's phase, run counters, ChainComplete
// condition, event and run metric from its hard failures, soft failures
// (continueOnFailure, failed fan-out items, exhausted loops) and successes.
func (r *ChainReconciler) completeRun(ctx context.Context, chain *aiv1alpha1.Chain, specMap map[string]*aiv1alpha1.ChainStep) {
	now := metav1.Now()
	chain.Status.CompletedAt = &now

	// Count hard failures, soft failures, and successes
	hardFailures := 0
	var hardFailed []aiv1alpha1.ChainStepStatus
	softFailures := 0
	succeededSteps := 0
	totalSteps := len(chain.Status.StepStatuses)

	for _, ss := range chain.Status.StepStatuses {
		switch ss.Phase {
		case aiv1alpha1.ChainStepPhaseSucceeded:
			succeededSteps++
			if ss.FanOut != nil && ss.FanOut.Failed > 0 {
				softFailures++ // failurePolicy=continue with failed items
			}
			if ss.Loop != nil && ss.Loop.Outcome == aiv1alpha1.ChainLoopExhausted {
				softFailures++ // onExhausted=continue
			}
		case aiv1alpha1.ChainStepPhaseFailed:
			spec := specMap[ss.Name]
			if spec != nil && spec.ContinueOnFailure {
				softFailures++
			} else {
				hardFailures++
				hardFailed = append(hardFailed, ss)
			}
		}
	}

	if hardFailures > 0 {
		// At least one hard failure — chain fails
		failMsg := failedStepsMessage(hardFailed)
		chain.Status.Phase = aiv1alpha1.ChainPhaseFailed
		chain.Status.RunsFailed++
		meta.SetStatusCondition(&chain.Status.Conditions, metav1.Condition{
			Type:               aiv1alpha1.ConditionChainComplete,
			Status:             metav1.ConditionTrue,
			Reason:             aiv1alpha1.ReasonChainFailed,
			Message:            failMsg,
			ObservedGeneration: chain.Generation,
		})
		r.Recorder.Eventf(chain, corev1.EventTypeWarning, "Failed", "Chain failed: %s", failMsg)
		metrics.ChainRunsTotal.WithLabelValues(r.chainTable(ctx, chain), chain.Name, "failed").Inc()
	} else if softFailures > 0 {
		// No hard failures, but some soft failures
		chain.Status.Phase = aiv1alpha1.ChainPhasePartiallySucceeded
		chain.Status.RunsCompleted++ // Count as completed (not failed)
		meta.SetStatusCondition(&chain.Status.Conditions, metav1.Condition{
			Type:               aiv1alpha1.ConditionChainComplete,
			Status:             metav1.ConditionTrue,
			Reason:             aiv1alpha1.ReasonChainPartiallySucceeded,
			Message:            fmt.Sprintf("%d/%d steps succeeded (%d failed with continueOnFailure)", succeededSteps, totalSteps, softFailures),
			ObservedGeneration: chain.Generation,
		})
		r.Recorder.Eventf(chain, corev1.EventTypeNormal, "Succeeded", "Chain partially succeeded: %d/%d steps succeeded", succeededSteps, totalSteps)
		metrics.ChainRunsTotal.WithLabelValues(r.chainTable(ctx, chain), chain.Name, "partially_succeeded").Inc()
	} else {
		// All steps succeeded
		chain.Status.Phase = aiv1alpha1.ChainPhaseSucceeded
		chain.Status.RunsCompleted++
		meta.SetStatusCondition(&chain.Status.Conditions, metav1.Condition{
			Type:               aiv1alpha1.ConditionChainComplete,
			Status:             metav1.ConditionTrue,
			Reason:             aiv1alpha1.ReasonChainSucceeded,
			Message:            fmt.Sprintf("All %d steps completed successfully", totalSteps),
			ObservedGeneration: chain.Generation,
		})
		r.Recorder.Event(chain, corev1.EventTypeNormal, "Succeeded", "Chain completed successfully")
		metrics.ChainRunsTotal.WithLabelValues(r.chainTable(ctx, chain), chain.Name, "succeeded").Inc()
	}
}

// warnNoStepsExecuted flags a completed run that never published a single
// task (every terminal step was restored from cache or skipped). It did no
// real work, which usually means stale KV entries are masking a problem.
func (r *ChainReconciler) warnNoStepsExecuted(ctx context.Context, chain *aiv1alpha1.Chain) {
	executedSteps := 0
	for _, ss := range chain.Status.StepStatuses {
		if ss.TaskID != "" || (ss.FanOut != nil && len(ss.FanOut.Items) > 0) || (ss.Loop != nil && ss.Loop.Iteration > 0) {
			executedSteps++
		}
	}
	if executedSteps == 0 && len(chain.Status.StepStatuses) > 0 {
		logf.FromContext(ctx).Info("Chain run completed without executing any steps")
		r.Recorder.Event(chain, corev1.EventTypeWarning, "NoStepsExecuted",
			"Chain run completed without executing any steps (all outputs restored from cache)")
		metrics.ChainNoOpRunsTotal.WithLabelValues(r.chainTable(ctx, chain), chain.Name).Inc()
	}
}

// hasHardFailure reports whether a step already failed without
// continueOnFailure, i.e. the run is going to fail.
func hasHardFailure(chain *aiv1alpha1.Chain, specMap map[string]*aiv1alpha1.ChainStep) bool {
	for _, ss := range chain.Status.StepStatuses {
		if ss.Phase != aiv1alpha1.ChainStepPhaseFailed {
			continue
		}
		if spec := specMap[ss.Name]; spec == nil || !spec.ContinueOnFailure {
			return true
		}
	}
	return false
}

const (
	maxFailedStepsNamed = 3
	maxStepErrorRunes   = 200
)

// failedStepsMessage names the failed steps and an excerpt of each step's
// error: "step a: err; step b: err; and N more". With no steps it returns "".
func failedStepsMessage(failed []aiv1alpha1.ChainStepStatus) string {
	if len(failed) == 0 {
		return ""
	}
	parts := make([]string, 0, maxFailedStepsNamed+1)
	for i, ss := range failed {
		if i == maxFailedStepsNamed {
			parts = append(parts, fmt.Sprintf("and %d more", len(failed)-maxFailedStepsNamed))
			break
		}
		part := "step " + ss.Name
		if e := truncateRunes(strings.Join(strings.Fields(ss.Error), " "), maxStepErrorRunes); e != "" {
			part += ": " + e
		}
		parts = append(parts, part)
	}
	return strings.Join(parts, "; ")
}

// truncateRunes cuts s to at most n runes, adding an ellipsis when it cut.
func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}
