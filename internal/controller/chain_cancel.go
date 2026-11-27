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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/pkg/metrics"
)

// handleCancel acts on the ai.roundtable.io/cancel annotation. On an active
// run (running, or a run trigger not yet started) it cancels the run; on
// a chain without one it just drops the annotation, so a stale cancel can't
// kill a later run. Terminal runs are left as they are.
func (r *ChainReconciler) handleCancel(ctx context.Context, chain *aiv1alpha1.Chain) (bool, ctrl.Result, error) {
	reason, ok := chain.Annotations[aiv1alpha1.AnnotationChainCancel]
	if !ok {
		return false, ctrl.Result{}, nil
	}
	runVal := chain.Annotations[aiv1alpha1.AnnotationChainRun]
	pending := runVal != "" && runVal != chain.Status.LastRunTrigger
	if chain.Status.Phase != aiv1alpha1.ChainPhaseRunning && !pending {
		return true, ctrl.Result{}, r.clearCancelAnnotation(ctx, chain)
	}

	if chain.Status.Phase != aiv1alpha1.ChainPhaseRunning {
		// Cancelled before it started: consume the trigger so it never runs.
		if len(chain.Status.StepStatuses) == 0 {
			r.initStepStatuses(chain)
		}
		chain.Status.LastRunTrigger = runVal
		chain.Status.RunID = string(uuid.NewUUID())
		now := metav1.Now()
		chain.Status.StartedAt = &now
	}
	r.cancelRun(ctx, chain, reason)
	// Mark the current generation observed, as the timeout path does: a spec
	// edit made during the run must not make the chain look changed and reset
	// it to Idle after the cancel.
	chain.Status.ObservedGeneration = chain.Generation
	if err := r.Status().Update(ctx, chain); err != nil {
		if apierrors.IsConflict(err) {
			return true, ctrl.Result{RequeueAfter: RequeueFast}, nil
		}
		return true, ctrl.Result{}, err
	}
	return true, ctrl.Result{}, r.clearCancelAnnotation(ctx, chain)
}

// clearCancelAnnotation removes the cancel annotation once it was handled.
func (r *ChainReconciler) clearCancelAnnotation(ctx context.Context, chain *aiv1alpha1.Chain) error {
	patch := client.MergeFrom(chain.DeepCopy())
	delete(chain.Annotations, aiv1alpha1.AnnotationChainCancel)
	return client.IgnoreNotFound(r.Patch(ctx, chain, patch))
}

// cancelRun fails every unfinished step with the cancellation reason and ends
// the run Failed with Cancelled=True. Nothing is dispatched afterwards: the
// chain is terminal. Tasks already published to a knight are not recalled.
func (r *ChainReconciler) cancelRun(ctx context.Context, chain *aiv1alpha1.Chain, reason string) {
	msg := "cancelled"
	if reason != "" {
		msg = "cancelled: " + reason
	}
	now := metav1.Now()
	cancelled := 0
	for i := range chain.Status.StepStatuses {
		ss := &chain.Status.StepStatuses[i]
		switch ss.Phase {
		case aiv1alpha1.ChainStepPhasePending, aiv1alpha1.ChainStepPhaseRunning, aiv1alpha1.ChainStepPhaseDeferred, "":
		default:
			continue
		}
		if ss.FanOut != nil {
			skipPendingItems(ss.FanOut)
			compactItemDetails(ss.FanOut)
		}
		if ss.Loop != nil {
			ss.Loop.Outcome = aiv1alpha1.ChainLoopFailed
		}
		ss.Phase = aiv1alpha1.ChainStepPhaseFailed
		ss.Error = msg
		ss.CompletedAt = &now
		cancelled++
	}

	chain.Status.Phase = aiv1alpha1.ChainPhaseFailed
	chain.Status.CompletedAt = &now
	chain.Status.RunsFailed++
	meta.SetStatusCondition(&chain.Status.Conditions, metav1.Condition{
		Type:               aiv1alpha1.ConditionChainComplete,
		Status:             metav1.ConditionTrue,
		Reason:             aiv1alpha1.ReasonChainCancelled,
		Message:            msg,
		ObservedGeneration: chain.Generation,
	})
	meta.SetStatusCondition(&chain.Status.Conditions, metav1.Condition{
		Type:               aiv1alpha1.ConditionChainCancelled,
		Status:             metav1.ConditionTrue,
		Reason:             aiv1alpha1.ReasonChainCancelled,
		Message:            msg,
		ObservedGeneration: chain.Generation,
	})
	r.Recorder.Eventf(chain, corev1.EventTypeWarning, "RunCancelled", "Run cancelled (%d step(s) unfinished): %s", cancelled, msg)
	logf.FromContext(ctx).Info("Chain run cancelled", "runId", chain.Status.RunID, "steps", cancelled, "reason", reason)
	r.recordRunOutcome(ctx, chain, "failed")
}
