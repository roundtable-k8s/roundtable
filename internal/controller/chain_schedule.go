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
	"time"

	"github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// handleRunTriggers processes the run-now and retry-failed annotations. It
// returns handled=true when it changed (or deliberately consumed) state and the
// caller should return the given result.
func (r *ChainReconciler) handleRunTriggers(ctx context.Context, chain *aiv1alpha1.Chain) (bool, ctrl.Result, error) {
	log := logf.FromContext(ctx)
	runVal := chain.Annotations[aiv1alpha1.AnnotationChainRun]
	retryVal := chain.Annotations[aiv1alpha1.AnnotationChainRetryFailed]
	runPending := runVal != "" && runVal != chain.Status.LastRunTrigger
	retryPending := retryVal != "" && retryVal != chain.Status.LastRetryTrigger
	if !runPending && !retryPending {
		return false, ctrl.Result{}, nil
	}

	// Never clobber a run in flight: leave the trigger pending; it fires once
	// the run finishes (the status update re-queues this chain).
	if chain.Status.Phase == aiv1alpha1.ChainPhaseRunning {
		return false, ctrl.Result{}, nil
	}

	// A new run or a resume leaves the previous cancellation behind.
	meta.RemoveStatusCondition(&chain.Status.Conditions, aiv1alpha1.ConditionChainCancelled)

	now := metav1.Now()
	if runPending {
		r.initStepStatuses(chain)
		meta.RemoveStatusCondition(&chain.Status.Conditions, aiv1alpha1.ConditionNotificationSent)
		meta.RemoveStatusCondition(&chain.Status.Conditions, aiv1alpha1.ConditionChainComplete)
		chain.Status.RunID = string(uuid.NewUUID())
		chain.Status.Phase = aiv1alpha1.ChainPhaseRunning
		chain.Status.StartedAt = &now
		chain.Status.CompletedAt = nil
		resetRunTrace(chain)
		chain.Status.LastRunTrigger = runVal
		if retryPending {
			chain.Status.LastRetryTrigger = retryVal // run-now supersedes retry
		}
		chain.Status.ObservedGeneration = chain.Generation
		r.Recorder.Eventf(chain, corev1.EventTypeNormal, "RunTriggered", "New run %s started by %s=%s", chain.Status.RunID, aiv1alpha1.AnnotationChainRun, runVal)
		log.Info("Started new run from annotation", "runId", chain.Status.RunID, "value", runVal)
		res, err := r.updateStatus(ctx, chain, RequeueFast)
		return true, res, err
	}

	// retry-failed only: resume the current run from its failed/skipped steps.
	chain.Status.LastRetryTrigger = retryVal
	retried := 0
	specs := make(map[string]*aiv1alpha1.ChainStep, len(chain.Spec.Steps))
	for i := range chain.Spec.Steps {
		specs[chain.Spec.Steps[i].Name] = &chain.Spec.Steps[i]
	}
	if chain.Status.Phase == aiv1alpha1.ChainPhaseFailed || chain.Status.Phase == aiv1alpha1.ChainPhasePartiallySucceeded {
		for i := range chain.Status.StepStatuses {
			ss := &chain.Status.StepStatuses[i]
			if ss.Phase != aiv1alpha1.ChainStepPhaseFailed && ss.Phase != aiv1alpha1.ChainStepPhaseSkipped {
				continue
			}
			// A fan-out resumes from its failed/skipped items only; finished
			// items are never redone.
			fanOut := ss.FanOut
			if fanOut != nil {
				resetFailedItems(fanOut)
			}
			// A loop resumes in the round it stopped in (a failed body step
			// re-runs) or, when exhausted, gets another maxIterations rounds.
			loop := resetLoopForRetry(ss.Loop, specs[ss.Name])
			// The attempts already run keep their cost and tokens: the run
			// totals and the Mission budget include them, as the metrics do.
			costUSD, tokens := ss.CostUSD, ss.Tokens.DeepCopy()
			*ss = aiv1alpha1.ChainStepStatus{Name: ss.Name, Phase: aiv1alpha1.ChainStepPhasePending, FanOut: fanOut, Loop: loop, CostUSD: costUSD, Tokens: tokens}
			retried++
		}
	}
	if retried == 0 {
		r.Recorder.Eventf(chain, corev1.EventTypeWarning, "RetryIgnored", "%s=%s ignored: no failed steps to retry", aiv1alpha1.AnnotationChainRetryFailed, retryVal)
		res, err := r.updateStatus(ctx, chain, 0)
		return true, res, err
	}
	meta.RemoveStatusCondition(&chain.Status.Conditions, aiv1alpha1.ConditionNotificationSent)
	meta.RemoveStatusCondition(&chain.Status.Conditions, aiv1alpha1.ConditionChainComplete)
	if chain.Status.RunID == "" {
		chain.Status.RunID = string(uuid.NewUUID())
	}
	chain.Status.Phase = aiv1alpha1.ChainPhaseRunning
	// The overall timeout applies to the resumed leg, not the whole old run.
	chain.Status.StartedAt = &now
	chain.Status.CompletedAt = nil
	// The previous leg's trace ended with it; the resumed leg gets its own.
	resetRunTrace(chain)
	chain.Status.ObservedGeneration = chain.Generation
	r.Recorder.Eventf(chain, corev1.EventTypeNormal, "RetryTriggered", "Retrying %d failed step(s) of run %s", retried, chain.Status.RunID)
	res, err := r.updateStatus(ctx, chain, RequeueFast)
	return true, res, err
}

// reconcileSchedule manages the cron schedule for the chain. It returns true
// if a scheduled fire was missed (e.g. the operator was down) and a catch-up
// run should be triggered.
func (r *ChainReconciler) reconcileSchedule(ctx context.Context, chain *aiv1alpha1.Chain) bool {
	key := chain.Namespace + "/" + chain.Name

	if chain.Spec.Schedule == "" || chain.Spec.Suspend {
		r.removeCronEntry(types.NamespacedName{Namespace: chain.Namespace, Name: chain.Name})
		chain.Status.NextScheduledAt = nil
		return false
	}

	sched, err := parseSchedule(chain)
	if err != nil {
		logf.FromContext(ctx).Error(err, "Invalid schedule", "schedule", chain.Spec.Schedule)
		return false
	}
	spec := cronSpec(chain)

	r.mu.Lock()

	if r.cron == nil {
		r.cron = cron.New()
		r.cronEntries = make(map[string]cron.EntryID)
		r.cron.Start()
	}
	if r.cronSpecs == nil {
		r.cronSpecs = make(map[string]string)
	}

	// Re-register when the schedule or time zone was edited.
	if id, ok := r.cronEntries[key]; ok && r.cronSpecs[key] != spec {
		r.cron.Remove(id)
		delete(r.cronEntries, key)
	}

	if _, ok := r.cronEntries[key]; !ok {
		nn := types.NamespacedName{Namespace: chain.Namespace, Name: chain.Name}
		entryID, err := r.cron.AddFunc(spec, func() {
			r.triggerChain(context.Background(), nn)
		})
		if err != nil {
			r.mu.Unlock()
			logf.FromContext(ctx).Error(err, "Failed to add cron schedule", "schedule", chain.Spec.Schedule)
			return false
		}
		r.cronEntries[key] = entryID
		r.cronSpecs[key] = spec
	}
	r.mu.Unlock()

	next := metav1.NewTime(sched.Next(time.Now()))
	chain.Status.NextScheduledAt = &next

	return r.missedSchedule(chain)
}

// cronSpec returns the cron expression prefixed with the chain's time zone
// (robfig/cron's "CRON_TZ=" syntax). An unset zone means UTC.
func cronSpec(chain *aiv1alpha1.Chain) string {
	return cronExpr(chain.Spec.Schedule, chain.Spec.TimeZone)
}

// cronExpr prefixes schedule with its time zone; an unset zone means UTC.
func cronExpr(schedule, tz string) string {
	if tz == "" {
		tz = "UTC"
	}
	return "CRON_TZ=" + tz + " " + schedule
}

// parseSchedule parses the chain's schedule in its time zone. It returns a
// nil schedule and no error when the chain has no schedule.
func parseSchedule(chain *aiv1alpha1.Chain) (cron.Schedule, error) {
	return parseCron(chain.Spec.Schedule, chain.Spec.TimeZone, "spec.timeZone")
}

// parseCron parses a cron schedule in time zone tz (tzField names the field
// that sets the zone, for error messages). It returns a nil schedule and no
// error when schedule is empty. Shared by Chain.spec.schedule and schedule
// Triggers.
func parseCron(schedule, tz, tzField string) (cron.Schedule, error) {
	if tz != "" {
		if tz == "Local" {
			return nil, fmt.Errorf("invalid timeZone %q: must be an IANA time zone name such as \"America/New_York\"", tz)
		}
		if _, err := time.LoadLocation(tz); err != nil {
			return nil, fmt.Errorf("invalid timeZone %q: must be an IANA time zone name such as \"America/New_York\": %w", tz, err)
		}
	}
	if schedule == "" {
		return nil, nil
	}
	if strings.Contains(schedule, "TZ=") {
		return nil, fmt.Errorf("invalid schedule %q: set the time zone with %s instead of a TZ= prefix", schedule, tzField)
	}
	sched, err := cron.ParseStandard(cronExpr(schedule, tz))
	if err != nil {
		return nil, fmt.Errorf("invalid schedule %q: %w", schedule, err)
	}
	return sched, nil
}

// missedSchedule reports whether the chain's next fire after lastScheduledAt
// has already passed without a run starting, within the optional
// startingDeadlineSeconds window.
func (r *ChainReconciler) missedSchedule(chain *aiv1alpha1.Chain) bool {
	if chain.Status.LastScheduledAt == nil || chain.Status.Phase == aiv1alpha1.ChainPhaseRunning {
		return false
	}

	sched, err := parseSchedule(chain)
	if err != nil || sched == nil {
		return false
	}

	expected := sched.Next(chain.Status.LastScheduledAt.Time)
	now := time.Now()
	if !expected.Before(now) {
		return false
	}
	if dl := chain.Spec.StartingDeadlineSeconds; dl != nil && now.Sub(expected) > time.Duration(*dl)*time.Second {
		return false
	}
	return true
}

// triggerChain starts a new chain run: it resets step statuses, assigns a
// fresh run ID, and sets the phase to Running. Called from cron goroutines
// (with context.Background()) and from reconcile for missed-schedule catch-up.
func (r *ChainReconciler) triggerChain(ctx context.Context, nn types.NamespacedName) {
	log := logf.Log.WithName("chain-cron")

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		chain := &aiv1alpha1.Chain{}
		if err := r.Get(ctx, nn, chain); err != nil {
			return err
		}

		if chain.Spec.Suspend {
			return nil
		}

		// Guard against overlapping runs: resetting step statuses while a
		// previous run is still in flight orphans its in-progress steps and
		// lets stale outputs masquerade as results for the new run.
		if chain.Status.Phase == aiv1alpha1.ChainPhaseRunning {
			log.Info("Skipping cron trigger, previous run still in progress", "chain", nn.String())
			r.Recorder.Event(chain, corev1.EventTypeWarning, "CronTriggerSkipped",
				"Skipped scheduled trigger: previous run still in progress")
			return nil
		}

		r.initStepStatuses(chain)
		// A new run gets its own completion notification.
		meta.RemoveStatusCondition(&chain.Status.Conditions, aiv1alpha1.ConditionNotificationSent)
		now := metav1.Now()
		chain.Status.RunID = string(uuid.NewUUID())
		chain.Status.Phase = aiv1alpha1.ChainPhaseRunning
		chain.Status.StartedAt = &now
		chain.Status.CompletedAt = nil
		chain.Status.LastScheduledAt = &now
		resetRunTrace(chain)

		if err := r.Status().Update(ctx, chain); err != nil {
			return err
		}
		r.Recorder.Event(chain, corev1.EventTypeNormal, "CronTriggered", "Chain triggered by cron schedule")
		return nil
	})
	if err != nil {
		log.Error(err, "Failed to trigger chain", "chain", nn.String())
	}
}

// removeCronEntry removes a cron entry for a chain.
func (r *ChainReconciler) removeCronEntry(nn types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := nn.Namespace + "/" + nn.Name
	if r.cronEntries == nil {
		return
	}
	if id, ok := r.cronEntries[key]; ok {
		if r.cron != nil {
			r.cron.Remove(id)
		}
		delete(r.cronEntries, key)
		delete(r.cronSpecs, key)
	}
}
