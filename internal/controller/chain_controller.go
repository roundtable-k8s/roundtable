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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/roundtable-k8s/roundtable/internal/util"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
	"github.com/roundtable-k8s/roundtable/internal/notify"
	"github.com/roundtable-k8s/roundtable/pkg/metrics"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

const (
	chainFinalizer = "ai.roundtable.io/chain-finalizer"
)

// natsConfig holds resolved NATS configuration for a chain's target RoundTable.
type natsConfig struct {
	SubjectPrefix string // e.g. "table-prefix" or "chelonian"
	TasksStream   string // e.g. "fleet_a_tasks" or "chelonian_tasks"
	ResultsStream string // e.g. "fleet_a_results" or "chelonian_results"
	// ModelAliases is the RoundTable's spec.modelAliases, used to resolve
	// per-step model overrides. Not NATS config, but resolved from the same
	// RoundTable lookup.
	ModelAliases map[string]string
}

// ChainReconciler reconciles a Chain object.
type ChainReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	NATS   *natspkg.Provider
	Notify *notify.Notifier
	// DefaultKnightImage is the operator's DEFAULT_KNIGHT_IMAGE. It is used to
	// infer a knight's agent runtime (claude-knight/pi-knight) when the Knight
	// has no spec.image, for per-step model validation.
	DefaultKnightImage string
	cron               *cron.Cron
	mu                 sync.Mutex
	// cronEntries maps chain namespace/name to cron entry ID
	cronEntries map[string]cron.EntryID
}

// natsClient returns the shared NATS client, or an error if the provider is not configured.
func (r *ChainReconciler) natsClient() (natspkg.Client, error) {
	if r.NATS == nil {
		return nil, fmt.Errorf("NATS provider not configured")
	}
	return r.NATS.Client()
}

// +kubebuilder:rbac:groups=ai.roundtable.io,resources=chains,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=chains/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=chains/finalizers,verbs=update
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=knights,verbs=get;list;watch
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=missions,verbs=get;list;watch
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=roundtables,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

func (r *ChainReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	chain := &aiv1alpha1.Chain{}
	if err := r.Get(ctx, req.NamespacedName, chain); err != nil {
		if client.IgnoreNotFound(err) == nil {
			// Chain is gone — make sure no cron entry outlives it.
			r.removeCronEntry(req.NamespacedName)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Handle deletion
	if chain.DeletionTimestamp != nil {
		r.removeCronEntry(req.NamespacedName)
		chain.Finalizers = util.RemoveString(chain.Finalizers, chainFinalizer)
		if err := r.Update(ctx, chain); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Add finalizer
	if !util.ContainsString(chain.Finalizers, chainFinalizer) {
		chain.Finalizers = append(chain.Finalizers, chainFinalizer)
		if err := r.Update(ctx, chain); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Validate roundTableRef is present
	if aiv1alpha1.RefName(chain.Spec.RoundTableRef) == "" && aiv1alpha1.RefName(chain.Spec.MissionRef) == "" {
		meta.SetStatusCondition(&chain.Status.Conditions, metav1.Condition{
			Type:               aiv1alpha1.ConditionChainValid,
			Status:             metav1.ConditionFalse,
			Reason:             aiv1alpha1.ReasonMissingRoundTableRef,
			Message:            "Chain must have either roundTableRef or missionRef configured",
			ObservedGeneration: chain.Generation,
		})
		chain.Status.Phase = aiv1alpha1.ChainPhaseFailed
		chain.Status.ObservedGeneration = chain.Generation
		if statusErr := r.Status().Update(ctx, chain); statusErr != nil {
			log.Error(statusErr, "Failed to update status during validation error")
		}
		return ctrl.Result{}, fmt.Errorf("chain %s/%s missing roundTableRef or missionRef", chain.Namespace, chain.Name)
	}

	// Validate knight refs
	if err := r.validateKnightRefs(ctx, chain); err != nil {
		// A knight that disappears after the owning mission started cleanup
		// (or is gone / terminal) was deleted BY that cleanup — mission-scoped
		// ephemeral knights are removed before the chain itself is
		// garbage-collected. Reporting InvalidKnightRef then is post-cleanup
		// noise that looks like the dispatch failure cause, so stop
		// reconciling quietly instead of setting conditions and requeuing.
		if apierrors.IsNotFound(err) && r.owningMissionInactive(ctx, chain) {
			log.V(1).Info("Knight missing after owning mission cleanup, stopping reconcile",
				"mission", chain.Labels[aiv1alpha1.LabelMission])
			return ctrl.Result{}, nil
		}
		meta.SetStatusCondition(&chain.Status.Conditions, metav1.Condition{
			Type:               aiv1alpha1.ConditionChainValid,
			Status:             metav1.ConditionFalse,
			Reason:             aiv1alpha1.ReasonInvalidKnightRef,
			Message:            err.Error(),
			ObservedGeneration: chain.Generation,
		})
		chain.Status.ObservedGeneration = chain.Generation
		if statusErr := r.Status().Update(ctx, chain); statusErr != nil {
			log.Error(statusErr, "Failed to update status during validation error")
		}
		return ctrl.Result{}, err
	}

	// Validate DAG
	if err := r.validateDAG(chain); err != nil {
		meta.SetStatusCondition(&chain.Status.Conditions, metav1.Condition{
			Type:               aiv1alpha1.ConditionChainValid,
			Status:             metav1.ConditionFalse,
			Reason:             aiv1alpha1.ReasonCyclicDependency,
			Message:            err.Error(),
			ObservedGeneration: chain.Generation,
		})
		chain.Status.ObservedGeneration = chain.Generation
		if statusErr := r.Status().Update(ctx, chain); statusErr != nil {
			log.Error(statusErr, "Failed to update status during validation error")
		}
		return ctrl.Result{}, err
	}

	// Validate forEach / maxParallel / failurePolicy
	if err := validateForEach(chain); err != nil {
		meta.SetStatusCondition(&chain.Status.Conditions, metav1.Condition{
			Type:               aiv1alpha1.ConditionChainValid,
			Status:             metav1.ConditionFalse,
			Reason:             aiv1alpha1.ReasonInvalidForEach,
			Message:            err.Error(),
			ObservedGeneration: chain.Generation,
		})
		chain.Status.ObservedGeneration = chain.Generation
		if statusErr := r.Status().Update(ctx, chain); statusErr != nil {
			log.Error(statusErr, "Failed to update status during validation error")
		}
		return ctrl.Result{}, err
	}

	// Validate when / loop.until expressions and loop settings
	if reason, err := validateConditionsAndLoops(chain); err != nil {
		meta.SetStatusCondition(&chain.Status.Conditions, metav1.Condition{
			Type:               aiv1alpha1.ConditionChainValid,
			Status:             metav1.ConditionFalse,
			Reason:             reason,
			Message:            err.Error(),
			ObservedGeneration: chain.Generation,
		})
		chain.Status.ObservedGeneration = chain.Generation
		if statusErr := r.Status().Update(ctx, chain); statusErr != nil {
			log.Error(statusErr, "Failed to update status during validation error")
		}
		return ctrl.Result{}, err
	}

	// Validate templates parse correctly
	if err := r.validateTemplates(chain); err != nil {
		meta.SetStatusCondition(&chain.Status.Conditions, metav1.Condition{
			Type:               aiv1alpha1.ConditionChainValid,
			Status:             metav1.ConditionFalse,
			Reason:             aiv1alpha1.ReasonInvalidTemplate,
			Message:            err.Error(),
			ObservedGeneration: chain.Generation,
		})
		chain.Status.ObservedGeneration = chain.Generation
		if statusErr := r.Status().Update(ctx, chain); statusErr != nil {
			log.Error(statusErr, "Failed to update status during validation error")
		}
		return ctrl.Result{}, err
	}

	meta.SetStatusCondition(&chain.Status.Conditions, metav1.Condition{
		Type:               aiv1alpha1.ConditionChainValid,
		Status:             metav1.ConditionTrue,
		Reason:             aiv1alpha1.ReasonChainValid,
		Message:            "Chain spec is valid",
		ObservedGeneration: chain.Generation,
	})

	// Handle schedule, catching up a missed fire (e.g. operator downtime)
	if r.reconcileSchedule(ctx, chain) {
		log.Info("Missed scheduled run detected, triggering catch-up")
		r.triggerChain(ctx, req.NamespacedName)
		return ctrl.Result{Requeue: true}, nil
	}

	// Handle suspended
	if chain.Spec.Suspend {
		chain.Status.Phase = aiv1alpha1.ChainPhaseSuspended
		chain.Status.ObservedGeneration = chain.Generation
		return r.updateStatus(ctx, chain, 0)
	}

	// Initialize status if empty
	if chain.Status.Phase == "" {
		chain.Status.Phase = aiv1alpha1.ChainPhaseIdle
		r.initStepStatuses(chain)
		chain.Status.ObservedGeneration = chain.Generation
		return r.updateStatus(ctx, chain, 0)
	}

	// Reset to Idle when spec changes (generation drift) and chain is not running
	if chain.Status.ObservedGeneration != chain.Generation &&
		chain.Status.Phase != aiv1alpha1.ChainPhaseRunning {
		log.Info("Spec changed, resetting chain to Idle",
			"oldGen", chain.Status.ObservedGeneration,
			"newGen", chain.Generation)
		chain.Status.Phase = aiv1alpha1.ChainPhaseIdle
		r.initStepStatuses(chain)
		// A new run gets its own completion notification.
		meta.RemoveStatusCondition(&chain.Status.Conditions, aiv1alpha1.ConditionNotificationSent)

		// A spec change (e.g. new input) never resumes the previous run: drop
		// its identity so cached outputs from it can't be picked up.
		chain.Status.RunID = ""
		chain.Status.StartedAt = nil
		chain.Status.CompletedAt = nil

		chain.Status.ObservedGeneration = chain.Generation
		return r.updateStatus(ctx, chain, 0)
	}

	// Handle run-now / retry-failed annotations.
	if handled, res, err := r.handleRunTriggers(ctx, chain); handled {
		return res, err
	}

	switch chain.Status.Phase {
	case aiv1alpha1.ChainPhaseIdle:
		// Nothing to do unless triggered (manual trigger sets phase to Running externally)
		return ctrl.Result{}, nil

	case aiv1alpha1.ChainPhaseRunning:
		return r.reconcileRunning(ctx, chain)

	case aiv1alpha1.ChainPhaseSucceeded, aiv1alpha1.ChainPhaseFailed, aiv1alpha1.ChainPhasePartiallySucceeded:
		// Terminal — only a pending completion notification still needs work.
		// Notification state never affects the phase itself.
		if notificationPending(chain.Spec.Notify, chain.Status.Conditions) {
			completedAt := notifyCompletedAt(chain.Status.CompletedAt, chain.Status.Conditions, aiv1alpha1.ConditionChainComplete)
			requeue := deliverNotification(ctx, r.Client, r.Recorder, r.Notify, chain,
				&chain.Status.Conditions, chain.Generation, completedAt, chainNotifyPayload(chain))
			return r.updateStatus(ctx, chain, requeue)
		}
		return ctrl.Result{}, nil

	case aiv1alpha1.ChainPhaseSuspended:
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, nil
}

// updateStatus writes the chain status, converting optimistic-concurrency
// conflicts into a requeue instead of a reconcile error. On success the
// result carries requeueAfter (zero means no requeue).
func (r *ChainReconciler) updateStatus(ctx context.Context, chain *aiv1alpha1.Chain, requeueAfter time.Duration) (ctrl.Result, error) {
	if err := r.Status().Update(ctx, chain); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

// validateKnightRefs checks that all knightRef values resolve to Knight CRs.
func (r *ChainReconciler) validateKnightRefs(ctx context.Context, chain *aiv1alpha1.Chain) error {
	check := func(stepName, knightRef string) error {
		if knightRef == "" {
			return fmt.Errorf("step %q has no knightRef", stepName)
		}
		knight := &aiv1alpha1.Knight{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      knightRef,
			Namespace: chain.Namespace,
		}, knight); err != nil {
			return fmt.Errorf("step %q references non-existent knight %q: %w", stepName, knightRef, err)
		}
		return nil
	}
	for _, step := range chain.Spec.Steps {
		if step.Loop == nil {
			if err := check(step.Name, step.KnightRef.Name); err != nil {
				return err
			}
			continue
		}
		for _, b := range step.Loop.Steps {
			if err := check(step.Name+"/"+b.Name, b.KnightRef.Name); err != nil {
				return err
			}
		}
	}
	return nil
}

// owningMissionInactive reports whether the chain belongs to a mission that no
// longer needs it reconciled: the mission is gone, being deleted, cleaning up,
// or already in a terminal phase (Succeeded/Failed/Expired). Mission cleanup
// deletes the mission's ephemeral knights before the chain is garbage-collected,
// so a chain can transiently reference knights that were legitimately removed.
// A missing knight while the mission is still active (pre-dispatch) is a real
// error and is NOT covered here.
func (r *ChainReconciler) owningMissionInactive(ctx context.Context, chain *aiv1alpha1.Chain) bool {
	missionName := chain.Labels[aiv1alpha1.LabelMission]
	if missionName == "" {
		return false
	}
	mission := &aiv1alpha1.Mission{}
	if err := r.Get(ctx, types.NamespacedName{Name: missionName, Namespace: chain.Namespace}, mission); err != nil {
		// Mission deleted entirely — the chain is an orphan awaiting GC.
		return apierrors.IsNotFound(err)
	}
	if mission.DeletionTimestamp != nil {
		return true
	}
	switch mission.Status.Phase {
	case aiv1alpha1.MissionPhaseCleaningUp,
		aiv1alpha1.MissionPhaseSucceeded,
		aiv1alpha1.MissionPhaseFailed,
		aiv1alpha1.MissionPhaseExpired:
		return true
	}
	return false
}

// validateDAG performs topological sort to detect cycles.
// validateTemplates pre-parses all step task templates to catch syntax errors early.
// Also warns about common mistakes like using lowercase field names.
func (r *ChainReconciler) validateTemplates(chain *aiv1alpha1.Chain) error {
	// Dry-run execute with mock data to catch field access errors
	mockSteps := make(map[string]map[string]string)
	for _, s := range chain.Spec.Steps {
		mockSteps[s.Name] = stepTemplateData(aiv1alpha1.ChainStepStatus{})
	}
	check := func(name, task string, extra map[string]any) error {
		if !strings.Contains(task, "{{") {
			return nil
		}
		tmpl, err := template.New("validate").Parse(task)
		if err != nil {
			return fmt.Errorf("step %q has invalid template: %w", name, err)
		}
		mockData := map[string]any{
			"Steps":  mockSteps,
			"Input":  "",
			"Fields": map[string]any{},
		}
		maps.Copy(mockData, extra)
		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, mockData); err != nil {
			return fmt.Errorf("step %q template execution error (hint: use .Steps.stepname.Output not steps.stepname.output): %w", name, err)
		}
		return nil
	}
	for _, step := range chain.Spec.Steps {
		var extra map[string]any
		if step.ForEach != nil {
			extra = map[string]any{"Item": map[string]any{}, "item": map[string]any{}, "ItemIndex": 0, "ItemJSON": "{}"}
		}
		if err := check(step.Name, step.Task, extra); err != nil {
			return err
		}
		if step.Loop == nil {
			continue
		}
		body := make(map[string]map[string]string, len(step.Loop.Steps))
		for _, b := range step.Loop.Steps {
			body[b.Name] = map[string]string{"Output": "", "Error": "", "Phase": ""}
		}
		loopData := map[string]any{"Loop": map[string]any{
			"Iteration": 1, "MaxIterations": 1, "Steps": body,
			"Last": map[string]string{"Step": "", "Phase": "", "Output": "", "Error": ""},
		}}
		for _, b := range step.Loop.Steps {
			if err := check(step.Name+"/"+b.Name, b.Task, loopData); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *ChainReconciler) validateDAG(chain *aiv1alpha1.Chain) error {
	// Convert ChainSteps to DAGNodes
	nodes := make([]util.DAGNode, len(chain.Spec.Steps))
	for i, step := range chain.Spec.Steps {
		nodes[i] = util.DAGNode{
			Name:      step.Name,
			DependsOn: step.DependsOn,
		}
	}
	return util.ValidateDAG(nodes)
}

// initStepStatuses initializes step status entries for all steps.
func (r *ChainReconciler) initStepStatuses(chain *aiv1alpha1.Chain) {
	chain.Status.StepStatuses = make([]aiv1alpha1.ChainStepStatus, len(chain.Spec.Steps))
	for i, step := range chain.Spec.Steps {
		chain.Status.StepStatuses[i] = aiv1alpha1.ChainStepStatus{
			Name:  step.Name,
			Phase: aiv1alpha1.ChainStepPhasePending,
		}
	}
}

// reconcileRunning processes the DAG execution for a running chain.
func (r *ChainReconciler) reconcileRunning(ctx context.Context, chain *aiv1alpha1.Chain) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Resolve NATS config from the chain's RoundTable reference
	nc, err := r.resolveNATSConfig(ctx, chain)
	if err != nil {
		log.Error(err, "Failed to resolve NATS config for chain")
		return ctrl.Result{}, err
	}

	// Initialize step statuses and startedAt if missing (manual trigger via status patch)
	if len(chain.Status.StepStatuses) == 0 {
		log.Info("Initializing step statuses for manually triggered chain")
		r.initStepStatuses(chain)
		// A new run gets its own completion notification.
		meta.RemoveStatusCondition(&chain.Status.Conditions, aiv1alpha1.ConditionNotificationSent)

		// A manual trigger starts a new run with its own identity, so KV
		// restore below only picks up outputs this run produced (none yet) —
		// stale outputs from earlier runs can no longer masquerade as results.
		chain.Status.RunID = string(uuid.NewUUID())

		// Attempt to restore completed steps from NATS KV (resume capability)
		restored := r.restoreStepOutputsFromKV(ctx, chain)
		if restored > 0 {
			log.Info("Restored step outputs from NATS KV", "restored", restored)
		}

		now := metav1.Now()
		chain.Status.StartedAt = &now
		chain.Status.ObservedGeneration = chain.Generation
		return r.updateStatus(ctx, chain, RequeueFast)
	}

	// Runs started before run identity existed get an ID on first reconcile.
	if chain.Status.RunID == "" {
		chain.Status.RunID = string(uuid.NewUUID())
	}

	if chain.Status.StartedAt == nil {
		now := metav1.Now()
		chain.Status.StartedAt = &now
		r.Recorder.Event(chain, corev1.EventTypeNormal, "Started", "Chain execution started")
	}

	// Check overall timeout
	if chain.Status.StartedAt != nil {
		elapsed := time.Since(chain.Status.StartedAt.Time)
		if elapsed > time.Duration(chain.Spec.TimeoutSeconds)*time.Second {
			log.Info("Chain timed out", "elapsed", elapsed)
			chain.Status.Phase = aiv1alpha1.ChainPhaseFailed
			now := metav1.Now()
			chain.Status.CompletedAt = &now
			chain.Status.RunsFailed++
			meta.SetStatusCondition(&chain.Status.Conditions, metav1.Condition{
				Type:               aiv1alpha1.ConditionChainComplete,
				Status:             metav1.ConditionTrue,
				Reason:             aiv1alpha1.ReasonChainTimeout,
				Message:            fmt.Sprintf("Chain timed out after %ds", chain.Spec.TimeoutSeconds),
				ObservedGeneration: chain.Generation,
			})
			r.Recorder.Eventf(chain, corev1.EventTypeWarning, "Failed", "Chain timed out after %ds", chain.Spec.TimeoutSeconds)
			chain.Status.ObservedGeneration = chain.Generation
			return ctrl.Result{}, r.Status().Update(ctx, chain)
		}
	}

	// Build step status map
	statusMap := make(map[string]*aiv1alpha1.ChainStepStatus)
	for i := range chain.Status.StepStatuses {
		statusMap[chain.Status.StepStatuses[i].Name] = &chain.Status.StepStatuses[i]
	}

	// Build spec step map
	specMap := make(map[string]*aiv1alpha1.ChainStep)
	for i := range chain.Spec.Steps {
		specMap[chain.Spec.Steps[i].Name] = &chain.Spec.Steps[i]
	}

	// A chain that already has a hard failure starts no further loop rounds.
	failing := hasHardFailure(chain, specMap)

	// Check for completed running steps (poll NATS results)
	for i := range chain.Status.StepStatuses {
		ss := &chain.Status.StepStatuses[i]
		if ss.Phase == aiv1alpha1.ChainStepPhaseRunning {
			if spec := specMap[ss.Name]; spec != nil && spec.ForEach != nil {
				r.reconcileFanOut(ctx, nc, chain, spec, ss)
				continue
			}
			if spec := specMap[ss.Name]; spec != nil && spec.Loop != nil {
				r.reconcileLoop(ctx, nc, chain, spec, ss, failing)
				continue
			}
			// Skip polling if no taskID — step was set to Running by a previous
			// operator version or before the status was persisted. Requeue will
			// handle it once the taskID is saved.
			if ss.TaskID == "" {
				log.Info("Step running but no taskID — skipping poll, will requeue", "step", ss.Name)
				continue
			}
			// The step timeout runs from knight pickup, not from publish, so
			// time spent queued behind a busy knight doesn't eat the budget.
			spec := specMap[ss.Name]
			r.detectPickup(ctx, nc, ss, specMap[ss.Name])
			if ss.StartedAt != nil && spec != nil {
				now := metav1.Now()
				if ss.PickedUpAt != nil {
					if time.Since(ss.PickedUpAt.Time) > time.Duration(spec.TimeoutSeconds)*time.Second {
						log.Info("Step timed out", "step", ss.Name)
						ss.Phase = aiv1alpha1.ChainStepPhaseFailed
						ss.Error = fmt.Sprintf("step timed out after %ds", spec.TimeoutSeconds)
						ss.CompletedAt = &now
						continue
					}
				} else if wait := maxQueueWait(chain); time.Since(ss.StartedAt.Time) > wait {
					log.Info("Step not picked up in time", "step", ss.Name)
					ss.Phase = aiv1alpha1.ChainStepPhaseFailed
					ss.Error = fmt.Sprintf("task not picked up by knight within %ds (max queue wait)", int64(wait.Seconds()))
					ss.CompletedAt = &now
					continue
				}
			}

			// Try to get result from NATS
			result, err := r.pollResult(ctx, nc, chain.Name, ss.Name, ss.TaskID)
			if err != nil {
				log.Error(err, "Failed to poll result", "step", ss.Name)
				continue
			}
			if result != nil {
				now := metav1.Now()
				ss.CompletedAt = &now
				if result.Model != "" {
					ss.Model = result.Model
				}
				resultErr := result.GetError()
				resultOutput := result.GetOutput()
				if resultErr == "" && isEmptyStepOutput(resultOutput) {
					resultErr = errEmptyOutput
					r.Recorder.Eventf(chain, corev1.EventTypeWarning, "StepEmptyOutput",
						"Step %s returned empty output, treating as failure", ss.Name)
				}
				if resultErr != "" {
					ss.Phase = aiv1alpha1.ChainStepPhaseFailed
					ss.Error = resultErr
					// Check retry (per-step policy overrides chain-level)
					retryPolicy := chain.Spec.RetryPolicy
					if spec != nil && spec.Retry != nil {
						retryPolicy = &aiv1alpha1.ChainRetryPolicy{
							MaxRetries:     spec.Retry.MaxAttempts,
							BackoffSeconds: spec.Retry.BackoffSeconds,
						}
					}
					if retryPolicy != nil && ss.Retries < retryPolicy.MaxRetries {
						ss.Retries++
						ss.Phase = aiv1alpha1.ChainStepPhasePending
						ss.CompletedAt = nil
						ss.Error = ""
						log.Info("Retrying step", "step", ss.Name, "retry", ss.Retries, "maxRetries", retryPolicy.MaxRetries)
					}
				} else if jsonRetry, failMsg := checkJSONOutput(chain, ss, resultOutput); jsonRetry {
					// Later steps parse this output as JSON: re-run once with a reminder.
					_, perr := parseJSONDocument(resultOutput)
					ss.Phase = aiv1alpha1.ChainStepPhasePending
					ss.Reason = stepReasonJSONRetry
					ss.Error = perr.Error()
					ss.CompletedAt = nil
					log.Info("Retrying step: output has no parseable JSON", "step", ss.Name, "error", perr.Error())
					r.Recorder.Eventf(chain, corev1.EventTypeWarning, "StepJSONRetry",
						"Step %s output has no parseable JSON block, retrying once: %v", ss.Name, perr)
				} else if failMsg != "" {
					ss.Phase = aiv1alpha1.ChainStepPhaseFailed
					ss.Error = failMsg
					ss.Output = truncateStatusOutput(resultOutput, chain.Name+"."+ss.Name)
					if spec != nil {
						r.storeStepOutputToKV(ctx, chain.Name, chain.Status.RunID, ss.Name, "", failMsg, spec.KnightRef.Name, ss.StartedAt, &now)
					}
					r.Recorder.Eventf(chain, corev1.EventTypeWarning, "StepInvalidJSON", "Step %s: %s", ss.Name, failMsg)
				} else {
					ss.Phase = aiv1alpha1.ChainStepPhaseSucceeded
					ss.Output = resultOutput
					ss.Reason = ""

					r.Recorder.Eventf(chain, corev1.EventTypeNormal, "StepCompleted", "Step %s completed", ss.Name)

					// Store full output to NATS KV (best-effort)
					if spec := specMap[ss.Name]; spec != nil {
						r.storeStepOutputToKV(ctx, chain.Name, chain.Status.RunID, ss.Name, resultOutput, resultErr, spec.KnightRef.Name, ss.StartedAt, &now, result.Structured())
					}

					// Truncate CRD status output to avoid etcd bloat (4000 chars allows
					// meaningful summaries for template resolution while staying well
					// under etcd's 1.5MB object limit — 10 steps × 4KB = 40KB max)
					ss.Output = truncateStatusOutput(ss.Output, chain.Name+"."+ss.Name)

					// Best-effort artifact write if outputPath is set
					if spec != nil && spec.OutputPath != "" {
						outputPath, err := r.renderOutputPath(chain, spec)
						if err != nil {
							log.Error(err, "Failed to render outputPath", "step", ss.Name)
						} else {
							if err := r.writeArtifact(ctx, nc, chain, spec.Name, outputPath, resultOutput); err != nil {
								log.Error(err, "Failed to dispatch artifact write", "step", ss.Name, "path", outputPath)
							} else {
								log.Info("Dispatched artifact write", "step", ss.Name, "path", outputPath)
							}
						}
					}
				}
			}
		}
	}

	// Find ready steps and publish
	for i := range chain.Spec.Steps {
		step := &chain.Spec.Steps[i]
		ss := statusMap[step.Name]
		if ss.Phase != aiv1alpha1.ChainStepPhasePending {
			continue
		}

		// Check if retry backoff applies (per-step policy overrides chain-level)
		if ss.Retries > 0 && ss.CompletedAt != nil {
			retryPolicy := chain.Spec.RetryPolicy
			spec := specMap[step.Name]
			if spec != nil && spec.Retry != nil {
				retryPolicy = &aiv1alpha1.ChainRetryPolicy{
					BackoffSeconds: spec.Retry.BackoffSeconds,
				}
			}
			if retryPolicy != nil {
				backoff := time.Duration(retryPolicy.BackoffSeconds) * time.Second
				if time.Since(ss.CompletedAt.Time) < backoff {
					continue
				}
			}
		}

		// Check dependencies
		ready := true
		for _, dep := range step.DependsOn {
			depStatus := statusMap[dep]
			depSpec := specMap[dep]
			if depStatus == nil {
				ready = false
				break
			}
			switch depStatus.Phase {
			case aiv1alpha1.ChainStepPhaseSucceeded, aiv1alpha1.ChainStepPhaseSkipped:
				// OK — a step skipped by its when expression doesn't block
				// its dependents (a chain-failure skip ends the run anyway).
			case aiv1alpha1.ChainStepPhaseFailed:
				if depSpec != nil && depSpec.ContinueOnFailure {
					// OK
				} else {
					ready = false
				}
			default:
				ready = false
			}
		}
		if !ready {
			continue
		}

		// when: skip the step without dispatching anything. A resumed loop or
		// fan-out (retry-failed) already passed its gate.
		if step.When != "" && ss.Loop == nil && ss.FanOut == nil {
			run, err := r.evaluateWhen(ctx, chain, step)
			if err != nil {
				log.Info("Step when expression failed", "step", step.Name, "error", err.Error())
				ss.Phase = aiv1alpha1.ChainStepPhaseFailed
				ss.Error = fmt.Sprintf("when: %v", err)
				now := metav1.Now()
				ss.CompletedAt = &now
				r.Recorder.Eventf(chain, corev1.EventTypeWarning, "StepWhenError", "Step %s: when: %v", step.Name, err)
				continue
			}
			if !run {
				log.Info("Skipping step, when is false", "step", step.Name)
				skipStep(ss, stepReasonWhenFalse)
				r.Recorder.Eventf(chain, corev1.EventTypeNormal, "StepSkipped", "Step %s skipped: when is false", step.Name)
				continue
			}
		}

		// loop: run the body round after round.
		if step.Loop != nil {
			r.startLoop(chain, step, ss)
			r.reconcileLoop(ctx, nc, chain, step, ss, failing)
			continue
		}

		// forEach: fan out into one task per item.
		if step.ForEach != nil {
			if msg := r.startFanOut(ctx, chain, step, ss); msg != "" {
				r.failFanOutStep(chain, ss, msg)
				continue
			}
			r.reconcileFanOut(ctx, nc, chain, step, ss)
			continue
		}

		// Render task template
		taskStr, err := r.renderTemplate(chain, step.Task)
		if err != nil {
			log.Error(err, "Failed to render template", "step", step.Name)
			ss.Phase = aiv1alpha1.ChainStepPhaseFailed
			ss.Error = fmt.Sprintf("template render error: %v", err)
			now := metav1.Now()
			ss.CompletedAt = &now
			continue
		}

		// Get knight domain
		knight := &aiv1alpha1.Knight{}
		if err := r.Get(ctx, types.NamespacedName{Name: step.KnightRef.Name, Namespace: chain.Namespace}, knight); err != nil {
			log.Error(err, "Failed to get knight", "knightRef", step.KnightRef.Name)
			continue
		}

		// Resolve the per-step model override. A model the knight's runtime
		// can't run is a spec error: fail the step (no retry) rather than let
		// the knight silently fall back to its default model.
		model, err := knightpkg.ResolveModel(step.Model, nc.ModelAliases, knightpkg.EffectiveImage(knight, r.DefaultKnightImage))
		if err != nil {
			log.Info("Rejected step model", "step", step.Name, "knight", step.KnightRef.Name, "reason", err.Error())
			r.Recorder.Eventf(chain, corev1.EventTypeWarning, "InvalidStepModel", "Step %s: %v", step.Name, err)
			ss.Phase = aiv1alpha1.ChainStepPhaseFailed
			ss.Error = fmt.Sprintf("invalid model for knight %q: %v", step.KnightRef.Name, err)
			ss.Model = ""
			now := metav1.Now()
			ss.CompletedAt = &now
			continue
		}

		// The run ID shares the final subject token with the timestamp (joined
		// by "-") so the result subject keeps the same token count and the
		// wildcard fallback in pollResult still matches.
		taskID := fmt.Sprintf("chain-%s-%s.%s-%d", chain.Name, step.Name, chain.Status.RunID, time.Now().UnixMilli())

		// resumeRunId: carry an earlier run's ID so the knight resumes that
		// session. It does not change the chain's own run identity (KV keys,
		// taskId).
		runID := chain.Status.RunID
		if step.ResumeRunID != "" {
			resumed, err := evalString(step.ResumeRunID, r.conditionVars(ctx, chain, step.ResumeRunID))
			if err != nil {
				log.Info("Step resumeRunId expression failed", "step", step.Name, "error", err.Error())
				ss.Phase = aiv1alpha1.ChainStepPhaseFailed
				ss.Error = fmt.Sprintf("resumeRunId: %v", err)
				now := metav1.Now()
				ss.CompletedAt = &now
				r.Recorder.Eventf(chain, corev1.EventTypeWarning, "StepResumeRunIDError", "Step %s: resumeRunId: %v", step.Name, err)
				continue
			}
			if resumed != "" {
				runID = resumed
			}
		}

		if ss.Reason == stepReasonJSONRetry {
			taskStr = jsonRetryTask(taskStr, ss.Error)
		}

		payload := natspkg.TaskPayload{
			TaskID:    taskID,
			ChainName: chain.Name,
			StepName:  step.Name,
			RunID:     runID,
			Task:      taskStr,
			Model:     model,
		}

		seq, err := r.publishTask(nc, knight.Spec.Domain, step.KnightRef.Name, payload)
		if err != nil {
			log.Error(err, "Failed to publish task", "step", step.Name)
			continue
		}

		now := metav1.Now()
		ss.Phase = aiv1alpha1.ChainStepPhaseRunning
		ss.StartedAt = &now
		ss.PickedUpAt = nil
		ss.TaskSeq = seq
		ss.TaskID = taskID
		ss.Model = model
		ss.Error = ""
		log.Info("Published step task", "step", step.Name, "taskId", taskID, "knight", step.KnightRef.Name, "model", model)
	}

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

	// If a step failed without continueOnFailure, skip remaining pending steps and fail chain
	if anyFailed {
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
		stillRunning := false
		for _, ss := range chain.Status.StepStatuses {
			if ss.Phase == aiv1alpha1.ChainStepPhaseRunning {
				stillRunning = true
				break
			}
		}
		if !stillRunning {
			allTerminal = true
		} else {
			allTerminal = false
		}
	}

	if allTerminal {
		now := metav1.Now()
		chain.Status.CompletedAt = &now

		// Count hard failures, soft failures, and successes
		hardFailures := 0
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
				}
			}
		}

		if hardFailures > 0 {
			// At least one hard failure — chain fails
			chain.Status.Phase = aiv1alpha1.ChainPhaseFailed
			chain.Status.RunsFailed++
			meta.SetStatusCondition(&chain.Status.Conditions, metav1.Condition{
				Type:               aiv1alpha1.ConditionChainComplete,
				Status:             metav1.ConditionTrue,
				Reason:             aiv1alpha1.ReasonChainFailed,
				Message:            fmt.Sprintf("%d step(s) failed without continueOnFailure", hardFailures),
				ObservedGeneration: chain.Generation,
			})
			r.Recorder.Eventf(chain, corev1.EventTypeWarning, "Failed", "Chain failed: %d step(s) failed without continueOnFailure", hardFailures)
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
		}

		// A run that never published a single task (every terminal step was
		// restored from cache or skipped) did no real work. That usually means
		// stale KV entries are masking a problem — make it visible.
		executedSteps := 0
		for _, ss := range chain.Status.StepStatuses {
			if ss.TaskID != "" || (ss.FanOut != nil && len(ss.FanOut.Items) > 0) || (ss.Loop != nil && ss.Loop.Iteration > 0) {
				executedSteps++
			}
		}
		if executedSteps == 0 && totalSteps > 0 {
			log.Info("Chain run completed without executing any steps")
			r.Recorder.Event(chain, corev1.EventTypeWarning, "NoStepsExecuted",
				"Chain run completed without executing any steps (all outputs restored from cache)")
			metrics.ChainNoOpRunsTotal.WithLabelValues(chain.Name).Inc()
		}

		chain.Status.ObservedGeneration = chain.Generation
		return r.updateStatus(ctx, chain, 0)
	}

	chain.Status.ObservedGeneration = chain.Generation

	// Requeue to poll for results
	return r.updateStatus(ctx, chain, RequeueDefault)
}

// renderTemplate renders Go templates in the task string with step outputs and input.
func (r *ChainReconciler) renderTemplate(chain *aiv1alpha1.Chain, taskStr string) (string, error) {
	return r.renderTemplateWith(chain, taskStr, nil)
}

// renderTemplateWith renders a task template with extra top-level data
// (forEach items add Item, item, ItemIndex and ItemJSON).
func (r *ChainReconciler) renderTemplateWith(chain *aiv1alpha1.Chain, taskStr string, extra map[string]any) (string, error) {
	if !strings.Contains(taskStr, "{{") {
		return taskStr, nil
	}

	// Build template data
	steps := make(map[string]map[string]string)
	for _, ss := range chain.Status.StepStatuses {
		steps[ss.Name] = stepTemplateData(ss)
	}

	data := map[string]any{
		"Steps":  steps,
		"Input":  chain.Spec.Input,
		"Fields": inputFields(chain.Spec.Input),
	}
	maps.Copy(data, extra)

	tmpl, err := template.New("task").Parse(taskStr)
	if err != nil {
		return "", fmt.Errorf("template parse error: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("template execute error: %w", err)
	}

	return buf.String(), nil
}

// resolveNATSConfig looks up the chain's RoundTable and returns the NATS configuration.
// Falls back to defaultNATSConfig if no roundTableRef is specified.
func (r *ChainReconciler) resolveNATSConfig(ctx context.Context, chain *aiv1alpha1.Chain) (natsConfig, error) {
	if aiv1alpha1.RefName(chain.Spec.RoundTableRef) == "" {
		return natsConfig{}, fmt.Errorf("chain %s/%s has no roundTableRef configured", chain.Namespace, chain.Name)
	}

	rt := &aiv1alpha1.RoundTable{}
	if err := r.Get(ctx, types.NamespacedName{Name: aiv1alpha1.RefName(chain.Spec.RoundTableRef), Namespace: chain.Namespace}, rt); err != nil {
		return natsConfig{}, fmt.Errorf("RoundTable %q not found: %w", aiv1alpha1.RefName(chain.Spec.RoundTableRef), err)
	}

	return natsConfig{
		SubjectPrefix: rt.Spec.NATS.SubjectPrefix,
		TasksStream:   rt.Spec.NATS.TasksStream,
		ResultsStream: rt.Spec.NATS.ResultsStream,
		ModelAliases:  rt.Spec.ModelAliases,
	}, nil
}

// publishTask publishes a task to NATS JetStream.
// It returns the tasks-stream sequence of the published message.
func (r *ChainReconciler) publishTask(nc natsConfig, domain, knightName string, payload natspkg.TaskPayload) (uint64, error) {
	natsCli, err := r.natsClient()
	if err != nil {
		return 0, err
	}

	subject := natspkg.TaskSubject(nc.SubjectPrefix, domain, knightName)
	return natsCli.PublishJSONSeq(subject, payload)
}

// defaultMaxQueueWait bounds how long a task may sit unclaimed by its knight.
const defaultMaxQueueWait = 1800 * time.Second

func maxQueueWait(chain *aiv1alpha1.Chain) time.Duration {
	if chain.Spec.MaxQueueWaitSeconds > 0 {
		return time.Duration(chain.Spec.MaxQueueWaitSeconds) * time.Second
	}
	return defaultMaxQueueWait
}

// detectPickup records ss.PickedUpAt once the knight has picked the task up.
//
// Pickup signal: knights consume the tasks stream through a durable
// "<knight>-consumer" and ack each message immediately on receipt (pi-knight
// delivers at-most-once and processes afterwards), so the consumer's ack floor
// reaching the task's stream sequence means the knight started it. No knight
// runtime change is needed. A step without a recorded sequence (published by
// an older operator) falls back to the legacy behaviour: the clock starts at
// publish time.
func (r *ChainReconciler) detectPickup(ctx context.Context, nc natsConfig, ss *aiv1alpha1.ChainStepStatus, spec *aiv1alpha1.ChainStep) {
	if ss.PickedUpAt != nil || ss.StartedAt == nil {
		return
	}
	if ss.TaskSeq == 0 || spec == nil {
		ss.PickedUpAt = ss.StartedAt
		return
	}
	if r.ackFloorReached(ctx, nc, spec.KnightRef.Name, ss.TaskSeq) {
		now := metav1.Now()
		ss.PickedUpAt = &now
		logf.FromContext(ctx).Info("Knight picked up step task", "step", ss.Name, "queuedFor", now.Sub(ss.StartedAt.Time).Round(time.Second).String())
	}
}

// ackFloorReached reports whether the knight's task consumer has acknowledged
// the tasks-stream message with sequence seq (i.e. picked the task up).
func (r *ChainReconciler) ackFloorReached(ctx context.Context, nc natsConfig, knightRef string, seq uint64) bool {
	natsCli, err := r.natsClient()
	if err != nil {
		return false
	}
	floor, err := natsCli.ConsumerAckFloor(nc.TasksStream, natspkg.KnightTaskConsumerName(knightRef))
	if err != nil {
		logf.FromContext(ctx).V(1).Info("Could not read knight consumer state", "knight", knightRef, "error", err.Error())
		return false
	}
	return floor >= seq
}

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

	now := metav1.Now()
	if runPending {
		r.initStepStatuses(chain)
		meta.RemoveStatusCondition(&chain.Status.Conditions, aiv1alpha1.ConditionNotificationSent)
		meta.RemoveStatusCondition(&chain.Status.Conditions, aiv1alpha1.ConditionChainComplete)
		chain.Status.RunID = string(uuid.NewUUID())
		chain.Status.Phase = aiv1alpha1.ChainPhaseRunning
		chain.Status.StartedAt = &now
		chain.Status.CompletedAt = nil
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
			*ss = aiv1alpha1.ChainStepStatus{Name: ss.Name, Phase: aiv1alpha1.ChainStepPhasePending, FanOut: fanOut, Loop: loop}
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
	chain.Status.ObservedGeneration = chain.Generation
	r.Recorder.Eventf(chain, corev1.EventTypeNormal, "RetryTriggered", "Retrying %d failed step(s) of run %s", retried, chain.Status.RunID)
	res, err := r.updateStatus(ctx, chain, RequeueFast)
	return true, res, err
}

// pollResult checks for a result message for a given chain step.
// If taskID is provided, polls for that exact result subject; otherwise falls back to wildcard.
func (r *ChainReconciler) pollResult(ctx context.Context, nc natsConfig, chainName, stepName, taskID string) (*natspkg.TaskResult, error) {
	log := logf.FromContext(ctx)

	natsCli, err := r.natsClient()
	if err != nil {
		return nil, err
	}

	// Use exact taskID subject when available (prevents stale result replay)
	var subject string
	if taskID != "" {
		subject = natspkg.ResultSubject(nc.SubjectPrefix, taskID)
	} else {
		taskPrefix := fmt.Sprintf("chain-%s-%s", chainName, stepName)
		subject = natspkg.ResultSubjectWildcard(nc.SubjectPrefix, taskPrefix)
	}

	// Use ephemeral consumer with explicit ack (compatible with both Limits and WorkQueue retention)
	consumerName := natspkg.ChainConsumerName(chainName, stepName)

	msg, err := natsCli.PollMessage(subject, 2*time.Second,
		natspkg.WithDurable(consumerName),
		natspkg.WithAckExplicit(),
		natspkg.WithBindStream(nc.ResultsStream),
		natspkg.WithDeliverAll(),
		natspkg.WithFallbackAutoDetect(),
	)

	// Clean up ephemeral consumer
	defer func() {
		_ = natsCli.DeleteConsumer(nc.ResultsStream, consumerName)
	}()

	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, nil // Timeout, no result yet
	}

	// Ack the message (required for WorkQueue retention)
	if err := msg.Ack(); err != nil {
		log.Error(err, "Failed to ack result message")
	}

	log.Info("Received result message", "subject", msg.Subject, "dataLen", len(msg.Data))

	var result natspkg.TaskResult
	if err := json.Unmarshal(msg.Data, &result); err != nil {
		log.Error(err, "Failed to unmarshal result", "subject", msg.Subject, "rawPreview", string(msg.Data[:min(200, len(msg.Data))]))
		return nil, fmt.Errorf("unmarshal result: %w", err)
	}

	log.Info("Parsed result", "taskId", result.GetTaskID(), "outputLen", len(result.GetOutput()), "error", result.GetError())

	return &result, nil
}

// reconcileSchedule manages the cron schedule for the chain. It returns true
// if a scheduled fire was missed (e.g. the operator was down) and a catch-up
// run should be triggered.
func (r *ChainReconciler) reconcileSchedule(ctx context.Context, chain *aiv1alpha1.Chain) bool {
	key := chain.Namespace + "/" + chain.Name

	if chain.Spec.Schedule == "" || chain.Spec.Suspend {
		r.removeCronEntry(types.NamespacedName{Namespace: chain.Namespace, Name: chain.Name})
		return false
	}

	r.mu.Lock()

	if r.cron == nil {
		r.cron = cron.New()
		r.cronEntries = make(map[string]cron.EntryID)
		r.cron.Start()
	}

	if _, ok := r.cronEntries[key]; !ok {
		nn := types.NamespacedName{Namespace: chain.Namespace, Name: chain.Name}
		entryID, err := r.cron.AddFunc(chain.Spec.Schedule, func() {
			r.triggerChain(context.Background(), nn)
		})
		if err != nil {
			r.mu.Unlock()
			logf.FromContext(ctx).Error(err, "Failed to add cron schedule", "schedule", chain.Spec.Schedule)
			return false
		}
		r.cronEntries[key] = entryID
	}
	r.mu.Unlock()

	return r.missedSchedule(chain)
}

// missedSchedule reports whether the chain's next fire after lastScheduledAt
// has already passed without a run starting, within the optional
// startingDeadlineSeconds window.
func (r *ChainReconciler) missedSchedule(chain *aiv1alpha1.Chain) bool {
	if chain.Status.LastScheduledAt == nil || chain.Status.Phase == aiv1alpha1.ChainPhaseRunning {
		return false
	}

	sched, err := cron.ParseStandard(chain.Spec.Schedule)
	if err != nil {
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
	}
}

// renderOutputPath renders template variables in the outputPath.
func (r *ChainReconciler) renderOutputPath(chain *aiv1alpha1.Chain, step *aiv1alpha1.ChainStep) (string, error) {
	path := step.OutputPath
	if !strings.Contains(path, "{{") {
		return path, nil
	}

	data := map[string]string{
		"Date":  time.Now().UTC().Format("2006-01-02"),
		"Chain": chain.Name,
		"Step":  step.Name,
	}

	tmpl, err := template.New("outputPath").Parse(path)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// writeArtifact dispatches a write task to the outputKnightRef knight.
func (r *ChainReconciler) writeArtifact(ctx context.Context, nc natsConfig, chain *aiv1alpha1.Chain, stepName, outputPath, content string) error {
	natsCli, err := r.natsClient()
	if err != nil {
		return err
	}

	knightName := aiv1alpha1.RefName(chain.Spec.OutputKnightRef)
	if knightName == "" {
		knightName = defaultOutputKnight
	}

	// Get knight to find its domain
	knight := &aiv1alpha1.Knight{}
	if err := r.Get(ctx, types.NamespacedName{Name: knightName, Namespace: chain.Namespace}, knight); err != nil {
		return fmt.Errorf("output knight %q not found: %w", knightName, err)
	}

	taskID := fmt.Sprintf("chain-%s-%s-artifact.%s-%d", chain.Name, stepName, chain.Status.RunID, time.Now().UnixMilli())

	// The task instructs the knight to write the content to the path
	task := fmt.Sprintf("Write the following content to the file at path '%s'. Create any missing directories. Write ONLY the content below, do not modify or summarize it.\n\n---\n%s", outputPath, content)

	payload := natspkg.TaskPayload{
		TaskID:    taskID,
		ChainName: chain.Name,
		StepName:  stepName + "-artifact",
		RunID:     chain.Status.RunID,
		Task:      task,
	}

	subject := natspkg.TaskSubject(nc.SubjectPrefix, knight.Spec.Domain, knightName)
	return natsCli.PublishJSON(subject, payload)
}

// emptyOutputSentinels are placeholder strings produced by knights when an
// agent session yields no real content. They carry no usable output and must
// not be treated as a successful step result.
var emptyOutputSentinels = []string{
	"[No output from agent]",
}

// isEmptyStepOutput reports whether a step output contains no usable content.
func isEmptyStepOutput(output string) bool {
	trimmed := strings.TrimSpace(output)
	if trimmed == "" {
		return true
	}
	return slices.Contains(emptyOutputSentinels, trimmed)
}

// storeStepOutputToKV stores the full step output to the NATS KV "chain-outputs" bucket.
// This is best-effort — failures are logged but do not block chain execution.
func (r *ChainReconciler) storeStepOutputToKV(ctx context.Context, chainName, runID, stepName, output, errStr, knight string, startedAt, completedAt *metav1.Time, structured ...natspkg.Structured) {
	log := logf.FromContext(ctx)

	// Never persist empty outputs — a poisoned KV entry would be restored as a
	// "successful" step on later runs, silently skipping real work.
	if errStr == "" && isEmptyStepOutput(output) {
		log.Info("Refusing to store empty step output to KV", "step", stepName)
		return
	}

	natsCli, err := r.natsClient()
	if err != nil {
		log.Error(err, "Failed to connect NATS for KV store", "step", stepName)
		return
	}

	var durationStr string
	if startedAt != nil && completedAt != nil {
		durationStr = completedAt.Time.Sub(startedAt.Time).String()
	}

	kvValue := map[string]any{
		"output":   output,
		"error":    errStr,
		"knight":   knight,
		"runId":    runID,
		"duration": durationStr,
		"storedAt": time.Now().UTC().Format(time.RFC3339),
	}

	if len(structured) > 0 {
		if len(structured[0].PRs) > 0 {
			kvValue["prs"] = structured[0].PRs
		}
		if structured[0].Verdict != "" {
			kvValue["verdict"] = structured[0].Verdict
		}
	}

	data, err := json.Marshal(kvValue)
	if err != nil {
		log.Error(err, "Failed to marshal KV value", "step", stepName)
		return
	}

	key := chainName + "." + stepName
	if err := natsCli.KVPut("chain-outputs", key, data); err != nil {
		log.Error(err, "Failed to store step output to KV", "key", key)
	} else {
		log.Info("Stored step output to NATS KV", "bucket", "chain-outputs", "key", key, "size", len(data))
	}
}

// restoreStepOutputsFromKV attempts to restore step outputs from NATS KV.
// Returns the number of steps successfully restored.
func (r *ChainReconciler) restoreStepOutputsFromKV(ctx context.Context, chain *aiv1alpha1.Chain) int {
	log := logf.FromContext(ctx)

	natsCli, err := r.natsClient()
	if err != nil {
		log.Error(err, "Failed to connect NATS for KV restore")
		return 0
	}

	restored := 0
	for i := range chain.Status.StepStatuses {
		ss := &chain.Status.StepStatuses[i]
		if ss.Phase != aiv1alpha1.ChainStepPhasePending {
			continue // Only restore pending steps
		}

		key := chain.Name + "." + ss.Name
		data, err := natsCli.KVGet("chain-outputs", key)
		if err != nil {
			log.V(1).Info("No stored output found for step", "step", ss.Name, "key", key)
			continue
		}

		var kvValue map[string]any
		if err := json.Unmarshal(data, &kvValue); err != nil {
			log.Error(err, "Failed to unmarshal KV value", "step", ss.Name)
			continue
		}

		output, _ := kvValue["output"].(string)
		errStr, _ := kvValue["error"].(string)

		// A stored entry with no usable output is poison left by a run that
		// produced nothing — leave the step Pending so it re-executes, and
		// delete the entry so it cannot mask future runs.
		if errStr == "" && isEmptyStepOutput(output) {
			log.Info("Skipping restore of empty stored output, deleting poisoned KV entry", "step", ss.Name, "key", key)
			if err := natsCli.KVDelete("chain-outputs", key); err != nil {
				log.Error(err, "Failed to delete poisoned KV entry", "key", key)
			}
			continue
		}

		// Only outputs produced by the current run may be restored — entries
		// from earlier runs (or pre-run-identity entries with no runId) would
		// silently stand in for work this run never did.
		storedRunID, _ := kvValue["runId"].(string)
		if storedRunID == "" || storedRunID != chain.Status.RunID {
			log.V(1).Info("Skipping stored output from a different run", "step", ss.Name, "storedRunId", storedRunID)
			continue
		}

		if errStr != "" {
			ss.Phase = aiv1alpha1.ChainStepPhaseFailed
			ss.Error = errStr
			log.Info("Restored failed step from KV", "step", ss.Name)
		} else {
			ss.Phase = aiv1alpha1.ChainStepPhaseSucceeded
			ss.Output = output
			if len(ss.Output) > 4000 {
				ss.Output = ss.Output[:4000] + "\n\n... [truncated — full output in NATS KV bucket 'chain-outputs', key '" + chain.Name + "." + ss.Name + "']"
			}
			log.Info("Restored successful step from KV", "step", ss.Name, "outputLen", len(output))
		}

		// Set completion time (approximate)
		now := metav1.Now()
		ss.CompletedAt = &now
		restored++
	}

	return restored
}

// SetupWithManager sets up the controller with the Manager.
func (r *ChainReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Initialize cron scheduler
	r.cron = cron.New()
	r.cron.Start()
	r.cronEntries = make(map[string]cron.EntryID)

	// Stop the cron scheduler on manager shutdown, waiting for any in-flight
	// trigger to finish — otherwise the cron goroutine outlives the manager.
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		<-ctx.Done()
		stopCtx := r.cron.Stop()
		<-stopCtx.Done()
		return nil
	})); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&aiv1alpha1.Chain{}).
		Named("chain").
		Complete(r)
}

// defaultOutputKnight is the knight that receives chain output when spec.outputKnightRef is unset.
const defaultOutputKnight = "gawain"

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

// inputFields returns the top-level fields of spec.input when it is a JSON
// object (else an empty map), for templates that must pick single fields
// instead of rendering all of .Input, e.g. {{ .Fields.url }}.
func inputFields(input string) map[string]any {
	fields := map[string]any{}
	if strings.HasPrefix(strings.TrimSpace(input), "{") {
		if err := json.Unmarshal([]byte(input), &fields); err != nil {
			return map[string]any{}
		}
	}
	return fields
}
