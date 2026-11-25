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
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/roundtable-k8s/roundtable/internal/util"
	"go.opentelemetry.io/otel/trace"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/tools/record"
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
	// Table is the RoundTable name, used for the roundtable.table span attribute.
	Table string
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
	// TracerProvider makes the chain run spans; nil uses the global one.
	TracerProvider trace.TracerProvider
	cron           *cron.Cron
	mu             sync.Mutex
	// cronEntries maps chain namespace/name to cron entry ID
	cronEntries map[string]cron.EntryID
	// cronSpecs maps chain namespace/name to the effective cron spec (with
	// time zone) its entry was registered with, so edits re-register it.
	cronSpecs map[string]string
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
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

func (r *ChainReconciler) Reconcile(ctx context.Context, req ctrl.Request) (_ ctrl.Result, retErr error) {
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

	if table := aiv1alpha1.RefName(chain.Spec.RoundTableRef); table != "" {
		log = log.WithValues("table", table)
		ctx = logf.IntoContext(ctx, log)
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

	// Observe the duration of every step that reaches a terminal phase during
	// this reconcile, once the status has been persisted.
	prevStepPhases := stepPhaseSnapshot(chain)
	defer func() {
		if retErr == nil {
			r.observeFinishedSteps(ctx, chain, prevStepPhases)
		}
	}()

	// Add finalizer
	if !util.ContainsString(chain.Finalizers, chainFinalizer) {
		chain.Finalizers = append(chain.Finalizers, chainFinalizer)
		if err := r.Update(ctx, chain); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Emit the run spans that finish during this reconcile, once the status
	// that records it is written (its resourceVersion moved). A failed or
	// conflicting write leaves them for the next reconcile.
	prevRunSpans := chainRunSpans(chain, time.Now())
	baseRV := chain.ResourceVersion
	defer func() {
		if chain.ResourceVersion != baseRV {
			r.emitRunSpans(ctx, chain, prevRunSpans)
		}
	}()

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

	// Validate schedule and time zone
	if _, err := parseSchedule(chain); err != nil {
		meta.SetStatusCondition(&chain.Status.Conditions, metav1.Condition{
			Type:               aiv1alpha1.ConditionChainValid,
			Status:             metav1.ConditionFalse,
			Reason:             aiv1alpha1.ReasonInvalidSchedule,
			Message:            err.Error(),
			ObservedGeneration: chain.Generation,
		})
		chain.Status.NextScheduledAt = nil
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
		resetRunTrace(chain)

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

// chainTable returns the name of the RoundTable a chain runs on: its
// roundTableRef, else the table of its mission. Empty when unresolved.
func (r *ChainReconciler) chainTable(ctx context.Context, chain *aiv1alpha1.Chain) string {
	if name := aiv1alpha1.RefName(chain.Spec.RoundTableRef); name != "" {
		return name
	}
	if mName := aiv1alpha1.RefName(chain.Spec.MissionRef); mName != "" {
		m := &aiv1alpha1.Mission{}
		if err := r.Get(ctx, client.ObjectKey{Namespace: chain.Namespace, Name: mName}, m); err == nil {
			return missionTable(m)
		}
	}
	return ""
}

func isTerminalStepPhase(p aiv1alpha1.ChainStepPhase) bool {
	return p == aiv1alpha1.ChainStepPhaseSucceeded || p == aiv1alpha1.ChainStepPhaseFailed || p == aiv1alpha1.ChainStepPhaseSkipped
}

// stepPhaseSnapshot records each step's phase by name.
func stepPhaseSnapshot(chain *aiv1alpha1.Chain) map[string]aiv1alpha1.ChainStepPhase {
	out := make(map[string]aiv1alpha1.ChainStepPhase, len(chain.Status.StepStatuses))
	for _, ss := range chain.Status.StepStatuses {
		out[ss.Name] = ss.Phase
	}
	return out
}

// observeFinishedSteps records roundtable_chain_step_duration_seconds for the
// steps that were not terminal in prev but are now. Steps without both a start
// and a completion time (skipped, restored) are not observed.
func (r *ChainReconciler) observeFinishedSteps(ctx context.Context, chain *aiv1alpha1.Chain, prev map[string]aiv1alpha1.ChainStepPhase) {
	var table string
	resolved := false
	for _, ss := range chain.Status.StepStatuses {
		if !isTerminalStepPhase(ss.Phase) || isTerminalStepPhase(prev[ss.Name]) {
			continue
		}
		if ss.StartedAt == nil || ss.CompletedAt == nil {
			continue
		}
		if !resolved {
			table, resolved = r.chainTable(ctx, chain), true
		}
		metrics.ChainStepDurationSeconds.WithLabelValues(table, chain.Name, ss.Name, strings.ToLower(string(ss.Phase))).
			Observe(ss.CompletedAt.Sub(ss.StartedAt.Time).Seconds())
	}
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
		resetRunTrace(chain)

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

	// The run's trace: its root span context is stored once, then every
	// later reconcile parents its spans from it.
	r.ensureRunTrace(ctx, chain)

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
					// #321: a plan-quota refusal is "not yet", not a failure. With a
					// fallback knight the same step re-runs there (no model override:
					// the aliases resolve to Claude models); otherwise the step is
					// deferred until the plan window resets and the chain requeues.
					// Neither path fails the chain or escalates.
					if resetsAt, refused := quotaRefusal(result); refused {
						if fb := stepFallbackKnight(spec, ss); fb != "" {
							ss.Phase = aiv1alpha1.ChainStepPhasePending
							ss.Reason = stepReasonFallbackQuota
							ss.Error = ""
							ss.CompletedAt = nil
							ss.Knight = fb
							ss.Model = ""
							log.Info("Plan-quota refusal: re-running step on fallback knight", "step", ss.Name, "fallback", fb)
							r.Recorder.Eventf(chain, corev1.EventTypeNormal, "StepFallbackQuota",
								"Step %s refused for plan quota, re-running on fallback knight %s (no model override)", ss.Name, fb)
						} else {
							ss.Phase = aiv1alpha1.ChainStepPhaseDeferred
							ss.Reason = stepReasonQuotaRefused
							ss.Error = resultErr
							ss.CompletedAt = nil
							until := metav1.NewTime(quotaDeferUntil(resetsAt))
							ss.DeferUntil = &until
							log.Info("Plan-quota refusal: deferring step until the plan window resets", "step", ss.Name, "deferUntil", ss.DeferUntil.Unix())
							r.Recorder.Eventf(chain, corev1.EventTypeNormal, "StepQuotaRefused",
								"Step %s refused for plan quota, deferred until %s", ss.Name, ss.DeferUntil)
						}
						continue
					}
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

	// #321: a quota-deferred step parks the chain: nothing further is
	// dispatched (so nothing fails and nothing escalates, no needs-human)
	// until the refused plan window resets; then the deferred step(s) run
	// again on their primary knight and the chain picks up where it left off.
	if next := wakeDeferredSteps(chain, time.Now()); next != nil {
		log.Info("Chain has a quota-deferred step; holding further steps", "requeueAfter", next.Time)
		chain.Status.ObservedGeneration = chain.Generation
		return r.updateStatus(ctx, chain, time.Until(next.Time))
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

		// A plan-quota refusal re-runs on the step's fallback knight with NO
		// model override (#321): the cheap/fast/deep aliases are Claude models.
		knightName, stepModel := step.KnightRef.Name, step.Model
		if ss.Knight != "" && ss.Knight != step.KnightRef.Name {
			knightName, stepModel = ss.Knight, ""
		}

		// Get knight domain
		knight := &aiv1alpha1.Knight{}
		if err := r.Get(ctx, types.NamespacedName{Name: knightName, Namespace: chain.Namespace}, knight); err != nil {
			log.Error(err, "Failed to get knight", "knightRef", knightName)
			continue
		}

		// Resolve the per-step model override. A model the knight's runtime
		// can't run is a spec error: fail the step (no retry) rather than let
		// the knight silently fall back to its default model.
		model, err := knightpkg.ResolveModel(stepModel, nc.ModelAliases, knightpkg.EffectiveImage(knightpkg.WithTableDefaults(ctx, r.Client, knight), r.DefaultKnightImage))
		if err != nil {
			log.Info("Rejected step model", "step", step.Name, "knight", knightName, "reason", err.Error())
			r.Recorder.Eventf(chain, corev1.EventTypeWarning, "InvalidStepModel", "Step %s: %v", step.Name, err)
			ss.Phase = aiv1alpha1.ChainStepPhaseFailed
			ss.Error = fmt.Sprintf("invalid model for knight %q: %v", knightName, err)
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

		seq, err := r.publishTask(withRunSpan(ctx, chain, plainStepSpanKey(step.Name, taskID)), nc, knight.Spec.Domain, knightName, payload)
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
		ss.Knight = knightName
		ss.Error = ""
		log.Info("Published step task", "step", step.Name, "taskId", taskID, "knight", knightName, "model", model)
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
			metrics.ChainNoOpRunsTotal.WithLabelValues(r.chainTable(ctx, chain), chain.Name).Inc()
		}

		chain.Status.ObservedGeneration = chain.Generation
		return r.updateStatus(ctx, chain, 0)
	}

	chain.Status.ObservedGeneration = chain.Generation

	// Requeue to poll for results
	return r.updateStatus(ctx, chain, RequeueDefault)
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
