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
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/notify"
	"github.com/roundtable-k8s/roundtable/internal/telemetry"
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

	// conflictWarned holds, per Chain (types.NamespacedName), the generation a
	// ScheduleTriggerConflict event was last recorded for. Entries are dropped
	// when the Chain is gone, has no schedule or leaves trigger mode.
	conflictWarned sync.Map

	NATS   *natspkg.Provider
	Notify *notify.Notifier
	// DefaultKnightImage is the operator's DEFAULT_KNIGHT_IMAGE. It is used to
	// infer a knight's agent runtime (claude-knight/pi-knight) when the Knight
	// has no spec.image, for per-step model validation.
	DefaultKnightImage string
	// ProviderMaxRetries is how many times a step is retried after a
	// transient model provider failure (#481). 0 disables the retry.
	ProviderMaxRetries int
	// TracerProvider makes the chain run spans; nil uses the global one.
	TracerProvider trace.TracerProvider
	// ScheduleMode selects how spec.schedule runs: the in-process cron
	// (empty or inprocess) or an owned schedule Trigger (trigger).
	ScheduleMode ChainScheduleMode
	// MaxConcurrentReconciles is how many Chains reconcile in parallel;
	// 0 or less means 1.
	MaxConcurrentReconciles int
	cron                    *cron.Cron
	mu                      sync.Mutex
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
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=triggers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

// requeuePriority is the workqueue priority a requeued chain is added back
// with. controller-runtime's priority queue gives an item enqueued by the
// initial cache list (every chain after an operator restart) a low priority,
// and an unset Result.Priority keeps it, so the 5s result-polling requeues of
// in-flight steps would be starved behind other work (#700).
const requeuePriority = 0

// Reconcile runs one reconcile of a Chain and resets the requeue priority.
func (r *ChainReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	res, err := r.reconcile(ctx, req)
	return quietConflict(ctx, res, err)
}

func (r *ChainReconciler) reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	return resetRequeuePriority(r.reconcileOnce(ctx, req))
}

// resetRequeuePriority sets the requeue priority of a result that requeues
// (or errors); shared by the Chain and Mission reconcilers.
func resetRequeuePriority(res ctrl.Result, err error) (ctrl.Result, error) {
	if err != nil || res.RequeueAfter > 0 {
		p := requeuePriority
		res.Priority = &p
	}
	return res, err
}

func (r *ChainReconciler) reconcileOnce(ctx context.Context, req ctrl.Request) (_ ctrl.Result, retErr error) {
	log := logf.FromContext(ctx)

	chain := &aiv1alpha1.Chain{}
	if err := r.Get(ctx, req.NamespacedName, chain); err != nil {
		if client.IgnoreNotFound(err) == nil {
			// Chain is gone — make sure no cron entry outlives it.
			r.removeCronEntry(req.NamespacedName)
			r.conflictWarned.Delete(req.NamespacedName)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if table := aiv1alpha1.RefName(chain.Spec.RoundTableRef); table != "" {
		log = log.WithValues("table", table)
	}
	log = log.WithValues(telemetry.LogValues(chain.Status.TraceID)...)
	ctx = logf.IntoContext(ctx, log)

	// Handle deletion
	if chain.DeletionTimestamp != nil {
		r.removeCronEntry(req.NamespacedName)
		// Garbage collection removes the owned schedule Trigger too; deleting
		// it here stops its CronJob without waiting for that.
		if err := r.deleteScheduleTrigger(ctx, chain); err != nil {
			return ctrl.Result{}, err
		}
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

	// Handle cancel annotation before anything can start or advance a run.
	if handled, res, err := r.handleCancel(ctx, chain); handled {
		return res, err
	}

	if reason, message, err := r.validateSpec(ctx, chain); err != nil {
		return ctrl.Result{}, r.rejectSpec(ctx, chain, reason, message, err)
	}

	meta.SetStatusCondition(&chain.Status.Conditions, metav1.Condition{
		Type:               aiv1alpha1.ConditionChainValid,
		Status:             metav1.ConditionTrue,
		Reason:             aiv1alpha1.ReasonChainValid,
		Message:            "Chain spec is valid",
		ObservedGeneration: chain.Generation,
	})

	// Handle schedule. In trigger mode an owned Trigger runs it (its CronJob
	// catches up missed fires); in-process mode drops any such Trigger so a
	// mode switch never fires a schedule twice.
	if r.triggerMode() {
		changed, err := r.reconcileScheduleTrigger(ctx, chain)
		if err != nil {
			return ctrl.Result{}, err
		}
		if changed {
			return r.updateStatus(ctx, chain, 0)
		}
	} else if err := r.deleteScheduleTrigger(ctx, chain); err != nil {
		return ctrl.Result{}, err
	} else if r.reconcileSchedule(ctx, chain) {
		// Catch up a missed fire (e.g. operator downtime).
		log.Info("Missed scheduled run detected, triggering catch-up")
		r.triggerChain(ctx, req.NamespacedName)
		return ctrl.Result{RequeueAfter: RequeueFast}, nil
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

	// A run in flight keeps going when the spec changes (a chart upgrade bumps
	// the generation), but the new spec is accepted: record that right away so
	// kstatus (Helm --wait, Flux) never reads an active run as InProgress.
	// Reconciles of a running Chain can end before reconcileRunning writes it
	// (resolveNATSConfig failing while the RoundTable is unreadable, retried
	// with backoff; run-trigger and schedule-trigger returns), which left the
	// generation unobserved for as long as that lasted.
	if chain.Status.ObservedGeneration != chain.Generation {
		chain.Status.ObservedGeneration = chain.Generation
		if err := r.Status().Update(ctx, chain); err != nil {
			if apierrors.IsConflict(err) {
				return ctrl.Result{RequeueAfter: RequeueFast}, nil
			}
			return ctrl.Result{}, err
		}
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
			return ctrl.Result{RequeueAfter: RequeueFast}, nil
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

// chainRun is the state one reconcile of a running chain threads through its
// phases: the resolved NATS config and the step status and spec maps by name.
type chainRun struct {
	nc        natsConfig
	statusMap map[string]*aiv1alpha1.ChainStepStatus
	specMap   map[string]*aiv1alpha1.ChainStep
	// failing is true when a step already failed without continueOnFailure;
	// such a chain starts no further loop rounds.
	failing bool
}

// newChainRun builds the step status and spec maps of a running chain.
func newChainRun(nc natsConfig, chain *aiv1alpha1.Chain) *chainRun {
	run := &chainRun{
		nc:        nc,
		statusMap: make(map[string]*aiv1alpha1.ChainStepStatus),
		specMap:   make(map[string]*aiv1alpha1.ChainStep),
	}
	for i := range chain.Status.StepStatuses {
		run.statusMap[chain.Status.StepStatuses[i].Name] = &chain.Status.StepStatuses[i]
	}
	for i := range chain.Spec.Steps {
		run.specMap[chain.Spec.Steps[i].Name] = &chain.Spec.Steps[i]
	}
	run.failing = hasHardFailure(chain, run.specMap)
	return run
}

// reconcileRunning processes the DAG execution for a running chain.
func (r *ChainReconciler) reconcileRunning(ctx context.Context, chain *aiv1alpha1.Chain) (ctrl.Result, error) {
	run, res, err := r.startOrResumeRun(ctx, chain)
	if run == nil {
		return res, err
	}
	r.collectResults(ctx, chain, run)
	if wait, held := r.dispatchReadySteps(ctx, chain, run); held {
		chain.Status.ObservedGeneration = chain.Generation
		return r.updateStatus(ctx, chain, wait)
	}
	return r.finishRun(ctx, chain, run)
}

// startOrResumeRun gives the run its identity, start time and trace, and
// fails it on the overall timeout. A manually triggered chain without step
// statuses is initialized (restoring this run's completed steps from KV) and
// requeued. It returns a nil run, with the result to return, when the
// reconcile ends here.
func (r *ChainReconciler) startOrResumeRun(ctx context.Context, chain *aiv1alpha1.Chain) (*chainRun, ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Resolve NATS config from the chain's RoundTable reference
	nc, err := r.resolveNATSConfig(ctx, chain)
	if err != nil {
		log.Error(err, "Failed to resolve NATS config for chain")
		return nil, ctrl.Result{}, err
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
		res, err := r.updateStatus(ctx, chain, RequeueFast)
		return nil, res, err
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
	hadTraceID := chain.Status.TraceID != ""
	r.ensureRunTrace(ctx, chain)
	if !hadTraceID {
		log = log.WithValues(telemetry.LogValues(chain.Status.TraceID)...)
		ctx = logf.IntoContext(ctx, log)
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
			return nil, ctrl.Result{}, r.Status().Update(ctx, chain)
		}
	}

	return newChainRun(nc, chain), ctrl.Result{}, nil
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
		Owns(&aiv1alpha1.Trigger{}).
		Named("chain").
		WithOptions(controller.Options{MaxConcurrentReconciles: max(r.MaxConcurrentReconciles, 1)}).
		Complete(r)
}
