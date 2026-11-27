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

	"go.opentelemetry.io/otel/trace"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	missionpkg "github.com/roundtable-k8s/roundtable/internal/mission"
	"github.com/roundtable-k8s/roundtable/internal/notify"
	"github.com/roundtable-k8s/roundtable/internal/status"
	"github.com/roundtable-k8s/roundtable/internal/telemetry"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

const (
	missionFinalizer = "ai.roundtable.io/mission-finalizer"
)

// MissionReconciler reconciles a Mission object.
type MissionReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	NATS        *natspkg.Provider
	Notify      *notify.Notifier
	Planner     *missionpkg.Planner
	Assembler   *missionpkg.KnightAssembler
	Cleaner     *missionpkg.Cleaner
	CostTracker *missionpkg.CostTracker
	Briefer     *missionpkg.Briefer
	Chains      *missionpkg.ChainManager

	// TracerProvider makes the mission spans; nil uses the global one.
	TracerProvider trace.TracerProvider
}

// emitMissionSpan emits the mission's root span as it ends. errMsg marks it failed.
func (r *MissionReconciler) emitMissionSpan(ctx context.Context, mission *aiv1alpha1.Mission, errMsg string) {
	var start time.Time
	if mission.Status.StartedAt != nil {
		start = mission.Status.StartedAt.Time
	} else {
		start = mission.CreationTimestamp.Time
	}
	missionpkg.EmitSpan(ctx, r.TracerProvider, mission, "", "mission "+mission.Name,
		missionpkg.SpanEnd{Start: start, End: time.Now(), ErrMsg: errMsg})
}

// +kubebuilder:rbac:groups=ai.roundtable.io,resources=missions,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=missions/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=missions/finalizers,verbs=update
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=knights,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=chains,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=roundtables,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

// Reconcile runs one reconcile of a Mission and resets the requeue priority
// (see requeuePriority, #700).
func (r *MissionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	res, err := r.reconcile(ctx, req)
	return quietConflict(ctx, res, err)
}

func (r *MissionReconciler) reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	return resetRequeuePriority(r.reconcileOnce(ctx, req))
}

func (r *MissionReconciler) reconcileOnce(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Count table-less missions (roundtable_missions{table="none"}); the
	// RoundTable reconciler only counts missions of a table.
	defer updateUnownedMissionMetrics(ctx, r.Client)

	mission := &aiv1alpha1.Mission{}
	if err := r.Get(ctx, req.NamespacedName, mission); err != nil {
		if client.IgnoreNotFound(err) == nil {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Log lines of this reconcile, including the helpers' (they read the
	// logger from ctx), carry the mission's table and trace.
	if table := aiv1alpha1.RefName(mission.Spec.RoundTableRef); table != "" {
		log = log.WithValues("table", table)
	}
	log = log.WithValues(telemetry.LogValues(mission.Status.TraceID)...)
	ctx = logf.IntoContext(ctx, log)

	// Handle deletion
	if mission.DeletionTimestamp != nil {
		if controllerutil.ContainsFinalizer(mission, missionFinalizer) {
			log.Info("Cleaning up mission resources", "mission", mission.Name)
			// Streams are not owned by any Kubernetes object, so GC will not remove
			// them. Delete them here unless the Cleanup phase already did. This applies
			// regardless of cleanupPolicy: the Mission itself is going away.
			if !meta.IsStatusConditionTrue(mission.Status.Conditions, aiv1alpha1.ConditionCleanupComplete) &&
				(mission.Status.NATSTasksStream != "" || mission.Status.NATSResultsStream != "") {
				_ = r.Cleaner.DeleteNATSConsumers(ctx, mission) // best effort
				if err := r.Cleaner.DeleteNATSStreams(ctx, mission); err != nil {
					log.Error(err, "Failed to delete NATS streams during mission deletion, retrying")
					return ctrl.Result{RequeueAfter: RequeueModerate}, nil
				}
			}
			controllerutil.RemoveFinalizer(mission, missionFinalizer)
			if err := r.Update(ctx, mission); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Add finalizer
	if !controllerutil.ContainsFinalizer(mission, missionFinalizer) {
		controllerutil.AddFinalizer(mission, missionFinalizer)
		if err := r.Update(ctx, mission); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Initialize status
	if mission.Status.Phase == "" {
		now := metav1.Now()
		mission.Status.StartedAt = &now
		expiresAt := metav1.NewTime(now.Add(time.Duration(mission.Spec.TTLSeconds) * time.Second))
		mission.Status.ExpiresAt = &expiresAt
		r.initKnightStatuses(mission)
		prevTraceID := mission.Status.TraceID
		missionpkg.EnsureTrace(ctx, r.TracerProvider, mission)
		if mission.Status.TraceID != prevTraceID {
			log = log.WithValues(telemetry.LogValues(mission.Status.TraceID)...)
			ctx = logf.IntoContext(ctx, log)
		}
		err := status.ForMission(mission).
			Phase(aiv1alpha1.MissionPhasePending).
			Apply(ctx, r.Client)
		if apierrors.IsConflict(err) {
			return ctrl.Result{RequeueAfter: RequeueFast}, nil
		}
		r.Recorder.Eventf(mission, corev1.EventTypeNormal, "PhaseTransition", "Mission transitioned to %s", aiv1alpha1.MissionPhasePending)
		return ctrl.Result{}, err
	}

	// A mission from before tracing existed gets its trace on first reconcile
	// (kept by the next status write).
	hadTraceID := mission.Status.TraceID != ""
	missionpkg.EnsureTrace(ctx, r.TracerProvider, mission)
	if !hadTraceID {
		log = log.WithValues(telemetry.LogValues(mission.Status.TraceID)...)
		ctx = logf.IntoContext(ctx, log)
	}

	// Check TTL expiration in any non-terminal phase
	if res, handled, err := r.reconcileTTLExpiry(ctx, mission); handled {
		return res, err
	}

	// Fire the completion webhook once the terminal outcome is recorded —
	// before the phase switch, so it lands ahead of cleanup/self-deletion.
	if res, handled := r.reconcileNotification(ctx, mission); handled {
		return res, nil
	}

	switch mission.Status.Phase {
	case aiv1alpha1.MissionPhasePending:
		return r.reconcilePending(ctx, mission)
	case aiv1alpha1.MissionPhaseProvisioning:
		return r.reconcileProvisioning(ctx, mission)
	case aiv1alpha1.MissionPhasePlanning:
		return r.Planner.ReconcilePlanning(ctx, mission)
	case aiv1alpha1.MissionPhaseAssembling:
		return r.reconcileAssembling(ctx, mission)
	case aiv1alpha1.MissionPhaseBriefing:
		return r.reconcileBriefing(ctx, mission)
	case aiv1alpha1.MissionPhaseActive:
		return r.reconcileActive(ctx, mission)
	case aiv1alpha1.MissionPhaseSucceeded, aiv1alpha1.MissionPhaseFailed:
		// Only transition to cleanup if not already cleaned up (prevents infinite loop)
		if meta.IsStatusConditionTrue(mission.Status.Conditions, aiv1alpha1.ConditionCleanupComplete) {
			return ctrl.Result{}, nil
		}
		// Record the outcome in the Complete condition before overwriting the
		// phase — paths like the assembly timeout set Failed without one, and
		// cleanup needs it to restore the right terminal phase afterwards.
		if !meta.IsStatusConditionTrue(mission.Status.Conditions, aiv1alpha1.ConditionMissionComplete) {
			reason := aiv1alpha1.ReasonMissionSucceeded
			if mission.Status.Phase == aiv1alpha1.MissionPhaseFailed {
				reason = aiv1alpha1.ReasonMissionFailed
			}
			message := mission.Status.Result
			if message == "" {
				message = fmt.Sprintf("Mission %s", mission.Status.Phase)
			}
			meta.SetStatusCondition(&mission.Status.Conditions, metav1.Condition{
				Type:               aiv1alpha1.ConditionMissionComplete,
				Status:             metav1.ConditionTrue,
				Reason:             reason,
				Message:            message,
				ObservedGeneration: mission.Generation,
			})
		}
		outcome := mission.Status.Phase
		mission.Status.Phase = aiv1alpha1.MissionPhaseCleaningUp
		mission.Status.ObservedGeneration = mission.Generation
		err := r.Status().Update(ctx, mission)
		if apierrors.IsConflict(err) {
			return ctrl.Result{RequeueAfter: RequeueFast}, nil
		}
		if err == nil {
			// The mission ends here; once its status says so, so does its trace.
			errMsg := ""
			if outcome == aiv1alpha1.MissionPhaseFailed {
				errMsg = mission.Status.Result
				if errMsg == "" {
					errMsg = "mission failed"
				}
			}
			r.emitMissionSpan(ctx, mission, errMsg)
		}
		return ctrl.Result{RequeueAfter: RequeueDefault}, err
	case aiv1alpha1.MissionPhaseCleaningUp:
		return r.reconcileCleaningUp(ctx, mission)
	case aiv1alpha1.MissionPhaseExpired:
		// Already handled above, but if we get here directly just clean up
		mission.Status.Phase = aiv1alpha1.MissionPhaseCleaningUp
		err := r.Status().Update(ctx, mission)
		if apierrors.IsConflict(err) {
			return ctrl.Result{RequeueAfter: RequeueFast}, nil
		}
		return ctrl.Result{RequeueAfter: RequeueDefault}, err
	}

	return ctrl.Result{}, nil
}

// reconcileTTLExpiry moves a mission whose TTL has lapsed into CleaningUp,
// recording the Expired outcome. Returns handled=false when the TTL has not
// expired (or the mission is already cleaning up / expired) and the caller
// should continue reconciling.
func (r *MissionReconciler) reconcileTTLExpiry(ctx context.Context, mission *aiv1alpha1.Mission) (ctrl.Result, bool, error) {
	if mission.Status.ExpiresAt == nil || !time.Now().After(mission.Status.ExpiresAt.Time) {
		return ctrl.Result{}, false, nil
	}
	if mission.Status.Phase == aiv1alpha1.MissionPhaseCleaningUp ||
		mission.Status.Phase == aiv1alpha1.MissionPhaseExpired {
		return ctrl.Result{}, false, nil
	}

	logf.FromContext(ctx).Info("Mission TTL expired", "mission", mission.Name)
	// Go straight to CleaningUp in a single status update to avoid
	// double-update conflicts (the old code set Expired then immediately
	// overwrote to CleaningUp — the second update stomped the first).
	err := status.ForMission(mission).
		Complete("Mission expired (TTL exceeded)", aiv1alpha1.MissionPhaseCleaningUp).
		Condition(aiv1alpha1.ConditionMissionComplete, aiv1alpha1.ReasonMissionExpired, "Mission TTL expired", metav1.ConditionTrue).
		Apply(ctx, r.Client)
	if apierrors.IsConflict(err) {
		return ctrl.Result{RequeueAfter: RequeueFast}, true, nil
	}
	if err == nil {
		r.emitMissionSpan(ctx, mission, "mission expired (TTL exceeded)")
	}
	r.Recorder.Event(mission, corev1.EventTypeWarning, "Timeout", "Mission exceeded TTL")
	r.Recorder.Eventf(mission, corev1.EventTypeNormal, "PhaseTransition", "Mission transitioned to %s", aiv1alpha1.MissionPhaseCleaningUp)
	return ctrl.Result{RequeueAfter: RequeueDefault}, true, err
}

// reconcileNotification runs the completion-webhook pass for a mission whose
// terminal outcome has been recorded. It keys off the Complete condition, not
// the phase, because the phase passes through CleaningUp (and is only
// restored to Succeeded/Failed/Expired afterwards) — and running before the
// phase machine gets the notification out before a short-TTL mission
// self-deletes. Notification state never gates the phase machine; a changed
// condition gets its own status update, and the requeue (backoff on failure,
// RequeueFast otherwise) resumes normal reconciliation.
func (r *MissionReconciler) reconcileNotification(ctx context.Context, mission *aiv1alpha1.Mission) (ctrl.Result, bool) {
	if !meta.IsStatusConditionTrue(mission.Status.Conditions, aiv1alpha1.ConditionMissionComplete) ||
		!notificationPending(mission.Spec.Notify, mission.Status.Conditions) {
		return ctrl.Result{}, false
	}

	completedAt := notifyCompletedAt(mission.Status.CompletedAt, mission.Status.Conditions, aiv1alpha1.ConditionMissionComplete)
	requeue := deliverNotification(ctx, r.Client, r.Recorder, r.Notify, mission,
		&mission.Status.Conditions, mission.Generation, completedAt, missionNotifyPayload(mission))
	if err := r.Status().Update(ctx, mission); err != nil {
		// Conflict or transient — retry the whole pass; the delivery guard is
		// the (unpersisted) condition, so the next attempt re-evaluates it.
		logf.FromContext(ctx).Error(err, "Failed to update status after notification attempt")
		return ctrl.Result{RequeueAfter: RequeueFast}, true
	}
	if requeue == 0 {
		requeue = RequeueFast
	}
	return ctrl.Result{RequeueAfter: requeue}, true
}

// initKnightStatuses initializes knight status entries.
func (r *MissionReconciler) initKnightStatuses(mission *aiv1alpha1.Mission) {
	mission.Status.KnightStatuses = make([]aiv1alpha1.MissionKnightStatus, len(mission.Spec.Knights))
	for i, mk := range mission.Spec.Knights {
		mission.Status.KnightStatuses[i] = aiv1alpha1.MissionKnightStatus{
			Name:      mk.Name,
			Ephemeral: mk.Ephemeral,
		}
	}
}

// reconcilePending validates the mission spec before provisioning.
func (r *MissionReconciler) reconcilePending(ctx context.Context, mission *aiv1alpha1.Mission) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Validate knight templates have unique names
	templateNames := make(map[string]bool)
	for _, template := range mission.Spec.KnightTemplates {
		if templateNames[template.Name] {
			mission.Status.Phase = aiv1alpha1.MissionPhaseFailed
			msg := fmt.Sprintf("Duplicate knight template name: %s", template.Name)
			mission.Status.Result = msg
			meta.SetStatusCondition(&mission.Status.Conditions, metav1.Condition{
				Type:               aiv1alpha1.ConditionMissionComplete,
				Status:             metav1.ConditionTrue,
				Reason:             aiv1alpha1.ReasonMissionFailed,
				Message:            msg,
				ObservedGeneration: mission.Generation,
			})
			mission.Status.ObservedGeneration = mission.Generation
			err := r.Status().Update(ctx, mission)
			if err == nil {
				r.Recorder.Event(mission, corev1.EventTypeWarning, "Failed", msg)
			}
			return ctrl.Result{}, err
		}
		templateNames[template.Name] = true
	}

	// Validate knights have unique names and valid template refs
	knightNames := make(map[string]bool)
	for _, knight := range mission.Spec.Knights {
		if knightNames[knight.Name] {
			return ctrl.Result{}, status.ForMission(mission).
				Failed(fmt.Sprintf("Duplicate knight name: %s", knight.Name)).
				Apply(ctx, r.Client)
		}
		knightNames[knight.Name] = true

		// If using templateName, validate it exists in mission-level templates.
		// Note: RoundTable-level templates are validated later during assembling
		// (they require a Get call we defer to avoid premature fetches).
		if knight.TemplateName != "" && !templateNames[knight.TemplateName] && aiv1alpha1.RefName(mission.Spec.RoundTableRef) == "" {
			return ctrl.Result{}, status.ForMission(mission).
				Failed(fmt.Sprintf("Knight %s references unknown template: %s", knight.Name, knight.TemplateName)).
				Apply(ctx, r.Client)
		}

		// Validate ephemeral knights have spec OR templateName (not both, not neither)
		if knight.Ephemeral {
			hasSpec := knight.EphemeralSpec != nil
			hasTemplate := knight.TemplateName != ""
			if hasSpec == hasTemplate { // XOR check
				return ctrl.Result{}, status.ForMission(mission).
					Failed(fmt.Sprintf("Ephemeral knight %s must have exactly one of ephemeralSpec or templateName", knight.Name)).
					Apply(ctx, r.Client)
			}
		}
	}

	// Hold the mission back while its RoundTable is at policies.maxMissions.
	if res, held, err := r.admitMission(ctx, mission); held || err != nil {
		return res, err
	}

	// Validate referenced chains exist. This runs after admission so a held
	// mission does not repeat the Gets on every requeue.
	for _, chainRef := range mission.Spec.Chains {
		chain := &aiv1alpha1.Chain{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      chainRef.Name,
			Namespace: mission.Namespace,
		}, chain); err != nil {
			if client.IgnoreNotFound(err) == nil {
				return ctrl.Result{}, status.ForMission(mission).
					Failed(fmt.Sprintf("Referenced chain not found: %s", chainRef.Name)).
					Apply(ctx, r.Client)
			}
			return ctrl.Result{}, err
		}
	}

	log.Info("Mission spec validation passed", "mission", mission.Name)
	// The execution timeout runs from admission, not creation, so time spent
	// queued behind maxMissions does not eat into timeoutSeconds. ExpiresAt (TTL)
	// is left alone: queue time does count toward ttlSeconds.
	admittedAt := metav1.Now()
	mission.Status.StartedAt = &admittedAt
	err := status.ForMission(mission).
		Phase(aiv1alpha1.MissionPhaseProvisioning).
		Condition(aiv1alpha1.ConditionMissionAdmitted, aiv1alpha1.ReasonMissionAdmitted, "Mission admitted", metav1.ConditionTrue).
		Apply(ctx, r.Client)
	if apierrors.IsConflict(err) {
		return ctrl.Result{RequeueAfter: RequeueFast}, nil
	}
	r.Recorder.Eventf(mission, corev1.EventTypeNormal, "PhaseTransition", "Mission transitioned to %s", aiv1alpha1.MissionPhaseProvisioning)
	return ctrl.Result{RequeueAfter: RequeueFast}, err
}

// reconcileProvisioning creates the ephemeral RoundTable and NATS streams if needed.

// nextPhaseAfterProvisioning returns Planning if metaMission, otherwise Assembling.
func nextPhaseAfterProvisioning(mission *aiv1alpha1.Mission) aiv1alpha1.MissionPhase {
	if mission.Spec.MetaMission {
		return aiv1alpha1.MissionPhasePlanning
	}
	return aiv1alpha1.MissionPhaseAssembling
}

func (r *MissionReconciler) reconcileProvisioning(ctx context.Context, mission *aiv1alpha1.Mission) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// If roundTableRef is already set, skip provisioning (using existing RT)
	if aiv1alpha1.RefName(mission.Spec.RoundTableRef) != "" {
		log.Info("Using existing RoundTable", "roundTable", aiv1alpha1.RefName(mission.Spec.RoundTableRef))
		mission.Status.Phase = nextPhaseAfterProvisioning(mission)
		mission.Status.ObservedGeneration = mission.Generation
		return ctrl.Result{RequeueAfter: RequeueFast}, r.Status().Update(ctx, mission)
	}

	// If no ephemeral knights, skip ephemeral RT creation (v1 compatibility)
	hasEphemeral := false
	for _, mk := range mission.Spec.Knights {
		if mk.Ephemeral {
			hasEphemeral = true
			break
		}
	}
	if !hasEphemeral {
		log.Info("No ephemeral knights, skipping ephemeral RoundTable creation")
		mission.Status.Phase = nextPhaseAfterProvisioning(mission)
		mission.Status.ObservedGeneration = mission.Generation
		return ctrl.Result{RequeueAfter: RequeueFast}, r.Status().Update(ctx, mission)
	}

	// Generate resource names
	uid8 := string(mission.UID)[:8]
	roundTableName := fmt.Sprintf("mission-%s-%s", mission.Name, uid8)
	natsPrefix := fmt.Sprintf("msn-%s-%s", mission.Name, uid8)
	serviceAccountName := fmt.Sprintf("mission-%s", mission.Name)
	// Use underscores in stream names (NATS requirement)
	tasksStream := fmt.Sprintf("msn_%s_%s_tasks", strings.ReplaceAll(mission.Name, "-", "_"), uid8)
	resultsStream := fmt.Sprintf("msn_%s_%s_results", strings.ReplaceAll(mission.Name, "-", "_"), uid8)

	// 1. Create ServiceAccount (if not exists)
	if err := r.Assembler.EnsureMissionServiceAccount(ctx, mission, serviceAccountName); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to ensure ServiceAccount: %w", err)
	}

	// 2. Create NetworkPolicy (if not exists)
	if err := r.Assembler.EnsureMissionNetworkPolicy(ctx, mission); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to ensure NetworkPolicy: %w", err)
	}

	// 3. Check if ephemeral RoundTable already exists
	rt := &aiv1alpha1.RoundTable{}
	rtKey := types.NamespacedName{Name: roundTableName, Namespace: mission.Namespace}
	err := r.Get(ctx, rtKey, rt)

	if err != nil {
		if client.IgnoreNotFound(err) != nil {
			return ctrl.Result{}, fmt.Errorf("failed to get ephemeral RoundTable: %w", err)
		}

		// RoundTable doesn't exist, create it
		rt = r.Assembler.BuildEphemeralRoundTable(ctx, mission, roundTableName, natsPrefix, tasksStream, resultsStream)

		log.Info("Creating ephemeral RoundTable", "name", roundTableName)
		if err := r.Create(ctx, rt); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to create ephemeral RoundTable: %w", err)
		}

		// Update Mission status with RoundTable name and stream names
		mission.Status.RoundTableName = roundTableName
		mission.Status.NATSTasksStream = tasksStream
		mission.Status.NATSResultsStream = resultsStream
		mission.Status.ObservedGeneration = mission.Generation

		if err := r.Status().Update(ctx, mission); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to update mission status: %w", err)
		}

		// Requeue to wait for RoundTable to become Ready
		return ctrl.Result{RequeueAfter: RequeueMedium}, nil
	}

	// RoundTable exists, check if it's Ready
	if rt.Status.Phase != aiv1alpha1.RoundTablePhaseReady {
		log.Info("Waiting for ephemeral RoundTable to become Ready",
			"roundTable", roundTableName,
			"currentPhase", rt.Status.Phase)
		return ctrl.Result{RequeueAfter: RequeueMedium}, nil
	}

	// RoundTable is Ready, streams are created
	log.Info("Ephemeral RoundTable ready, transitioning to Assembling",
		"roundTable", roundTableName,
		"tasksStream", tasksStream,
		"resultsStream", resultsStream)

	// Transition to Assembling phase
	mission.Status.Phase = nextPhaseAfterProvisioning(mission)
	mission.Status.ObservedGeneration = mission.Generation
	if err := r.Status().Update(ctx, mission); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{RequeueAfter: RequeueFast}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to transition to Assembling: %w", err)
	}

	return ctrl.Result{RequeueAfter: RequeueFast}, nil
}

// reconcileAssembling creates ephemeral knights and validates all knight references for readiness.
func (r *MissionReconciler) reconcileAssembling(ctx context.Context, mission *aiv1alpha1.Mission) (ctrl.Result, error) {
	oldPhase := mission.Status.Phase

	// Delegate to KnightAssembler
	result, err := r.Assembler.ReconcileAssembling(ctx, mission)
	if err != nil {
		return result, err
	}

	// Update status after assembly
	if err := r.Status().Update(ctx, mission); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update mission status: %w", err)
	}

	// Emit events for phase transitions and assembly completion
	if mission.Status.Phase != oldPhase {
		r.Recorder.Eventf(mission, corev1.EventTypeNormal, "PhaseTransition", "Mission transitioned to %s", mission.Status.Phase)
	}
	if mission.Status.Phase == aiv1alpha1.MissionPhaseBriefing {
		knightCount := len(mission.Status.KnightStatuses)
		r.Recorder.Eventf(mission, corev1.EventTypeNormal, "KnightsAssembled", "%d knights assembled", knightCount)
	}

	return result, nil
}

// reconcileBriefing publishes the mission briefing to NATS and transitions to Active.
func (r *MissionReconciler) reconcileBriefing(ctx context.Context, mission *aiv1alpha1.Mission) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Publish briefing to NATS
	if mission.Spec.Briefing != "" {
		if err := r.Briefer.PublishBriefing(ctx, mission); err != nil {
			log.Error(err, "Failed to publish briefing, will retry")
			meta.SetStatusCondition(&mission.Status.Conditions, metav1.Condition{
				Type:               aiv1alpha1.ConditionBriefingPublished,
				Status:             metav1.ConditionFalse,
				Reason:             aiv1alpha1.ReasonBriefingPublishFailed,
				Message:            fmt.Sprintf("Failed to publish briefing: %v", err),
				ObservedGeneration: mission.Generation,
			})
			mission.Status.ObservedGeneration = mission.Generation
			if statusErr := r.Status().Update(ctx, mission); statusErr != nil {
				log.Error(statusErr, "Failed to update status after briefing publish failure")
			}
			return ctrl.Result{RequeueAfter: RequeueDefault}, nil
		}

		log.Info("Briefing published", "mission", mission.Name)
		meta.SetStatusCondition(&mission.Status.Conditions, metav1.Condition{
			Type:               aiv1alpha1.ConditionBriefingPublished,
			Status:             metav1.ConditionTrue,
			Reason:             aiv1alpha1.ReasonBriefingPublished,
			Message:            "Mission briefing published to all knights",
			ObservedGeneration: mission.Generation,
		})
	} else {
		meta.SetStatusCondition(&mission.Status.Conditions, metav1.Condition{
			Type:               aiv1alpha1.ConditionBriefingPublished,
			Status:             metav1.ConditionTrue,
			Reason:             aiv1alpha1.ReasonNoBriefing,
			Message:            "No briefing text configured",
			ObservedGeneration: mission.Generation,
		})
	}

	// Bug #3 Fix: Trigger mission-generated chains to Running phase.
	// Generated chains remain in Idle after the planner creates them.
	// The chain controller only triggers chains via cron schedule, so mission-generated
	// chains need to be manually started by setting their status.phase to Running.
	if err := r.Chains.TriggerGeneratedChains(ctx, mission); err != nil {
		log.Error(err, "Failed to trigger generated chains, will retry")
		return ctrl.Result{RequeueAfter: RequeueMedium}, nil
	}

	mission.Status.Phase = aiv1alpha1.MissionPhaseActive
	mission.Status.ObservedGeneration = mission.Generation
	err := r.Status().Update(ctx, mission)
	if apierrors.IsConflict(err) {
		return ctrl.Result{RequeueAfter: RequeueFast}, nil
	}
	r.Recorder.Eventf(mission, corev1.EventTypeNormal, "PhaseTransition", "Mission transitioned to %s", aiv1alpha1.MissionPhaseActive)
	return ctrl.Result{RequeueAfter: RequeueFast}, err
}

// reconcileActive monitors chain execution, timeout, and knight status.
func (r *MissionReconciler) reconcileActive(ctx context.Context, mission *aiv1alpha1.Mission) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Check timeout
	if mission.Status.StartedAt != nil {
		elapsed := time.Since(mission.Status.StartedAt.Time)
		if elapsed > time.Duration(mission.Spec.TimeoutSeconds)*time.Second {
			log.Info("Mission timed out", "mission", mission.Name, "elapsed", elapsed)
			err := status.ForMission(mission).
				Failed(fmt.Sprintf("Mission timed out after %ds", mission.Spec.TimeoutSeconds)).
				Condition(aiv1alpha1.ConditionMissionComplete, aiv1alpha1.ReasonMissionTimeout,
					fmt.Sprintf("Mission timed out after %ds", mission.Spec.TimeoutSeconds),
					metav1.ConditionTrue).
				Apply(ctx, r.Client)
			if apierrors.IsConflict(err) {
				return ctrl.Result{RequeueAfter: RequeueFast}, nil
			}
			return ctrl.Result{}, err
		}
	}

	// Check cost budget
	budget, err := r.CostTracker.CheckBudget(ctx, mission)
	if err != nil {
		log.Error(err, "Failed to aggregate mission cost")
	} else if budget.Exceeded {
		log.Info("Mission cost budget exceeded", "totalCost", budget.Total, "budget", budget.Budget)

		// Suspend all mission-owned chains to prevent further cost
		if err := r.CostTracker.SuspendMissionChains(ctx, mission); err != nil {
			log.Error(err, "Failed to suspend mission chains")
		}

		err := status.ForMission(mission).
			Failed(fmt.Sprintf("Cost budget exceeded: $%.2f > $%.2f", budget.Total, budget.Budget)).
			Condition(aiv1alpha1.ConditionMissionComplete, aiv1alpha1.ReasonOverBudget,
				fmt.Sprintf("Cost $%.2f exceeded budget $%.2f", budget.Total, budget.Budget),
				metav1.ConditionTrue).
			Apply(ctx, r.Client)
		if apierrors.IsConflict(err) {
			return ctrl.Result{RequeueAfter: RequeueFast}, nil
		}
		return ctrl.Result{}, err
	}

	// The budget check already read every chain; remember what it saw so the
	// breakdown below is only recomputed when the chain statuses changed.
	var aggregatedFrom []aiv1alpha1.MissionChainStatus
	if err == nil && budget.Aggregated {
		aggregatedFrom = append(aggregatedFrom, mission.Status.ChainStatuses...)
	}

	// Create Chain CRs for any referenced chains that don't exist yet
	if len(mission.Spec.Chains) > 0 {
		allChainsComplete, anyChainFailed, err := r.Chains.ReconcileChains(ctx, mission)
		if err != nil {
			log.Error(err, "Failed to reconcile mission chains")
			return ctrl.Result{RequeueAfter: RequeueDefault}, nil
		}

		// Expose the chains' cost and tokens on the mission.
		missionpkg.ApplyChainUsage(mission)
		// Per-knight cost breakdown, with or without a budget.
		// ReconcileChains may have changed the chain statuses since the budget
		// check; reuse its breakdown only when it did not.
		if aggregatedFrom == nil || !apiequality.Semantic.DeepEqual(aggregatedFrom, mission.Status.ChainStatuses) {
			if _, err := r.CostTracker.AggregateMissionCost(ctx, mission); err != nil {
				log.Error(err, "Failed to aggregate mission cost breakdown")
			}
		}

		// FIX #2: Guard transition to terminal state - ensure all chains in status.chainStatuses
		// have reached terminal phase before allowing mission to transition to Succeeded/Failed.
		// This prevents premature cleanup while chains are still running.
		if anyChainFailed || allChainsComplete {
			// Double-check all chains in status have reached terminal state
			hasNonTerminalChains := false
			for _, cs := range mission.Status.ChainStatuses {
				if cs.Phase != aiv1alpha1.ChainPhaseSucceeded && cs.Phase != aiv1alpha1.ChainPhaseFailed {
					hasNonTerminalChains = true
					log.V(1).Info("Waiting for chain to reach terminal state",
						"chain", cs.Name,
						"currentPhase", cs.Phase)
					break
				}
			}

			if hasNonTerminalChains {
				// Requeue and wait for all chains to finish
				log.Info("Chains still running, waiting for completion before transitioning mission")
				return ctrl.Result{RequeueAfter: RequeueDefault}, nil
			}
		}

		if anyChainFailed {
			mission.Status.Phase = aiv1alpha1.MissionPhaseFailed
			now := metav1.Now()
			mission.Status.CompletedAt = &now
			failMsg := r.failedChainsMessage(ctx, mission)
			mission.Status.Result = failMsg
			meta.SetStatusCondition(&mission.Status.Conditions, metav1.Condition{
				Type:               aiv1alpha1.ConditionMissionComplete,
				Status:             metav1.ConditionTrue,
				Reason:             aiv1alpha1.ReasonMissionChainFailed,
				Message:            failMsg,
				ObservedGeneration: mission.Generation,
			})
			mission.Status.ObservedGeneration = mission.Generation
			err := r.Status().Update(ctx, mission)
			if apierrors.IsConflict(err) {
				return ctrl.Result{RequeueAfter: RequeueFast}, nil
			}
			return ctrl.Result{}, err
		}

		if allChainsComplete {
			mission.Status.Phase = aiv1alpha1.MissionPhaseSucceeded
			now := metav1.Now()
			mission.Status.CompletedAt = &now
			mission.Status.Result = "All mission chains completed successfully"
			meta.SetStatusCondition(&mission.Status.Conditions, metav1.Condition{
				Type:               aiv1alpha1.ConditionMissionComplete,
				Status:             metav1.ConditionTrue,
				Reason:             aiv1alpha1.ReasonMissionSucceeded,
				Message:            "All mission chains completed successfully",
				ObservedGeneration: mission.Generation,
			})
			mission.Status.ObservedGeneration = mission.Generation
			err := r.Status().Update(ctx, mission)
			if apierrors.IsConflict(err) {
				return ctrl.Result{RequeueAfter: RequeueFast}, nil
			}
			return ctrl.Result{}, err
		}
	} else {
		// No chains — stay Active until TTL expires or external completion.
		// Knights may still receive ad-hoc tasks via NATS during the mission window.
		log.V(1).Info("No chains defined, mission remains Active awaiting TTL or external completion",
			"mission", mission.Name)
	}

	// Update knight statuses
	r.updateKnightStatuses(ctx, mission)
	mission.Status.ObservedGeneration = mission.Generation
	if statusErr := r.Status().Update(ctx, mission); statusErr != nil {
		log.Error(statusErr, "Failed to update status with knight statuses")
	}
	return ctrl.Result{RequeueAfter: RequeueDefault}, nil
}

// reconcileCleaningUp handles resource cleanup and optional self-deletion.
func (r *MissionReconciler) reconcileCleaningUp(ctx context.Context, mission *aiv1alpha1.Mission) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// If cleanup already completed, skip directly to terminal phase transition.
	// This prevents repeated delete attempts on every reconcile loop.
	if meta.IsStatusConditionTrue(mission.Status.Conditions, aiv1alpha1.ConditionCleanupComplete) {
		return r.transitionToTerminalPhase(ctx, mission)
	}

	// Run teardown chains if any
	for _, chainRef := range mission.Spec.Chains {
		if chainRef.Phase != "Teardown" {
			continue
		}
		chain := &aiv1alpha1.Chain{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      chainRef.Name,
			Namespace: mission.Namespace,
		}, chain); err != nil {
			log.Info("Teardown chain not found, skipping", "chain", chainRef.Name)
			continue
		}
		// If teardown chain hasn't run yet, trigger it
		if chain.Status.Phase == aiv1alpha1.ChainPhaseIdle {
			now := metav1.Now()
			chain.Status.Phase = aiv1alpha1.ChainPhaseRunning
			chain.Status.StartedAt = &now
			if err := r.Status().Update(ctx, chain); err != nil {
				log.Error(err, "Failed to trigger teardown chain", "chain", chainRef.Name)
			}
			// Requeue to wait for teardown
			return ctrl.Result{RequeueAfter: RequeueDefault}, nil
		}
		// If teardown chain is still running, wait
		if chain.Status.Phase == aiv1alpha1.ChainPhaseRunning {
			return ctrl.Result{RequeueAfter: RequeueDefault}, nil
		}
	}

	// Store results to NATS KV if retainResults is true and not already stored
	if mission.Spec.RetainResults && mission.Status.ResultsRef == nil {
		if err := r.Cleaner.StoreResultsToKV(ctx, mission); err != nil {
			log.Error(err, "Failed to store results to NATS KV")
			// Continue with cleanup. resultsRef stays unset: it would point at an
			// entry that was never written. Cleanup runs once, so there is no retry.
			msg := boundMessage("Failed to store results to NATS KV: " + err.Error())
			// Cleanup can retry this step; warn once, not on every retry.
			alreadyReported := meta.IsStatusConditionFalse(mission.Status.Conditions, aiv1alpha1.ConditionMissionResultsRetained)
			meta.SetStatusCondition(&mission.Status.Conditions, metav1.Condition{
				Type:               aiv1alpha1.ConditionMissionResultsRetained,
				Status:             metav1.ConditionFalse,
				Reason:             aiv1alpha1.ReasonResultsStoreFailed,
				Message:            msg,
				ObservedGeneration: mission.Generation,
			})
			if !alreadyReported {
				r.Recorder.Event(mission, corev1.EventTypeWarning, "ResultsNotRetained", msg)
			}
		} else {
			meta.SetStatusCondition(&mission.Status.Conditions, metav1.Condition{
				Type:               aiv1alpha1.ConditionMissionResultsRetained,
				Status:             metav1.ConditionTrue,
				Reason:             aiv1alpha1.ReasonResultsStored,
				Message:            "Results stored in NATS KV",
				ObservedGeneration: mission.Generation,
			})
		}
	}

	// Determine if we should actually delete resources based on cleanupPolicy
	shouldDelete := r.Cleaner.ShouldDeleteResources(mission)

	if shouldDelete {
		// Step 1: Delete ephemeral Knight CRs
		log.Info("Deleting ephemeral Knight CRs")
		if err := r.Cleaner.DeleteEphemeralKnights(ctx, mission); err != nil {
			log.Error(err, "Failed to delete ephemeral knights, retrying")
			return ctrl.Result{RequeueAfter: RequeueDefault}, nil
		}

		// Step 2: Delete NATS consumers (best effort)
		log.Info("Deleting NATS consumers")
		if err := r.Cleaner.DeleteNATSConsumers(ctx, mission); err != nil {
			log.Error(err, "Failed to delete NATS consumers (best effort, continuing)")
			// Continue even if this fails - consumers will be deleted when stream is deleted
		}

		// Step 3: Delete NATS streams
		if mission.Status.NATSTasksStream != "" || mission.Status.NATSResultsStream != "" {
			log.Info("Deleting NATS streams",
				"tasksStream", mission.Status.NATSTasksStream,
				"resultsStream", mission.Status.NATSResultsStream)
			if err := r.Cleaner.DeleteNATSStreams(ctx, mission); err != nil {
				log.Error(err, "Failed to delete NATS streams, retrying with backoff")
				return ctrl.Result{RequeueAfter: RequeueModerate}, nil
			}
		}

		// Step 4: Delete ephemeral RoundTable (owner ref handles cascade)
		if mission.Status.RoundTableName != "" {
			log.Info("Deleting ephemeral RoundTable", "name", mission.Status.RoundTableName)
			if err := r.Cleaner.DeleteEphemeralRoundTable(ctx, mission); err != nil {
				log.Error(err, "Failed to delete RoundTable, retrying")
				return ctrl.Result{RequeueAfter: RequeueDefault}, nil
			}
		}
	}

	// Mark cleanup as done
	meta.SetStatusCondition(&mission.Status.Conditions, metav1.Condition{
		Type:               aiv1alpha1.ConditionCleanupComplete,
		Status:             metav1.ConditionTrue,
		Reason:             aiv1alpha1.ReasonCleanupComplete,
		Message:            "Mission cleanup completed",
		ObservedGeneration: mission.Generation,
	})

	r.Recorder.Event(mission, corev1.EventTypeNormal, "CleanupComplete", "Mission resources cleaned up")

	return r.transitionToTerminalPhase(ctx, mission)
}

// transitionToTerminalPhase determines the final mission state and handles self-deletion.
func (r *MissionReconciler) transitionToTerminalPhase(ctx context.Context, mission *aiv1alpha1.Mission) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Transition to terminal phase based on original outcome
	mission.Status.Phase = missionpkg.TerminalOutcome(mission)

	mission.Status.ObservedGeneration = mission.Generation
	if err := r.Status().Update(ctx, mission); err != nil {
		log.Error(err, "Failed to update status during terminal phase transition")
	}

	// Self-delete if cleanupPolicy=Delete and TTL expired
	if mission.Spec.CleanupPolicy == missionpkg.CleanupPolicyDelete &&
		mission.Status.ExpiresAt != nil &&
		time.Now().After(mission.Status.ExpiresAt.Time) {
		log.Info("Deleting expired mission", "mission", mission.Name)
		if err := r.Delete(ctx, mission); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// If cleanupPolicy=Delete but TTL hasn't expired yet, requeue
	if mission.Spec.CleanupPolicy == missionpkg.CleanupPolicyDelete && mission.Status.ExpiresAt != nil {
		remaining := time.Until(mission.Status.ExpiresAt.Time)
		if remaining > 0 {
			return ctrl.Result{RequeueAfter: remaining}, nil
		}
	}

	return ctrl.Result{}, nil
}

// updateKnightStatuses refreshes knight readiness from current Knight CRs.
func (r *MissionReconciler) updateKnightStatuses(ctx context.Context, mission *aiv1alpha1.Mission) {
	for i, mk := range mission.Spec.Knights {
		if mk.Ephemeral {
			continue
		}
		knight := &aiv1alpha1.Knight{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      mk.Name,
			Namespace: mission.Namespace,
		}, knight); err != nil {
			mission.Status.KnightStatuses[i].Ready = false
			continue
		}
		mission.Status.KnightStatuses[i].Ready = knight.Status.Ready
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *MissionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	for _, dep := range []struct {
		name string
		nil  bool
	}{
		{"Planner", r.Planner == nil},
		{"Assembler", r.Assembler == nil},
		{"Cleaner", r.Cleaner == nil},
		{"CostTracker", r.CostTracker == nil},
		{"Briefer", r.Briefer == nil},
		{"Chains", r.Chains == nil},
	} {
		if dep.nil {
			return fmt.Errorf("MissionReconciler.%s must be set", dep.name)
		}
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&aiv1alpha1.Mission{}).
		Owns(&aiv1alpha1.Chain{}).
		Owns(&aiv1alpha1.Knight{}).
		Owns(&aiv1alpha1.RoundTable{}).
		Watches(&aiv1alpha1.Mission{},
			handler.EnqueueRequestsFromMapFunc(r.pendingMissionsOfFreedTable),
			builder.WithPredicates(missionLeftActivePhase())).
		Named("mission").
		Complete(r)
}

// failedChainsMessage names the failed chains of a mission, each with its
// Chain's Complete condition message (which names the failed steps).
func (r *MissionReconciler) failedChainsMessage(ctx context.Context, mission *aiv1alpha1.Mission) string {
	var failed []string
	for _, cs := range mission.Status.ChainStatuses {
		if cs.Phase != aiv1alpha1.ChainPhaseFailed {
			continue
		}
		part := "chain " + cs.Name
		crName := cs.ChainCRName
		if crName == "" {
			crName = cs.Name
		}
		chain := &aiv1alpha1.Chain{}
		if err := r.Get(ctx, types.NamespacedName{Name: crName, Namespace: mission.Namespace}, chain); err == nil {
			if c := meta.FindStatusCondition(chain.Status.Conditions, aiv1alpha1.ConditionChainComplete); c != nil && c.Message != "" {
				part += ": " + c.Message
			}
		}
		failed = append(failed, part)
	}
	if len(failed) == 0 {
		return "One or more mission chains failed"
	}
	if len(failed) > maxFailedStepsNamed {
		n := len(failed) - maxFailedStepsNamed
		failed = append(failed[:maxFailedStepsNamed], fmt.Sprintf("and %d more", n))
	}
	return "Mission chains failed: " + strings.Join(failed, "; ")
}

// maxConditionMessage bounds error text embedded in condition messages
// (the API limit is 32768 characters).
const maxConditionMessage = 1000

func boundMessage(s string) string { return missionpkg.TruncateMessage(s, maxConditionMessage) }

// missionLeftActivePhase passes only events that free a maxMissions slot: a
// phase transition out of an active phase, or the deletion of an active mission.
func missionLeftActivePhase() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc:  func(event.CreateEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
		UpdateFunc: func(e event.UpdateEvent) bool {
			o, ok1 := e.ObjectOld.(*aiv1alpha1.Mission)
			n, ok2 := e.ObjectNew.(*aiv1alpha1.Mission)
			return ok1 && ok2 && o.Status.Phase.IsActive() && !n.Status.Phase.IsActive()
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			m, ok := e.Object.(*aiv1alpha1.Mission)
			return ok && m.Status.Phase.IsActive()
		},
	}
}

// pendingMissionsOfFreedTable enqueues the Pending missions of the freed
// mission's RoundTable so a held mission is admitted without waiting for its
// requeue poll.
func (r *MissionReconciler) pendingMissionsOfFreedTable(ctx context.Context, obj client.Object) []reconcile.Request {
	m, ok := obj.(*aiv1alpha1.Mission)
	if !ok {
		return nil
	}
	table := aiv1alpha1.RefName(m.Spec.RoundTableRef)
	if table == "" {
		return nil
	}
	list := &aiv1alpha1.MissionList{}
	if err := r.List(ctx, list, client.InNamespace(m.Namespace)); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list missions to wake held ones", "table", table)
		return nil
	}
	var reqs []reconcile.Request
	for i := range list.Items {
		p := &list.Items[i]
		if p.UID == m.UID || aiv1alpha1.RefName(p.Spec.RoundTableRef) != table || !p.DeletionTimestamp.IsZero() {
			continue
		}
		if p.Status.Phase == aiv1alpha1.MissionPhasePending || p.Status.Phase == "" {
			reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(p)})
		}
	}
	return reqs
}
