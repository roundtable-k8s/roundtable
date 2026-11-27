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
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/deadletter"
	rtmetrics "github.com/roundtable-k8s/roundtable/pkg/metrics"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// labelValueTrue is the label value used for boolean-style labels.
const labelValueTrue = "true"

// RoundTableReconciler reconciles a RoundTable object.
type RoundTableReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	NATS *natspkg.Provider

	// DeadLetters captures each table's lost and failed tasks into its
	// dead-letter stream (#396). Nil disables it.
	DeadLetters *deadletter.Manager

	// CardsBucket is the NATS KV bucket holding the agent cards (#393);
	// empty means natspkg.DefaultCardsBucket.
	CardsBucket string

	// ServiceInfoTimeout bounds the $SRV.INFO request for the skills of
	// running knights; zero means one second.
	ServiceInfoTimeout time.Duration

	// PlannerDefaults are the install-wide defaults for table-owned planner
	// knights (Helm planner.*); zero fields use DefaultPlannerDefaults.
	PlannerDefaults PlannerDefaults

	// Now overrides the clock (tests).
	Now func() time.Time

	skillsMu        sync.Mutex
	skillsQueriedAt map[string]time.Time
}

// natsClient returns the shared NATS client, or an error if the provider is not configured.
func (r *RoundTableReconciler) natsClient() (natspkg.Client, error) {
	if r.NATS == nil {
		return nil, fmt.Errorf("NATS provider not configured")
	}
	return r.NATS.Client()
}

// +kubebuilder:rbac:groups=ai.roundtable.io,resources=roundtables,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=roundtables/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=roundtables/finalizers,verbs=update
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=knights,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=missions,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

func (r *RoundTableReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	rt := &aiv1alpha1.RoundTable{}
	if err := r.Get(ctx, req.NamespacedName, rt); err != nil {
		if client.IgnoreNotFound(err) == nil {
			if r.DeadLetters != nil {
				r.DeadLetters.Stop(req.String())
			}
			r.cleanupDeletedTableCards(ctx, req.Name)
			r.skillsMu.Lock()
			delete(r.skillsQueriedAt, req.Namespace+"/"+req.Name)
			r.skillsMu.Unlock()
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	log = log.WithValues("table", rt.Name)
	ctx = logf.IntoContext(ctx, log)

	// The table's planner knight (#566). It scales to zero when idle, so it
	// is kept while the table is suspended.
	plannerKnight, err := r.reconcilePlanner(ctx, rt)
	if err != nil {
		log.Error(err, "Failed to reconcile planner Knight")
		r.Recorder.Event(rt, corev1.EventTypeWarning, "PlannerFailed", err.Error())
	}
	rt.Status.PlannerKnight = plannerKnight

	// Handle suspended state
	if rt.Spec.Suspend {
		// Cards outlive suspension: knights at zero keep theirs.
		if knights, err := r.discoverKnights(ctx, rt); err == nil {
			r.reconcileCards(ctx, rt, knights)
		}
		rt.Status.Phase = aiv1alpha1.RoundTablePhaseSuspended
		meta.SetStatusCondition(&rt.Status.Conditions, metav1.Condition{
			Type:               aiv1alpha1.ConditionRoundTableAvailable,
			Status:             metav1.ConditionFalse,
			Reason:             aiv1alpha1.ReasonRoundTableSuspended,
			Message:            "RoundTable is suspended",
			ObservedGeneration: rt.Generation,
		})
		rt.Status.ObservedGeneration = rt.Generation
		if err := r.Status().Update(ctx, rt); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: RequeueVerySlow}, nil
	}

	// 1. Knight Discovery
	knights, err := r.discoverKnights(ctx, rt)
	if err != nil {
		log.Error(err, "Failed to discover knights")
		return ctrl.Result{RequeueAfter: RequeueSlow}, err
	}

	// 2. Health Aggregation
	knightSummaries := make([]aiv1alpha1.RoundTableKnightSummary, 0, len(knights))
	for _, k := range knights {
		knightSummaries = append(knightSummaries, aiv1alpha1.RoundTableKnightSummary{
			Name:  k.Name,
			Phase: k.Status.Phase,
			Ready: k.Status.Ready,
		})
	}
	counts := countKnights(knights)

	// Cost and task totals come from Chains and Missions; the Knight status
	// fields are not written by anything (#547).
	totalTasksCompleted, totalCost, usageErr := r.tableUsage(ctx, rt)
	if usageErr != nil {
		log.Error(usageErr, "Failed to compute table usage")
	}

	total := int32(len(knights))
	rt.Status.KnightsTotal = total
	rt.Status.KnightsReady = counts.ready
	rt.Status.KnightsScaledToZero = counts.scaledToZero
	rt.Status.KnightsSuspended = counts.suspended
	rt.Status.KnightsNotReady = counts.notReady
	rt.Status.Knights = knightSummaries
	rt.Status.TotalTasksCompleted = totalTasksCompleted
	rt.Status.TotalCost = fmt.Sprintf("%.4f", totalCost)

	// 3. NATS Stream Management
	if rt.Spec.NATS.CreateStreams {
		if err := r.ensureStreams(ctx, rt); err != nil {
			log.Error(err, "Failed to ensure NATS streams")
			meta.SetStatusCondition(&rt.Status.Conditions, metav1.Condition{
				Type:               aiv1alpha1.ConditionNATSReady,
				Status:             metav1.ConditionFalse,
				Reason:             aiv1alpha1.ReasonStreamError,
				Message:            err.Error(),
				ObservedGeneration: rt.Generation,
			})
		} else {
			meta.SetStatusCondition(&rt.Status.Conditions, metav1.Condition{
				Type:               aiv1alpha1.ConditionNATSReady,
				Status:             metav1.ConditionTrue,
				Reason:             aiv1alpha1.ReasonStreamsReady,
				Message:            "JetStream streams are configured",
				ObservedGeneration: rt.Generation,
			})
		}
	}

	// 3b. Agent cards (#393)
	r.reconcileCards(ctx, rt, knights)

	// 4. Cost Budget Check
	phase := r.computePhase(rt, counts, totalCost)
	rt.Status.Phase = phase

	// 5. Active Missions count
	activeMissions, err := r.countActiveMissions(ctx, rt)
	if err != nil {
		log.Error(err, "Failed to count active missions")
	}
	rt.Status.ActiveMissions = activeMissions

	// Set availability condition
	switch phase {
	case aiv1alpha1.RoundTablePhaseReady:
		meta.SetStatusCondition(&rt.Status.Conditions, metav1.Condition{
			Type:               aiv1alpha1.ConditionRoundTableAvailable,
			Status:             metav1.ConditionTrue,
			Reason:             aiv1alpha1.ReasonAllKnightsReady,
			Message:            counts.String(),
			ObservedGeneration: rt.Generation,
		})
	case aiv1alpha1.RoundTablePhaseDegraded:
		meta.SetStatusCondition(&rt.Status.Conditions, metav1.Condition{
			Type:               aiv1alpha1.ConditionRoundTableAvailable,
			Status:             metav1.ConditionFalse,
			Reason:             aiv1alpha1.ReasonKnightsDegraded,
			Message:            counts.String(),
			ObservedGeneration: rt.Generation,
		})
	case aiv1alpha1.RoundTablePhaseOverBudget:
		meta.SetStatusCondition(&rt.Status.Conditions, metav1.Condition{
			Type:               aiv1alpha1.ConditionRoundTableAvailable,
			Status:             metav1.ConditionFalse,
			Reason:             aiv1alpha1.ReasonOverBudget,
			Message:            fmt.Sprintf("Cost %.4f exceeds budget %s", totalCost, rt.Spec.Policies.CostBudgetUSD),
			ObservedGeneration: rt.Generation,
		})
		r.Recorder.Event(rt, corev1.EventTypeWarning, "BudgetExceeded", "Cost budget exceeded")
	default:
		meta.SetStatusCondition(&rt.Status.Conditions, metav1.Condition{
			Type:               aiv1alpha1.ConditionRoundTableAvailable,
			Status:             metav1.ConditionFalse,
			Reason:             aiv1alpha1.ReasonRoundTableProvisioning,
			Message:            "RoundTable is provisioning",
			ObservedGeneration: rt.Generation,
		})
	}

	r.setDeprecatedFieldsCondition(rt)
	r.setPlannerKnightCondition(ctx, rt, knights)

	rt.Status.ObservedGeneration = rt.Generation

	// Update Prometheus metrics
	r.updateMetrics(ctx, rt, knights, totalCost)

	if err := r.Status().Update(ctx, rt); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: RequeueVerySlow}, nil
}

// setDeprecatedFieldsCondition reports unenforced policies on a standing
// table: policies.costResetSchedule. It sets DeprecatedFieldsUsed=True naming
// the field and records one Warning event per generation. The condition is
// removed when it is unset; ephemeral tables (mission-owned) never get it.
func (r *RoundTableReconciler) setDeprecatedFieldsCondition(rt *aiv1alpha1.RoundTable) {
	var used []string
	if p := rt.Spec.Policies; p != nil && !rt.Spec.Ephemeral {
		if p.CostResetSchedule != "" { //nolint:staticcheck // reading the deprecated field to report it
			used = append(used, "policies.costResetSchedule (not enforced: nothing resets the cost counter, reported cost is cumulative)")
		}
	}
	if len(used) == 0 {
		meta.RemoveStatusCondition(&rt.Status.Conditions, aiv1alpha1.ConditionRoundTableDeprecatedFieldsUsed)
		return
	}
	msg := "deprecated and ignored: " + strings.Join(used, "; ")
	prev := meta.FindStatusCondition(rt.Status.Conditions, aiv1alpha1.ConditionRoundTableDeprecatedFieldsUsed)
	if prev == nil || prev.ObservedGeneration != rt.Generation {
		// event-reasons: DeprecatedFieldsUsed
		r.Recorder.Event(rt, corev1.EventTypeWarning, aiv1alpha1.ReasonRoundTableDeprecatedFieldsUsed, msg)
	}
	meta.SetStatusCondition(&rt.Status.Conditions, metav1.Condition{
		Type:               aiv1alpha1.ConditionRoundTableDeprecatedFieldsUsed,
		Status:             metav1.ConditionTrue,
		Reason:             aiv1alpha1.ReasonRoundTableDeprecatedFieldsUsed,
		Message:            msg,
		ObservedGeneration: rt.Generation,
	})
}

// setPlannerKnightCondition checks spec.planner.knightRef: a missing Knight, or
// one outside the table (not in the discovered knights), sets
// PlannerKnightMissing=True with a message and a Warning event once per
// generation. It is advisory and never fails the reconcile.
func (r *RoundTableReconciler) setPlannerKnightCondition(ctx context.Context, rt *aiv1alpha1.RoundTable, members []aiv1alpha1.Knight) {
	name := ""
	if rt.PlannerEnabled() && rt.Spec.Planner != nil {
		name = aiv1alpha1.RefName(rt.Spec.Planner.KnightRef)
	}
	if name == "" {
		meta.RemoveStatusCondition(&rt.Status.Conditions, aiv1alpha1.ConditionRoundTablePlannerKnightMissing)
		return
	}
	var reason, msg string
	k := &aiv1alpha1.Knight{}
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: rt.Namespace}, k)
	switch {
	case apierrors.IsNotFound(err):
		reason = aiv1alpha1.ReasonPlannerKnightMissing
		msg = fmt.Sprintf("spec.planner.knightRef names Knight %q, which does not exist in namespace %q", name, rt.Namespace)
	case err != nil:
		return // transient: keep the previous condition
	default:
		inTable := false
		for i := range members {
			if members[i].Name == name {
				inTable = true
				break
			}
		}
		if inTable {
			meta.RemoveStatusCondition(&rt.Status.Conditions, aiv1alpha1.ConditionRoundTablePlannerKnightMissing)
			return
		}
		reason = aiv1alpha1.ReasonPlannerKnightNotInTable
		msg = fmt.Sprintf("spec.planner.knightRef names Knight %q, which is not a member of this table (check spec.knightSelector)", name)
	}
	prev := meta.FindStatusCondition(rt.Status.Conditions, aiv1alpha1.ConditionRoundTablePlannerKnightMissing)
	if prev == nil || prev.ObservedGeneration != rt.Generation || prev.Reason != reason {
		// event-reasons: PlannerKnightMissing PlannerKnightNotInTable
		r.Recorder.Event(rt, corev1.EventTypeWarning, reason, msg)
	}
	meta.SetStatusCondition(&rt.Status.Conditions, metav1.Condition{
		Type:               aiv1alpha1.ConditionRoundTablePlannerKnightMissing,
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: rt.Generation,
	})
}

// discoverKnights lists Knight CRs matching the RoundTable's knightSelector.
// For ephemeral RoundTables, it returns only knights with the matching round-table label.
// For non-ephemeral RoundTables, it excludes all ephemeral knights.
func (r *RoundTableReconciler) discoverKnights(ctx context.Context, rt *aiv1alpha1.RoundTable) ([]aiv1alpha1.Knight, error) {
	knightList := &aiv1alpha1.KnightList{}
	listOpts := []client.ListOption{
		client.InNamespace(rt.Namespace),
	}

	if rt.Spec.Ephemeral {
		// Ephemeral RoundTable: only manage knights that belong to this specific table
		listOpts = append(listOpts, client.MatchingLabels{
			aiv1alpha1.LabelRoundTable: rt.Name,
		})
	} else {
		// Non-ephemeral RoundTable: manage all non-ephemeral knights
		// Apply knight selector if specified
		if rt.Spec.KnightSelector != nil {
			selector, err := metav1.LabelSelectorAsSelector(rt.Spec.KnightSelector)
			if err != nil {
				return nil, fmt.Errorf("invalid knightSelector: %w", err)
			}
			listOpts = append(listOpts, client.MatchingLabelsSelector{Selector: selector})
		}
	}

	if err := r.List(ctx, knightList, listOpts...); err != nil {
		return nil, fmt.Errorf("failed to list knights: %w", err)
	}
	r.appendOwnedPlanner(ctx, rt, knightList)

	// For non-ephemeral RoundTables, filter out any ephemeral knights
	// (in case knightSelector didn't exclude them)
	if !rt.Spec.Ephemeral {
		filtered := make([]aiv1alpha1.Knight, 0, len(knightList.Items))
		for _, knight := range knightList.Items {
			if knight.Labels[aiv1alpha1.LabelEphemeral] != labelValueTrue {
				filtered = append(filtered, knight)
			}
		}
		return filtered, nil
	}

	return knightList.Items, nil
}

// appendOwnedPlanner adds the table-owned planner knight to the list when the
// knightSelector left it out, so it gets a card and counts as the table's.
func (r *RoundTableReconciler) appendOwnedPlanner(ctx context.Context, rt *aiv1alpha1.RoundTable, list *aiv1alpha1.KnightList) {
	if !rt.OwnsPlanner() {
		return
	}
	name := rt.OwnedPlannerName()
	for i := range list.Items {
		if list.Items[i].Name == name {
			return
		}
	}
	k := &aiv1alpha1.Knight{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: rt.Namespace, Name: name}, k); err != nil {
		return
	}
	if metav1.IsControlledBy(k, rt) {
		list.Items = append(list.Items, *k)
	}
}

// knightCounts splits a table's knights by health. Knights scaled to zero by
// KEDA and knights suspended by spec.suspend are at rest on purpose; only
// notReady knights (should be running but are not) degrade the table.
type knightCounts struct {
	ready, scaledToZero, suspended, notReady int32
}

func (c knightCounts) total() int32 {
	return c.ready + c.scaledToZero + c.suspended + c.notReady
}

// String is the Available condition message, e.g.
// "6 knights: 1 ready, 4 scaled to zero, 1 suspended, 0 not ready".
func (c knightCounts) String() string {
	return fmt.Sprintf("%d knights: %d ready, %d scaled to zero, %d suspended, %d not ready",
		c.total(), c.ready, c.scaledToZero, c.suspended, c.notReady)
}

// countKnights classifies each knight as ready, scaled to zero (an autoscaled
// knight at zero, idle or waking), suspended (spec.suspend) or not ready.
func countKnights(knights []aiv1alpha1.Knight) knightCounts {
	var c knightCounts
	for i := range knights {
		k := &knights[i]
		switch {
		case k.Status.Ready:
			c.ready++
		case k.Spec.Suspend:
			c.suspended++
		case isScaledToZero(k):
			c.scaledToZero++
		default:
			c.notReady++
		}
	}
	return c
}

// isScaledToZero reports whether KEDA scaled the knight to zero: phase
// Suspended with Available reason ScaledToZero (idle) or Waking (a task is
// queued and its pod is starting).
func isScaledToZero(k *aiv1alpha1.Knight) bool {
	if k.Status.Phase != aiv1alpha1.KnightPhaseSuspended {
		return false
	}
	cond := meta.FindStatusCondition(k.Status.Conditions, aiv1alpha1.ConditionKnightAvailable)
	return cond != nil &&
		(cond.Reason == aiv1alpha1.ReasonKnightScaledToZero || cond.Reason == aiv1alpha1.ReasonKnightWaking)
}

// computePhase determines the RoundTable phase based on knight health and
// cost. The table is Degraded only when a knight that should be running is
// not; knights scaled to zero or suspended on purpose keep it Ready.
func (r *RoundTableReconciler) computePhase(rt *aiv1alpha1.RoundTable, counts knightCounts, totalCost float64) aiv1alpha1.RoundTablePhase {
	// Check cost budget
	if rt.Spec.Policies != nil && rt.Spec.Policies.CostBudgetUSD != "" && rt.Spec.Policies.CostBudgetUSD != "0" {
		budget, err := strconv.ParseFloat(rt.Spec.Policies.CostBudgetUSD, 64)
		if err == nil && totalCost > budget {
			return aiv1alpha1.RoundTablePhaseOverBudget
		}
	}

	if counts.total() == 0 {
		return aiv1alpha1.RoundTablePhaseProvisioning
	}

	if counts.notReady == 0 {
		return aiv1alpha1.RoundTablePhaseReady
	}

	return aiv1alpha1.RoundTablePhaseDegraded
}

// tableUsage sums the tasks completed and cost of the table's work: the
// runsCompleted and current-run costUSD of its Chains, plus the totalCost of
// its Missions. A mission-scoped chain's cost is already inside its mission's
// totalCost, so only its runs are counted. Chain cost is the current (or most
// recent) run's; use roundtable_chain_step_cost_usd_total for lifetime spend.
func (r *RoundTableReconciler) tableUsage(ctx context.Context, rt *aiv1alpha1.RoundTable) (int64, float64, error) {
	missions := &aiv1alpha1.MissionList{}
	if err := r.List(ctx, missions, client.InNamespace(rt.Namespace)); err != nil {
		return 0, 0, fmt.Errorf("failed to list missions: %w", err)
	}
	chains := &aiv1alpha1.ChainList{}
	if err := r.List(ctx, chains, client.InNamespace(rt.Namespace)); err != nil {
		return 0, 0, fmt.Errorf("failed to list chains: %w", err)
	}

	var tasks int64
	var cost float64
	missionTables := map[string]string{}
	for i := range missions.Items {
		m := &missions.Items[i]
		table := missionTable(m)
		missionTables[m.Name] = table
		if table == rt.Name {
			cost += parseCost(m.Status.TotalCost)
		}
	}
	for i := range chains.Items {
		c := &chains.Items[i]
		if mission := aiv1alpha1.RefName(c.Spec.MissionRef); mission != "" {
			if missionTables[mission] == rt.Name {
				tasks += c.Status.RunsCompleted
			}
			continue
		}
		if aiv1alpha1.RefName(c.Spec.RoundTableRef) == rt.Name {
			tasks += c.Status.RunsCompleted
			cost += parseCost(c.Status.CostUSD)
		}
	}
	return tasks, cost, nil
}

func parseCost(s string) float64 {
	if s == "" {
		return 0
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return v
}

// countActiveMissions counts missions referencing this RoundTable that are in active phases.
func (r *RoundTableReconciler) countActiveMissions(ctx context.Context, rt *aiv1alpha1.RoundTable) (int32, error) {
	missionList := &aiv1alpha1.MissionList{}
	if err := r.List(ctx, missionList, client.InNamespace(rt.Namespace)); err != nil {
		return 0, fmt.Errorf("failed to list missions: %w", err)
	}

	var count int32
	for _, m := range missionList.Items {
		if aiv1alpha1.RefName(m.Spec.RoundTableRef) == rt.Name {
			if m.Status.Phase.IsActive() {
				count++
			}
		}
	}
	return count, nil
}

// ensureStreams creates or verifies JetStream streams for this RoundTable.
func (r *RoundTableReconciler) ensureStreams(ctx context.Context, rt *aiv1alpha1.RoundTable) error {
	// Get shared NATS client
	natsCli, err := r.natsClient()
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}

	// Map retention policy string to enum
	retention := natspkg.RetentionWorkQueue
	switch rt.Spec.NATS.StreamRetention {
	case "Limits":
		retention = natspkg.RetentionLimits
	case "Interest":
		retention = natspkg.RetentionInterest
	}

	// Tasks stream
	tasksSubject := natspkg.StreamSubject(rt.Spec.NATS.SubjectPrefix, "tasks")
	tasksStreamConfig := natspkg.StreamConfig{
		Name:      rt.Spec.NATS.TasksStream,
		Subjects:  []string{tasksSubject},
		Retention: retention,
		Storage:   natspkg.StorageFile,
	}
	if err := natsCli.CreateStream(ctx, tasksStreamConfig); err != nil {
		return fmt.Errorf("tasks stream: %w", err)
	}

	// Results stream
	resultsSubject := natspkg.StreamSubject(rt.Spec.NATS.SubjectPrefix, "results")
	resultsStreamConfig := natspkg.StreamConfig{
		Name:      rt.Spec.NATS.ResultsStream,
		Subjects:  []string{resultsSubject},
		Retention: retention,
		Storage:   natspkg.StorageFile,
	}
	if rt.Spec.NATS.ResultsMaxAge != nil && rt.Spec.NATS.ResultsMaxAge.Duration > 0 {
		resultsStreamConfig.MaxAge = rt.Spec.NATS.ResultsMaxAge.Duration
	}
	if err := natsCli.CreateStream(ctx, resultsStreamConfig); err != nil {
		return fmt.Errorf("results stream: %w", err)
	}
	// CreateStream leaves an existing stream alone; apply a changed (or
	// removed) resultsMaxAge. No-op when it already matches.
	if _, err := natsCli.EnsureStreamMaxAge(ctx, rt.Spec.NATS.ResultsStream, resultsStreamConfig.MaxAge); err != nil {
		return fmt.Errorf("results stream: %w", err)
	}

	// Ephemeral (Mission) tables get no events stream: Mission cleanup only
	// deletes the tasks and results streams, so it would be left behind.
	if !rt.Spec.Ephemeral {
		// Events stream: what NATS-source Triggers read. Limits retention with a
		// max age, so events never pile up and several Triggers can read the
		// same subjects (#402).
		eventsStreamConfig := natspkg.StreamConfig{
			Name:      rt.Spec.NATS.EventsStreamName(),
			Subjects:  []string{natspkg.EventsStreamSubject(rt.Spec.NATS.SubjectPrefix)},
			Retention: natspkg.RetentionLimits,
			Storage:   natspkg.StorageFile,
			MaxAge:    rt.Spec.NATS.EventsRetention(),
		}
		if err := natsCli.CreateStream(ctx, eventsStreamConfig); err != nil {
			return fmt.Errorf("events stream: %w", err)
		}
	}

	return r.ensureDeadLetters(ctx, rt, natsCli)
}

// ensureDeadLetters creates the table's advisories and dead-letter streams and
// keeps its dead-letter watcher running. Skipped when no Manager is wired or
// the client exposes no raw connection (test fakes).
func (r *RoundTableReconciler) ensureDeadLetters(ctx context.Context, rt *aiv1alpha1.RoundTable, natsCli natspkg.Client) error {
	if r.DeadLetters == nil {
		return nil
	}
	nc := deadletter.Connection(natsCli)
	if nc == nil {
		return nil
	}
	cfg := deadletter.TableConfig{
		Table:       rt.Name,
		Prefix:      rt.Spec.NATS.SubjectPrefix,
		TasksStream: rt.Spec.NATS.TasksStream,
	}
	if d := rt.Spec.NATS.DeadLetterMaxAge; d != nil {
		cfg.MaxAge = d.Duration
	}
	key := client.ObjectKeyFromObject(rt).String()
	if err := r.DeadLetters.Ensure(ctx, nc, key, cfg); err != nil {
		return fmt.Errorf("dead-letter: %w", err)
	}
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *RoundTableReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aiv1alpha1.RoundTable{}).
		// A Knight change updates its card within seconds (#393).
		Watches(&aiv1alpha1.Knight{}, handler.EnqueueRequestsFromMapFunc(r.knightTables),
			builder.WithPredicates(knightChangedForTable())).
		Named("roundtable").
		Complete(r)
}

// missionTable returns the name of the RoundTable a mission runs on: its
// roundTableRef, else the ephemeral table recorded in status.
func missionTable(m *aiv1alpha1.Mission) string {
	if name := aiv1alpha1.RefName(m.Spec.RoundTableRef); name != "" {
		return name
	}
	return m.Status.RoundTableName
}

// updateMetrics recomputes the table-scoped gauges (knights and missions per
// phase, cost). Each vector is reset for the table first so phases
// that no longer have members disappear instead of going stale.
func (r *RoundTableReconciler) updateMetrics(ctx context.Context, rt *aiv1alpha1.RoundTable, knights []aiv1alpha1.Knight, totalCost float64) {
	log := logf.FromContext(ctx)
	tableLabel := prometheus.Labels{"table": rt.Name}

	rtmetrics.Knights.DeletePartialMatch(tableLabel)
	knightPhases := map[string]int{}
	for i := range knights {
		knightPhases[string(knights[i].Status.Phase)]++
	}
	for phase, n := range knightPhases {
		rtmetrics.Knights.WithLabelValues(rt.Name, phase).Set(float64(n))
	}

	missions := &aiv1alpha1.MissionList{}
	if err := r.List(ctx, missions, client.InNamespace(rt.Namespace)); err != nil {
		log.Error(err, "Failed to list missions for metrics", "table", rt.Name)
	} else {
		rtmetrics.Missions.DeletePartialMatch(tableLabel)
		missionPhases := map[string]int{}
		for i := range missions.Items {
			if missionTable(&missions.Items[i]) == rt.Name {
				missionPhases[string(missions.Items[i].Status.Phase)]++
			}
		}
		for phase, n := range missionPhases {
			rtmetrics.Missions.WithLabelValues(rt.Name, phase).Set(float64(n))
		}
	}

	rtmetrics.TableCostUSD.WithLabelValues(rt.Name).Set(totalCost)
}
