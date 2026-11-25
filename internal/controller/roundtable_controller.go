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
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
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
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	log = log.WithValues("table", rt.Name)
	ctx = logf.IntoContext(ctx, log)

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
	var readyCount int32
	knightSummaries := make([]aiv1alpha1.RoundTableKnightSummary, 0, len(knights))
	var totalTasksCompleted int64
	var totalCost float64

	for _, k := range knights {
		summary := aiv1alpha1.RoundTableKnightSummary{
			Name:  k.Name,
			Phase: k.Status.Phase,
			Ready: k.Status.Ready,
		}
		knightSummaries = append(knightSummaries, summary)
		if k.Status.Ready {
			readyCount++
		}
		totalTasksCompleted += k.Status.TasksCompleted
		if k.Status.TotalCost != "" {
			if cost, err := strconv.ParseFloat(k.Status.TotalCost, 64); err == nil {
				totalCost += cost
			}
		}
	}

	total := int32(len(knights))
	rt.Status.KnightsTotal = total
	rt.Status.KnightsReady = readyCount
	rt.Status.Knights = knightSummaries
	rt.Status.TotalTasksCompleted = totalTasksCompleted
	rt.Status.TotalCost = fmt.Sprintf("%.4f", totalCost)

	// 3. NATS Stream Management
	if rt.Spec.NATS.CreateStreams {
		if err := r.ensureStreams(rt); err != nil {
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

	// 4. Warm Pool Reconciliation
	if rt.Spec.WarmPool != nil && rt.Spec.WarmPool.Size > 0 {
		if err := r.reconcileWarmPool(ctx, rt); err != nil {
			log.Error(err, "Failed to reconcile warm pool")
		}
	}

	// 5. Cost Budget Check
	phase := r.computePhase(rt, readyCount, total, totalCost)
	rt.Status.Phase = phase

	// 6. Active Missions count
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
			Message:            fmt.Sprintf("All %d knights are ready", total),
			ObservedGeneration: rt.Generation,
		})
	case aiv1alpha1.RoundTablePhaseDegraded:
		meta.SetStatusCondition(&rt.Status.Conditions, metav1.Condition{
			Type:               aiv1alpha1.ConditionRoundTableAvailable,
			Status:             metav1.ConditionFalse,
			Reason:             aiv1alpha1.ReasonKnightsDegraded,
			Message:            fmt.Sprintf("%d/%d knights ready", readyCount, total),
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
		r.Recorder.Event(rt, corev1.EventTypeWarning, "BudgetExceeded", "Cost budget exceeded, suspending knights")
	default:
		meta.SetStatusCondition(&rt.Status.Conditions, metav1.Condition{
			Type:               aiv1alpha1.ConditionRoundTableAvailable,
			Status:             metav1.ConditionFalse,
			Reason:             aiv1alpha1.ReasonRoundTableProvisioning,
			Message:            "RoundTable is provisioning",
			ObservedGeneration: rt.Generation,
		})
	}

	rt.Status.ObservedGeneration = rt.Generation

	// Update Prometheus metrics
	r.updateMetrics(ctx, rt, knights, totalCost)

	if err := r.Status().Update(ctx, rt); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: RequeueVerySlow}, nil
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

// computePhase determines the RoundTable phase based on knight health and cost.
func (r *RoundTableReconciler) computePhase(rt *aiv1alpha1.RoundTable, readyCount, total int32, totalCost float64) aiv1alpha1.RoundTablePhase {
	// Check cost budget
	if rt.Spec.Policies != nil && rt.Spec.Policies.CostBudgetUSD != "" && rt.Spec.Policies.CostBudgetUSD != "0" {
		budget, err := strconv.ParseFloat(rt.Spec.Policies.CostBudgetUSD, 64)
		if err == nil && totalCost > budget {
			return aiv1alpha1.RoundTablePhaseOverBudget
		}
	}

	if total == 0 {
		return aiv1alpha1.RoundTablePhaseProvisioning
	}

	if readyCount == total {
		return aiv1alpha1.RoundTablePhaseReady
	}

	return aiv1alpha1.RoundTablePhaseDegraded
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
			switch m.Status.Phase {
			case aiv1alpha1.MissionPhaseAssembling, aiv1alpha1.MissionPhaseBriefing, aiv1alpha1.MissionPhaseActive:
				count++
			}
		}
	}
	return count, nil
}

// ensureStreams creates or verifies JetStream streams for this RoundTable.
func (r *RoundTableReconciler) ensureStreams(rt *aiv1alpha1.RoundTable) error {
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
	if err := natsCli.CreateStream(tasksStreamConfig); err != nil {
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
	if err := natsCli.CreateStream(resultsStreamConfig); err != nil {
		return fmt.Errorf("results stream: %w", err)
	}

	return r.ensureDeadLetters(rt, natsCli)
}

// ensureDeadLetters creates the table's advisories and dead-letter streams and
// keeps its dead-letter watcher running. Skipped when no Manager is wired or
// the client exposes no raw connection (test fakes).
func (r *RoundTableReconciler) ensureDeadLetters(rt *aiv1alpha1.RoundTable, natsCli natspkg.Client) error {
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
	if err := r.DeadLetters.Ensure(nc, key, cfg); err != nil {
		return fmt.Errorf("dead-letter: %w", err)
	}
	return nil
}

// reconcileWarmPool ensures the warm pool has the desired number of pre-warmed knights.
// It creates new warm knights when the pool is below capacity and recycles idle ones.
func (r *RoundTableReconciler) reconcileWarmPool(ctx context.Context, rt *aiv1alpha1.RoundTable) error {
	log := logf.FromContext(ctx)
	wp := rt.Spec.WarmPool

	// List all unclaimed warm pool knights for this RoundTable
	unclaimedKnights, err := r.listWarmPoolKnights(ctx, rt, false)
	if err != nil {
		return fmt.Errorf("failed to list warm pool knights: %w", err)
	}

	// Count available (Ready) and provisioning (non-Ready)
	var available, provisioning int32
	for _, k := range unclaimedKnights {
		if k.Status.Phase == aiv1alpha1.KnightPhaseReady && k.Status.Ready {
			available++
		} else {
			provisioning++
		}
	}

	// Idle recycling: delete warm knights that have been idle too long
	if maxIdle := time.Duration(wp.MaxIdleTimeSeconds) * time.Second; maxIdle > 0 {
		for i := range unclaimedKnights {
			k := &unclaimedKnights[i]
			createdAtStr, ok := k.Annotations[aiv1alpha1.AnnotationWarmPoolCreatedAt]
			if !ok {
				continue
			}
			createdAt, err := time.Parse(time.RFC3339, createdAtStr)
			if err != nil {
				continue
			}
			if time.Since(createdAt) > maxIdle {
				log.Info("Recycling idle warm pool knight", "knight", k.Name,
					"age", time.Since(createdAt).String())
				if err := r.Delete(ctx, k); err != nil {
					log.Error(err, "Failed to delete idle warm knight", "knight", k.Name)
				} else {
					r.Recorder.Event(rt, corev1.EventTypeNormal, "WarmKnightRecycled", "Idle warm knight recycled")
					// Adjust counts — the deleted knight is no longer available
					if k.Status.Phase == aiv1alpha1.KnightPhaseReady && k.Status.Ready {
						available--
					} else {
						provisioning--
					}
				}
			}
		}
	}

	// Scale-down: delete excess unclaimed knights if pool size was reduced
	currentTotal := available + provisioning
	if currentTotal > wp.Size {
		excess := currentTotal - wp.Size
		log.Info("Scaling down warm pool", "current", currentTotal, "desired", wp.Size, "excess", excess)

		// Re-list remaining unclaimed knights for sorting
		remainingKnights, err := r.listWarmPoolKnights(ctx, rt, false)
		if err == nil {
			// Sort: non-ready first, then oldest first
			sort.Slice(remainingKnights, func(i, j int) bool {
				ki, kj := &remainingKnights[i], &remainingKnights[j]
				readyI := ki.Status.Phase == aiv1alpha1.KnightPhaseReady && ki.Status.Ready
				readyJ := kj.Status.Phase == aiv1alpha1.KnightPhaseReady && kj.Status.Ready
				if readyI != readyJ {
					return !readyI // non-ready first
				}
				createdAtI, okI := ki.Annotations[aiv1alpha1.AnnotationWarmPoolCreatedAt]
				createdAtJ, okJ := kj.Annotations[aiv1alpha1.AnnotationWarmPoolCreatedAt]
				if okI && okJ {
					timeI, errI := time.Parse(time.RFC3339, createdAtI)
					timeJ, errJ := time.Parse(time.RFC3339, createdAtJ)
					if errI == nil && errJ == nil {
						return timeI.Before(timeJ) // oldest first
					}
				}
				return ki.Name < kj.Name
			})

			deleted := int32(0)
			for i := range remainingKnights {
				if deleted >= excess {
					break
				}
				k := &remainingKnights[i]
				log.Info("Deleting excess warm pool knight", "knight", k.Name)
				if err := r.Delete(ctx, k); err != nil {
					log.Error(err, "Failed to delete excess warm knight", "knight", k.Name)
				} else {
					if k.Status.Phase == aiv1alpha1.KnightPhaseReady && k.Status.Ready {
						available--
					} else {
						provisioning--
					}
					deleted++
				}
			}
		}
	}

	// Create new warm knights if pool is under capacity (burst-limited)
	const maxWarmCreationsPerReconcile int32 = 5
	deficit := min(wp.Size-(available+provisioning), maxWarmCreationsPerReconcile)
	for range deficit {
		if err := r.createWarmKnight(ctx, rt); err != nil {
			log.Error(err, "Failed to create warm pool knight")
			return err
		}
		provisioning++
	}
	if deficit > 0 {
		r.Recorder.Eventf(rt, corev1.EventTypeNormal, "WarmPoolScaled", "Warm pool scaled to %d", available+provisioning)
	}

	// Count claimed knights for status
	claimedKnights, err := r.listWarmPoolKnights(ctx, rt, true)
	if err != nil {
		log.Error(err, "Failed to count claimed warm pool knights")
	}

	// Update warm pool status
	rt.Status.WarmPool = &aiv1alpha1.WarmPoolStatus{
		Available:    available,
		Provisioning: provisioning,
		Claimed:      int32(len(claimedKnights)),
	}

	log.Info("Warm pool reconciled",
		"desired", wp.Size,
		"available", available,
		"provisioning", provisioning,
		"claimed", len(claimedKnights))

	return nil
}

// listWarmPoolKnights lists warm pool knights for this RoundTable.
// If claimed is true, returns claimed knights; if false, returns unclaimed.
func (r *RoundTableReconciler) listWarmPoolKnights(ctx context.Context, rt *aiv1alpha1.RoundTable, claimed bool) ([]aiv1alpha1.Knight, error) {
	knightList := &aiv1alpha1.KnightList{}
	claimedVal := "false"
	if claimed {
		claimedVal = labelValueTrue
	}
	if err := r.List(ctx, knightList,
		client.InNamespace(rt.Namespace),
		client.MatchingLabels{
			aiv1alpha1.LabelWarmPool:        labelValueTrue,
			aiv1alpha1.LabelWarmPoolClaimed: claimedVal,
			aiv1alpha1.LabelRoundTable:      rt.Name,
		},
	); err != nil {
		return nil, fmt.Errorf("failed to list warm pool knights: %w", err)
	}
	return knightList.Items, nil
}

// createWarmKnight creates a single warm pool Knight from the RoundTable's warm pool template.
func (r *RoundTableReconciler) createWarmKnight(ctx context.Context, rt *aiv1alpha1.RoundTable) error {
	log := logf.FromContext(ctx)
	wp := rt.Spec.WarmPool

	// Generate random suffix
	randBytes := make([]byte, 3)
	if _, err := rand.Read(randBytes); err != nil {
		return fmt.Errorf("failed to generate random suffix: %w", err)
	}
	name := fmt.Sprintf("%s-warm-%s", rt.Name, hex.EncodeToString(randBytes))

	// Build the knight spec from the template
	spec := wp.Template.DeepCopy()

	// Set runtime from warm pool config
	if wp.Runtime != "" {
		spec.Runtime = wp.Runtime
	}

	// Inherit the table's GitHub token Secret unless the template sets one
	if (spec.GitHub == nil || spec.GitHub.TokenSecretRef == nil) && rt.Spec.GitHub != nil {
		spec.GitHub = rt.Spec.GitHub.DeepCopy()
	}

	// Set domain to warm-pool if not specified in template
	if spec.Domain == "" {
		spec.Domain = "warm-pool"
	}

	// Ensure NATS config uses the RoundTable's infrastructure
	if spec.NATS.URL == "" {
		spec.NATS.URL = rt.Spec.NATS.URL
	}
	if spec.NATS.Stream == "" {
		spec.NATS.Stream = rt.Spec.NATS.TasksStream
	}
	if spec.NATS.ResultsStream == "" {
		spec.NATS.ResultsStream = rt.Spec.NATS.ResultsStream
	}
	// Give warm pool knights a generic subject — missions will patch this on claim
	if len(spec.NATS.Subjects) == 0 {
		spec.NATS.Subjects = []string{fmt.Sprintf("%s.tasks.warm-pool.>", rt.Spec.NATS.SubjectPrefix)}
	}

	// Ensure not suspended
	spec.Suspend = false

	knight := &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: rt.Namespace,
			Labels: map[string]string{
				aiv1alpha1.LabelWarmPool:        labelValueTrue,
				aiv1alpha1.LabelWarmPoolClaimed: "false",
				aiv1alpha1.LabelRoundTable:      rt.Name,
			},
			Annotations: map[string]string{
				aiv1alpha1.AnnotationWarmPoolCreatedAt: time.Now().UTC().Format(time.RFC3339),
			},
		},
		Spec: *spec,
	}

	// Set owner reference so warm knights get GC'd with the RoundTable
	if err := controllerutil.SetControllerReference(rt, knight, r.Scheme); err != nil {
		return fmt.Errorf("failed to set owner reference: %w", err)
	}

	log.Info("Creating warm pool knight", "knight", name, "roundTable", rt.Name)
	return r.Create(ctx, knight)
}

// SetupWithManager sets up the controller with the Manager.
func (r *RoundTableReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aiv1alpha1.RoundTable{}).
		// A Knight change updates its card within seconds (#393).
		Watches(&aiv1alpha1.Knight{}, handler.EnqueueRequestsFromMapFunc(r.knightTables)).
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
// phase, cost, warm pool). Each vector is reset for the table first so phases
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

	if totalCost > 0 {
		rtmetrics.TableCostUSD.WithLabelValues(rt.Name).Set(totalCost)
	}

	if rt.Status.WarmPool != nil {
		rtmetrics.WarmPoolKnights.WithLabelValues(rt.Name, "available").Set(float64(rt.Status.WarmPool.Available))
		rtmetrics.WarmPoolKnights.WithLabelValues(rt.Name, "provisioning").Set(float64(rt.Status.WarmPool.Provisioning))
		rtmetrics.WarmPoolKnights.WithLabelValues(rt.Name, "claimed").Set(float64(rt.Status.WarmPool.Claimed))
	}
}
