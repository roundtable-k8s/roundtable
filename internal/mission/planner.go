package mission

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
	"github.com/roundtable-k8s/roundtable/internal/util"
	"go.opentelemetry.io/otel/trace"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// Planner encapsulates all planning logic extracted from MissionReconciler.
type Planner struct {
	Client client.Client
	Scheme *runtime.Scheme
	NATS   *natspkg.Provider

	// TracerProvider makes the plan span; nil uses the global one.
	TracerProvider trace.TracerProvider

	// Recorder records the deprecated role-label planner fallback; nil
	// records nothing.
	Recorder record.EventRecorder
}

// natsClient returns the NATS client from the provider.
func (p *Planner) natsClient() (natspkg.Client, error) {
	if p.NATS == nil {
		return nil, fmt.Errorf("NATS provider not configured")
	}
	return p.NATS.Client()
}

// natsPrefix returns the NATS subject prefix for this mission.
func natsPrefix(mission *aiv1alpha1.Mission) string {
	if mission.Spec.NATSPrefix != "" {
		return mission.Spec.NATSPrefix
	}
	return fmt.Sprintf("mission-%s", mission.Name)
}

// ReconcilePlanning handles the Planning phase for meta-missions.
func (p *Planner) ReconcilePlanning(ctx context.Context, mission *aiv1alpha1.Mission) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Skip if not a meta-mission — transition directly to Assembling
	if !mission.Spec.MetaMission {
		log.Info("Not a meta-mission, skipping Planning phase")
		mission.Status.Phase = aiv1alpha1.MissionPhaseAssembling
		mission.Status.ObservedGeneration = mission.Generation
		return ctrl.Result{}, p.Client.Status().Update(ctx, mission)
	}

	// Auto-initialize Planner spec with defaults if metaMission but no explicit planner config
	plannerChanged := false
	if mission.Spec.Planner == nil {
		log.Info("MetaMission enabled without explicit planner config, using defaults")
		mission.Spec.Planner = &aiv1alpha1.MissionPlanner{
			TimeoutSeconds: 600,
			MaxChains:      10,
			MaxKnights:     10,
		}
		plannerChanged = true
	}
	// A planner that names no knight, template or inline spec (absent, or
	// options only) uses the table's planner, else the knight labelled
	// ai.roundtable.io/role=planner. The given options (timeout, limits,
	// context) are kept.
	found, wait, err := p.defaultPlannerKnight(ctx, mission)
	if err != nil {
		return ctrl.Result{}, err
	}
	if found {
		plannerChanged = true
	}
	if plannerChanged {
		if err := p.Client.Update(ctx, mission); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to initialize planner config: %w", err)
		}
	}
	// Wait for the table's planner knight, but no longer than the planning
	// timeout; then planning fails with errNoPlanner.
	if wait && !planningTimedOut(mission) {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Initialize planning result if needed
	if mission.Status.PlanningResult == nil {
		log.Info("Initializing planning phase")
		mission.Status.PlanningResult = &aiv1alpha1.PlanningResult{}
		return ctrl.Result{}, p.Client.Status().Update(ctx, mission)
	}

	pr := mission.Status.PlanningResult

	// Bug #85: Check if plan has already been applied (idempotency guard)
	// If PlanApplied condition is True, skip plan application and transition to Assembling
	planAppliedCondition := meta.FindStatusCondition(mission.Status.Conditions, "PlanApplied")
	if planAppliedCondition != nil && planAppliedCondition.Status == metav1.ConditionTrue {
		log.Info("Plan already applied, transitioning to Assembling phase")
		mission.Status.Phase = aiv1alpha1.MissionPhaseAssembling
		mission.Status.ObservedGeneration = mission.Generation
		return ctrl.Result{}, p.Client.Status().Update(ctx, mission)
	}

	// Check for planning error first (terminal state — must precede CompletedAt check)
	if pr.Error != "" {
		log.Error(fmt.Errorf("%s", pr.Error), "Planning failed, marking mission as failed")
		mission.Status.Phase = aiv1alpha1.MissionPhaseFailed
		failMsg := fmt.Sprintf("Planning failed: %s", pr.Error)
		mission.Status.Result = failMsg
		meta.SetStatusCondition(&mission.Status.Conditions, metav1.Condition{
			Type:               aiv1alpha1.ConditionMissionComplete,
			Status:             metav1.ConditionTrue,
			Reason:             aiv1alpha1.ReasonMissionFailed,
			Message:            failMsg,
			ObservedGeneration: mission.Generation,
		})
		mission.Status.ObservedGeneration = mission.Generation
		planErr := pr.Error
		err := p.Client.Status().Update(ctx, mission)
		if err == nil {
			if p.Recorder != nil {
				p.Recorder.Event(mission, corev1.EventTypeWarning, "Failed", failMsg)
			}
			// Once: the mission is Failed from here on.
			EmitPlanSpan(ctx, p.TracerProvider, mission, planErr)
		}
		return ctrl.Result{}, err
	}

	// Check if planning already completed successfully
	if pr.CompletedAt != nil {
		log.Info("Planning already complete, transitioning to Assembling",
			"chains", pr.ChainsGenerated,
			"knights", pr.KnightsGenerated)
		mission.Status.Phase = aiv1alpha1.MissionPhaseAssembling
		mission.Status.ObservedGeneration = mission.Generation
		return ctrl.Result{}, p.Client.Status().Update(ctx, mission)
	}

	// Check timeout
	timeout := planningTimeout(mission)
	elapsed := time.Since(mission.Status.StartedAt.Time)
	if elapsed > time.Duration(timeout)*time.Second {
		log.Error(fmt.Errorf("planning timeout"), "Planning exceeded timeout",
			"timeout", timeout,
			"elapsed", elapsed)
		pr.Error = fmt.Sprintf("planning timeout after %d seconds", timeout)
		now := metav1.Now()
		pr.CompletedAt = &now
		return ctrl.Result{}, p.Client.Status().Update(ctx, mission)
	}

	// Ensure planner knight exists
	plannerKnight, err := p.ensurePlannerKnight(ctx, mission)
	if err != nil {
		log.Error(err, "Failed to ensure planner knight")
		pr.Error = fmt.Sprintf("failed to create planner knight: %v", err)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, p.Client.Status().Update(ctx, mission)
	}

	// Wait for planner knight to be ready. A knight scaled to zero by its
	// suspendPolicy auto (a table planner) wakes on the queued task, so the
	// task is dispatched to it.
	if plannerKnight.Status.Phase != aiv1alpha1.KnightPhaseReady && !wakesOnTask(plannerKnight) {
		log.Info("Waiting for planner knight to be ready",
			"knight", plannerKnight.Name,
			"phase", plannerKnight.Status.Phase)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Dispatch planning task if not already dispatched.
	// The deterministic taskID (mission name + generation) ensures that even
	// if this block re-executes due to a status update conflict, the same
	// taskID is published — preventing stream flooding.
	if mission.Status.PlanningTaskID == "" {
		taskID, err := p.dispatchPlanningTask(ctx, mission, plannerKnight)
		if err != nil {
			log.Error(err, "Failed to dispatch planning task")
			pr.Error = fmt.Sprintf("failed to dispatch planning task: %v", err)
			return ctrl.Result{RequeueAfter: 10 * time.Second}, p.Client.Status().Update(ctx, mission)
		}
		log.Info("Dispatched planning task", "taskID", taskID, "knight", plannerKnight.Name)
		mission.Status.PlanningTaskID = taskID
		now := metav1.Now()
		pr.StartedAt = &now
		if err := p.Client.Status().Update(ctx, mission); err != nil {
			// Status update failed (likely conflict). Log but don't requeue
			// aggressively — the deterministic taskID prevents duplicate work.
			log.V(1).Info("Status update after dispatch failed, will retry on next reconcile",
				"taskID", taskID, "error", err)
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Poll for planning result
	taskID := mission.Status.PlanningTaskID

	result, err := p.pollPlanningResult(ctx, mission, taskID)
	if err != nil {
		log.Error(err, "Failed to poll planning result")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Still waiting for result
	if result == nil {
		log.V(1).Info("Waiting for planning result", "taskID", taskID)
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}

	// Check for task error
	if taskErr := result.GetError(); taskErr != "" {
		log.Error(fmt.Errorf("%s", taskErr), "Planner knight returned error")
		pr.Error = fmt.Sprintf("planner error: %s", taskErr)
		pr.RawOutput = util.Truncate(result.GetOutput(), 10000)
		now := metav1.Now()
		pr.CompletedAt = &now
		return ctrl.Result{}, p.Client.Status().Update(ctx, mission)
	}

	// Parse planner output
	output := result.GetOutput()
	plan, err := p.parsePlannerOutput(output)
	if err != nil {
		log.Error(err, "Failed to parse planner output")
		pr.Error = fmt.Sprintf("failed to parse planner output: %v", err)
		pr.RawOutput = util.Truncate(output, 10000)
		now := metav1.Now()
		pr.CompletedAt = &now
		return ctrl.Result{}, p.Client.Status().Update(ctx, mission)
	}

	// Validate plan
	// Bug #2 Fix: Do NOT set CompletedAt on validation failure.
	// This allows the planner to retry instead of skipping to Assembling with 0 chains/knights.
	if err := p.validatePlan(ctx, mission, plan); err != nil {
		log.Error(err, "Plan validation failed")
		pr.Error = fmt.Sprintf("plan validation failed: %v", err)
		pr.RawOutput = util.Truncate(output, 10000)
		// Do NOT set pr.CompletedAt here - allow retry
		return ctrl.Result{RequeueAfter: 10 * time.Second}, p.Client.Status().Update(ctx, mission)
	}

	// Apply plan to mission spec
	if err := p.applyPlan(ctx, mission, plan); err != nil {
		log.Error(err, "Failed to apply plan")
		pr.Error = fmt.Sprintf("failed to apply plan: %v", err)
		now := metav1.Now()
		pr.CompletedAt = &now
		return ctrl.Result{}, p.Client.Status().Update(ctx, mission)
	}

	// Mark planning complete
	now := metav1.Now()
	pr.CompletedAt = &now
	pr.ChainsGenerated = int32(len(plan.Chains))
	pr.KnightsGenerated = int32(len(plan.Knights))
	pr.SkillsGenerated = int32(len(plan.Skills))
	pr.RawOutput = util.Truncate(output, 10000)

	log.Info("Planning completed successfully",
		"chains", pr.ChainsGenerated,
		"knights", pr.KnightsGenerated,
		"skills", pr.SkillsGenerated)

	// Clear temporary annotation
	delete(mission.Annotations, "ai.roundtable.io/planning-task-id")

	// Bug #85: Update spec first, then combine phase transition with PlanApplied condition
	// in a single status update to reduce conflict window
	if err := p.Client.Update(ctx, mission); err != nil {
		return ctrl.Result{}, err
	}

	// The spec update decoded the server response into `mission`, replacing
	// Status with the server's copy (spec updates ignore status) — restore the
	// completed planning result before persisting status, or the counters and
	// CompletedAt are silently lost.
	mission.Status.PlanningResult = pr

	// Bug #85: Set PlanApplied condition to prevent duplicate applications on retry
	meta.SetStatusCondition(&mission.Status.Conditions, metav1.Condition{
		Type:               "PlanApplied",
		Status:             metav1.ConditionTrue,
		Reason:             "PlanningComplete",
		Message:            fmt.Sprintf("Generated %d chains, %d knights, %d skills", pr.ChainsGenerated, pr.KnightsGenerated, pr.SkillsGenerated),
		ObservedGeneration: mission.Generation,
	})

	// Transition to Assembling phase with the PlanApplied condition in one update
	mission.Status.Phase = aiv1alpha1.MissionPhaseAssembling
	mission.Status.ObservedGeneration = mission.Generation

	if err := p.Client.Status().Update(ctx, mission); err != nil {
		// If status update fails, return error for requeue with fresh object
		return ctrl.Result{}, fmt.Errorf("failed to update mission status after plan application: %w", err)
	}
	EmitPlanSpan(ctx, p.TracerProvider, mission, "")

	return ctrl.Result{}, nil
}

// planningTimeout is spec.planner.timeoutSeconds, default 300.
func planningTimeout(mission *aiv1alpha1.Mission) int32 {
	if mission.Spec.Planner != nil && mission.Spec.Planner.TimeoutSeconds != 0 {
		return mission.Spec.Planner.TimeoutSeconds
	}
	return 300 // Default 5 minutes
}

// planningTimedOut reports whether planning has run past its timeout.
func planningTimedOut(mission *aiv1alpha1.Mission) bool {
	if mission.Status.StartedAt == nil {
		return false
	}
	return time.Since(mission.Status.StartedAt.Time) > time.Duration(planningTimeout(mission))*time.Second
}

// wakesOnTask reports whether a knight is asleep (scaled to zero by
// lifecycle.suspendPolicy auto) and will wake when a task is queued for it.
func wakesOnTask(k *aiv1alpha1.Knight) bool {
	return k.Status.Phase == aiv1alpha1.KnightPhaseSuspended && !k.Spec.Suspend && knightpkg.ScaleToZero(k)
}

// plannerHasSource reports whether the planner names a knight, a template or
// an inline spec.
func plannerHasSource(planner *aiv1alpha1.MissionPlanner) bool {
	return aiv1alpha1.RefName(planner.KnightRef) != "" || planner.TemplateName != "" || planner.EphemeralSpec != nil
}

// defaultPlannerKnight sets spec.planner.knightRef when the planner names no
// source, in this order (#566): the planner of the mission's RoundTable, then
// (deprecated, with a warning event) a knight labelled
// ai.roundtable.io/role=planner that belongs to no other table. It reports
// whether it changed the spec, and wait when the table's own planner knight is
// not created yet. With no planner at all the spec is left as it is and
// ensurePlannerKnight fails with errNoPlanner.
func (p *Planner) defaultPlannerKnight(ctx context.Context, mission *aiv1alpha1.Mission) (changed, wait bool, err error) {
	if plannerHasSource(mission.Spec.Planner) {
		return false, false, nil
	}
	log := logf.FromContext(ctx)

	table := aiv1alpha1.RefName(mission.Spec.RoundTableRef)
	if table != "" {
		rt := &aiv1alpha1.RoundTable{}
		err := p.Client.Get(ctx, types.NamespacedName{Name: table, Namespace: mission.Namespace}, rt)
		if err != nil && !apierrors.IsNotFound(err) {
			return false, false, fmt.Errorf("failed to get RoundTable %s: %w", table, err)
		}
		if err == nil {
			if name := rt.PlannerKnightName(); name != "" {
				if rt.OwnsPlanner() {
					// The RoundTable controller creates it; wait for it
					// rather than fail on a just-created table.
					k := &aiv1alpha1.Knight{}
					err := p.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: mission.Namespace}, k)
					if apierrors.IsNotFound(err) {
						log.Info("Waiting for the table's planner Knight", "name", name, "table", table)
						return false, true, nil
					}
					if err != nil {
						return false, false, err
					}
				}
				mission.Spec.Planner.KnightRef = aiv1alpha1.LocalRef(name)
				log.Info("Using the table's planner Knight", "name", name, "table", table)
				return true, false, nil
			}
		}
	}

	plannerKnights := &aiv1alpha1.KnightList{}
	if err := p.Client.List(ctx, plannerKnights, client.InNamespace(mission.Namespace),
		client.MatchingLabels{aiv1alpha1.LabelRole: "planner"}); err != nil {
		return false, false, fmt.Errorf("failed to list planner knights: %w", err)
	}
	sort.Slice(plannerKnights.Items, func(i, j int) bool {
		return plannerKnights.Items[i].Name < plannerKnights.Items[j].Name
	})
	for i := range plannerKnights.Items {
		k := &plannerKnights.Items[i]
		// Skip another mission's ephemeral planner knight.
		if k.Labels[aiv1alpha1.LabelEphemeral] == "true" {
			continue
		}
		// Skip another table's planner: a mission is never planned by a
		// knight of a table it does not run on.
		if kt := k.Labels[aiv1alpha1.LabelRoundTable]; kt != "" && kt != table {
			continue
		}
		mission.Spec.Planner.KnightRef = aiv1alpha1.LocalRef(k.Name)
		log.Info("Using role-labelled planner knight", "name", k.Name)
		if p.Recorder != nil {
			p.Recorder.Eventf(mission, corev1.EventTypeWarning, "DeprecatedPlanner",
				"Planning with knight %s found by its %s=planner label; this is deprecated: "+
					"give RoundTable %q a planner (spec.planner) or set spec.planner.knightRef",
				k.Name, aiv1alpha1.LabelRole, table)
		}
		return true, false, nil
	}
	return false, false, nil
}

// errNoPlanner is the error for a meta-mission with no planner to use.
func errNoPlanner(namespace string) error {
	return fmt.Errorf("no planner knight: set spec.planner.knightRef (or templateName or ephemeralSpec), "+
		"enable the RoundTable's spec.planner, "+
		"or label a Knight in namespace %q with %s=planner", namespace, aiv1alpha1.LabelRole)
}

// ensurePlannerKnight creates or retrieves the planner knight.
func (p *Planner) ensurePlannerKnight(ctx context.Context, mission *aiv1alpha1.Mission) (*aiv1alpha1.Knight, error) {
	log := logf.FromContext(ctx)
	planner := mission.Spec.Planner

	// If knightRef provided, fetch existing knight
	if aiv1alpha1.RefName(planner.KnightRef) != "" && planner.TemplateName == "" && planner.EphemeralSpec == nil {
		knight := &aiv1alpha1.Knight{}
		err := p.Client.Get(ctx, types.NamespacedName{
			Name:      aiv1alpha1.RefName(planner.KnightRef),
			Namespace: mission.Namespace,
		}, knight)
		if err != nil {
			return nil, fmt.Errorf("planner knight %q not found: %w", aiv1alpha1.RefName(planner.KnightRef), err)
		}
		log.Info("Using existing planner knight", "knight", knight.Name)
		return knight, nil
	}

	// Create ephemeral planner knight
	plannerKnightName := fmt.Sprintf("%s-planner", mission.Name)

	// Check if already exists
	knight := &aiv1alpha1.Knight{}
	err := p.Client.Get(ctx, types.NamespacedName{
		Name:      plannerKnightName,
		Namespace: mission.Namespace,
	}, knight)
	if err == nil {
		log.Info("Planner knight already exists", "knight", knight.Name)
		return knight, nil
	}
	if client.IgnoreNotFound(err) != nil {
		return nil, err
	}

	// Resolve spec from template or inline
	var spec *aiv1alpha1.KnightSpec
	if planner.TemplateName != "" {
		// Fetch from RoundTable templates
		rt, err := p.resolveRoundTable(ctx, mission)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve RoundTable: %w", err)
		}

		// Check mission templates first (local override)
		found := false
		for _, template := range mission.Spec.KnightTemplates {
			if template.Name == planner.TemplateName {
				spec = template.Spec.DeepCopy()
				found = true
				log.Info("Using mission template for planner", "template", planner.TemplateName)
				break
			}
		}

		// Fall back to RoundTable templates
		if !found {
			templateSpec, ok := rt.Spec.KnightTemplates[planner.TemplateName]
			if !ok {
				return nil, fmt.Errorf("planner template %q not found in mission or RoundTable", planner.TemplateName)
			}
			spec = &templateSpec
			log.Info("Using RoundTable template for planner", "template", planner.TemplateName)
		}
	} else if planner.EphemeralSpec != nil {
		spec = planner.EphemeralSpec.DeepCopy()
		log.Info("Using inline spec for planner")
	} else {
		return nil, errNoPlanner(mission.Namespace)
	}

	// Create knight CR
	knight = &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{
			Name:      plannerKnightName,
			Namespace: mission.Namespace,
			Labels: map[string]string{
				aiv1alpha1.LabelMission:   mission.Name,
				aiv1alpha1.LabelEphemeral: "true",
				"ai.roundtable.io/role":   "planner",
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(mission, aiv1alpha1.GroupVersion.WithKind("Mission")),
			},
		},
		Spec: *spec,
	}

	if err := p.Client.Create(ctx, knight); err != nil {
		return nil, fmt.Errorf("failed to create planner knight: %w", err)
	}

	log.Info("Created ephemeral planner knight", "knight", knight.Name)
	return knight, nil
}

// dispatchPlanningTask sends the planning task to the planner knight via NATS.
func (p *Planner) dispatchPlanningTask(ctx context.Context, mission *aiv1alpha1.Mission, plannerKnight *aiv1alpha1.Knight) (string, error) {
	log := logf.FromContext(ctx)

	natsClient, err := p.natsClient()
	if err != nil {
		return "", err
	}

	// Generate deterministic task ID based on mission name + generation.
	// This ensures idempotency: if the status update after dispatch fails and
	// we re-enter this function, we publish the same taskID. Combined with
	// NATS dedup window, this prevents flooding the stream on reconciliation
	// loops (previously caused 5000+ duplicate messages).
	taskID := fmt.Sprintf("planning-%s-gen%d", mission.Name, mission.Generation)

	// Build planning prompt
	prompt := p.buildPlanningPrompt(ctx, mission)

	// Construct task payload
	payload := natspkg.TaskPayload{
		TaskID: taskID,
		Task:   prompt,
	}

	// Publish to planner knight's task subject.
	prefix := natsPrefix(mission)
	if len(plannerKnight.Spec.NATS.Subjects) > 0 {
		parts := strings.SplitN(plannerKnight.Spec.NATS.Subjects[0], ".tasks.", 2)
		if len(parts) == 2 {
			prefix = parts[0]
		}
	}
	subject := natspkg.TaskSubject(prefix, plannerKnight.Spec.Domain, plannerKnight.Name)

	// The planner's task is a child of the mission's plan span.
	if err := natsClient.PublishJSONCtx(WithSpan(ctx, mission, planSpanKey), subject, payload, natspkg.WithTable(aiv1alpha1.RefName(mission.Spec.RoundTableRef))); err != nil {
		return "", fmt.Errorf("failed to publish planning task: %w", err)
	}

	log.Info("Published planning task",
		"taskID", taskID,
		"subject", subject,
		"knight", plannerKnight.Name)

	return taskID, nil
}

// buildPlanningPrompt constructs the planning prompt for the planner knight.
func (p *Planner) buildPlanningPrompt(ctx context.Context, mission *aiv1alpha1.Mission) string {
	var sb strings.Builder

	sb.WriteString("You are a mission planner for the Round Table AI agent orchestration system. ")
	sb.WriteString("Your task is to generate a detailed execution plan for the following mission.\n\n")

	sb.WriteString("**Mission Objective:**\n")
	sb.WriteString(mission.Spec.Objective)
	sb.WriteString("\n\n")

	if mission.Spec.SuccessCriteria != "" {
		sb.WriteString("**Success Criteria:**\n")
		sb.WriteString(mission.Spec.SuccessCriteria)
		sb.WriteString("\n\n")
	}

	if mission.Spec.Planner.Context != "" {
		sb.WriteString("**Additional Context:**\n")
		sb.WriteString(mission.Spec.Planner.Context)
		sb.WriteString("\n\n")
	}

	// List available resources
	sb.WriteString("**Available Resources:**\n")

	// Mission knight templates
	if len(mission.Spec.KnightTemplates) > 0 {
		sb.WriteString("Mission Knight Templates:\n")
		for _, t := range mission.Spec.KnightTemplates {
			sb.WriteString(fmt.Sprintf("- %s: domain=%s, skills=%v\n",
				t.Name, t.Spec.Domain, t.Spec.Skills))
		}
	}

	// Existing knights (if recruiting allowed)
	if mission.Spec.RecruitExisting {
		existingFound := false
		for _, k := range mission.Spec.Knights {
			if !k.Ephemeral {
				if !existingFound {
					sb.WriteString("Existing Knights Available (use ephemeral=false to recruit these):\n")
					existingFound = true
				}
				sb.WriteString(fmt.Sprintf("- %s (role: %s)\n", k.Name, k.Role))
			}
		}

		if aiv1alpha1.RefName(mission.Spec.RoundTableRef) != "" {
			var rt aiv1alpha1.RoundTable
			if err := p.Client.Get(ctx, types.NamespacedName{
				Name:      aiv1alpha1.RefName(mission.Spec.RoundTableRef),
				Namespace: mission.Namespace,
			}, &rt); err == nil {
				var knightList aiv1alpha1.KnightList
				if err := p.Client.List(ctx, &knightList,
					client.InNamespace(mission.Namespace),
				); err == nil {
					// Match either the current or the legacy table label;
					// filtering in code avoids duplicates for knights with both.
					matched := knightList.Items[:0]
					for _, k := range knightList.Items {
						if k.Labels[aiv1alpha1.LabelRoundTable] == rt.Name || k.Labels[knightpkg.LegacyTableLabel] == rt.Name {
							matched = append(matched, k)
						}
					}
					knightList.Items = matched
				}
				if len(knightList.Items) > 0 {
					if !existingFound {
						sb.WriteString("Existing Knights Available (use ephemeral=false to recruit these):\n")
						existingFound = true
					}
					if rt.Spec.Description != "" {
						sb.WriteString(fmt.Sprintf("Table %q: %s\n", rt.Name, rt.Spec.Description))
					}
					for _, k := range knightList.Items {
						sb.WriteString(fmt.Sprintf("- %s (domain: %s, model: %s, skills: %v)\n",
							k.Name, k.Spec.Domain, k.Spec.Model, k.Spec.Skills))
					}
				}
			}
		}

		if existingFound {
			sb.WriteString("\n**IMPORTANT: When recruitExisting=true, prefer using existing knights with ephemeral=false. ")
			sb.WriteString("Only create ephemeral knights if the existing ones don't cover the needed domains.**\n")
		}
	}

	// Existing chains the planner may schedule
	existingChains := p.listTemplateChains(ctx, mission)
	if len(existingChains) > 0 {
		sb.WriteString("\nExisting Chains (reusable; schedule these via \"existingChains\" instead of generating a new chain):\n")
		for _, c := range existingChains {
			sb.WriteString(fmt.Sprintf("- %s", c.Name))
			if c.Spec.Description != "" {
				sb.WriteString(": " + c.Spec.Description)
			}
			sb.WriteString("\n")
			steps := make([]string, 0, len(c.Spec.Steps))
			for _, st := range c.Spec.Steps {
				steps = append(steps, fmt.Sprintf("%s (knight: %s)", st.Name, st.KnightRef.Name))
			}
			sb.WriteString("  steps: " + strings.Join(steps, ", ") + "\n")
			if c.Spec.Input != "" {
				sb.WriteString("  example input: " + truncateString(c.Spec.Input, maxChainInputExample) + "\n")
			}
		}
	}
	if rtName := aiv1alpha1.RefName(mission.Spec.RoundTableRef); rtName != "" {
		sb.WriteString(fmt.Sprintf("\nGenerated and existing chains run on the subject prefix of RoundTable %q, so only knights of that table can be recruited.\n", rtName))
	}

	sb.WriteString("\n")

	// Constraints
	sb.WriteString("**Constraints:**\n")
	if mission.Spec.Planner.MaxChains > 0 {
		sb.WriteString(fmt.Sprintf("- Maximum Chains: %d\n", mission.Spec.Planner.MaxChains))
	}
	if mission.Spec.Planner.MaxKnights > 0 {
		sb.WriteString(fmt.Sprintf("- Maximum Knights: %d\n", mission.Spec.Planner.MaxKnights))
	}
	if !mission.Spec.Planner.AllowSkillGeneration {
		sb.WriteString("- Skill Generation: Not allowed (use existing skills only)\n")
	}
	if templatesOnly(mission) {
		sb.WriteString("- Ephemeral knights: templateName only (ephemeralSpec, specOverrides.env and specOverrides.tools are rejected)\n")
	}
	if mission.Spec.TimeoutSeconds > 0 {
		sb.WriteString(fmt.Sprintf("- Mission Timeout: %d seconds\n", mission.Spec.TimeoutSeconds))
	}
	if mission.Spec.CostBudgetUSD != "" && mission.Spec.CostBudgetUSD != "0" {
		sb.WriteString(fmt.Sprintf("- Cost Budget: $%s USD\n", mission.Spec.CostBudgetUSD))
	}

	sb.WriteString("\n")

	// Instructions
	sb.WriteString("**Instructions:**\n")
	sb.WriteString("Generate a complete execution plan in JSON format with the following structure:\n")
	sb.WriteString("```json\n")
	sb.WriteString(`{
  "planVersion": "v1alpha1",
  "metadata": {
    "objective": "Echo the mission objective here for validation",
    "reasoning": "Brief explanation of your planning strategy"
  },
  "knights": [
    {
      "name": "existing-knight-name",
      "role": "description of role",
      "ephemeral": false
    },
    {
      "name": "new-knight-name",
      "role": "description of role in this mission",
      "ephemeral": true,
      "templateName": "base",
      "specOverrides": {
        "domain": "the-knights-domain",
        "skills": ["shared", "relevant-skill"],
        "tools": {
          "nix": ["specific", "nixpkgs", "this", "knight", "needs"]
        }
      }
    }
  ],
  "chains": [
    {
      "name": "chain-name",
      "description": "what this chain does",
      "phase": "Active",
      "steps": [
        {
          "name": "step-name",
          "knightRef": {"name": "knight-name"},
          "task": "detailed task description",
          "timeoutSeconds": 300
        }
      ],
      "timeoutSeconds": 1800
    }
  ],
  "existingChains": [
    {
      "name": "existing-chain-name",
      "input": "mission-specific input for this chain",
      "phase": "Active"
    }
  ]
}
`)
	sb.WriteString("```\n\n")

	sb.WriteString("**Important Guidelines:**\n")
	sb.WriteString("1. Every chain step's knightRef.name must match a knight name in the knights array\n")
	sb.WriteString("2. When recruitExisting=true, prefer ephemeral=false to use existing knights when they fit the task\n")
	sb.WriteString(p.ephemeralGuideline(ctx, mission))
	sb.WriteString("3b. Do NOT set specOverrides.model unless the task truly needs a stronger model — omitting it inherits the base template's cheap default\n")
	sb.WriteString("4. Design each ephemeral knight's tools.nix for its specific role — pick the right nixpkgs (e.g. go, python3, nmap, gh, nodejs_22, ripgrep, gopls, golangci-lint, kubectl, terraform)\n")
	sb.WriteString("5. Chain phases can be: Setup, Active, or Teardown\n")
	sb.WriteString("6. A step can read an earlier step's output with {{ .Steps.step_name.Output }} (long outputs are cut to about 4000 characters with a \"[truncated\" marker). Output passes ONLY between steps of the SAME chain, and only from steps listed in the reader's dependsOn (directly or through other dependsOn steps). Chains in a plan start at the same time and cannot read each other's output, so put work that needs another step's result (for example gather, then write a report) into ONE chain with dependsOn steps; a plan that references another chain's step is rejected\n")
	sb.WriteString("7. Use underscores (not hyphens) in STEP NAMES — hyphens break Go templates\n")
	sb.WriteString("8. Use hyphens (not underscores) in KNIGHT NAMES — they become Kubernetes resource names (RFC 1123 DNS labels)\n")
	sb.WriteString("9. Ensure step dependencies (dependsOn) form a valid DAG (no cycles)\n")
	sb.WriteString("10. Keep task descriptions clear and actionable\n")
	sb.WriteString("11. Prefer an existing chain (\"existingChains\") when one fits; generate a new chain only for work no existing chain covers. Only chains listed above may be used; their knights are recruited automatically\n")
	sb.WriteString("12. Return ONLY the JSON plan, no other text or explanation\n\n")

	sb.WriteString("Generate the plan now:")

	return sb.String()
}

// pollPlanningResult polls the NATS results stream for the planning result.
func (p *Planner) pollPlanningResult(ctx context.Context, mission *aiv1alpha1.Mission, taskID string) (*natspkg.TaskResult, error) {
	log := logf.FromContext(ctx)

	natsClient, err := p.natsClient()
	if err != nil {
		return nil, err
	}

	// Resolve the planner knight to determine which results stream to poll
	plannerKnight, err := p.ensurePlannerKnight(ctx, mission)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve planner knight: %w", err)
	}

	resultsStream := plannerKnight.Spec.NATS.ResultsStream

	var subjectPrefix string
	if len(plannerKnight.Spec.NATS.Subjects) > 0 {
		parts := strings.SplitN(plannerKnight.Spec.NATS.Subjects[0], ".tasks.", 2)
		if len(parts) == 2 {
			subjectPrefix = parts[0]
		}
	}

	if subjectPrefix == "" {
		return nil, fmt.Errorf("cannot derive NATS subject prefix from planner knight %q subjects: %v",
			plannerKnight.Name, plannerKnight.Spec.NATS.Subjects)
	}

	subject := natspkg.ResultSubject(subjectPrefix, taskID)
	consumerName := fmt.Sprintf("mission-planner-%s", mission.Name)

	log.V(1).Info("Polling for planning result",
		"taskID", taskID,
		"stream", resultsStream,
		"subject", subject,
		"consumer", consumerName)

	// FetchOne acks the result (required for WorkQueue retention).
	msg, err := natsClient.FetchOne(ctx, resultsStream,
		natspkg.ResultConsumerConfig(consumerName, subject), 2*time.Second)

	if err != nil {
		log.V(1).Info("Planning result not yet available", "taskID", taskID, "error", err.Error())
		return nil, nil
	}
	if msg == nil {
		log.V(1).Info("Planning result not yet available", "taskID", taskID)
		return nil, nil
	}

	ctx, consumeSpan := natspkg.StartConsumeSpan(ctx, msg,
		natspkg.WithTable(aiv1alpha1.RefName(mission.Spec.RoundTableRef)), natspkg.WithTaskID(taskID))
	defer consumeSpan.End()

	_ = natsClient.DeleteConsumer(ctx, resultsStream, consumerName)

	var taskResult natspkg.TaskResult
	if err := json.Unmarshal(msg.Data(), &taskResult); err != nil {
		return nil, fmt.Errorf("failed to parse planning result: %w", err)
	}

	log.Info("Retrieved planning result from stream",
		"taskID", taskID,
		"stream", resultsStream,
		"outputLen", len(taskResult.GetOutput()))
	return &taskResult, nil
}

// parsePlannerOutput parses the JSON output from the planner knight.
func (p *Planner) parsePlannerOutput(output string) (*PlannerOutput, error) {
	output = extractJSON(output)

	var plan PlannerOutput
	if err := json.Unmarshal([]byte(output), &plan); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	return &plan, nil
}

// extractJSON extracts JSON from markdown code blocks if present.
func extractJSON(s string) string {
	if idx := strings.Index(s, "```json"); idx >= 0 {
		s = s[idx+7:]
		if idx := strings.Index(s, "```"); idx >= 0 {
			return strings.TrimSpace(s[:idx])
		}
	}
	if idx := strings.Index(s, "```"); idx >= 0 {
		s = s[idx+3:]
		if before, _, ok := strings.Cut(s, "```"); ok {
			return strings.TrimSpace(before)
		}
	}
	return strings.TrimSpace(s)
}

// templatesOnly reports whether the mission's plan may only build ephemeral
// knights from templates. Missions created through the invoke gateway are
// restricted this way so that a caller can't reach an image, a Secret or a
// pod spec through the planner (e.g. by prompt-injecting the objective).
func templatesOnly(mission *aiv1alpha1.Mission) bool {
	return mission.Labels[aiv1alpha1.LabelCreatedBy] == aiv1alpha1.CreatedByGateway
}

// checkTemplatesOnly rejects a planned knight that sets its own spec, env
// (which can reference Secrets) or tools on a templates-only mission.
func checkTemplatesOnly(mission *aiv1alpha1.Mission, k PlannerKnight) error {
	if !templatesOnly(mission) {
		return nil
	}
	if k.EphemeralSpec != nil {
		return fmt.Errorf("knight %q: ephemeralSpec is not allowed for gateway-created missions; use templateName", k.Name)
	}
	if o := k.SpecOverrides; o != nil && (len(o.Env) > 0 || o.Tools != nil) {
		return fmt.Errorf("knight %q: specOverrides.env and specOverrides.tools are not allowed for gateway-created missions", k.Name)
	}
	return nil
}

// validatePlan validates the planner output against schema and constraints.
func (p *Planner) validatePlan(ctx context.Context, mission *aiv1alpha1.Mission, plan *PlannerOutput) error {
	log := logf.FromContext(ctx)

	if plan.PlanVersion != "v1alpha1" {
		return fmt.Errorf("unsupported plan version: %s (expected v1alpha1)", plan.PlanVersion)
	}

	if plan.Metadata.Objective == "" {
		return fmt.Errorf("metadata.objective is required")
	}

	if len(plan.Chains) == 0 && len(plan.Knights) == 0 && len(plan.ExistingChains) == 0 {
		return fmt.Errorf("plan must contain at least one chain or knight")
	}

	maxChains := mission.Spec.Planner.MaxChains
	if maxChains == 0 {
		maxChains = 5
	}
	if total := len(plan.Chains) + len(plan.ExistingChains); int32(total) > maxChains {
		return fmt.Errorf("too many chains: %d > %d", total, maxChains)
	}

	maxKnights := mission.Spec.Planner.MaxKnights
	if maxKnights == 0 {
		maxKnights = 10
	}
	if int32(len(plan.Knights)) > maxKnights {
		return fmt.Errorf("too many knights: %d > %d", len(plan.Knights), maxKnights)
	}

	if len(plan.Skills) > 0 && !mission.Spec.Planner.AllowSkillGeneration {
		return fmt.Errorf("skill generation not allowed but plan includes %d skills", len(plan.Skills))
	}
	if len(plan.Skills) > 10 {
		return fmt.Errorf("too many skills: %d > 10 (hard limit)", len(plan.Skills))
	}

	knightNames := make(map[string]bool)
	for ki, k := range plan.Knights {
		if k.Name == "" {
			return fmt.Errorf("knight name is required")
		}
		if knightNames[k.Name] {
			return fmt.Errorf("duplicate knight name: %s", k.Name)
		}
		knightNames[k.Name] = true

		if err := checkTemplatesOnly(mission, k); err != nil {
			return err
		}

		if !k.Ephemeral {
			knight := &aiv1alpha1.Knight{}
			err := p.Client.Get(ctx, types.NamespacedName{
				Name:      k.Name,
				Namespace: mission.Namespace,
			}, knight)
			if err != nil {
				return fmt.Errorf("non-ephemeral knight %q not found: %w", k.Name, err)
			}
		} else {
			if k.TemplateName == "" && k.EphemeralSpec == nil {
				return fmt.Errorf("ephemeral knight %q must have templateName or ephemeralSpec", k.Name)
			}

			if k.TemplateName != "" {
				if err := p.validateTemplateExists(ctx, mission, k.TemplateName); err != nil {
					return fmt.Errorf("knight %q: %w", k.Name, err)
				}
			}
		}

		if !util.IsValidK8sName(k.Name) {
			sanitized := util.SanitizeK8sName(k.Name)
			if sanitized == "" {
				return fmt.Errorf("invalid knight name %q: cannot be sanitized to valid DNS label", k.Name)
			}
			log.Info("Auto-sanitized knight name", "original", k.Name, "sanitized", sanitized)
			// Update all references to this knight in chain steps
			for ci := range plan.Chains {
				for si := range plan.Chains[ci].Steps {
					if plan.Chains[ci].Steps[si].KnightRef.Name == k.Name {
						plan.Chains[ci].Steps[si].KnightRef.Name = sanitized
					}
				}
			}
			plan.Knights[ki].Name = sanitized
		}
	}

	chainNames := make(map[string]bool)
	for i, chain := range plan.Chains {
		if chain.Name == "" {
			return fmt.Errorf("chain[%d]: name is required", i)
		}
		if chainNames[chain.Name] {
			return fmt.Errorf("duplicate chain name: %s", chain.Name)
		}
		chainNames[chain.Name] = true

		if !util.IsValidK8sName(chain.Name) {
			sanitized := util.SanitizeK8sName(chain.Name)
			if sanitized == "" {
				return fmt.Errorf("invalid chain name %q: cannot be sanitized to valid DNS label", chain.Name)
			}
			log.Info("Auto-sanitized chain name", "original", chain.Name, "sanitized", sanitized)
			plan.Chains[i].Name = sanitized
			// Register the sanitized name too, so existingChains cannot collide with it.
			if chainNames[sanitized] {
				return fmt.Errorf("duplicate chain name: %s", sanitized)
			}
			chainNames[sanitized] = true
		}

		if !validChainPhase(chain.Phase) {
			return fmt.Errorf("chain %q: invalid phase %q (must be Setup, Active, or Teardown)", chain.Name, chain.Phase)
		}

		if len(chain.Steps) == 0 {
			return fmt.Errorf("chain %q: at least one step is required", chain.Name)
		}

		stepNames := make(map[string]bool)
		for j, step := range chain.Steps {
			if step.Name == "" {
				return fmt.Errorf("chain %q step[%d]: name is required", chain.Name, j)
			}
			if stepNames[step.Name] {
				return fmt.Errorf("chain %q: duplicate step name %q", chain.Name, step.Name)
			}
			stepNames[step.Name] = true

			if step.KnightRef.Name == "" {
				return fmt.Errorf("chain %q step %q: knightRef is required", chain.Name, step.Name)
			}
			if !knightNames[step.KnightRef.Name] {
				return fmt.Errorf("chain %q step %q: knight %q not found in plan", chain.Name, step.Name, step.KnightRef.Name)
			}
			if step.Task == "" {
				return fmt.Errorf("chain %q step %q: task is required", chain.Name, step.Name)
			}
		}

		nodes := make([]util.DAGNode, len(chain.Steps))
		for i, step := range chain.Steps {
			nodes[i] = util.DAGNode{
				Name:      step.Name,
				DependsOn: step.DependsOn,
			}
		}
		if err := util.ValidateDAG(nodes); err != nil {
			return fmt.Errorf("chain %q: %w", chain.Name, err)
		}
	}

	if err := validateStepOutputRefs(plan.Chains); err != nil {
		return err
	}

	if err := p.validateExistingChains(ctx, mission, plan, chainNames); err != nil {
		return err
	}

	skillNames := make(map[string]bool)
	for i, skill := range plan.Skills {
		if skill.Name == "" {
			return fmt.Errorf("skill[%d]: name is required", i)
		}
		if skillNames[skill.Name] {
			return fmt.Errorf("duplicate skill name: %s", skill.Name)
		}
		skillNames[skill.Name] = true

		if !util.IsValidSkillName(skill.Name) {
			return fmt.Errorf("invalid skill name %q: must be alphanumeric with hyphens", skill.Name)
		}
	}

	log.Info("Plan validation passed",
		"chains", len(plan.Chains),
		"knights", len(plan.Knights),
		"skills", len(plan.Skills))

	return nil
}

// validateTemplateExists checks if a knight template exists in mission or RoundTable.
func (p *Planner) validateTemplateExists(ctx context.Context, mission *aiv1alpha1.Mission, templateName string) error {
	for _, t := range mission.Spec.KnightTemplates {
		if t.Name == templateName {
			return nil
		}
	}

	rt, err := p.resolveRoundTable(ctx, mission)
	if err != nil {
		return fmt.Errorf("failed to resolve RoundTable: %w", err)
	}

	if _, ok := rt.Spec.KnightTemplates[templateName]; ok {
		return nil
	}

	return fmt.Errorf("template %q not found in mission or RoundTable", templateName)
}

// sanitizeStepName replaces hyphens with underscores to ensure Go template compatibility.
func sanitizeStepName(name string) string {
	return strings.ReplaceAll(name, "-", "_")
}

// applyPlan applies the validated plan to the mission spec.
func (p *Planner) applyPlan(ctx context.Context, mission *aiv1alpha1.Mission, plan *PlannerOutput) error {
	log := logf.FromContext(ctx)

	for _, pk := range plan.Knights {
		mk := aiv1alpha1.MissionKnight{
			Name:          pk.Name,
			Role:          pk.Role,
			Ephemeral:     pk.Ephemeral,
			TemplateName:  pk.TemplateName,
			EphemeralSpec: pk.EphemeralSpec,
			SpecOverrides: pk.SpecOverrides,
		}
		mission.Spec.GeneratedKnights = append(mission.Spec.GeneratedKnights, mk)
	}

	for _, pc := range plan.Chains {
		// Bug #83: Sanitize step names to replace hyphens with underscores
		// Build mapping of old names to new sanitized names
		nameMap := make(map[string]string)
		sanitizedSteps := make([]aiv1alpha1.ChainStep, len(pc.Steps))

		// Build a set of ephemeral knight names for knightRef prefixing
		ephemeralKnights := make(map[string]bool)
		for _, pk := range plan.Knights {
			if pk.Ephemeral {
				ephemeralKnights[pk.Name] = true
			}
		}

		for i, step := range pc.Steps {
			oldName := step.Name
			newName := sanitizeStepName(oldName)
			nameMap[oldName] = newName

			sanitizedSteps[i] = step
			sanitizedSteps[i].Name = newName

			// Prefix knightRef for ephemeral knights (they get mission-name prefix)
			if ephemeralKnights[step.KnightRef.Name] {
				sanitizedSteps[i].KnightRef.Name = fmt.Sprintf("%s-%s", mission.Name, step.KnightRef.Name)
			}
		}

		// Sanitize dependsOn references and task templates
		for i := range sanitizedSteps {
			// Sanitize dependsOn array
			if len(sanitizedSteps[i].DependsOn) > 0 {
				sanitizedDeps := make([]string, len(sanitizedSteps[i].DependsOn))
				for j, dep := range sanitizedSteps[i].DependsOn {
					sanitizedDeps[j] = sanitizeStepName(dep)
				}
				sanitizedSteps[i].DependsOn = sanitizedDeps
			}

			// Replace old hyphenated names in task templates with sanitized versions
			task := sanitizedSteps[i].Task
			for oldName, newName := range nameMap {
				if oldName != newName {
					// Replace template references like {{ .Steps.old-name.Output }}
					task = strings.ReplaceAll(task, oldName, newName)
				}
			}
			sanitizedSteps[i].Task = task
		}

		gc := aiv1alpha1.GeneratedChain{
			Name:           pc.Name,
			Description:    pc.Description,
			Steps:          sanitizedSteps,
			Phase:          pc.Phase,
			Input:          pc.Input,
			TimeoutSeconds: pc.TimeoutSeconds,
			RetryPolicy:    pc.RetryPolicy,
		}

		if gc.Phase == "" {
			gc.Phase = "Active"
		}

		mission.Spec.GeneratedChains = append(mission.Spec.GeneratedChains, gc)

		// FIX #3: Use consistent naming convention with mission controller
		chainName := fmt.Sprintf("mission-%s-%s", mission.Name, pc.Name)
		chain := &aiv1alpha1.Chain{
			ObjectMeta: metav1.ObjectMeta{
				Name:      chainName,
				Namespace: mission.Namespace,
				Labels: map[string]string{
					aiv1alpha1.LabelMission:         mission.Name,
					aiv1alpha1.LabelEphemeral:       "true",
					"ai.roundtable.io/generated-by": "planner",
				},
				Annotations: ParentAnnotations(mission),
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(mission, aiv1alpha1.GroupVersion.WithKind("Mission")),
				},
			},
			Spec: aiv1alpha1.ChainSpec{
				Description:   pc.Description,
				Steps:         sanitizedSteps,
				Input:         pc.Input,
				MissionRef:    aiv1alpha1.LocalRef(mission.Name),
				RoundTableRef: mission.Spec.RoundTableRef, // Bug #84: Inherit roundTableRef from parent Mission
			},
		}

		if pc.TimeoutSeconds != nil {
			chain.Spec.TimeoutSeconds = *pc.TimeoutSeconds
		}

		if pc.RetryPolicy != nil {
			chain.Spec.RetryPolicy = pc.RetryPolicy
		}

		if err := p.Client.Create(ctx, chain); err != nil {
			if client.IgnoreAlreadyExists(err) != nil {
				return fmt.Errorf("failed to create chain %q: %w", chainName, err)
			}
			log.Info("Chain already exists, skipping", "chain", chainName)
		} else {
			log.Info("Created chain CR", "chain", chainName, "steps", len(sanitizedSteps))
		}

		mission.Spec.Chains = append(mission.Spec.Chains, aiv1alpha1.MissionChainRef{
			Name:  pc.Name,
			Phase: pc.Phase,
		})
	}

	for _, ec := range plan.ExistingChains {
		mission.Spec.Chains = append(mission.Spec.Chains, aiv1alpha1.MissionChainRef{
			Name:          ec.Name,
			InputOverride: ec.Input,
			Phase:         ec.Phase,
		})
	}

	if len(plan.Skills) > 0 {
		if err := p.createSkillConfigMaps(ctx, mission, plan.Skills); err != nil {
			log.Error(err, "Failed to create skill ConfigMaps (non-fatal)")
		}
	}

	log.Info("Successfully applied plan to mission",
		"generatedKnights", len(mission.Spec.GeneratedKnights),
		"generatedChains", len(mission.Spec.GeneratedChains),
		"chains", len(mission.Spec.Chains))

	return nil
}

// createSkillConfigMaps creates ConfigMaps for generated skills.
func (p *Planner) createSkillConfigMaps(ctx context.Context, mission *aiv1alpha1.Mission, skills []PlannerSkill) error {
	log := logf.FromContext(ctx)

	for _, skill := range skills {
		cmName := fmt.Sprintf("%s-skill-%s", mission.Name, skill.Name)

		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      cmName,
				Namespace: mission.Namespace,
				Labels: map[string]string{
					aiv1alpha1.LabelMission:   mission.Name,
					aiv1alpha1.LabelEphemeral: "true",
					"ai.roundtable.io/skill":  skill.Name,
				},
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(mission, aiv1alpha1.GroupVersion.WithKind("Mission")),
				},
			},
			Data: map[string]string{
				"skill.sh":    skill.Content,
				"description": skill.Description,
			},
		}

		if err := p.Client.Create(ctx, cm); err != nil {
			if client.IgnoreAlreadyExists(err) != nil {
				return fmt.Errorf("failed to create skill ConfigMap %q: %w", cmName, err)
			}
			log.Info("Skill ConfigMap already exists", "name", cmName)
		} else {
			log.Info("Created skill ConfigMap", "name", cmName, "skill", skill.Name)
		}
	}

	return nil
}

// resolveRoundTable resolves the RoundTable for this mission.
func (p *Planner) resolveRoundTable(ctx context.Context, mission *aiv1alpha1.Mission) (*aiv1alpha1.RoundTable, error) {
	if aiv1alpha1.RefName(mission.Spec.RoundTableRef) == "" {
		return nil, fmt.Errorf("no RoundTable reference configured")
	}

	rt := &aiv1alpha1.RoundTable{}
	err := p.Client.Get(ctx, types.NamespacedName{
		Name:      aiv1alpha1.RefName(mission.Spec.RoundTableRef),
		Namespace: mission.Namespace,
	}, rt)
	if err != nil {
		return nil, err
	}

	return rt, nil
}

const (
	// maxPlannerChains caps how many existing Chains are offered to the planner.
	maxPlannerChains = 30
	// maxChainInputExample caps the example input shown per existing Chain.
	maxChainInputExample = 500
)

func truncateString(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "..."
}

// listTemplateChains returns the table's existing Chains that are templates
// (not mission copies, issue-pickup chains or planner-generated chains and not
// bound to another table), sorted by name and capped at maxPlannerChains.
// It returns nothing when the mission has no roundTableRef.
func (p *Planner) listTemplateChains(ctx context.Context, mission *aiv1alpha1.Mission) []aiv1alpha1.Chain {
	rtName := aiv1alpha1.RefName(mission.Spec.RoundTableRef)
	if rtName == "" {
		return nil
	}
	var list aiv1alpha1.ChainList
	if err := p.Client.List(ctx, &list, client.InNamespace(mission.Namespace)); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list chains for planning prompt")
		return nil
	}
	var out []aiv1alpha1.Chain
	for _, c := range list.Items {
		if _, ok := c.Labels[aiv1alpha1.LabelMission]; ok {
			continue
		}
		if _, ok := c.Labels[aiv1alpha1.LabelPickupRoundTable]; ok {
			continue
		}
		if _, ok := c.Labels["ai.roundtable.io/generated-by"]; ok {
			continue
		}
		if ref := aiv1alpha1.RefName(c.Spec.RoundTableRef); ref != "" && ref != rtName {
			continue
		}
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	if len(out) > maxPlannerChains {
		out = out[:maxPlannerChains]
	}
	return out
}

// ephemeralGuideline returns guideline 3: which template ephemeral knights
// should use, depending on what templates are actually available.
func (p *Planner) ephemeralGuideline(ctx context.Context, mission *aiv1alpha1.Mission) string {
	names := map[string]bool{}
	for _, t := range mission.Spec.KnightTemplates {
		names[t.Name] = true
	}
	if rt, err := p.resolveRoundTable(ctx, mission); err == nil {
		for n := range rt.Spec.KnightTemplates {
			names[n] = true
		}
	}
	if names["base"] {
		return "3. For ephemeral knights, ALWAYS use templateName=\"base\" and customize via specOverrides\n"
	}
	if len(names) == 0 {
		return "3. Ephemeral knights are not available (no knight templates exist): use only existing knights with ephemeral=false\n"
	}
	sorted := make([]string, 0, len(names))
	for n := range names {
		sorted = append(sorted, n)
	}
	sort.Strings(sorted)
	return fmt.Sprintf("3. For ephemeral knights, use templateName set to one of: %s (there is no \"base\" template) and customize via specOverrides\n", strings.Join(sorted, ", "))
}

// validateExistingChains checks the plan's existingChains: each must be a
// listed template Chain, its name must not collide with a generated chain, and
// the standing knights its steps use are recruited into the plan implicitly.
func (p *Planner) validateExistingChains(ctx context.Context, mission *aiv1alpha1.Mission, plan *PlannerOutput, chainNames map[string]bool) error {
	if len(plan.ExistingChains) == 0 {
		return nil
	}
	templates := map[string]aiv1alpha1.Chain{}
	for _, c := range p.listTemplateChains(ctx, mission) {
		templates[c.Name] = c
	}
	seen := map[string]bool{}
	// standing knights already recruited by the plan
	recruited := map[string]bool{}
	for _, k := range plan.Knights {
		if !k.Ephemeral {
			recruited[k.Name] = true
		}
	}
	for i, ec := range plan.ExistingChains {
		if ec.Name == "" {
			return fmt.Errorf("existingChains[%d]: name is required", i)
		}
		if seen[ec.Name] || chainNames[ec.Name] {
			return fmt.Errorf("duplicate chain name: %s", ec.Name)
		}
		seen[ec.Name] = true
		if !validChainPhase(ec.Phase) {
			return fmt.Errorf("existing chain %q: invalid phase %q (must be Setup, Active, or Teardown)", ec.Name, ec.Phase)
		}
		c, ok := templates[ec.Name]
		if !ok {
			return fmt.Errorf("existing chain %q not found among the table's reusable chains", ec.Name)
		}
		var used []string
		for _, st := range c.Spec.Steps {
			if st.KnightRef.Name != "" {
				used = append(used, st.KnightRef.Name)
			}
			if st.Loop != nil {
				for _, ls := range st.Loop.Steps {
					used = append(used, ls.KnightRef.Name)
				}
			}
		}
		for _, kn := range used {
			if recruited[kn] {
				continue
			}
			knight := &aiv1alpha1.Knight{}
			if err := p.Client.Get(ctx, types.NamespacedName{Name: kn, Namespace: mission.Namespace}, knight); err != nil {
				return fmt.Errorf("existing chain %q: knight %q not found: %w", ec.Name, kn, err)
			}
			if c.Spec.RoundTableRef == nil || aiv1alpha1.RefName(c.Spec.RoundTableRef) == "" {
				// Unbound chain: its knights must belong to the mission's table.
				if rt := aiv1alpha1.RefName(mission.Spec.RoundTableRef); rt != "" {
					if kt := knightpkg.KnightTable(knight); kt != "" && kt != rt {
						return fmt.Errorf("existing chain %q: knight %q belongs to table %q, not %q", ec.Name, kn, kt, rt)
					}
				}
			}
			recruited[kn] = true
			plan.Knights = append(plan.Knights, PlannerKnight{
				Name:      kn,
				Role:      fmt.Sprintf("used by existing chain %s", ec.Name),
				Ephemeral: false,
			})
		}
	}
	maxKnights := mission.Spec.Planner.MaxKnights
	if maxKnights == 0 {
		maxKnights = 10
	}
	if int32(len(plan.Knights)) > maxKnights {
		return fmt.Errorf("too many knights: %d > %d (including knights recruited by existing chains)", len(plan.Knights), maxKnights)
	}
	return nil
}

// validChainPhase reports whether phase is empty or a known mission phase.
func validChainPhase(phase string) bool {
	return phase == "" || phase == "Setup" || phase == "Active" || phase == "Teardown"
}
