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

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
	rtmetrics "github.com/roundtable-k8s/roundtable/pkg/metrics"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
	rtruntime "github.com/roundtable-k8s/roundtable/pkg/runtime"
)

const (
	knightFinalizer    = "ai.roundtable.io/knight-finalizer"
	fieldOwner         = "roundtable-operator"
	specHashAnnotation = "roundtable.io/spec-hash"
)

// KnightReconciler reconciles a Knight object
type KnightReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	Recorder     record.EventRecorder
	DefaultImage string // Default pi-knight image (set via DEFAULT_KNIGHT_IMAGE env var)

	// KnightSecurity is the pod-level security context applied to both knight
	// Deployments and Nix build Jobs. Chart-driven (KNIGHT_* env vars); zero
	// value falls back to DefaultPodSecurity.
	KnightSecurity knightpkg.PodSecurity

	// NATS is the shared NATS provider used to read task consumer backlog for
	// the Waking status. Optional; without it scaled-to-zero knights report
	// ScaledToZero only.
	NATS *natspkg.Provider

	// PendingSource overrides the consumer backlog lookup (tests).
	PendingSource ConsumerPendingSource

	// RuntimeBackend abstracts the lifecycle of Knight runtime resources.
	// When set, the controller delegates Deployment reconciliation and
	// suspend/resume to this backend. When nil, falls back to the
	// inline reconcileDeployment / reconcileSuspended methods.
	RuntimeBackend rtruntime.RuntimeBackend

	// RuntimeBackends maps runtime type names to their backend implementations.
	// The controller selects the backend based on knight.Spec.Runtime.
	// If nil or the key is missing, falls back to RuntimeBackend.
	RuntimeBackends map[string]rtruntime.RuntimeBackend

	// SandboxEnabled reports whether the agent-sandbox v1beta1 API is served
	// and the sandbox backend is registered. When false, the controller does
	// not watch Sandbox objects (the watch would never sync and the manager
	// would exit, see #186) and knights with runtime: sandbox are parked with
	// RuntimeAvailable=False instead of being reconciled.
	SandboxEnabled bool

	// KEDAEnabled reports whether keda.sh/v1alpha1 ScaledObject is served.
	// When false the operator never touches ScaledObjects, and knights that
	// ask for autoscaling run spec.replicas pods with Autoscaled=False.
	KEDAEnabled bool

	// NATSMonitoringEndpoint overrides the NATS HTTP monitoring endpoint
	// (host:port) written into ScaledObject triggers. Empty derives it from
	// each knight's spec.nats.url host on port 8222.
	NATSMonitoringEndpoint string

	// ToolsImageMissing reports whether the registry positively says the
	// tools image manifest at the given URL does not exist. Nil uses an HTTP
	// HEAD probe; tests replace it.
	ToolsImageMissing func(ctx context.Context, manifestURL string) bool

	// toolsImageVerifiedAt records, per image reference, when the operator
	// last confirmed the image was in the registry (throttles the probe).
	toolsImageVerifiedAt sync.Map
}

// runtimeSandbox is the spec.runtime value for the agent-sandbox backend.
const runtimeSandbox = "sandbox"

// runtimeDeployment is the default spec.runtime value (a Deployment).
const runtimeDeployment = "deployment"

// +kubebuilder:rbac:groups=ai.roundtable.io,resources=knights,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=knights/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=knights/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=node.k8s.io,resources=runtimeclasses,verbs=get;list;watch

func (r *KnightReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Fetch the Knight resource
	knight := &aiv1alpha1.Knight{}
	if err := r.Get(ctx, req.NamespacedName, knight); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Knight resource not found — likely deleted")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Handle deletion via finalizer
	if knight.DeletionTimestamp != nil {
		if controllerutil.ContainsFinalizer(knight, knightFinalizer) {
			log.Info("Cleaning up knight resources", "knight", knight.Name)
			// NATS consumer cleanup would go here (future: NATS admin API call)
			controllerutil.RemoveFinalizer(knight, knightFinalizer)
			if err := r.Update(ctx, knight); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(knight, knightFinalizer) {
		// Merge patch so the operator never rewrites user-owned spec fields.
		base := knight.DeepCopy()
		controllerutil.AddFinalizer(knight, knightFinalizer)
		if err := r.Patch(ctx, knight, client.MergeFrom(base)); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Set initial status
	if knight.Status.Phase == "" {
		knight.Status.Phase = aiv1alpha1.KnightPhaseProvisioning
		if err := r.Status().Update(ctx, knight); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Inherit the table's defaults and vault, then the operator defaults (in
	// memory only). Every status update below reports the effective model.
	ownRuntimeClass := knight.Spec.RuntimeClassName != nil
	knightpkg.ApplyTableDefaults(ctx, r.Client, knight)
	knight.Status.Model = knight.Spec.Model

	// A sandbox knight without the sandbox backend can't be reconciled. Park it
	// with a clear condition and leave any existing runtime resources alone.
	if knight.Spec.Runtime == runtimeSandbox && !r.SandboxEnabled {
		return r.reconcileRuntimeUnavailable(ctx, knight)
	}

	// Resolve the runtime backend for this knight
	backend := r.runtimeBackendFor(knight)

	// Clean up resources from a previous runtime type (e.g., Deployment → Sandbox transition)
	if err := r.cleanupStaleRuntime(ctx, knight); err != nil {
		log.Error(err, "Failed to clean up stale runtime resources")
		// Don't block reconciliation — the cleanup will retry on next reconcile
	}

	// Handle suspended state. The ScaledObject goes first, or KEDA would
	// scale the Deployment straight back up.
	if knight.Spec.Suspend {
		if err := r.deleteScaledObject(ctx, knight); err != nil {
			return ctrl.Result{}, err
		}
		if backend != nil {
			if err := backend.Suspend(ctx, knight); err != nil {
				return ctrl.Result{}, err
			}
			return r.finishSuspended(ctx, knight)
		}
		return r.reconcileSuspended(ctx, knight)
	}

	// (After the suspend step, so a parked knight can still be suspended.)
	// A knight naming a RuntimeClass that doesn't exist would only produce pods
	// the API server rejects; park it with a clear condition instead.
	if name := runtimeClassOf(knight); name != "" {
		if err := r.Get(ctx, types.NamespacedName{Name: name}, &nodev1.RuntimeClass{}); err != nil {
			if !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
			field := "spec.runtimeClassName"
			if !ownRuntimeClass {
				field = "the RoundTable's spec.defaults.runtimeClassName (or set spec.runtimeClassName on the knight)"
			}
			return r.parkKnight(ctx, knight, aiv1alpha1.ReasonRuntimeClassNotFound,
				fmt.Sprintf("RuntimeClass %q does not exist; create it or change %s", name, field))
		}
	}

	// Reconcile each owned resource
	var reconcileErr error

	// 0. Replica plan: spec.replicas/maxReplicas, KEDA availability and the
	//    table's plan-task cap.
	plan, err := r.resolveScaling(ctx, knight)
	if err != nil {
		reconcileErr = err
		log.Error(err, "Failed to resolve knight scaling")
		plan.Scaling = plan.CapReplicas(1)
	}

	// 1. ConfigMap (tools + prompt config)
	if err := r.reconcileConfigMap(ctx, knight); err != nil {
		reconcileErr = err
		log.Error(err, "Failed to reconcile ConfigMap")
	}

	// 2. PVC (persistent workspace)
	if err := r.reconcilePVC(ctx, knight); err != nil {
		reconcileErr = err
		log.Error(err, "Failed to reconcile PVC")
	}

	// 2b. Nix tools — built into a per-tool-list OCI image the knight mounts.
	//     Returns an interval to come back on while an image is published, to
	//     re-verify it is still in the registry.
	var nixRequeue time.Duration
	if d, err := r.reconcileNixTools(ctx, knight); err != nil {
		reconcileErr = err
		log.Error(err, "Failed to reconcile Nix tools")
	} else {
		nixRequeue = d
	}

	// 3. Runtime (Deployment or Sandbox, depending on knight.Spec.Runtime).
	//    Held while the tools image for the current spec is unpublished, so the
	//    pod doesn't roll onto a spec that expects tools its image lacks.
	if hold, err := r.holdRollout(ctx, knight); err != nil {
		reconcileErr = err
		log.Error(err, "Failed to check tools image rollout hold")
	} else if hold {
		log.Info("Holding pod rollout until the tools image is published", "image", knightpkg.ToolsImageRef(knight))
	} else if backend != nil {
		if err := backend.Reconcile(ctx, knight); err != nil {
			reconcileErr = err
			log.Error(err, "Failed to reconcile runtime", "backend", knight.Spec.Runtime)
		}
	} else {
		if err := r.reconcileDeployment(ctx, knight); err != nil {
			reconcileErr = err
			log.Error(err, "Failed to reconcile Deployment")
		}
	}

	// 4. Scaling: KEDA ScaledObject and PodDisruptionBudget
	if err := r.reconcileScaledObject(ctx, knight, plan.Scaling); err != nil {
		reconcileErr = err
		log.Error(err, "Failed to reconcile ScaledObject")
	}
	if err := r.reconcilePDB(ctx, knight, plan.Scaling); err != nil {
		reconcileErr = err
		log.Error(err, "Failed to reconcile PodDisruptionBudget")
	}
	r.setScalingConditions(knight, plan)

	// Update status based on reconciliation results
	if err := r.updateStatus(ctx, knight, plan.Scaling, reconcileErr); err != nil {
		log.Error(err, "Failed to update status")
		return ctrl.Result{}, err
	}

	if reconcileErr != nil {
		return ctrl.Result{RequeueAfter: RequeueSlow}, reconcileErr
	}

	// Waking: poll so the transition to Ready is visible without a pod event.
	if c := meta.FindStatusCondition(knight.Status.Conditions, aiv1alpha1.ConditionKnightAvailable); c != nil &&
		c.Reason == aiv1alpha1.ReasonKnightWaking {
		if nixRequeue == 0 || wakingRequeue < nixRequeue {
			nixRequeue = wakingRequeue
		}
	}

	if nixRequeue > 0 {
		return ctrl.Result{RequeueAfter: nixRequeue}, nil
	}

	return ctrl.Result{}, nil
}

// wakingRequeue is how often a waking knight is re-checked.
const wakingRequeue = 5 * time.Second

// ConsumerPendingSource reports how many tasks are queued or in flight on a
// JetStream consumer (num_pending + num_ack_pending).
type ConsumerPendingSource interface {
	ConsumerPending(stream, consumer string) (uint64, error)
}

// hasPendingTasks reports whether the knight's task consumer has queued or
// in-flight tasks. Lookup failures count as "none" (the knight then reports
// plain ScaledToZero).
func (r *KnightReconciler) hasPendingTasks(ctx context.Context, knight *aiv1alpha1.Knight) bool {
	src := r.PendingSource
	if src == nil && r.NATS != nil {
		if cli, err := r.NATS.Client(); err == nil {
			src, _ = cli.(ConsumerPendingSource)
		}
	}
	if src == nil || knight.Spec.NATS.Stream == "" {
		return false
	}
	n, err := src.ConsumerPending(knight.Spec.NATS.Stream, knightpkg.TaskConsumerName(knight))
	if err != nil {
		logf.FromContext(ctx).V(1).Info("Could not read knight consumer pending count", "knight", knight.Name, "error", err.Error())
		return false
	}
	return n > 0
}

// cleanupStaleRuntime removes runtime resources from a previous runtime type.
// When a Knight transitions between "deployment" and "sandbox" runtimes,
// the old resource (Deployment or Sandbox) must be removed to avoid
// Multi-Attach errors on RWO PVCs.
func (r *KnightReconciler) cleanupStaleRuntime(ctx context.Context, knight *aiv1alpha1.Knight) error {
	log := logf.FromContext(ctx)
	nn := types.NamespacedName{Name: knight.Name, Namespace: knight.Namespace}

	if knight.Spec.Runtime == runtimeSandbox {
		// Sandbox runtime — clean up any stale Deployment
		deploy := &appsv1.Deployment{}
		if err := r.Get(ctx, nn, deploy); err == nil {
			log.Info("Runtime transition: deleting stale Deployment for sandbox knight",
				"knight", knight.Name)
			if err := r.Delete(ctx, deploy); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("failed to delete stale deployment during runtime transition: %w", err)
			}
		}
	} else if r.SandboxEnabled {
		// Deployment runtime (default) — clean up any stale Sandbox. Skipped
		// when the Sandbox API isn't served: there can be nothing to clean up.
		sandbox := rtruntime.NewSandboxObject(knight.Name, knight.Namespace)
		if err := r.Get(ctx, nn, sandbox); err == nil {
			log.Info("Runtime transition: deleting stale Sandbox for deployment knight",
				"knight", knight.Name)
			if err := r.Delete(ctx, sandbox); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("failed to delete stale sandbox during runtime transition: %w", err)
			}
		}
	}

	return nil
}

// reconcileRuntimeUnavailable marks a knight whose runtime backend is not
// enabled (runtime: sandbox without the agent-sandbox v1beta1 API) and
// requeues slowly. The status is only written when it changes.
func (r *KnightReconciler) reconcileRuntimeUnavailable(ctx context.Context, knight *aiv1alpha1.Knight) (ctrl.Result, error) {
	msg := fmt.Sprintf("Knight requests runtime %q, but the operator started without the sandbox backend: "+
		"%s Sandbox is not served by the API server (or --enable-sandbox-backend=false). "+
		"Install agent-sandbox v1.0.0 or later and restart the operator, or set spec.runtime to \"deployment\".",
		knight.Spec.Runtime, rtruntime.SandboxGVK.GroupVersion())

	return r.parkKnight(ctx, knight, aiv1alpha1.ReasonSandboxUnavailable, msg)
}

// runtimeClassOf returns the knight's effective RuntimeClass name ("" if none).
func runtimeClassOf(knight *aiv1alpha1.Knight) string {
	if knight.Spec.RuntimeClassName == nil {
		return ""
	}
	return *knight.Spec.RuntimeClassName
}

// parkKnight marks a knight RuntimeAvailable=False / Available=False with the
// given reason, leaves its runtime resources alone and requeues slowly. The
// status is only written when it changes.
func (r *KnightReconciler) parkKnight(ctx context.Context, knight *aiv1alpha1.Knight, reason, msg string) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	changed := meta.SetStatusCondition(&knight.Status.Conditions, metav1.Condition{
		Type:               aiv1alpha1.ConditionKnightRuntimeAvailable,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: knight.Generation,
	})
	if meta.SetStatusCondition(&knight.Status.Conditions, metav1.Condition{
		Type:               aiv1alpha1.ConditionKnightAvailable,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: knight.Generation,
	}) {
		changed = true
	}
	if knight.Status.Phase != aiv1alpha1.KnightPhaseDegraded || knight.Status.Ready ||
		knight.Status.ObservedGeneration != knight.Generation {
		changed = true
	}

	if changed {
		log.Info("Runtime unavailable; knight parked", "knight", knight.Name, "reason", reason)
		r.Recorder.Event(knight, corev1.EventTypeWarning, reason, msg)
		knight.Status.Phase = aiv1alpha1.KnightPhaseDegraded
		knight.Status.Ready = false
		knight.Status.ObservedGeneration = knight.Generation
		if err := r.Status().Update(ctx, knight); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{RequeueAfter: RequeueVerySlow}, nil
}

// reconcileSuspended scales the deployment to 0 and updates status.
func (r *KnightReconciler) reconcileSuspended(ctx context.Context, knight *aiv1alpha1.Knight) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	deploy := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: knight.Name, Namespace: knight.Namespace}, deploy)
	if err == nil {
		zero := int32(0)
		if deploy.Spec.Replicas == nil || *deploy.Spec.Replicas != zero {
			deploy.Spec.Replicas = &zero
			if err := r.Update(ctx, deploy); err != nil {
				return ctrl.Result{}, err
			}
			log.Info("Suspended knight — scaled to 0", "knight", knight.Name)
			r.Recorder.Event(knight, corev1.EventTypeNormal, "Suspended", "Knight suspended")
		}
	}

	knight.Status.Phase = aiv1alpha1.KnightPhaseSuspended
	knight.Status.Ready = false
	meta.SetStatusCondition(&knight.Status.Conditions, metav1.Condition{
		Type:               aiv1alpha1.ConditionKnightAvailable,
		Status:             metav1.ConditionFalse,
		Reason:             aiv1alpha1.ReasonKnightSuspended,
		Message:            "Knight is suspended",
		ObservedGeneration: knight.Generation,
	})
	knight.Status.ObservedGeneration = knight.Generation
	if err := r.Status().Update(ctx, knight); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// finishSuspended updates the Knight status after the RuntimeBackend has suspended it.
func (r *KnightReconciler) finishSuspended(ctx context.Context, knight *aiv1alpha1.Knight) (ctrl.Result, error) {
	knight.Status.Phase = aiv1alpha1.KnightPhaseSuspended
	knight.Status.Ready = false
	meta.SetStatusCondition(&knight.Status.Conditions, metav1.Condition{
		Type:               aiv1alpha1.ConditionKnightAvailable,
		Status:             metav1.ConditionFalse,
		Reason:             aiv1alpha1.ReasonKnightSuspended,
		Message:            "Knight is suspended",
		ObservedGeneration: knight.Generation,
	})
	knight.Status.ObservedGeneration = knight.Generation
	if err := r.Status().Update(ctx, knight); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// reconcileConfigMap creates/updates the knight's tool and prompt configuration.
func (r *KnightReconciler) reconcileConfigMap(ctx context.Context, knight *aiv1alpha1.Knight) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("knight-%s-config", knight.Name),
			Namespace: knight.Namespace,
		},
	}

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		// Set owner reference
		if err := controllerutil.SetControllerReference(knight, cm, r.Scheme); err != nil {
			return err
		}

		if cm.Labels == nil {
			cm.Labels = make(map[string]string)
		}
		cm.Labels["app.kubernetes.io/name"] = "knight"
		cm.Labels["app.kubernetes.io/instance"] = knight.Name
		cm.Labels["app.kubernetes.io/managed-by"] = "roundtable-operator"
		cm.Labels["roundtable.io/domain"] = knight.Spec.Domain

		if cm.Data == nil {
			cm.Data = make(map[string]string)
		}

		// Generate mise.toml for tool provisioning
		cm.Data["mise.toml"] = knightpkg.GenerateMiseToml(knight)

		// Generate apt.txt for system packages
		if knight.Spec.Tools != nil && len(knight.Spec.Tools.Apt) > 0 {
			cm.Data["apt.txt"] = strings.Join(knight.Spec.Tools.Apt, "\n")
		}

		// Skill categories for the skill-filter sidecar
		cm.Data["KNIGHT_SKILLS"] = strings.Join(knight.Spec.Skills, ",")

		// flake.nix was the shared-store flake; nothing reads it any more.
		delete(cm.Data, "flake.nix")

		// Flake for the tools image build Job.
		if knightpkg.ToolsImageEnabled() && knightpkg.HasToolsImage(knight) {
			cm.Data[toolsImageFlakeKey] = knightpkg.GenerateToolsImageFlake(knight)
		} else {
			delete(cm.Data, toolsImageFlakeKey)
		}

		// Generate TOOLS.md listing available tools and paths
		if knightpkg.HasToolsImage(knight) {
			var toolsDoc strings.Builder
			toolsDoc.WriteString("# Available Tools\n\n")
			toolsDoc.WriteString("Tools are installed at `" + knightpkg.ToolsImageProfilePath + "/bin/` (a read-only tools image) and are in your PATH.\n\n")
			toolsDoc.WriteString("## Nix Packages\n")
			for _, pkg := range knight.Spec.Tools.Nix {
				toolsDoc.WriteString(fmt.Sprintf("- %s\n", pkg))
			}
			for _, f := range knight.Spec.Tools.Flakes {
				for _, pkg := range f.Packages {
					toolsDoc.WriteString(fmt.Sprintf("- %s (flake %s)\n", pkg, f.Ref))
				}
			}
			toolsDoc.WriteString("\n## Shared Workspace\n")
			toolsDoc.WriteString("- `/shared/` — RWX volume shared with all knights\n")
			toolsDoc.WriteString("- `/shared/repos/` — Pre-cloned git repositories\n")
			toolsDoc.WriteString("- `/shared/chains/` — Chain working directories\n")
			toolsDoc.WriteString("\n## Git Configuration\n")
			toolsDoc.WriteString("- `GH_TOKEN` / `GITHUB_TOKEN` env vars are set for GitHub API access\n")
			toolsDoc.WriteString("- Use `gh` CLI for PR creation: `gh pr create --title ... --body ...`\n")
			toolsDoc.WriteString("- Use authenticated clone: `git clone https://${GH_TOKEN}@github.com/...`\n")
			toolsDoc.WriteString("\n## Adding Tools\n")
			toolsDoc.WriteString("The tool set is fixed by `spec.tools.nix` and `spec.tools.flakes` on your Knight resource and is\n")
			toolsDoc.WriteString("read-only at runtime. To add a tool, request it via the\n")
			toolsDoc.WriteString("fleet-self-improvement chain.\n")
			cm.Data["TOOLS.md"] = toolsDoc.String()
		}

		// Prompt overrides
		if knight.Spec.Prompt != nil {
			if knight.Spec.Prompt.Identity != "" {
				cm.Data["SOUL.md"] = knight.Spec.Prompt.Identity
			}
			if knight.Spec.Prompt.Instructions != "" {
				cm.Data["AGENTS.md"] = knight.Spec.Prompt.Instructions
			}
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("configmap reconcile failed: %w", err)
	}

	logf.FromContext(ctx).Info("ConfigMap reconciled", "operation", op)
	return nil
}

// reconcilePVC creates the knight's persistent workspace volume.
// Skips creation when spec.workspace.existingClaim is set (migration mode) or
// the workspace is ephemeral. Switching to ephemeral leaves an existing PVC in
// place (it holds the knight's memory); it goes when the Knight is deleted.
//
// Nix tooling has no PVC: knights mount their tools image read-only (see
// PodBuilder.WithNixStore).
func (r *KnightReconciler) reconcilePVC(ctx context.Context, knight *aiv1alpha1.Knight) error {
	if knightpkg.EphemeralWorkspace(knight) {
		return nil
	}
	// Workspace PVC — skip if using an existing claim (migration mode)
	if knight.Spec.Workspace != nil && knight.Spec.Workspace.ExistingClaim != "" {
		logf.FromContext(ctx).Info("Using existing PVC", "claim", knight.Spec.Workspace.ExistingClaim)
		return nil
	}
	return r.ensureWorkspacePVC(ctx, knight)
}

// ensureWorkspacePVC creates a new workspace PVC if one doesn't exist.
func (r *KnightReconciler) ensureWorkspacePVC(ctx context.Context, knight *aiv1alpha1.Knight) error {
	pvcName := knight.Name
	pvc := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: knight.Namespace}, pvc)

	if apierrors.IsNotFound(err) {
		storageSize := resource.MustParse("1Gi")
		if knight.Spec.Workspace != nil && knight.Spec.Workspace.Size != nil && !knight.Spec.Workspace.Size.IsZero() {
			storageSize = knight.Spec.Workspace.Size.DeepCopy()
		}

		pvc = &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pvcName,
				Namespace: knight.Namespace,
				Labels: map[string]string{
					"app.kubernetes.io/name":       "knight",
					"app.kubernetes.io/instance":   knight.Name,
					"app.kubernetes.io/managed-by": "roundtable-operator",
					"roundtable.io/domain":         knight.Spec.Domain,
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: storageSize,
					},
				},
			},
		}
		if err := controllerutil.SetControllerReference(knight, pvc, r.Scheme); err != nil {
			return err
		}
		if err := r.Create(ctx, pvc); err != nil {
			return fmt.Errorf("PVC create failed: %w", err)
		}
		logf.FromContext(ctx).Info("PVC created", "name", pvcName)
	} else if err != nil {
		return fmt.Errorf("PVC get failed: %w", err)
	}
	return nil
}

// reconcileDeployment creates/updates the knight's Deployment when no
// RuntimeBackend is configured. It runs the same logic as the Deployment
// backend (spec-hash guarded updates, KEDA-owned replicas left alone).
func (r *KnightReconciler) reconcileDeployment(ctx context.Context, knight *aiv1alpha1.Knight) error {
	return r.NewDeploymentBackend().Reconcile(ctx, knight)
}

// NewDeploymentBackend returns a Deployment runtime backend wired to this
// reconciler's spec builder and autoscaling check.
func (r *KnightReconciler) NewDeploymentBackend() *rtruntime.DeploymentBackend {
	b := rtruntime.NewDeploymentBackend(r.Client, r.Scheme, r.DefaultImage, r.BuildDeploymentSpec)
	b.Autoscaled = r.isAutoscaled
	return b
}

// BuildDeploymentSpec constructs the full DeploymentSpec for a Knight.
// Exported so it can be passed as a PodSpecBuilder to RuntimeBackend.
//
// Replicas is the fixed replica count, or the initial count when KEDA owns
// it. A workspace that more than one pod can mount (ephemeral, or an
// existingClaim) rolls with maxSurge 1 / maxUnavailable 0 so the old pod
// drains in-flight tasks while the new one starts; a single ReadWriteOnce
// PVC keeps Recreate. terminationGracePeriodSeconds is taskTimeoutSeconds+60.
func (r *KnightReconciler) BuildDeploymentSpec(ctx context.Context, knight *aiv1alpha1.Knight) appsv1.DeploymentSpec {
	labels := knightLabels(knight)
	replicas := r.scalingOrFallback(ctx, knight).InitialReplicas()
	strategy := knightpkg.RolloutStrategy(knight)
	return appsv1.DeploymentSpec{
		Replicas: &replicas,
		Strategy: strategy,
		Selector: &metav1.LabelSelector{
			MatchLabels: labels,
		},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: labels,
			},
			Spec: r.BuildDrainablePodSpec(ctx, knight),
		},
	}
}

// BuildDrainablePodSpec is BuildPodSpec plus the drain grace period. Exported
// so the sandbox backend gets the same grace period as Deployment knights.
func (r *KnightReconciler) BuildDrainablePodSpec(ctx context.Context, knight *aiv1alpha1.Knight) corev1.PodSpec {
	spec := r.BuildPodSpec(ctx, knight)
	spec.TerminationGracePeriodSeconds = new(knightpkg.TerminationGracePeriod(knight))
	return spec
}

// BuildPodSpec constructs the complete pod spec for a knight using the composable builder.
// Exported so it can be passed to RuntimeBackend implementations (e.g., SandboxBackend).
func (r *KnightReconciler) BuildPodSpec(ctx context.Context, k *aiv1alpha1.Knight) corev1.PodSpec {
	configMapName := fmt.Sprintf("knight-%s-config", k.Name)

	builder := knightpkg.NewPodBuilder(k, r.DefaultImage).
		WithSecurity(r.KnightSecurity).
		WithReader(r.Client).
		WithWorkspace().
		WithConfig(configMapName).
		WithNixStore().
		WithVault().
		WithSharedWorkspace(ctx).
		WithGitHubToken(ctx).
		WithTableSecrets(ctx).
		WithArsenal().
		WithSkillFilter().
		WithGitSync()

	// Optional capabilities
	if k.Spec.Capabilities != nil && k.Spec.Capabilities.Browser {
		builder.WithBrowser()
	}

	return builder.Build(ctx)
}

func (r *KnightReconciler) updateStatus(ctx context.Context, knight *aiv1alpha1.Knight, scaling knightpkg.Scaling, reconcileErr error) error {
	// Check deployment readiness — prefer RuntimeBackend if available
	backend := r.runtimeBackendFor(knight)
	var isReady bool
	deploy := &appsv1.Deployment{}
	deployFound := false
	if runtimeName(knight) == runtimeDeployment {
		if err := r.Get(ctx, types.NamespacedName{Name: knight.Name, Namespace: knight.Namespace}, deploy); err == nil {
			deployFound = true
		}
	}
	if backend != nil {
		ready, err := backend.IsReady(ctx, knight)
		if err == nil {
			isReady = ready
		}
	} else if deployFound {
		isReady = deploy.Status.ReadyReplicas > 0
	}
	knight.Status.Replicas, knight.Status.ReadyReplicas = 0, 0
	if deployFound {
		knight.Status.Replicas = deploy.Status.Replicas
		knight.Status.ReadyReplicas = deploy.Status.ReadyReplicas
	}
	knight.Status.Scaling = r.scalingStatus(ctx, knight, scaling, deploy, deployFound)
	// KEDA scaled an idle knight to zero: report it as suspended, not as
	// stuck provisioning. It wakes when a task is pending on its consumer.
	atZero := scaling.Autoscaled && deployFound && !isReady &&
		deploy.Spec.Replicas != nil && *deploy.Spec.Replicas == 0
	// Waking: work is queued for a knight that is at zero, or whose pod is
	// still starting after being at zero. Phase stays Suspended; the
	// Available condition reason distinguishes it from idle ScaledToZero.
	wasAsleep := knight.Status.Phase == aiv1alpha1.KnightPhaseSuspended
	scaledToZero := atZero
	waking := false
	if scaling.Autoscaled && deployFound && !isReady && reconcileErr == nil &&
		(atZero || (wasAsleep && deploy.Status.ReadyReplicas == 0)) {
		scaledToZero = true
		waking = r.hasPendingTasks(ctx, knight)
	}

	if reconcileErr != nil {
		r.Recorder.Eventf(knight, corev1.EventTypeWarning, "ReconcileFailed", "Reconciliation failed: %v", reconcileErr)
		knight.Status.Phase = aiv1alpha1.KnightPhaseDegraded
		knight.Status.Ready = false
		meta.SetStatusCondition(&knight.Status.Conditions, metav1.Condition{
			Type:               aiv1alpha1.ConditionKnightAvailable,
			Status:             metav1.ConditionFalse,
			Reason:             aiv1alpha1.ReasonKnightReconcileError,
			Message:            reconcileErr.Error(),
			ObservedGeneration: knight.Generation,
		})
	} else if scaledToZero {
		knight.Status.Phase = aiv1alpha1.KnightPhaseSuspended
		knight.Status.Ready = false
		reason := aiv1alpha1.ReasonKnightScaledToZero
		msg := fmt.Sprintf("KEDA scaled the knight to zero; it wakes when tasks are pending on consumer %s",
			knightpkg.TaskConsumerName(knight))
		if waking {
			reason = aiv1alpha1.ReasonKnightWaking
			msg = fmt.Sprintf("Tasks are queued on consumer %s; waiting for the knight pod to start",
				knightpkg.TaskConsumerName(knight))
		}
		meta.SetStatusCondition(&knight.Status.Conditions, metav1.Condition{
			Type:               aiv1alpha1.ConditionKnightAvailable,
			Status:             metav1.ConditionFalse,
			Reason:             reason,
			Message:            msg,
			ObservedGeneration: knight.Generation,
		})
	} else if isReady {
		// Record event when transitioning to Ready (avoid duplicate events)
		if knight.Status.Phase != aiv1alpha1.KnightPhaseReady {
			r.Recorder.Event(knight, corev1.EventTypeNormal, "Ready", "Knight is ready and accepting tasks")
		}
		knight.Status.Phase = aiv1alpha1.KnightPhaseReady
		knight.Status.Ready = true
		meta.SetStatusCondition(&knight.Status.Conditions, metav1.Condition{
			Type:               aiv1alpha1.ConditionKnightAvailable,
			Status:             metav1.ConditionTrue,
			Reason:             aiv1alpha1.ReasonKnightReady,
			Message:            fmt.Sprintf("Knight %s is ready and accepting tasks", knight.Name),
			ObservedGeneration: knight.Generation,
		})
	} else {
		knight.Status.Phase = aiv1alpha1.KnightPhaseProvisioning
		knight.Status.Ready = false
		meta.SetStatusCondition(&knight.Status.Conditions, metav1.Condition{
			Type:               aiv1alpha1.ConditionKnightAvailable,
			Status:             metav1.ConditionFalse,
			Reason:             aiv1alpha1.ReasonKnightProvisioning,
			Message:            "Knight deployment is being provisioned",
			ObservedGeneration: knight.Generation,
		})
	}

	meta.SetStatusCondition(&knight.Status.Conditions, metav1.Condition{
		Type:               aiv1alpha1.ConditionKnightRuntimeAvailable,
		Status:             metav1.ConditionTrue,
		Reason:             aiv1alpha1.ReasonRuntimeBackendAvailable,
		Message:            fmt.Sprintf("Runtime backend %q is enabled", runtimeName(knight)),
		ObservedGeneration: knight.Generation,
	})

	// Set NATS consumer name in status
	consumerName := knight.Spec.NATS.ConsumerName
	if consumerName == "" {
		consumerName = fmt.Sprintf("knight-%s", knight.Name)
	}
	knight.Status.NATSConsumer = consumerName
	knight.Status.ObservedGeneration = knight.Generation

	// Update Prometheus metrics
	tableName := knight.Labels[aiv1alpha1.LabelRoundTable]
	if tableName == "" {
		tableName = "none"
	}
	// Note: KnightsTotal is a gauge. In a real aggregation, we'd track all knights,
	// but for now we set 1 for this knight's current phase. A separate aggregator
	// or the RoundTable controller should reset/recompute totals.
	rtmetrics.KnightsTotal.WithLabelValues(string(knight.Status.Phase), tableName).Set(1)

	return r.Status().Update(ctx, knight)
}

// runtimeBackendFor returns the appropriate RuntimeBackend for a Knight.
// It checks knight.Spec.Runtime against the RuntimeBackends map, falling back
// to the default RuntimeBackend, and finally to nil (inline reconciliation).
func (r *KnightReconciler) runtimeBackendFor(knight *aiv1alpha1.Knight) rtruntime.RuntimeBackend {
	if r.RuntimeBackends != nil && knight.Spec.Runtime != "" {
		if backend, ok := r.RuntimeBackends[knight.Spec.Runtime]; ok {
			return backend
		}
	}
	return r.RuntimeBackend
}

// runtimeName returns the effective spec.runtime (empty means deployment).
func runtimeName(knight *aiv1alpha1.Knight) string {
	if knight.Spec.Runtime == "" {
		return runtimeDeployment
	}
	return knight.Spec.Runtime
}

// SetupWithManager sets up the controller with the Manager.
// Sandbox objects are only watched when SandboxEnabled is true: a watch on a
// kind the API server doesn't serve never syncs, and the manager exits.
func (r *KnightReconciler) SetupWithManager(mgr ctrl.Manager) error {
	b := ctrl.NewControllerManagedBy(mgr).
		For(&aiv1alpha1.Knight{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&batchv1.Job{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		// Plan-task cap: a knight's share depends on its table and siblings.
		Watches(&aiv1alpha1.RoundTable{},
			handler.EnqueueRequestsFromMapFunc(r.roundTableKnightRequests),
			ctrlbuilder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(&aiv1alpha1.Knight{},
			handler.EnqueueRequestsFromMapFunc(r.planSiblingRequests),
			ctrlbuilder.WithPredicates(predicate.GenerationChangedPredicate{}))
	if r.SandboxEnabled {
		b = b.Owns(rtruntime.NewSandboxObject("", ""))
	}
	return b.Named("knight").Complete(r)
}

// deriveResultsPrefix is a re-export from the knight package for backward compatibility with tests.
func deriveResultsPrefix(subjects []string) string {
	return knightpkg.DeriveResultsPrefix(subjects)
}
