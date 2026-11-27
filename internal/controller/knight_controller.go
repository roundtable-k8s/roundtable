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
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	networkingv1 "k8s.io/api/networking/v1"
	nodev1 "k8s.io/api/node/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
	missionpkg "github.com/roundtable-k8s/roundtable/internal/mission"
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
	// wakeBackoff holds per-knight probe state after a WakeTimeout.
	wakeBackoff sync.Map
	Scheme      *runtime.Scheme
	Recorder    record.EventRecorder
	// BrowserImage is the browser sidecar image (KNIGHT_BROWSER_IMAGE); empty uses the built-in pinned default.
	BrowserImage string
	DefaultImage string // Default pi-knight image (set via DEFAULT_KNIGHT_IMAGE env var)

	// APIReader reads Pods uncached, so the operator keeps no cluster-wide
	// Pod informer just to tell host-network endpoints apart. Optional;
	// without it Pods are read through Client.
	APIReader client.Reader

	// serviceIndexed is set once the egress field indexes are registered
	// (SetupWithManager); without it Service events list every Knight.
	serviceIndexed bool
	// hostNetPods caches pod UID -> spec.hostNetwork (immutable), so the
	// steady-state reconcile does no Pod read. Bounded by hostNetPodCacheMax.
	hostNetPods     sync.Map
	hostNetPodCount atomic.Int64
	hostNetPodSeq   atomic.Int64

	// KnightSecurity is the pod-level security context applied to both knight
	// Deployments and Nix build Jobs. Chart-driven (KNIGHT_* env vars); zero
	// value falls back to DefaultPodSecurity.
	KnightSecurity knightpkg.PodSecurity

	// CardsBucket is the agent cards KV bucket (#393): passed to knight pods
	// as KNIGHT_CARDS_BUCKET and cleaned up when a knight is deleted. Empty
	// means natspkg.DefaultCardsBucket.
	CardsBucket string

	// NATS is the shared NATS provider used to read task consumer backlog for
	// the Waking status. Optional; without it scaled-to-zero knights report
	// ScaledToZero only.
	NATS *natspkg.Provider

	// PendingSource overrides the consumer backlog lookup (tests).
	PendingSource ConsumerPendingSource

	// Consumers overrides the NATS client the knight's durable task consumer
	// is created, adopted and deleted with (tests). Nil uses NATS; without
	// either the operator leaves consumers to the runtimes.
	Consumers KnightConsumerClient

	// Now overrides the clock used for wake-latency measurement (tests).
	Now func() time.Time

	// RuntimeBackend abstracts the lifecycle of Knight runtime resources.
	// When set, the controller delegates Deployment reconciliation and
	// suspend/resume to this backend. When nil, falls back to the
	// inline reconcileDeployment / reconcileSuspended methods.
	RuntimeBackend rtruntime.RuntimeBackend

	// RuntimeBackends maps runtime type names to their backend implementations.
	// The controller selects the backend based on knight.Spec.Runtime.
	// If nil or the key is missing, falls back to RuntimeBackend.
	RuntimeBackends map[string]rtruntime.RuntimeBackend

	// MaxConcurrentRollouts limits how many knights of a namespace roll out a
	// changed pod template at once. 0 (default) means unlimited.
	MaxConcurrentRollouts int

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

	// CiliumEnabled reports whether cilium.io/v2 CiliumNetworkPolicy is
	// served. Knights with spec.egress then get a CiliumNetworkPolicy with
	// hostname rules; otherwise a NetworkPolicy without them and
	// EgressRestricted=False (FQDNPolicyUnavailable).
	CiliumEnabled bool

	// SnapshotsEnabled reports whether snapshot.storage.k8s.io/v1
	// VolumeSnapshot is served. When false the ai.roundtable.io/snapshot
	// annotation only sets WorkspaceSnapshots=False.
	SnapshotsEnabled bool

	// SnapshotAPIProbe re-detects the VolumeSnapshot API after startup. It is
	// consulted only while SnapshotsEnabled is false and a snapshot or restore
	// was requested, at most once per snapshotProbeInterval, so installing the
	// snapshot controller later works without an operator restart. Nil
	// disables the recheck.
	SnapshotAPIProbe func() (bool, error)

	snapProbeMu   sync.Mutex
	snapLastProbe time.Time
	snapLate      bool

	// EgressNetPol configures the DNS and NATS peers of knight egress
	// policies (the chart's missionNetworkPolicy values, shared with mission
	// NetworkPolicies). Nil uses the defaults.
	EgressNetPol *missionpkg.NetPolSettings

	// Isolation are the operator's isolation defaults (default RuntimeClass,
	// default-deny egress, base hosts), read from KNIGHT_DEFAULT_RUNTIME_CLASS,
	// KNIGHT_DEFAULT_EGRESS and KNIGHT_EGRESS_BASE_HOSTS. The zero value turns
	// them off.
	Isolation knightpkg.IsolationSettings

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
// +kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshots,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups=node.k8s.io,resources=runtimeclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups=agents.x-k8s.io,resources=sandboxes;sandboxes/status,verbs=get;list;watch;create;update;patch;delete

func (r *KnightReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	res, err := r.reconcile(ctx, req)
	return quietConflict(ctx, res, err)
}

func (r *KnightReconciler) reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Count table-less knights (roundtable_knights{table="none"}); the
	// RoundTable reconciler only counts knights of a table.
	defer updateUnownedKnightMetrics(ctx, r.Client)

	// Fetch the Knight resource
	knight := &aiv1alpha1.Knight{}
	if err := r.Get(ctx, req.NamespacedName, knight); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Knight resource not found — likely deleted")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if table := knightpkg.KnightTable(knight); table != "" {
		log = log.WithValues("table", table)
		ctx = logf.IntoContext(ctx, log)
	}

	// Handle deletion via finalizer
	if knight.DeletionTimestamp != nil {
		if controllerutil.ContainsFinalizer(knight, knightFinalizer) {
			log.Info("Cleaning up knight resources", "knight", knight.Name)
			// The task consumer goes first; retried for a bounded time.
			if done, retry := r.finalizeConsumer(ctx, knight); !done {
				return ctrl.Result{RequeueAfter: retry}, nil
			}
			deleteConsumerMetric(knight)
			// Best effort: the RoundTable reconcile also drops the card (#393).
			if err := deleteKnightCards(ctx, r.Client, r.NATS, r.CardsBucket, knight); err != nil {
				log.Error(err, "Failed to delete agent cards")
			}
			deleteIsolatedMetric(knight)
			deleteKnightWakeMetric(knight)
			// Merge patch, like the add path: never rewrites other fields.
			base := knight.DeepCopy()
			controllerutil.RemoveFinalizer(knight, knightFinalizer)
			if err := r.Patch(ctx, knight, client.MergeFrom(base)); err != nil {
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
	rcSource := runtimeClassFromKnight
	if knight.Spec.RuntimeClassName == nil {
		rcSource = runtimeClassFromTable
	}
	knightpkg.ApplyTableDefaults(ctx, r.Client, knight)
	if rcSource == runtimeClassFromTable && knight.Spec.RuntimeClassName == nil {
		rcSource = runtimeClassFromOperator
	}
	// Then the operator's isolation defaults and the knight's opt-out.
	knightpkg.ApplyIsolationDefaults(knight, r.Isolation)
	// A changed model is a status change even when the knight is parked.
	modelChanged := knight.Status.Model != knight.Spec.Model
	knight.Status.Model = knight.Spec.Model

	// The durable task consumer exists whatever the runtime state (parked,
	// suspended, scaled to zero), so tasks queue and KEDA can wake the knight.
	// A failure sets ConsumerReady=False and retries; it does not fail the
	// rest of the reconcile.
	consumerChanged, consumerErr := r.reconcileConsumer(ctx, knight)
	consumerChanged = consumerChanged || modelChanged
	if consumerErr != nil {
		log.Info("Knight consumer not ready", "error", consumerErr.Error())
	}

	// A sandbox knight without the sandbox backend can't be reconciled. Park it
	// with a clear condition and leave any existing runtime resources alone.
	if knight.Spec.Runtime == runtimeSandbox && !r.SandboxEnabled {
		return retryConsumer(consumerErr)(r.reconcileRuntimeUnavailable(ctx, knight, consumerChanged))
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
			return retryConsumer(consumerErr)(ctrl.Result{}, r.finishSuspended(ctx, knight))
		}
		return retryConsumer(consumerErr)(ctrl.Result{}, r.reconcileSuspended(ctx, knight))
	}

	// (After the suspend step, so a parked knight can still be suspended.)
	// A knight whose RuntimeClass doesn't exist would only produce pods the
	// API server rejects; park it with a clear condition instead (fail closed:
	// a missing gvisor never silently runs the knight unsandboxed).
	if name := runtimeClassOf(knight); name != "" {
		if err := r.Get(ctx, types.NamespacedName{Name: name}, &nodev1.RuntimeClass{}); err != nil {
			if !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
			msg := runtimeClassNotFoundMessage(name, rcSource)
			changed := r.setIsolated(knight, metav1.ConditionFalse, aiv1alpha1.ReasonRuntimeClassNotFound, msg)
			return retryConsumer(consumerErr)(
				r.parkKnightWith(ctx, knight, aiv1alpha1.ReasonRuntimeClassNotFound, msg, changed || consumerChanged))
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
	var pvcRequeue time.Duration
	if d, err := r.reconcilePVC(ctx, knight); err != nil {
		reconcileErr = err
		log.Error(err, "Failed to reconcile PVC")
	} else {
		pvcRequeue = d
	}

	// 2a. Memory PVC (spec.memory) and the MemoryShared condition.
	if err := r.reconcileMemory(ctx, knight); err != nil {
		reconcileErr = err
		log.Error(err, "Failed to reconcile memory PVC")
	}

	// 2a'. Workspace VolumeSnapshots (annotation- or schedule-triggered).
	snapRequeue, err := r.reconcileSnapshots(ctx, knight)
	if err != nil {
		reconcileErr = err
		log.Error(err, "Failed to reconcile workspace snapshots")
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

	// 2c. Egress policy from spec.egress, ahead of the runtime so new pods
	//     start restricted.
	if err := r.reconcileEgress(ctx, knight); err != nil {
		reconcileErr = err
		log.Error(err, "Failed to reconcile egress policy")
	}

	// 2d. Isolated condition and metric from the RuntimeClass and egress.
	r.reconcileIsolated(knight)

	rolloutDeferred := false

	// 3. Runtime (Deployment or Sandbox, depending on knight.Spec.Runtime).
	//    Held while the tools image for the current spec is unpublished, so the
	//    pod doesn't roll onto a spec that expects tools its image lacks.
	if hold, err := r.holdRollout(ctx, knight); err != nil {
		reconcileErr = err
		log.Error(err, "Failed to check tools image rollout hold")
	} else if hold {
		log.Info("Holding pod rollout until the tools image is published", "image", knightpkg.ToolsImageRef(knight))
	} else {
		var err error
		if backend != nil {
			err = backend.Reconcile(ctx, knight)
		} else {
			err = r.reconcileDeployment(ctx, knight)
		}
		switch {
		case errors.Is(err, rtruntime.ErrRolloutWaitingForIdle):
			rolloutDeferred = true
			log.Info("Holding pod rollout until the knight is idle", "reason", err.Error())
			setRolloutPending(knight, aiv1alpha1.ReasonWaitingForIdle, r.now(),
				"Waiting for the knight's queued and in-flight tasks to finish (spec.lifecycle.rollout whenIdle)")
		case errors.Is(err, rtruntime.ErrRolloutDeferred):
			rolloutDeferred = true
			log.Info("Deferring pod rollout", "reason", err.Error())
			setRolloutPending(knight, aiv1alpha1.ReasonWaitingForOtherRollouts, r.now(),
				fmt.Sprintf("Waiting for other knight rollouts to finish (limit %d concurrent)",
					r.MaxConcurrentRollouts))
		case err != nil:
			reconcileErr = err
			log.Error(err, "Failed to reconcile runtime", "backend", knight.Spec.Runtime)
		}
	}
	if !rolloutDeferred {
		meta.RemoveStatusCondition(&knight.Status.Conditions, aiv1alpha1.ConditionKnightRolloutPending)
	}
	if runtimeName(knight) != runtimeDeployment {
		meta.RemoveStatusCondition(&knight.Status.Conditions, aiv1alpha1.ConditionKnightRollingUpdate)
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

	// 5. Secret keys behind spec.mcpServers[].headersFrom. Secrets are read
	//    uncached and not watched, so poll while one is missing.
	mcpHeadersPending := r.setMCPHeadersCondition(ctx, knight)

	// Update status based on reconciliation results
	if err := r.updateStatus(ctx, knight, plan.Scaling, reconcileErr); err != nil {
		if apierrors.IsConflict(err) {
			// Returned as is: Reconcile wraps this function in quietConflict,
			// which turns the conflict into a quiet requeue (no error).
			return ctrl.Result{}, err
		}
		log.Error(err, "Failed to update status")
		return ctrl.Result{}, err
	}

	if reconcileErr != nil {
		return ctrl.Result{RequeueAfter: RequeueSlow}, reconcileErr
	}

	// Waking: poll so the transition to Ready is visible without a pod event.
	if c := meta.FindStatusCondition(knight.Status.Conditions, aiv1alpha1.ConditionKnightAvailable); c != nil &&
		(c.Reason == aiv1alpha1.ReasonKnightWaking || c.Reason == aiv1alpha1.ReasonKnightWakeTimeout) {
		d := wakingRequeue
		if c.Reason == aiv1alpha1.ReasonKnightWakeTimeout {
			d = r.wakeBackoffDelay(client.ObjectKeyFromObject(knight))
		}
		if nixRequeue == 0 || d < nixRequeue {
			nixRequeue = d
		}
	}

	if (mcpHeadersPending || consumerErr != nil) && (nixRequeue == 0 || RequeueSlow < nixRequeue) {
		nixRequeue = RequeueSlow
	}

	if rolloutDeferred && (nixRequeue == 0 || RequeueSlow < nixRequeue) {
		nixRequeue = RequeueSlow
	}

	if pvcRequeue > 0 && (nixRequeue == 0 || pvcRequeue < nixRequeue) {
		nixRequeue = pvcRequeue
	}

	if snapRequeue > 0 && (nixRequeue == 0 || snapRequeue < nixRequeue) {
		nixRequeue = snapRequeue
	}

	if nixRequeue > 0 {
		return ctrl.Result{RequeueAfter: nixRequeue}, nil
	}

	return ctrl.Result{}, nil
}

// now returns the reconciler clock (Now if set, else time.Now).
func (r *KnightReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// setRolloutPending sets the RolloutPending condition. A change of reason
// restarts LastTransitionTime. The whenIdle maximum wait does not rely on it
// across reasons: the deployment backend remembers the wait start per spec hash.
func setRolloutPending(knight *aiv1alpha1.Knight, reason string, now time.Time, msg string) {
	if c := meta.FindStatusCondition(knight.Status.Conditions, aiv1alpha1.ConditionKnightRolloutPending); c != nil &&
		c.Reason != reason {
		meta.RemoveStatusCondition(&knight.Status.Conditions, aiv1alpha1.ConditionKnightRolloutPending)
	}
	meta.SetStatusCondition(&knight.Status.Conditions, metav1.Condition{
		Type:               aiv1alpha1.ConditionKnightRolloutPending,
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: knight.Generation,
		LastTransitionTime: metav1.NewTime(now),
	})
}

// wakingRequeue is how often a waking knight is re-checked.
const wakingRequeue = 5 * time.Second

// wakeBackoffSteps is the requeue delay after a WakeTimeout, per probe.
// The last step repeats.
var wakeBackoffSteps = []time.Duration{15 * time.Second, 30 * time.Second, time.Minute}

type wakeBackoffState struct {
	attempt int
	next    time.Time
}

func (r *KnightReconciler) wakeBackoffStep(attempt int) time.Duration {
	if attempt >= len(wakeBackoffSteps) {
		attempt = len(wakeBackoffSteps) - 1
	}
	return wakeBackoffSteps[attempt]
}

// wakeBackingOff reports whether the next pending-task probe of a timed-out
// knight is still in the future.
func (r *KnightReconciler) wakeBackingOff(key client.ObjectKey) bool {
	v, ok := r.wakeBackoff.Load(key)
	return ok && r.now().Before(v.(wakeBackoffState).next)
}

// advanceWakeBackoff records a probe and schedules the next one further out.
func (r *KnightReconciler) advanceWakeBackoff(key client.ObjectKey) {
	st := wakeBackoffState{}
	if v, ok := r.wakeBackoff.Load(key); ok {
		st = v.(wakeBackoffState)
	}
	st.next = r.now().Add(r.wakeBackoffStep(st.attempt))
	st.attempt++
	r.wakeBackoff.Store(key, st)
}

// wakeBackoffDelay is the requeue delay for a timed-out knight: the time to
// its next probe, bounded by the backoff steps and never below wakingRequeue.
func (r *KnightReconciler) wakeBackoffDelay(key client.ObjectKey) time.Duration {
	v, ok := r.wakeBackoff.Load(key)
	if !ok {
		return wakeBackoffSteps[0]
	}
	d := v.(wakeBackoffState).next.Sub(r.now())
	d = max(d, wakingRequeue)
	return min(d, wakeBackoffSteps[len(wakeBackoffSteps)-1])
}

// knightWakeTimeout caps how long a knight reports Waking: past it the pod
// is considered stuck and the knight reports Degraded (WakeTimeout).
const knightWakeTimeout = 5 * time.Minute

// ConsumerPendingSource reports how many tasks are queued or in flight on a
// JetStream consumer (num_pending + num_ack_pending).
type ConsumerPendingSource interface {
	ConsumerPending(ctx context.Context, stream, consumer string) (uint64, error)
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
	n, err := src.ConsumerPending(ctx, knight.Spec.NATS.Stream, knightpkg.TaskConsumerName(knight))
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
func (r *KnightReconciler) reconcileRuntimeUnavailable(
	ctx context.Context, knight *aiv1alpha1.Knight, changed bool,
) (ctrl.Result, error) {
	msg := fmt.Sprintf("Knight requests runtime %q, but the operator started without the sandbox backend: "+
		"%s Sandbox is not served by the API server (or --enable-sandbox-backend=false). "+
		"Install agent-sandbox v1.0.0 or later and restart the operator, or set spec.runtime to \"deployment\".",
		knight.Spec.Runtime, rtruntime.SandboxGVK.GroupVersion())

	return r.parkKnightWith(ctx, knight, aiv1alpha1.ReasonSandboxUnavailable, msg, changed)
}

// runtimeClassOf returns the knight's effective RuntimeClass name ("" if none).
func runtimeClassOf(knight *aiv1alpha1.Knight) string {
	if knight.Spec.RuntimeClassName == nil {
		return ""
	}
	return *knight.Spec.RuntimeClassName
}

// parkKnightWith marks a knight RuntimeAvailable=False / Available=False
// with the given reason, leaves its runtime resources alone and requeues
// slowly. The status is written when it changes or when the caller already
// changed it (changed).
func (r *KnightReconciler) parkKnightWith(ctx context.Context, knight *aiv1alpha1.Knight, reason, msg string, changed bool) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	prevRT := meta.FindStatusCondition(knight.Status.Conditions, aiv1alpha1.ConditionKnightRuntimeAvailable)
	prevAv := meta.FindStatusCondition(knight.Status.Conditions, aiv1alpha1.ConditionKnightAvailable)
	needsEvent := prevRT == nil || prevRT.Reason != reason || prevRT.Message != msg ||
		prevAv == nil || prevAv.Reason != reason || prevAv.Message != msg
	changed = meta.SetStatusCondition(&knight.Status.Conditions, metav1.Condition{
		Type:               aiv1alpha1.ConditionKnightRuntimeAvailable,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: knight.Generation,
	}) || changed
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
	if knight.Status.Phase != aiv1alpha1.KnightPhaseDegraded || knight.Status.Ready {
		needsEvent = true
	}

	if changed || needsEvent {
		// The Warning is raised only when the reason or message changed,
		// not when the caller only changed e.g. the model.
		if needsEvent {
			log.Info("Runtime unavailable; knight parked", "knight", knight.Name, "reason", reason)
			// event-reasons: RuntimeClassNotFound SandboxUnavailable
			r.Recorder.Event(knight, corev1.EventTypeWarning, reason, msg)
		}
		knight.Status.Phase = aiv1alpha1.KnightPhaseDegraded
		knight.Status.Ready = false
		knight.Status.ObservedGeneration = knight.Generation
		if err := r.Status().Update(ctx, knight); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{RequeueAfter: RequeueVerySlow}, nil
}

// setRuntimeAvailable sets RuntimeAvailable=True for the knight's backend.
func (r *KnightReconciler) setRuntimeAvailable(knight *aiv1alpha1.Knight) {
	meta.SetStatusCondition(&knight.Status.Conditions, metav1.Condition{
		Type:               aiv1alpha1.ConditionKnightRuntimeAvailable,
		Status:             metav1.ConditionTrue,
		Reason:             aiv1alpha1.ReasonRuntimeBackendAvailable,
		Message:            fmt.Sprintf("Runtime backend %q is enabled", runtimeName(knight)),
		ObservedGeneration: knight.Generation,
	})
}

// reconcileSuspended scales the deployment to 0 and updates status.
func (r *KnightReconciler) reconcileSuspended(ctx context.Context, knight *aiv1alpha1.Knight) error {
	log := logf.FromContext(ctx)

	deploy := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: knight.Name, Namespace: knight.Namespace}, deploy)
	if err == nil {
		zero := int32(0)
		if deploy.Spec.Replicas == nil || *deploy.Spec.Replicas != zero {
			deploy.Spec.Replicas = &zero
			if err := r.Update(ctx, deploy); err != nil {
				return err
			}
			log.Info("Suspended knight — scaled to 0", "knight", knight.Name)
			r.Recorder.Event(knight, corev1.EventTypeNormal, "Suspended", "Knight suspended")
		}
	}

	knight.Status.Phase = aiv1alpha1.KnightPhaseSuspended
	knight.Status.Ready = false
	// A suspended knight is not parked: drop a stale RuntimeAvailable=False.
	r.setRuntimeAvailable(knight)
	// A suspended knight rolls nothing: drop a stale RolloutPending.
	meta.RemoveStatusCondition(&knight.Status.Conditions, aiv1alpha1.ConditionKnightRolloutPending)
	meta.SetStatusCondition(&knight.Status.Conditions, metav1.Condition{
		Type:               aiv1alpha1.ConditionKnightAvailable,
		Status:             metav1.ConditionFalse,
		Reason:             aiv1alpha1.ReasonKnightSuspended,
		Message:            "Knight is suspended",
		ObservedGeneration: knight.Generation,
	})
	knight.Status.ObservedGeneration = knight.Generation
	if err := r.Status().Update(ctx, knight); err != nil {
		return err
	}

	return nil
}

// finishSuspended updates the Knight status after the RuntimeBackend has suspended it.
func (r *KnightReconciler) finishSuspended(ctx context.Context, knight *aiv1alpha1.Knight) error {
	knight.Status.Phase = aiv1alpha1.KnightPhaseSuspended
	knight.Status.Ready = false
	// A suspended knight is not parked: drop a stale RuntimeAvailable=False.
	r.setRuntimeAvailable(knight)
	// A suspended knight rolls nothing: drop a stale RolloutPending.
	meta.RemoveStatusCondition(&knight.Status.Conditions, aiv1alpha1.ConditionKnightRolloutPending)
	meta.SetStatusCondition(&knight.Status.Conditions, metav1.Condition{
		Type:               aiv1alpha1.ConditionKnightAvailable,
		Status:             metav1.ConditionFalse,
		Reason:             aiv1alpha1.ReasonKnightSuspended,
		Message:            "Knight is suspended",
		ObservedGeneration: knight.Generation,
	})
	knight.Status.ObservedGeneration = knight.Generation
	if err := r.Status().Update(ctx, knight); err != nil {
		return err
	}
	return nil
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

		// mise.toml and apt.txt were removed with spec.tools.apt/mise.
		delete(cm.Data, "mise.toml")
		delete(cm.Data, "apt.txt")

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
func (r *KnightReconciler) reconcilePVC(ctx context.Context, knight *aiv1alpha1.Knight) (time.Duration, error) {
	if knightpkg.EphemeralWorkspace(knight) {
		return 0, nil
	}
	// Workspace PVC — skip if using an existing claim (migration mode)
	if knight.Spec.Workspace != nil && knight.Spec.Workspace.ExistingClaim != "" {
		logf.FromContext(ctx).Info("Using existing PVC", "claim", knight.Spec.Workspace.ExistingClaim)
		return 0, nil
	}
	return r.ensureWorkspacePVC(ctx, knight)
}

// ensureWorkspacePVC creates a new workspace PVC if one doesn't exist. With
// spec.workspace.fromSnapshot the PVC is cloned from that VolumeSnapshot, and
// isn't created until the snapshot is ready (the returned duration is how
// long to wait before looking again).
func (r *KnightReconciler) ensureWorkspacePVC(ctx context.Context, knight *aiv1alpha1.Knight) (time.Duration, error) {
	pvcName := knight.Name
	pvc := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: knight.Namespace}, pvc)
	fromSnapshot := ""
	if w := knight.Spec.Workspace; w != nil {
		fromSnapshot = w.FromSnapshot
	}

	if apierrors.IsNotFound(err) {
		storageSize := resource.MustParse("1Gi")
		if knight.Spec.Workspace != nil && knight.Spec.Workspace.Size != nil && !knight.Spec.Workspace.Size.IsZero() {
			storageSize = knight.Spec.Workspace.Size.DeepCopy()
		}

		var dataSource *corev1.TypedObjectReference
		if fromSnapshot != "" {
			restoreSize, wait, err := r.checkWorkspaceSnapshot(ctx, knight, fromSnapshot)
			if err != nil {
				return 0, err
			}
			if wait {
				return RequeueSlow, nil
			}
			if restoreSize != nil && restoreSize.Cmp(storageSize) > 0 {
				storageSize = restoreSize.DeepCopy()
			}
			apiGroup := rtruntime.VolumeSnapshotGVK.Group
			dataSource = &corev1.TypedObjectReference{APIGroup: &apiGroup, Kind: rtruntime.VolumeSnapshotGVK.Kind, Name: fromSnapshot}
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
				DataSourceRef: dataSource,
			},
		}
		if err := controllerutil.SetControllerReference(knight, pvc, r.Scheme); err != nil {
			return 0, err
		}
		if err := r.Create(ctx, pvc); err != nil {
			return 0, fmt.Errorf("PVC create failed: %w", err)
		}
		logf.FromContext(ctx).Info("PVC created", "name", pvcName, "fromSnapshot", fromSnapshot)
		if fromSnapshot != "" {
			r.setWorkspaceReady(knight, metav1.ConditionTrue, aiv1alpha1.ReasonRestoredFromSnapshot,
				fmt.Sprintf("workspace PVC created from VolumeSnapshot %q", fromSnapshot))
		} else {
			meta.RemoveStatusCondition(&knight.Status.Conditions, aiv1alpha1.ConditionKnightWorkspaceReady)
		}
		return 0, nil
	} else if err != nil {
		return 0, fmt.Errorf("PVC get failed: %w", err)
	}

	// The PVC exists; a data source only applies at creation.
	switch ds := pvc.Spec.DataSourceRef; {
	case fromSnapshot == "":
		meta.RemoveStatusCondition(&knight.Status.Conditions, aiv1alpha1.ConditionKnightWorkspaceReady)
	case ds != nil && ds.Kind == rtruntime.VolumeSnapshotGVK.Kind && ds.Name == fromSnapshot:
		r.setWorkspaceReady(knight, metav1.ConditionTrue, aiv1alpha1.ReasonRestoredFromSnapshot,
			fmt.Sprintf("workspace PVC created from VolumeSnapshot %q", fromSnapshot))
	default:
		r.setWorkspaceReady(knight, metav1.ConditionTrue, aiv1alpha1.ReasonFromSnapshotIgnored,
			fmt.Sprintf("workspace PVC %q already exists; fromSnapshot %q only applies when the PVC is created", pvcName, fromSnapshot))
	}
	return 0, nil
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
	b.MaxConcurrentRollouts = r.MaxConcurrentRollouts
	if r.APIReader != nil {
		b.Reader = r.APIReader // uncached: a cold cache must not hide mid-rollout knights
	}

	b.Busy = r.hasPendingTasks
	b.Now = r.now
	return b
}

// BuildDeploymentSpec constructs the full DeploymentSpec for a Knight.
// Exported so it can be passed as a PodSpecBuilder to RuntimeBackend.
//
// Replicas is the fixed replica count, or the initial count when KEDA owns
// it. When every PVC the pod mounts can be mounted by two pods (see
// knightpkg.RecreateReason) the Deployment rolls with maxSurge 1 /
// maxUnavailable 0 so the old pod drains in-flight tasks while the new one
// starts; otherwise it keeps Recreate and the RollingUpdate condition names
// the volume. terminationGracePeriodSeconds is taskTimeoutSeconds+60.
func (r *KnightReconciler) BuildDeploymentSpec(ctx context.Context, knight *aiv1alpha1.Knight) appsv1.DeploymentSpec {
	labels := knightLabels(knight)
	replicas := r.scalingOrFallback(ctx, knight).InitialReplicas()
	podSpec := r.BuildDrainablePodSpec(ctx, knight)
	strategy, recreateReason := knightpkg.RolloutStrategy(knight, &podSpec, r.claimAccessModes(ctx, knight, &podSpec))
	setRollingUpdateCondition(knight, recreateReason)
	// The table label goes on the pod template only: the selector is immutable.
	podLabels := maps.Clone(labels)
	if table := knightpkg.KnightTable(knight); table != "" {
		podLabels[aiv1alpha1.LabelRoundTable] = table
	}
	return appsv1.DeploymentSpec{
		Replicas: &replicas,
		Strategy: strategy,
		Selector: &metav1.LabelSelector{
			MatchLabels: labels,
		},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: podLabels,
			},
			Spec: podSpec,
		},
	}
}

// claimAccessModes reads spec.accessModes of every PVC the pod spec mounts.
// A claim that can't be read is left out, so RecreateReason treats its
// access modes as unknown and keeps Recreate.
func (r *KnightReconciler) claimAccessModes(ctx context.Context, knight *aiv1alpha1.Knight, spec *corev1.PodSpec) knightpkg.ClaimAccessModes {
	modes := knightpkg.ClaimAccessModes{}
	if r.Client == nil {
		return modes
	}
	for _, claim := range knightpkg.PodClaims(spec) {
		pvc := &corev1.PersistentVolumeClaim{}
		if err := r.Get(ctx, types.NamespacedName{Name: claim, Namespace: knight.Namespace}, pvc); err != nil {
			if !apierrors.IsNotFound(err) {
				logf.FromContext(ctx).Error(err, "Failed to read PVC access modes", "pvc", claim)
			}
			continue
		}
		modes[claim] = pvc.Spec.AccessModes
	}
	return modes
}

// setRollingUpdateCondition records the rollout strategy on the Knight:
// True when it rolls with RollingUpdate, False with the volume that forced
// Recreate.
func setRollingUpdateCondition(knight *aiv1alpha1.Knight, recreateReason string) {
	cond := metav1.Condition{
		Type:               aiv1alpha1.ConditionKnightRollingUpdate,
		Status:             metav1.ConditionTrue,
		Reason:             aiv1alpha1.ReasonVolumesShareable,
		Message:            "Rolls with maxSurge 1 / maxUnavailable 0: every mounted volume allows two pods",
		ObservedGeneration: knight.Generation,
	}
	if recreateReason != "" {
		cond.Status = metav1.ConditionFalse
		cond.Reason = aiv1alpha1.ReasonVolumeNotShareable
		cond.Message = "Recreate strategy: " + recreateReason
	}
	meta.SetStatusCondition(&knight.Status.Conditions, cond)
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
		WithCardsBucket(r.CardsBucket).
		WithBrowserImage(r.BrowserImage).
		WithReader(r.Client).
		WithWorkspace().
		WithMemory().
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
	// A knight that already timed out waking is Degraded, not Suspended; it
	// still counts as asleep so a never-ready pod at 1 replica keeps WakeTimeout.
	prevAvail := meta.FindStatusCondition(knight.Status.Conditions, aiv1alpha1.ConditionKnightAvailable)
	wasTimedOut := prevAvail != nil && prevAvail.Reason == aiv1alpha1.ReasonKnightWakeTimeout
	// An empty queue with a starting pod is Provisioning, not ScaledToZero.
	waking := false
	key := client.ObjectKeyFromObject(knight)
	if scaling.Autoscaled && deployFound && !isReady && reconcileErr == nil &&
		(atZero || ((wasAsleep || wasTimedOut) && deploy.Status.ReadyReplicas == 0)) {
		if wasTimedOut && r.wakeBackingOff(key) {
			// Backing off after the timeout: skip the NATS lookup.
			waking = true
		} else {
			waking = r.hasPendingTasks(ctx, knight)
			if waking && wasTimedOut {
				r.advanceWakeBackoff(key)
			}
		}
	}
	if !wasTimedOut || !waking {
		r.wakeBackoff.Delete(key)
	}
	scaledToZero := atZero || waking

	// Remember when the knight entered Waking so the Waking -> Ready
	// transition can be observed as wake latency. The observation is
	// recorded only after the status update succeeds, so a conflict retry
	// (which still sees Waking) cannot record it twice.
	var wakingSince time.Time
	var observeWake bool
	if c := meta.FindStatusCondition(knight.Status.Conditions, aiv1alpha1.ConditionKnightAvailable); c != nil &&
		c.Reason == aiv1alpha1.ReasonKnightWaking {
		wakingSince = c.LastTransitionTime.Time
	}
	// Waking for too long (or already timed out and still waiting): the pod
	// is stuck, so don't stay Waking on the prior Suspended phase forever.
	wakeTimedOut := false
	if waking {
		if c := meta.FindStatusCondition(knight.Status.Conditions, aiv1alpha1.ConditionKnightAvailable); c != nil {
			wakeTimedOut = c.Reason == aiv1alpha1.ReasonKnightWakeTimeout ||
				(c.Reason == aiv1alpha1.ReasonKnightWaking && r.now().Sub(c.LastTransitionTime.Time) > knightWakeTimeout)
		}
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
	} else if wakeTimedOut {
		knight.Status.Phase = aiv1alpha1.KnightPhaseDegraded
		knight.Status.Ready = false
		meta.SetStatusCondition(&knight.Status.Conditions, metav1.Condition{
			Type:   aiv1alpha1.ConditionKnightAvailable,
			Status: metav1.ConditionFalse,
			Reason: aiv1alpha1.ReasonKnightWakeTimeout,
			Message: fmt.Sprintf("Tasks are queued on consumer %s but the knight pod did not become ready within %s",
				knightpkg.TaskConsumerName(knight), knightWakeTimeout),
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
		prev := meta.FindStatusCondition(knight.Status.Conditions, aiv1alpha1.ConditionKnightAvailable)
		enteringWaking := waking && (prev == nil || prev.Reason != aiv1alpha1.ReasonKnightWaking)
		meta.SetStatusCondition(&knight.Status.Conditions, metav1.Condition{
			Type:               aiv1alpha1.ConditionKnightAvailable,
			Status:             metav1.ConditionFalse,
			Reason:             reason,
			Message:            msg,
			ObservedGeneration: knight.Generation,
		})
		if enteringWaking {
			// SetStatusCondition keeps LastTransitionTime when only the
			// reason changes (ScaledToZero -> Waking), so stamp it here.
			meta.FindStatusCondition(knight.Status.Conditions, aiv1alpha1.ConditionKnightAvailable).
				LastTransitionTime = metav1.NewTime(r.now())
		}
	} else if isReady {
		observeWake = !wakingSince.IsZero()
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

	r.setRuntimeAvailable(knight)

	r.setDeprecatedFieldsCondition(knight)
	r.setMCPServersCondition(knight)

	// Set NATS consumer name in status
	consumerName := knight.Spec.NATS.ConsumerName
	if consumerName == "" {
		consumerName = fmt.Sprintf("knight-%s", knight.Name)
	}
	knight.Status.NATSConsumer = consumerName
	knight.Status.ObservedGeneration = knight.Generation

	if err := r.Status().Update(ctx, knight); err != nil {
		return err
	}
	if observeWake {
		rtmetrics.KnightWakeSeconds.WithLabelValues(knightpkg.KnightTable(knight), knight.Name, knight.Namespace).
			Observe(r.now().Sub(wakingSince).Seconds())
	}
	return nil
}

// setMCPServersCondition sets Degraded=True (MCPServersUnsupported) when
// spec.mcpServers is set on a runtime that is not claude-knight; the pod
// builder renders nothing in that case. The condition is removed otherwise.
func (r *KnightReconciler) setMCPServersCondition(knight *aiv1alpha1.Knight) {
	c := meta.FindStatusCondition(knight.Status.Conditions, aiv1alpha1.ConditionKnightDegraded)
	unsupported := len(knight.Spec.MCPServers) > 0 &&
		knightpkg.AgentRuntimeForImage(knightpkg.EffectiveImage(knight, r.DefaultImage)) != knightpkg.AgentRuntimeClaude
	if !unsupported {
		if c != nil && c.Reason == aiv1alpha1.ReasonMCPServersUnsupported {
			meta.RemoveStatusCondition(&knight.Status.Conditions, aiv1alpha1.ConditionKnightDegraded)
		}
		return
	}
	meta.SetStatusCondition(&knight.Status.Conditions, metav1.Condition{
		Type:               aiv1alpha1.ConditionKnightDegraded,
		Status:             metav1.ConditionTrue,
		Reason:             aiv1alpha1.ReasonMCPServersUnsupported,
		Message:            "spec.mcpServers is only supported for claude-knight images; no MCP servers are rendered",
		ObservedGeneration: knight.Generation,
	})
}

// setMCPHeadersCondition checks that every Secret key named by
// spec.mcpServers[].headersFrom exists and sets MCPHeadersReady. It reports
// whether a reference is unresolved (the caller requeues: Secrets are not
// watched). A header repeated in a different case on one server is reported
// too (MCPHeaderDuplicate). Only key presence is checked; values never reach a
// condition, event or log line. The condition is removed when no headers are
// rendered (none declared, or not a claude-knight image).
func (r *KnightReconciler) setMCPHeadersCondition(ctx context.Context, knight *aiv1alpha1.Knight) bool {
	type ref struct{ secret, key string }
	var refs []ref
	var dups []string
	seen := map[ref]bool{}
	if knightpkg.AgentRuntimeForImage(knightpkg.EffectiveImage(knight, r.DefaultImage)) == knightpkg.AgentRuntimeClaude {
		for _, s := range knight.Spec.MCPServers {
			unique := knightpkg.UniqueMCPHeaders(s.HeadersFrom)
			if len(unique) < len(s.HeadersFrom) {
				dups = append(dups, fmt.Sprintf("server %q", s.Name))
			}
			for _, h := range unique {
				rf := ref{h.ValueFrom.SecretKeyRef.Name, h.ValueFrom.SecretKeyRef.Key}
				if !seen[rf] {
					seen[rf] = true
					refs = append(refs, rf)
				}
			}
		}
	}
	if len(refs) == 0 {
		meta.RemoveStatusCondition(&knight.Status.Conditions, aiv1alpha1.ConditionKnightMCPHeadersReady)
		return false
	}

	type lookup struct {
		data map[string][]byte
		err  error
	}
	secrets := map[string]lookup{}
	var missing, unreadable []string
	for _, rf := range refs {
		l, ok := secrets[rf.secret]
		if !ok {
			sec := &corev1.Secret{}
			l.err = r.Get(ctx, types.NamespacedName{Name: rf.secret, Namespace: knight.Namespace}, sec)
			l.data = sec.Data
			secrets[rf.secret] = l
			if l.err != nil && !apierrors.IsNotFound(l.err) {
				logf.FromContext(ctx).Error(l.err, "Failed to read MCP header Secret", "secret", rf.secret)
				unreadable = append(unreadable, fmt.Sprintf("Secret %q (%s)", rf.secret, apierrors.ReasonForError(l.err)))
			}
		}
		switch {
		case apierrors.IsNotFound(l.err):
			missing = append(missing, fmt.Sprintf("Secret %q (key %q)", rf.secret, rf.key))
		case l.err != nil:
			// reported once in unreadable
		default:
			if _, ok := l.data[rf.key]; !ok {
				missing = append(missing, fmt.Sprintf("key %q in Secret %q", rf.key, rf.secret))
			}
		}
	}

	cond := metav1.Condition{
		Type:               aiv1alpha1.ConditionKnightMCPHeadersReady,
		Status:             metav1.ConditionTrue,
		Reason:             aiv1alpha1.ReasonMCPHeaderSecretsFound,
		Message:            "All Secret keys referenced by spec.mcpServers[].headersFrom exist",
		ObservedGeneration: knight.Generation,
	}
	switch {
	case len(missing) > 0:
		cond.Status = metav1.ConditionFalse
		cond.Reason = aiv1alpha1.ReasonMCPHeaderSecretMissing
		cond.Message = fmt.Sprintf("spec.mcpServers[].headersFrom references missing %s in namespace %q; the pod cannot start until it exists",
			strings.Join(missing, ", "), knight.Namespace)
	case len(dups) > 0:
		cond.Status = metav1.ConditionFalse
		cond.Reason = aiv1alpha1.ReasonMCPHeaderDuplicate
		cond.Message = fmt.Sprintf("spec.mcpServers[].headersFrom repeats a header name in a different case on %s; only the first is sent",
			strings.Join(dups, ", "))
	case len(unreadable) > 0:
		cond.Status = metav1.ConditionUnknown
		cond.Reason = aiv1alpha1.ReasonMCPHeaderSecretUnreadable
		cond.Message = "cannot read " + strings.Join(unreadable, ", ")
	}
	meta.SetStatusCondition(&knight.Status.Conditions, cond)
	return len(missing) > 0 || len(unreadable) > 0
}

// eventReasonMaxDeliverIgnored is the Warning event reason for the deprecated,
// ignored spec.nats.maxDeliver.
const eventReasonMaxDeliverIgnored = "MaxDeliverIgnored"

// setDeprecatedFieldsCondition reports deprecated spec fields. It sets
// DeprecatedFieldsUsed=True when spec.nats.maxDeliver is above 1 (ignored:
// tasks run at most once). One Warning event (MaxDeliverIgnored) is recorded
// per generation and the message lists every deprecated field in use.
// The condition is removed when none is set.
func (r *KnightReconciler) setDeprecatedFieldsCondition(knight *aiv1alpha1.Knight) {
	type deprecated struct{ reason, msg string }
	var used []deprecated
	if md := knight.Spec.NATS.MaxDeliver; md > 1 { //nolint:staticcheck // reading the deprecated field to report it
		used = append(used, deprecated{eventReasonMaxDeliverIgnored, fmt.Sprintf(
			"spec.nats.maxDeliver=%d is deprecated and ignored: knights always run a task at most once; lost and failed tasks go to the dead-letter stream and are retried by hand", md)})
	}
	if len(used) == 0 {
		meta.RemoveStatusCondition(&knight.Status.Conditions, aiv1alpha1.ConditionKnightDeprecatedFieldsUsed)
		return
	}
	prev := meta.FindStatusCondition(knight.Status.Conditions, aiv1alpha1.ConditionKnightDeprecatedFieldsUsed)
	newGen := prev == nil || prev.ObservedGeneration != knight.Generation
	msgs := make([]string, 0, len(used))
	for _, d := range used {
		if newGen {
			// event-reasons: MaxDeliverIgnored
			r.Recorder.Event(knight, corev1.EventTypeWarning, d.reason, d.msg)
		}
		msgs = append(msgs, d.msg)
	}
	meta.SetStatusCondition(&knight.Status.Conditions, metav1.Condition{
		Type:               aiv1alpha1.ConditionKnightDeprecatedFieldsUsed,
		Status:             metav1.ConditionTrue,
		Reason:             aiv1alpha1.ReasonKnightDeprecatedFieldsUsed,
		Message:            strings.Join(msgs, "; "),
		ObservedGeneration: knight.Generation,
	})
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
	if err := r.registerEgressIndexes(context.Background(), mgr.GetFieldIndexer()); err != nil {
		return err
	}
	b := ctrl.NewControllerManagedBy(mgr).
		For(&aiv1alpha1.Knight{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&batchv1.Job{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Owns(&networkingv1.NetworkPolicy{}).
		// Plan-task cap: a knight's share depends on its table and siblings.
		Watches(&aiv1alpha1.RoundTable{},
			handler.EnqueueRequestsFromMapFunc(r.roundTableKnightRequests),
			ctrlbuilder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(&aiv1alpha1.Knight{},
			handler.EnqueueRequestsFromMapFunc(r.planSiblingRequests),
			ctrlbuilder.WithPredicates(predicate.GenerationChangedPredicate{})).
		// spec.egress.allowedServices: re-render when a named Service changes.
		Watches(&corev1.Service{},
			handler.EnqueueRequestsFromMapFunc(r.serviceKnightRequests),
			ctrlbuilder.WithPredicates(egressServiceChanged)).
		// A Service may move between pod and host network: re-render on its endpoints.
		Watches(&discoveryv1.EndpointSlice{},
			handler.EnqueueRequestsFromMapFunc(r.endpointSliceKnightRequests),
			ctrlbuilder.WithPredicates(egressEndpointSliceChanged)).
		// A knight parked with RuntimeClassNotFound recovers when the class appears.
		Watches(&nodev1.RuntimeClass{},
			handler.EnqueueRequestsFromMapFunc(r.runtimeClassKnightRequests),
			ctrlbuilder.WithPredicates(predicate.Funcs{
				UpdateFunc: func(event.UpdateEvent) bool { return false },
			}))
	if r.SandboxEnabled {
		b = b.Owns(rtruntime.NewSandboxObject("", ""))
	}
	if r.CiliumEnabled {
		cnp := &unstructured.Unstructured{}
		cnp.SetGroupVersionKind(rtruntime.CiliumNetworkPolicyGVK)
		b = b.Owns(cnp)
	}
	if r.SnapshotsEnabled {
		vs := &unstructured.Unstructured{}
		vs.SetGroupVersionKind(rtruntime.VolumeSnapshotGVK)
		b = b.Owns(vs)
	}
	return b.Named("knight").Complete(r)
}

// runtimeClassKnightRequests enqueues the knights whose effective
// RuntimeClass is the one that changed.
func (r *KnightReconciler) runtimeClassKnightRequests(ctx context.Context, obj client.Object) []reconcile.Request {
	knights := &aiv1alpha1.KnightList{}
	if err := r.List(ctx, knights); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list knights for RuntimeClass requeue")
		return nil
	}
	var reqs []reconcile.Request
	for i := range knights.Items {
		k := knightpkg.WithTableDefaults(ctx, r.Client, &knights.Items[i])
		knightpkg.ApplyIsolationDefaults(k, r.Isolation)
		if runtimeClassOf(k) == obj.GetName() {
			reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&knights.Items[i])})
		}
	}
	return reqs
}

// deriveResultsPrefix is a re-export from the knight package for backward compatibility with tests.
func deriveResultsPrefix(subjects []string) string {
	return knightpkg.DeriveResultsPrefix(subjects)
}
