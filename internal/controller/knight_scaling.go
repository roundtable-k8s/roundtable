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
	"net"
	"net/url"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
	rtruntime "github.com/roundtable-k8s/roundtable/pkg/runtime"
)

// +kubebuilder:rbac:groups=keda.sh,resources=scaledobjects,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=get;list;watch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=roundtables,verbs=get;list;watch

const (
	// natsMonitoringPort is the NATS HTTP monitoring port KEDA's JetStream
	// scaler queries when no endpoint is configured.
	natsMonitoringPort = "8222"
	// natsJetStreamTrigger is the KEDA scaler type for JetStream consumer lag.
	natsJetStreamTrigger = "nats-jetstream"
)

// scalingPlan is a knight's replica plan after KEDA availability and the
// plan-task cap are applied.
type scalingPlan struct {
	knightpkg.Scaling

	// planCap is set when the knight is plan-backed and at least one of its
	// tables caps plan-backed tasks; nil otherwise.
	planCap *planCapResult
}

// planCapResult describes how the plan-task cap affected a knight.
type planCapResult struct {
	table   string
	cap     int32
	allowed int32
	limited bool
}

// knightLabels are the labels on every resource the operator creates for a
// knight; the Deployment selector uses them too.
func knightLabels(k *aiv1alpha1.Knight) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "knight",
		"app.kubernetes.io/instance":   k.Name,
		"app.kubernetes.io/managed-by": "roundtable-operator",
		"roundtable.io/domain":         k.Spec.Domain,
	}
}

// autoscalingSupported reports whether this operator can autoscale the knight
// with KEDA: KEDA must be served and the knight must use the Deployment runtime.
func (r *KnightReconciler) autoscalingSupported(k *aiv1alpha1.Knight) bool {
	return r.KEDAEnabled && runtimeName(k) == runtimeDeployment
}

// isPlanBacked reports whether the knight's tasks spend the shared Claude
// plan quota: spec.planBacked when set, otherwise "runs a claude-knight image".
func (r *KnightReconciler) isPlanBacked(k *aiv1alpha1.Knight) bool {
	if k.Spec.PlanBacked != nil {
		return *k.Spec.PlanBacked
	}
	return knightpkg.AgentRuntimeForImage(knightpkg.EffectiveImage(k, r.DefaultImage)) == knightpkg.AgentRuntimeClaude
}

// roundTableIncludes reports whether a RoundTable manages the knight. It
// mirrors RoundTableReconciler.discoverKnights: an ephemeral table owns the
// knights labelled with its name; any other table owns the non-ephemeral
// knights its knightSelector matches (all of them without a selector).
func roundTableIncludes(rt *aiv1alpha1.RoundTable, k *aiv1alpha1.Knight) bool {
	if rt.Namespace != k.Namespace {
		return false
	}
	if rt.Spec.Ephemeral {
		return k.Labels[aiv1alpha1.LabelRoundTable] == rt.Name
	}
	if k.Labels[aiv1alpha1.LabelEphemeral] == labelValueTrue {
		return false
	}
	if rt.Spec.KnightSelector == nil {
		return true
	}
	sel, err := metav1.LabelSelectorAsSelector(rt.Spec.KnightSelector)
	if err != nil {
		return false
	}
	return sel.Matches(labels.Set(k.Labels))
}

// planCapOf returns the table's maxConcurrentPlanTasks, or 0.
func planCapOf(rt *aiv1alpha1.RoundTable) int32 {
	if rt.Spec.Policies == nil {
		return 0
	}
	return rt.Spec.Policies.MaxConcurrentPlanTasks
}

// resolveScaling computes the knight's replica plan: the spec's request,
// without autoscaling if KEDA can't be used, lowered by the plan-task cap of
// every table that includes the knight (the tightest one wins).
func (r *KnightReconciler) resolveScaling(ctx context.Context, k *aiv1alpha1.Knight) (scalingPlan, error) {
	plan := scalingPlan{Scaling: knightpkg.RequestedScaling(k, r.autoscalingSupported(k))}
	if !r.isPlanBacked(k) {
		return plan, nil
	}

	tables := &aiv1alpha1.RoundTableList{}
	if err := r.List(ctx, tables, client.InNamespace(k.Namespace)); err != nil {
		return plan, fmt.Errorf("listing RoundTables for the plan-task cap: %w", err)
	}
	var knights *aiv1alpha1.KnightList
	for i := range tables.Items {
		rt := &tables.Items[i]
		capTasks := planCapOf(rt)
		if capTasks <= 0 || !roundTableIncludes(rt, k) {
			continue
		}
		if knights == nil {
			knights = &aiv1alpha1.KnightList{}
			if err := r.List(ctx, knights, client.InNamespace(k.Namespace)); err != nil {
				return plan, fmt.Errorf("listing knights for the plan-task cap: %w", err)
			}
		}
		demands := []knightpkg.PlanDemand{{
			Name: k.Name, Want: plan.MaxReplicas, Concurrency: knightpkg.Concurrency(k),
		}}
		for j := range knights.Items {
			sib := &knights.Items[j]
			if sib.Name == k.Name || sib.Spec.Suspend || sib.DeletionTimestamp != nil ||
				!roundTableIncludes(rt, sib) {
				continue
			}
			// The sibling's image and concurrency may come from its table.
			sib = knightpkg.WithTableDefaults(ctx, r.Client, sib)
			if !r.isPlanBacked(sib) {
				continue
			}
			demands = append(demands, knightpkg.PlanDemand{
				Name:        sib.Name,
				Want:        knightpkg.RequestedScaling(sib, r.autoscalingSupported(sib)).MaxReplicas,
				Concurrency: knightpkg.Concurrency(sib),
			})
		}
		allowed := knightpkg.AllotPlanReplicas(capTasks, demands)[k.Name]
		if plan.planCap == nil || allowed < plan.planCap.allowed {
			plan.planCap = &planCapResult{table: rt.Name, cap: capTasks, allowed: allowed}
		}
	}
	if plan.planCap != nil {
		before := plan.MaxReplicas
		plan.Scaling = plan.CapReplicas(plan.planCap.allowed)
		plan.planCap.limited = plan.MaxReplicas < before
	}
	return plan, nil
}

// scalingOrFallback resolves the knight's scaling for spec builders that
// can't return an error. On error it falls back to a single pod, the safe
// choice under a plan-task cap.
func (r *KnightReconciler) scalingOrFallback(ctx context.Context, k *aiv1alpha1.Knight) knightpkg.Scaling {
	plan, err := r.resolveScaling(ctx, k)
	if err != nil {
		logf.FromContext(ctx).Error(err, "Failed to resolve knight scaling; using one replica")
		return plan.CapReplicas(1)
	}
	return plan.Scaling
}

// isAutoscaled reports whether a KEDA ScaledObject owns the knight's replica
// count (DeploymentBackend.Autoscaled).
func (r *KnightReconciler) isAutoscaled(ctx context.Context, k *aiv1alpha1.Knight) bool {
	return r.scalingOrFallback(ctx, k).Autoscaled
}

// natsMonitoringEndpoint returns the NATS HTTP monitoring endpoint
// (host:port) for KEDA's JetStream scaler: the operator-wide override, else
// the host of the knight's first NATS URL on port 8222.
func (r *KnightReconciler) natsMonitoringEndpoint(k *aiv1alpha1.Knight) (string, error) {
	if r.NATSMonitoringEndpoint != "" {
		return r.NATSMonitoringEndpoint, nil
	}
	raw := strings.TrimSpace(strings.Split(k.Spec.NATS.URL, ",")[0])
	if raw == "" {
		return "", fmt.Errorf("spec.nats.url is empty; cannot derive the NATS monitoring endpoint for KEDA")
	}
	if !strings.Contains(raw, "://") {
		raw = "nats://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		return "", fmt.Errorf("cannot derive the NATS monitoring endpoint from spec.nats.url %q", k.Spec.NATS.URL)
	}
	return net.JoinHostPort(u.Hostname(), natsMonitoringPort), nil
}

// desiredScaledObjectSpec renders the KEDA ScaledObject spec for a knight:
// the knight's Deployment scaled on the lag of its durable task consumer.
func (r *KnightReconciler) desiredScaledObjectSpec(k *aiv1alpha1.Knight, s knightpkg.Scaling) (map[string]any, error) {
	endpoint, err := r.natsMonitoringEndpoint(k)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"scaleTargetRef": map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"name":       k.Name,
		},
		"minReplicaCount": int64(s.MinReplicas),
		"maxReplicaCount": int64(s.MaxReplicas),
		"cooldownPeriod":  knightpkg.CooldownSeconds(k),
		"advanced": map[string]any{
			"horizontalPodAutoscalerConfig": map[string]any{
				"behavior": map[string]any{
					"scaleDown": map[string]any{
						"stabilizationWindowSeconds": knightpkg.ScaleDownStabilizationSeconds(k),
					},
				},
			},
		},
		"triggers": []any{
			map[string]any{
				"type": natsJetStreamTrigger,
				"metadata": map[string]any{
					"natsServerMonitoringEndpoint": endpoint,
					"account":                      "$G",
					"stream":                       k.Spec.NATS.Stream,
					"consumer":                     knightpkg.TaskConsumerName(k),
					"lagThreshold":                 "1",
					"activationLagThreshold":       "0",
				},
			},
			// The nats-jetstream scaler only sees pending messages. Tasks a
			// pod is already running are ack-pending, so a second trigger
			// reads them from the monitoring endpoint: ack_pending /
			// concurrency pods. The HPA takes the larger of the two triggers.
			map[string]any{
				"type":       "metrics-api",
				"metricType": "AverageValue",
				"metadata": map[string]any{
					"url": fmt.Sprintf("http://%s/jsz?acc=%%24G&streams=true&consumers=true", endpoint),
					"valueLocation": fmt.Sprintf(
						"account_details.0.stream_detail.#(name==%s).consumer_detail.#(name==%s).num_ack_pending",
						strconv.Quote(k.Spec.NATS.Stream), strconv.Quote(knightpkg.TaskConsumerName(k))),
					"targetValue":           strconv.Itoa(int(knightpkg.Concurrency(k))),
					"activationTargetValue": "0",
				},
			},
		},
	}, nil
}

func newScaledObject(k *aiv1alpha1.Knight) *unstructured.Unstructured {
	so := &unstructured.Unstructured{}
	so.SetGroupVersionKind(rtruntime.ScaledObjectGVK)
	so.SetName(k.Name)
	so.SetNamespace(k.Namespace)
	return so
}

// reconcileScaledObject creates or updates the knight's ScaledObject when it
// is autoscaled and deletes it otherwise. It never touches the ScaledObject
// API when KEDA isn't served.
func (r *KnightReconciler) reconcileScaledObject(ctx context.Context, k *aiv1alpha1.Knight, s knightpkg.Scaling) error {
	if !r.KEDAEnabled {
		return nil
	}
	if !s.Autoscaled {
		return r.deleteScaledObject(ctx, k)
	}
	spec, err := r.desiredScaledObjectSpec(k, s)
	if err != nil {
		return err
	}
	so := newScaledObject(k)
	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, so, func() error {
		if err := controllerutil.SetControllerReference(k, so, r.Scheme); err != nil {
			return err
		}
		so.SetLabels(knightLabels(k))
		so.Object["spec"] = spec
		return nil
	})
	if err != nil {
		return fmt.Errorf("ScaledObject reconcile failed: %w", err)
	}
	if op != controllerutil.OperationResultNone {
		logf.FromContext(ctx).Info("ScaledObject reconciled", "operation", op,
			"minReplicas", s.MinReplicas, "maxReplicas", s.MaxReplicas)
	}
	return nil
}

// deleteScaledObject removes the knight's ScaledObject if the operator
// created one. KEDA then stops scaling the Deployment.
func (r *KnightReconciler) deleteScaledObject(ctx context.Context, k *aiv1alpha1.Knight) error {
	if !r.KEDAEnabled {
		return nil
	}
	so := newScaledObject(k)
	if err := r.Get(ctx, types.NamespacedName{Name: k.Name, Namespace: k.Namespace}, so); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get ScaledObject: %w", err)
	}
	if !metav1.IsControlledBy(so, k) {
		return nil
	}
	if err := r.Delete(ctx, so); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete ScaledObject: %w", err)
	}
	logf.FromContext(ctx).Info("ScaledObject deleted", "knight", k.Name)
	return nil
}

// reconcilePDB keeps a PodDisruptionBudget (maxUnavailable 1) for knights
// that can run more than one pod, so a drain doesn't evict every worker at
// once, and removes it otherwise. maxUnavailable (not minAvailable) never
// blocks a drain of a single-pod or scaled-to-zero knight.
func (r *KnightReconciler) reconcilePDB(ctx context.Context, k *aiv1alpha1.Knight, s knightpkg.Scaling) error {
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: k.Name, Namespace: k.Namespace},
	}
	if !s.MultiReplica() || runtimeName(k) != runtimeDeployment {
		if err := r.Get(ctx, client.ObjectKeyFromObject(pdb), pdb); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("failed to get PodDisruptionBudget: %w", err)
		}
		if !metav1.IsControlledBy(pdb, k) {
			return nil
		}
		if err := r.Delete(ctx, pdb); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete PodDisruptionBudget: %w", err)
		}
		return nil
	}

	lbls := knightLabels(k)
	maxUnavailable := intstr.FromInt32(1)
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		if err := controllerutil.SetControllerReference(k, pdb, r.Scheme); err != nil {
			return err
		}
		pdb.Labels = lbls
		pdb.Spec.MaxUnavailable = &maxUnavailable
		pdb.Spec.MinAvailable = nil
		pdb.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{
			"app.kubernetes.io/name":     lbls["app.kubernetes.io/name"],
			"app.kubernetes.io/instance": lbls["app.kubernetes.io/instance"],
		}}
		return nil
	})
	if err != nil {
		return fmt.Errorf("PodDisruptionBudget reconcile failed: %w", err)
	}
	return nil
}

// setScalingConditions records the Autoscaled and PlanCapLimited conditions
// on the knight's in-memory status (written by updateStatus). It emits a
// warning event when autoscaling is requested but unavailable.
func (r *KnightReconciler) setScalingConditions(k *aiv1alpha1.Knight, plan scalingPlan) {
	if !knightpkg.AutoscalingRequested(k) {
		meta.RemoveStatusCondition(&k.Status.Conditions, aiv1alpha1.ConditionKnightAutoscaled)
	} else {
		cond := metav1.Condition{
			Type:               aiv1alpha1.ConditionKnightAutoscaled,
			ObservedGeneration: k.Generation,
		}
		switch {
		case runtimeName(k) != runtimeDeployment:
			cond.Status = metav1.ConditionFalse
			cond.Reason = aiv1alpha1.ReasonAutoscalingUnsupported
			cond.Message = fmt.Sprintf("Autoscaling (maxReplicas or lifecycle.suspendPolicy: auto) is only supported "+
				"with runtime: deployment, not %q; the knight runs one pod", runtimeName(k))
		case !r.KEDAEnabled:
			cond.Status = metav1.ConditionFalse
			cond.Reason = aiv1alpha1.ReasonKEDAUnavailable
			cond.Message = fmt.Sprintf("Autoscaling is requested, but the operator started without KEDA: "+
				"%s ScaledObject is not served by the API server (or --enable-keda=false). "+
				"Running a fixed %d replica(s) from spec.replicas; install KEDA and restart the operator to autoscale.",
				rtruntime.ScaledObjectGVK.GroupVersion(), plan.MinReplicas)
		default:
			cond.Status = metav1.ConditionTrue
			cond.Reason = aiv1alpha1.ReasonScaledObjectReady
			cond.Message = fmt.Sprintf("KEDA scales the knight between %d and %d pods on pending tasks of consumer %s in stream %s",
				plan.MinReplicas, plan.MaxReplicas, knightpkg.TaskConsumerName(k), k.Spec.NATS.Stream)
		}
		prev := meta.FindStatusCondition(k.Status.Conditions, aiv1alpha1.ConditionKnightAutoscaled)
		if cond.Status == metav1.ConditionFalse && (prev == nil || prev.Reason != cond.Reason) && r.Recorder != nil {
			// event-reasons: UnsupportedRuntime KEDAUnavailable
			r.Recorder.Event(k, corev1.EventTypeWarning, cond.Reason, cond.Message)
		}
		meta.SetStatusCondition(&k.Status.Conditions, cond)
	}

	if plan.planCap == nil {
		meta.RemoveStatusCondition(&k.Status.Conditions, aiv1alpha1.ConditionKnightPlanCapLimited)
		return
	}
	cond := metav1.Condition{
		Type:               aiv1alpha1.ConditionKnightPlanCapLimited,
		Status:             metav1.ConditionFalse,
		Reason:             aiv1alpha1.ReasonWithinPlanCap,
		ObservedGeneration: k.Generation,
		Message: fmt.Sprintf("RoundTable %s allows %d concurrent plan-backed tasks; this knight may run %d pod(s)",
			plan.planCap.table, plan.planCap.cap, plan.MaxReplicas),
	}
	if plan.planCap.limited {
		cond.Status = metav1.ConditionTrue
		cond.Reason = aiv1alpha1.ReasonPlanCapApplied
		cond.Message = fmt.Sprintf("RoundTable %s allows %d concurrent plan-backed tasks; this knight's pods were capped at %d",
			plan.planCap.table, plan.planCap.cap, plan.MaxReplicas)
	}
	meta.SetStatusCondition(&k.Status.Conditions, cond)
}

// planSiblingRequests maps a Knight spec change to the other plan-backed
// knights in its namespace: their share of a table's plan-task cap depends on
// it. The watch filters on generation changes, so status updates don't
// ping-pong between knights.
func (r *KnightReconciler) planSiblingRequests(ctx context.Context, obj client.Object) []reconcile.Request {
	// Any spec change counts (not only plan-backed knights): a knight that
	// stopped being plan-backed frees its share too.
	return r.planKnightRequests(ctx, obj.GetNamespace(), obj.GetName())
}

// roundTableKnightRequests maps a RoundTable change to the plan-backed knights
// in its namespace, whose cap may have changed, and to the knights labelled
// with the table, which inherit its defaults and vault (so a default change
// rolls them).
func (r *KnightReconciler) roundTableKnightRequests(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.knightRequests(ctx, obj.GetNamespace(), "", obj.GetName())
}

func (r *KnightReconciler) planKnightRequests(ctx context.Context, namespace, skip string) []reconcile.Request {
	return r.knightRequests(ctx, namespace, skip, "")
}

// knightRequests lists the knights in namespace (except skip) that are
// plan-backed or, when table is set, labelled as members of that table.
func (r *KnightReconciler) knightRequests(ctx context.Context, namespace, skip, table string) []reconcile.Request {
	knights := &aiv1alpha1.KnightList{}
	if err := r.List(ctx, knights, client.InNamespace(namespace)); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list knights for plan-cap requeue")
		return nil
	}
	var reqs []reconcile.Request
	for i := range knights.Items {
		k := &knights.Items[i]
		if k.Name == skip {
			continue
		}
		member := table != "" && (k.Labels[aiv1alpha1.LabelRoundTable] == table || k.Labels[knightpkg.LegacyTableLabel] == table)
		if !member && !r.isPlanBacked(knightpkg.WithTableDefaults(ctx, r.Client, k)) {
			continue
		}
		reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(k)})
	}
	return reqs
}

// scalingStatus builds status.scaling. For an autoscaled knight, desired
// replicas and the last scale time come from the HPA KEDA generates
// (keda-hpa-<name>); when it isn't readable, desired falls back to the
// Deployment's spec.replicas.
func (r *KnightReconciler) scalingStatus(ctx context.Context, k *aiv1alpha1.Knight, s knightpkg.Scaling,
	deploy *appsv1.Deployment, deployFound bool) *aiv1alpha1.KnightScalingStatus {
	if runtimeName(k) != runtimeDeployment {
		return nil
	}
	st := &aiv1alpha1.KnightScalingStatus{
		Autoscaled:  s.Autoscaled,
		MinReplicas: s.MinReplicas,
		MaxReplicas: s.MaxReplicas,
	}
	if deployFound && deploy.Spec.Replicas != nil {
		d := *deploy.Spec.Replicas
		st.DesiredReplicas = &d
	}
	if s.Autoscaled {
		hpa := &autoscalingv2.HorizontalPodAutoscaler{}
		key := types.NamespacedName{Name: "keda-hpa-" + k.Name, Namespace: k.Namespace}
		if err := r.Get(ctx, key, hpa); err == nil {
			d := hpa.Status.DesiredReplicas
			st.DesiredReplicas = &d
			st.LastScaleTime = hpa.Status.LastScaleTime
		} else if !apierrors.IsNotFound(err) {
			logf.FromContext(ctx).V(1).Info("Could not read the KEDA HPA for status.scaling", "error", err.Error())
		}
	}
	return st
}
