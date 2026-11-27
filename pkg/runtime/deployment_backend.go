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

package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
)

const (
	specHashAnnotation = "roundtable.io/spec-hash"
)

// PodSpecBuilder is a function that constructs a PodSpec for a Knight.
// This allows the controller to inject its own pod-building logic
// (using PodBuilder) without the runtime package depending on internal packages.
type PodSpecBuilder func(ctx context.Context, knight *aiv1alpha1.Knight) appsv1.DeploymentSpec

// DeploymentBackend implements RuntimeBackend using Kubernetes Deployments.
// This is the default "always-on" backend where each Knight runs as a Deployment.
type DeploymentBackend struct {
	Client       client.Client
	Scheme       *runtime.Scheme
	DefaultImage string

	// BuildDeploymentSpec is a pluggable function that constructs the full
	// DeploymentSpec for a Knight. The controller injects this to keep
	// PodBuilder usage in the controller layer.
	BuildDeploymentSpec PodSpecBuilder

	// Autoscaled reports whether a KEDA ScaledObject owns the knight's
	// replica count. When it returns true, Reconcile sets replicas only on
	// create and otherwise leaves them to KEDA. Nil means never autoscaled.
	Autoscaled func(ctx context.Context, knight *aiv1alpha1.Knight) bool

	// MaxConcurrentRollouts caps how many knight Deployments of a namespace
	// may be mid-rollout when another one's pod template changes. 0 means
	// unlimited (the default).
	MaxConcurrentRollouts int

	// Reader lists the namespace's Deployments for the rollout gate. Set it
	// to an uncached reader: right after an operator restart the informer
	// cache can lag and miss mid-rollout Deployments, letting more than
	// MaxConcurrentRollouts roll. Nil falls back to Client.
	Reader client.Reader

	// Busy reports whether the knight's task consumer has queued or
	// in-flight tasks. spec.lifecycle.rollout whenIdle holds a Recreate
	// knight's template change while it returns true. Nil means never busy.
	Busy func(ctx context.Context, knight *aiv1alpha1.Knight) bool

	// Now is the clock for the whenIdle maximum wait (nil means time.Now).
	Now func() time.Time
}

// ErrRolloutDeferred is returned (wrapped) by Reconcile when a changed pod
// template was not applied because MaxConcurrentRollouts other knights are
// still rolling out. The Deployment is left untouched; retry later.
var ErrRolloutDeferred = errors.New("rollout deferred: waiting for other knight rollouts")

// ErrRolloutWaitingForIdle is returned (wrapped) by Reconcile when a Recreate
// knight with spec.lifecycle.rollout whenIdle has a changed pod template but
// still has tasks, and the maximum wait has not passed. The Deployment is
// left untouched; retry later.
var ErrRolloutWaitingForIdle = errors.New("rollout deferred: waiting for the knight to be idle")

// NewDeploymentBackend creates a new DeploymentBackend.
func NewDeploymentBackend(
	c client.Client, scheme *runtime.Scheme, defaultImage string, builder PodSpecBuilder,
) *DeploymentBackend {
	return &DeploymentBackend{
		Client:              c,
		Scheme:              scheme,
		DefaultImage:        defaultImage,
		BuildDeploymentSpec: builder,
	}
}

// Reconcile creates or updates the Deployment for the Knight.
func (b *DeploymentBackend) Reconcile(ctx context.Context, knight *aiv1alpha1.Knight) error {
	log := logf.FromContext(ctx)

	// Build the desired deployment spec using the injected builder
	desiredSpec := b.BuildDeploymentSpec(ctx, knight)

	labels := map[string]string{
		"app.kubernetes.io/name":       "knight",
		"app.kubernetes.io/instance":   knight.Name,
		"app.kubernetes.io/managed-by": "roundtable-operator",
		"roundtable.io/domain":         knight.Spec.Domain,
	}

	// Build a temporary deployment to compute the spec hash
	desired := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      knight.Name,
			Namespace: knight.Namespace,
		},
	}
	desired.Spec = desiredSpec

	podAnnotations := map[string]string{
		"roundtable.io/model":  knight.Spec.Model,
		"roundtable.io/skills": strings.Join(knight.Spec.Skills, ","),
		"roundtable.io/domain": knight.Spec.Domain,
	}
	desired.Spec.Template.Annotations = podAnnotations

	desiredHash := knightpkg.DeploymentSpecHash(desired)

	// Fetch or create the deployment
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      knight.Name,
			Namespace: knight.Namespace,
		},
	}

	replicas := int32(1)
	if desiredSpec.Replicas != nil {
		replicas = *desiredSpec.Replicas
	}
	strategy := desiredSpec.Strategy
	if strategy.Type == "" {
		strategy = appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType}
	}
	if err := b.gateIdle(ctx, knight, strategy, desiredHash); err != nil {
		return err
	}
	if err := b.gateRollout(ctx, knight, desiredHash); err != nil {
		return err
	}
	autoscaled := b.Autoscaled != nil && b.Autoscaled(ctx, knight)

	op, err := controllerutil.CreateOrUpdate(ctx, b.Client, deploy, func() error {
		if err := controllerutil.SetControllerReference(knight, deploy, b.Scheme); err != nil {
			return err
		}

		// Replicas are reconciled even when the template is unchanged, so
		// that un-suspending (which only touched replicas) or editing
		// spec.replicas takes effect. A KEDA-owned Deployment keeps whatever
		// replica count KEDA set; it only gets the initial count on create.
		if !autoscaled || deploy.Spec.Replicas == nil {
			deploy.Spec.Replicas = &replicas
		}
		// The strategy is not part of the spec hash and changing it doesn't
		// roll the pods, so it follows the desired spec on every reconcile
		// (e.g. a claim's access modes became known after it was created).
		deploy.Spec.Strategy = strategy

		// Check if the spec hash matches — if so, skip mutation
		existingHash := ""
		if deploy.Spec.Template.Annotations != nil {
			existingHash = deploy.Spec.Template.Annotations[specHashAnnotation]
		}
		if existingHash == desiredHash {
			return nil
		}

		// Apply desired state
		deploy.Labels = labels
		deploy.Spec.Selector = &metav1.LabelSelector{
			MatchLabels: labels,
		}
		deploy.Spec.Template.Labels = labels

		podAnnotations[specHashAnnotation] = desiredHash
		deploy.Spec.Template.Annotations = podAnnotations

		deploy.Spec.Template.Spec = desiredSpec.Template.Spec

		return nil
	})

	if err != nil {
		return fmt.Errorf("deployment reconcile failed: %w", err)
	}

	log.Info("Deployment reconciled", "operation", op,
		"specImage", knight.Spec.Image,
		"defaultImage", b.DefaultImage,
		"resolvedImage", deploy.Spec.Template.Spec.Containers[0].Image)
	return nil
}

// gateIdle returns ErrRolloutWaitingForIdle when a Recreate knight with
// spec.lifecycle.rollout whenIdle would get a new pod template while Busy,
// until the wait (counted from the RolloutPending WaitingForIdle condition)
// reaches rolloutMaxWaitSeconds. New Deployments, unchanged templates and
// scaled-to-zero knights are never held.
func (b *DeploymentBackend) gateIdle(
	ctx context.Context, knight *aiv1alpha1.Knight, strategy appsv1.DeploymentStrategy, desiredHash string,
) error {
	lc := knight.Spec.Lifecycle
	if lc == nil || lc.Rollout != aiv1alpha1.RolloutWhenIdle ||
		strategy.Type != appsv1.RecreateDeploymentStrategyType || b.Busy == nil {
		return nil
	}
	existing := &appsv1.Deployment{}
	err := b.Client.Get(ctx, types.NamespacedName{Name: knight.Name, Namespace: knight.Namespace}, existing)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get deployment: %w", err)
	}
	if existing.Spec.Template.Annotations[specHashAnnotation] == desiredHash || isScaledToZero(existing) {
		return nil
	}
	if !b.Busy(ctx, knight) {
		return nil
	}
	maxWait := time.Duration(aiv1alpha1.DefaultRolloutMaxWaitSeconds) * time.Second
	if lc.RolloutMaxWaitSeconds != nil {
		maxWait = time.Duration(*lc.RolloutMaxWaitSeconds) * time.Second
	}
	now := time.Now()
	if b.Now != nil {
		now = b.Now()
	}
	waited := time.Duration(0)
	if c := meta.FindStatusCondition(knight.Status.Conditions, aiv1alpha1.ConditionKnightRolloutPending); c != nil &&
		c.Status == metav1.ConditionTrue && c.Reason == aiv1alpha1.ReasonWaitingForIdle {
		waited = now.Sub(c.LastTransitionTime.Time)
	}
	if waited >= maxWait {
		logf.FromContext(ctx).Info("Rolling busy knight: rollout whenIdle maximum wait passed",
			"knight", knight.Name, "maxWait", maxWait.String())
		return nil
	}
	return fmt.Errorf("%w (waited %s of %s)", ErrRolloutWaitingForIdle,
		waited.Truncate(time.Second), maxWait)
}

// gateRollout returns ErrRolloutDeferred when applying desiredHash to the
// knight's existing Deployment would exceed MaxConcurrentRollouts. New
// Deployments, unchanged templates and scaled-to-zero knights are never held.
func (b *DeploymentBackend) gateRollout(ctx context.Context, knight *aiv1alpha1.Knight, desiredHash string) error {
	if b.MaxConcurrentRollouts <= 0 {
		return nil
	}
	existing := &appsv1.Deployment{}
	err := b.Client.Get(ctx, types.NamespacedName{Name: knight.Name, Namespace: knight.Namespace}, existing)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get deployment: %w", err)
	}
	if existing.Spec.Template.Annotations[specHashAnnotation] == desiredHash || isScaledToZero(existing) {
		return nil
	}
	// Already mid-rollout itself: let it carry on with the new template.
	if isRollingOut(existing) {
		return nil
	}
	list := &appsv1.DeploymentList{}
	var reader client.Reader = b.Client
	if b.Reader != nil {
		reader = b.Reader
	}
	if err := reader.List(ctx, list, client.InNamespace(knight.Namespace),
		client.MatchingLabels{"app.kubernetes.io/name": "knight", "app.kubernetes.io/managed-by": "roundtable-operator"},
	); err != nil {
		return fmt.Errorf("failed to list knight deployments: %w", err)
	}
	rolling := 0
	for i := range list.Items {
		d := &list.Items[i]
		if d.Name == knight.Name || !d.DeletionTimestamp.IsZero() || isScaledToZero(d) {
			continue
		}
		if isRollingOut(d) {
			rolling++
		}
	}
	if rolling >= b.MaxConcurrentRollouts {
		return fmt.Errorf("%w (limit %d, %d rolling)", ErrRolloutDeferred, b.MaxConcurrentRollouts, rolling)
	}
	return nil
}

func isScaledToZero(d *appsv1.Deployment) bool {
	return d.Spec.Replicas != nil && *d.Spec.Replicas == 0
}

// isRollingOut reports whether a Deployment has not settled on its current
// template: the controller has not observed the latest generation, replicas
// are not all updated, unavailable or surplus (surge) pods exist.
func isRollingOut(d *appsv1.Deployment) bool {
	want := int32(1)
	if d.Spec.Replicas != nil {
		want = *d.Spec.Replicas
	}
	st := d.Status
	return st.ObservedGeneration < d.Generation ||
		st.UpdatedReplicas < want ||
		st.UnavailableReplicas > 0 ||
		st.Replicas > want
}

// Cleanup deletes the Deployment owned by the Knight.
// ConfigMaps and PVCs are handled by owner references (garbage collection).
func (b *DeploymentBackend) Cleanup(ctx context.Context, knight *aiv1alpha1.Knight) error {
	log := logf.FromContext(ctx)

	deploy := &appsv1.Deployment{}
	err := b.Client.Get(ctx, types.NamespacedName{Name: knight.Name, Namespace: knight.Namespace}, deploy)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get deployment for cleanup: %w", err)
	}

	if err := b.Client.Delete(ctx, deploy); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete deployment: %w", err)
	}

	log.Info("Deployment cleaned up", "knight", knight.Name)
	return nil
}

// IsReady checks if the Knight's Deployment has at least one ready replica.
func (b *DeploymentBackend) IsReady(ctx context.Context, knight *aiv1alpha1.Knight) (bool, error) {
	deploy := &appsv1.Deployment{}
	err := b.Client.Get(ctx, types.NamespacedName{Name: knight.Name, Namespace: knight.Namespace}, deploy)
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to get deployment: %w", err)
	}
	return deploy.Status.ReadyReplicas >= 1, nil
}

// Suspend scales the Knight's Deployment to 0 replicas.
func (b *DeploymentBackend) Suspend(ctx context.Context, knight *aiv1alpha1.Knight) error {
	log := logf.FromContext(ctx)

	deploy := &appsv1.Deployment{}
	err := b.Client.Get(ctx, types.NamespacedName{Name: knight.Name, Namespace: knight.Namespace}, deploy)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to get deployment for suspend: %w", err)
	}

	zero := int32(0)
	if deploy.Spec.Replicas == nil || *deploy.Spec.Replicas != zero {
		deploy.Spec.Replicas = &zero
		if err := b.Client.Update(ctx, deploy); err != nil {
			return fmt.Errorf("failed to scale deployment to 0: %w", err)
		}
		log.Info("Suspended knight — scaled to 0", "knight", knight.Name)
	}

	return nil
}

// Resume scales the Knight's Deployment to 1 replica.
func (b *DeploymentBackend) Resume(ctx context.Context, knight *aiv1alpha1.Knight) error {
	log := logf.FromContext(ctx)

	deploy := &appsv1.Deployment{}
	err := b.Client.Get(ctx, types.NamespacedName{Name: knight.Name, Namespace: knight.Namespace}, deploy)
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("deployment not found for resume: %s", knight.Name)
	}
	if err != nil {
		return fmt.Errorf("failed to get deployment for resume: %w", err)
	}

	one := int32(1)
	if deploy.Spec.Replicas == nil || *deploy.Spec.Replicas != one {
		deploy.Spec.Replicas = &one
		if err := b.Client.Update(ctx, deploy); err != nil {
			return fmt.Errorf("failed to scale deployment to 1: %w", err)
		}
		log.Info("Resumed knight — scaled to 1", "knight", knight.Name)
	}

	return nil
}
