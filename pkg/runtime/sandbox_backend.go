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
	"fmt"
	"maps"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// Sandbox spec.operatingMode values (agents.x-k8s.io/v1beta1). A Suspended
// Sandbox keeps its object and volumes but has no pod; switching back to
// Running creates a new pod from the current spec.podTemplate.
const (
	SandboxOperatingModeRunning   = "Running"
	SandboxOperatingModeSuspended = "Suspended"
)

// sandboxConditionReady is the status condition agent-sandbox sets when the
// Sandbox's pod is ready.
const sandboxConditionReady = "Ready"

// NewSandboxObject returns an empty unstructured Sandbox (SandboxGVK) with the
// given name and namespace, ready for Get, Delete or CreateOrUpdate.
func NewSandboxObject(name, namespace string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(SandboxGVK)
	u.SetName(name)
	u.SetNamespace(namespace)
	return u
}

// SandboxBackend implements RuntimeBackend using Kubernetes Agent Sandbox CRs.
// Each Knight is backed by a Sandbox (agents.x-k8s.io/v1beta1) instead of a
// Deployment. Sandboxes are handled as unstructured objects, so agent-sandbox
// is not a build dependency.
type SandboxBackend struct {
	Client       client.Client
	Scheme       *k8sruntime.Scheme
	DefaultImage string

	// BuildPodSpec is a pluggable function that constructs the PodSpec for a Knight.
	// The controller injects this to keep PodBuilder usage in the controller layer.
	BuildPodSpec func(ctx context.Context, knight *aiv1alpha1.Knight) corev1.PodSpec
}

// NewSandboxBackend creates a new SandboxBackend.
func NewSandboxBackend(
	c client.Client, scheme *k8sruntime.Scheme, defaultImage string,
	builder func(ctx context.Context, knight *aiv1alpha1.Knight) corev1.PodSpec,
) *SandboxBackend {
	return &SandboxBackend{
		Client:       c,
		Scheme:       scheme,
		DefaultImage: defaultImage,
		BuildPodSpec: builder,
	}
}

// sandboxLabels are the labels on the Sandbox and its pod template. They match
// the Deployment backend, so NetworkPolicies, PodMonitors and the dashboard
// select sandbox pods too.
func sandboxLabels(knight *aiv1alpha1.Knight) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "knight",
		"app.kubernetes.io/instance":   knight.Name,
		"app.kubernetes.io/managed-by": "roundtable-operator",
		"roundtable.io/domain":         knight.Spec.Domain,
	}
}

// sandboxPodAnnotations are the pod annotations the Deployment backend sets.
func sandboxPodAnnotations(knight *aiv1alpha1.Knight) map[string]string {
	return map[string]string{
		"roundtable.io/model":  knight.Spec.Model,
		"roundtable.io/skills": strings.Join(knight.Spec.Skills, ","),
		"roundtable.io/domain": knight.Spec.Domain,
	}
}

// sandboxShutdownTime returns the Sandbox spec.shutdownTime for the Knight:
// its ai.roundtable.io/shutdown-time annotation, normalised to RFC 3339 UTC.
// Mission knights carry it (the mission's expiry); standing knights do not,
// and a missing or unparsable value leaves the Sandbox without a shutdown time.
// At shutdownTime agent-sandbox stops the pod; the default shutdownPolicy
// (Retain) keeps the Sandbox object, which the Mission deletes with the Knight.
func sandboxShutdownTime(knight *aiv1alpha1.Knight) (string, bool) {
	v := knight.Annotations[aiv1alpha1.AnnotationShutdownTime]
	if v == "" {
		return "", false
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return "", false
	}
	return t.UTC().Format(time.RFC3339), true
}

// Reconcile creates or updates the Sandbox CR for the Knight and sets it to
// Running, with spec.shutdownTime from the Knight's shutdown-time annotation
// (see sandboxShutdownTime). spec.replicas does not exist in v1beta1, and spec.service is left
// unset: knights need no Service.
func (b *SandboxBackend) Reconcile(ctx context.Context, knight *aiv1alpha1.Knight) error {
	log := logf.FromContext(ctx)

	podSpec := b.BuildPodSpec(ctx, knight)
	podSpecMap, err := k8sruntime.DefaultUnstructuredConverter.ToUnstructured(&podSpec)
	if err != nil {
		return fmt.Errorf("converting sandbox pod spec: %w", err)
	}
	labels := sandboxLabels(knight)

	sandbox := NewSandboxObject(knight.Name, knight.Namespace)
	op, err := controllerutil.CreateOrUpdate(ctx, b.Client, sandbox, func() error {
		// Set owner reference so Sandbox is garbage-collected with Knight
		if err := controllerutil.SetControllerReference(knight, sandbox, b.Scheme); err != nil {
			return err
		}

		objLabels := sandbox.GetLabels()
		if objLabels == nil {
			objLabels = make(map[string]string, len(labels))
		}
		maps.Copy(objLabels, labels)
		sandbox.SetLabels(objLabels)

		if err := unstructured.SetNestedField(sandbox.Object, podSpecMap, "spec", "podTemplate", "spec"); err != nil {
			return err
		}
		if err := unstructured.SetNestedStringMap(sandbox.Object, labels,
			"spec", "podTemplate", "metadata", "labels"); err != nil {
			return err
		}
		if err := unstructured.SetNestedStringMap(sandbox.Object, sandboxPodAnnotations(knight),
			"spec", "podTemplate", "metadata", "annotations"); err != nil {
			return err
		}
		if shutdown, ok := sandboxShutdownTime(knight); ok {
			if err := unstructured.SetNestedField(sandbox.Object, shutdown, "spec", "shutdownTime"); err != nil {
				return err
			}
		} else {
			unstructured.RemoveNestedField(sandbox.Object, "spec", "shutdownTime")
		}
		return unstructured.SetNestedField(sandbox.Object, SandboxOperatingModeRunning, "spec", "operatingMode")
	})
	if err != nil {
		return fmt.Errorf("sandbox reconcile failed: %w", err)
	}

	log.Info("Sandbox reconciled", "operation", op, "knight", knight.Name)
	return nil
}

// getSandbox fetches the Knight's Sandbox. A missing Sandbox returns nil, nil.
func (b *SandboxBackend) getSandbox(
	ctx context.Context, knight *aiv1alpha1.Knight,
) (*unstructured.Unstructured, error) {
	sandbox := NewSandboxObject(knight.Name, knight.Namespace)
	err := b.Client.Get(ctx, types.NamespacedName{Name: knight.Name, Namespace: knight.Namespace}, sandbox)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return sandbox, nil
}

// Cleanup deletes the Sandbox CR owned by the Knight.
func (b *SandboxBackend) Cleanup(ctx context.Context, knight *aiv1alpha1.Knight) error {
	log := logf.FromContext(ctx)

	sandbox, err := b.getSandbox(ctx, knight)
	if err != nil {
		return fmt.Errorf("failed to get sandbox for cleanup: %w", err)
	}
	if sandbox == nil {
		return nil
	}

	if err := b.Client.Delete(ctx, sandbox); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete sandbox: %w", err)
	}

	log.Info("Sandbox cleaned up", "knight", knight.Name)
	return nil
}

// IsReady reports whether the Knight's Sandbox has the Ready=True condition.
func (b *SandboxBackend) IsReady(ctx context.Context, knight *aiv1alpha1.Knight) (bool, error) {
	sandbox, err := b.getSandbox(ctx, knight)
	if err != nil {
		return false, fmt.Errorf("failed to get sandbox: %w", err)
	}
	if sandbox == nil {
		return false, nil
	}
	return SandboxReady(sandbox), nil
}

// SandboxReady reports whether status.conditions holds Ready=True.
func SandboxReady(sandbox *unstructured.Unstructured) bool {
	conditions, _, _ := unstructured.NestedSlice(sandbox.Object, "status", "conditions")
	for _, c := range conditions {
		m, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if m["type"] == sandboxConditionReady {
			return m["status"] == "True"
		}
	}
	return false
}

// setOperatingMode sets spec.operatingMode on the Knight's Sandbox. It returns
// whether the Sandbox exists.
func (b *SandboxBackend) setOperatingMode(ctx context.Context, knight *aiv1alpha1.Knight, mode string) (bool, error) {
	log := logf.FromContext(ctx)

	sandbox, err := b.getSandbox(ctx, knight)
	if err != nil {
		return false, fmt.Errorf("failed to get sandbox: %w", err)
	}
	if sandbox == nil {
		return false, nil
	}
	current, _, _ := unstructured.NestedString(sandbox.Object, "spec", "operatingMode")
	if current == mode {
		return true, nil
	}
	if err := unstructured.SetNestedField(sandbox.Object, mode, "spec", "operatingMode"); err != nil {
		return true, err
	}
	if err := b.Client.Update(ctx, sandbox); err != nil {
		return true, fmt.Errorf("failed to set sandbox operatingMode to %s: %w", mode, err)
	}
	log.Info("Set knight sandbox operatingMode", "knight", knight.Name, "operatingMode", mode)
	return true, nil
}

// Suspend sets the Knight's Sandbox to operatingMode Suspended. The Sandbox
// object and its volumes are kept.
func (b *SandboxBackend) Suspend(ctx context.Context, knight *aiv1alpha1.Knight) error {
	_, err := b.setOperatingMode(ctx, knight, SandboxOperatingModeSuspended)
	return err
}

// Resume sets the Knight's Sandbox to operatingMode Running.
func (b *SandboxBackend) Resume(ctx context.Context, knight *aiv1alpha1.Knight) error {
	found, err := b.setOperatingMode(ctx, knight, SandboxOperatingModeRunning)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("sandbox not found for resume: %s", knight.Name)
	}
	return nil
}
