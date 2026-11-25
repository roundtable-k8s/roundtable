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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// conformanceHarness hides the workload kind from the contract checks.
type conformanceHarness struct {
	backend RuntimeBackend
	// workload returns the (empty) workload object for a knight.
	workload func(knightName string) client.Object
	// template returns the pod template (labels, annotations, spec).
	template func(obj client.Object) (map[string]string, map[string]string, corev1.PodSpec)
	// replicas returns the desired replica count.
	replicas func(obj client.Object) int32
	// setReady marks the workload ready or not ready.
	setReady func(t *testing.T, obj client.Object, ready bool)
}

// conformancePodSpec is the fixed pod spec injected into every backend.
func conformancePodSpec(image string) corev1.PodSpec {
	return corev1.PodSpec{
		RuntimeClassName:              new("gvisor"),
		TerminationGracePeriodSeconds: new(int64(360)),
		SecurityContext:               &corev1.PodSecurityContext{RunAsNonRoot: new(true)},
		Containers: []corev1.Container{{
			Name:  "knight",
			Image: image,
			Env:   []corev1.EnvVar{{Name: "CONFORMANCE", Value: "yes"}},
		}},
	}
}

type conformanceBackendCase struct {
	name string
	// build returns a harness; the image used by the pod-spec builder can be
	// changed through the returned pointer.
	build func(t *testing.T) (*conformanceHarness, client.Client, *string)
}

func conformanceBackends() []conformanceBackendCase {
	return []conformanceBackendCase{
		{
			name: "deployment",
			build: func(t *testing.T) (*conformanceHarness, client.Client, *string) {
				s := newConformanceScheme(t)
				c := fake.NewClientBuilder().WithScheme(s).Build()
				image := "knight:v1"
				b := NewDeploymentBackend(c, s, "default:latest",
					func(_ context.Context, _ *aiv1alpha1.Knight) appsv1.DeploymentSpec {
						return appsv1.DeploymentSpec{
							Template: corev1.PodTemplateSpec{Spec: conformancePodSpec(image)},
						}
					})
				return &conformanceHarness{
					backend:  b,
					workload: func(n string) client.Object { return &appsv1.Deployment{ObjectMeta: objMeta(n)} },
					template: func(o client.Object) (map[string]string, map[string]string, corev1.PodSpec) {
						d := o.(*appsv1.Deployment)
						return d.Spec.Template.Labels, d.Spec.Template.Annotations, d.Spec.Template.Spec
					},
					replicas: func(o client.Object) int32 { return *o.(*appsv1.Deployment).Spec.Replicas },
					setReady: func(t *testing.T, o client.Object, ready bool) {
						d := o.(*appsv1.Deployment)
						d.Status.ReadyReplicas = 0
						if ready {
							d.Status.ReadyReplicas = 1
						}
						require.NoError(t, c.Status().Update(context.Background(), d))
					},
				}, c, &image
			},
		},
		{
			name: "sandbox",
			build: func(t *testing.T) (*conformanceHarness, client.Client, *string) {
				s := newConformanceScheme(t)
				c := fake.NewClientBuilder().WithScheme(s).
					WithStatusSubresource(NewSandboxObject("", "")).Build()
				image := "knight:v1"
				b := NewSandboxBackend(c, s, "default:latest",
					func(_ context.Context, _ *aiv1alpha1.Knight) corev1.PodSpec {
						return conformancePodSpec(image)
					})
				return &conformanceHarness{
					backend:  b,
					workload: func(n string) client.Object { return NewSandboxObject(n, "ai") },
					template: func(o client.Object) (map[string]string, map[string]string, corev1.PodSpec) {
						u := o.(*unstructured.Unstructured)
						labels, _, err := unstructured.NestedStringMap(u.Object, "spec", "podTemplate", "metadata", "labels")
						require.NoError(t, err)
						annotations, _, err := unstructured.NestedStringMap(u.Object, "spec", "podTemplate", "metadata", "annotations")
						require.NoError(t, err)
						specMap, _, err := unstructured.NestedMap(u.Object, "spec", "podTemplate", "spec")
						require.NoError(t, err)
						var spec corev1.PodSpec
						require.NoError(t, k8sruntime.DefaultUnstructuredConverter.FromUnstructured(specMap, &spec))
						return labels, annotations, spec
					},
					// Sandbox has no replicas: running means operatingMode Running.
					replicas: func(o client.Object) int32 {
						mode, _, _ := unstructured.NestedString(o.(*unstructured.Unstructured).Object, "spec", "operatingMode")
						if mode == SandboxOperatingModeRunning {
							return 1
						}
						return 0
					},
					setReady: func(t *testing.T, o client.Object, ready bool) {
						sb := o.(*unstructured.Unstructured)
						status := string(metav1.ConditionFalse)
						if ready {
							status = string(metav1.ConditionTrue)
						}
						require.NoError(t, unstructured.SetNestedSlice(sb.Object, []any{
							map[string]any{"type": "Ready", "status": status, "reason": "Test"},
						}, "status", "conditions"))
						require.NoError(t, c.Status().Update(context.Background(), sb))
					},
				}, c, &image
			},
		},
	}
}

func newConformanceScheme(t *testing.T) *k8sruntime.Scheme {
	t.Helper()
	s := k8sruntime.NewScheme()
	require.NoError(t, aiv1alpha1.AddToScheme(s))
	require.NoError(t, corev1.AddToScheme(s))
	require.NoError(t, appsv1.AddToScheme(s))
	return s
}

func objMeta(name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: name, Namespace: "ai"}
}

// TestRuntimeBackendConformance runs every contract check against each backend.
func TestRuntimeBackendConformance(t *testing.T) {
	checks := []struct {
		name string
		run  func(t *testing.T, h *conformanceHarness, c client.Client, image *string)
	}{
		{
			"Reconcile creates workload with controller owner ref",
			func(t *testing.T, h *conformanceHarness, c client.Client, _ *string) {
				knight := newTestKnight("lancelot")
				require.NoError(t, h.backend.Reconcile(context.Background(), knight))
				w := getWorkload(t, c, h)
				assert.Equal(t, "lancelot", w.GetName())
				assert.Equal(t, "ai", w.GetNamespace())
				ref := metav1.GetControllerOf(w)
				require.NotNil(t, ref)
				assert.Equal(t, "Knight", ref.Kind)
				assert.Equal(t, "lancelot", ref.Name)
				assert.Equal(t, knight.UID, ref.UID)
			}},
		{
			"pod template carries labels and builder pod spec unchanged",
			func(t *testing.T, h *conformanceHarness, c client.Client, _ *string) {
				knight := newTestKnight("lancelot")
				require.NoError(t, h.backend.Reconcile(context.Background(), knight))
				labels, annotations, spec := h.template(getWorkload(t, c, h))
				assert.Equal(t, "knight", labels["app.kubernetes.io/name"])
				assert.Equal(t, "lancelot", labels["app.kubernetes.io/instance"])
				assert.Equal(t, "roundtable-operator", labels["app.kubernetes.io/managed-by"])
				assert.Equal(t, knight.Spec.Domain, labels["roundtable.io/domain"])
				assert.Equal(t, knight.Spec.Model, annotations["roundtable.io/model"])
				assert.Equal(t, "recon,vuln-scan", annotations["roundtable.io/skills"])
				assert.Equal(t, knight.Spec.Domain, annotations["roundtable.io/domain"])
				want := conformancePodSpec("knight:v1")
				assert.Equal(t, want.RuntimeClassName, spec.RuntimeClassName)
				assert.Equal(t, want.SecurityContext, spec.SecurityContext)
				assert.Equal(t, want.TerminationGracePeriodSeconds, spec.TerminationGracePeriodSeconds)
				require.Len(t, spec.Containers, 1)
				assert.Equal(t, want.Containers[0].Env, spec.Containers[0].Env)
			}},
		{
			"Reconcile updates workload when builder output changes",
			func(t *testing.T, h *conformanceHarness, c client.Client, image *string) {
				knight := newTestKnight("lancelot")
				require.NoError(t, h.backend.Reconcile(context.Background(), knight))
				*image = "knight:v2"
				require.NoError(t, h.backend.Reconcile(context.Background(), knight))
				_, _, spec := h.template(getWorkload(t, c, h))
				assert.Equal(t, "knight:v2", spec.Containers[0].Image)
			}},
		{
			"IsReady follows workload readiness",
			func(t *testing.T, h *conformanceHarness, c client.Client, _ *string) {
				ctx := context.Background()
				knight := newTestKnight("lancelot")
				ready, err := h.backend.IsReady(ctx, knight)
				require.NoError(t, err)
				assert.False(t, ready, "missing workload")

				require.NoError(t, h.backend.Reconcile(ctx, knight))
				ready, err = h.backend.IsReady(ctx, knight)
				require.NoError(t, err)
				assert.False(t, ready, "before ready")

				h.setReady(t, getWorkload(t, c, h), true)
				ready, err = h.backend.IsReady(ctx, knight)
				require.NoError(t, err)
				assert.True(t, ready, "after ready")

				h.setReady(t, getWorkload(t, c, h), false)
				ready, err = h.backend.IsReady(ctx, knight)
				require.NoError(t, err)
				assert.False(t, ready, "ready cleared")
			}},
		{
			"Suspend and Resume round-trip, idempotent",
			func(t *testing.T, h *conformanceHarness, c client.Client, _ *string) {
				ctx := context.Background()
				knight := newTestKnight("lancelot")
				require.NoError(t, h.backend.Reconcile(ctx, knight))

				for range 2 {
					require.NoError(t, h.backend.Suspend(ctx, knight))
					assert.Equal(t, int32(0), h.replicas(getWorkload(t, c, h)))
				}
				for range 2 {
					require.NoError(t, h.backend.Resume(ctx, knight))
					assert.Equal(t, int32(1), h.replicas(getWorkload(t, c, h)))
				}
			}},
		{
			"Suspend on missing workload is not an error",
			func(t *testing.T, h *conformanceHarness, _ client.Client, _ *string) {
				require.NoError(t, h.backend.Suspend(context.Background(), newTestKnight("lancelot")))
			}},
		{
			"Reconcile after Suspend returns workload to running",
			func(t *testing.T, h *conformanceHarness, c client.Client, _ *string) {
				ctx := context.Background()
				knight := newTestKnight("lancelot")
				require.NoError(t, h.backend.Reconcile(ctx, knight))
				require.NoError(t, h.backend.Suspend(ctx, knight))
				require.NoError(t, h.backend.Reconcile(ctx, knight))
				assert.Equal(t, int32(1), h.replicas(getWorkload(t, c, h)))
			}},
		{
			"Cleanup removes workload and tolerates absence",
			func(t *testing.T, h *conformanceHarness, c client.Client, _ *string) {
				ctx := context.Background()
				knight := newTestKnight("lancelot")
				require.NoError(t, h.backend.Reconcile(ctx, knight))
				require.NoError(t, h.backend.Cleanup(ctx, knight))
				err := c.Get(ctx, types.NamespacedName{Name: "lancelot", Namespace: "ai"}, h.workload("lancelot"))
				assert.Error(t, err)
				require.NoError(t, h.backend.Cleanup(ctx, knight))
			}},
	}

	for _, bc := range conformanceBackends() {
		t.Run(bc.name, func(t *testing.T) {
			for _, check := range checks {
				t.Run(check.name, func(t *testing.T) {
					h, c, image := bc.build(t)
					check.run(t, h, c, image)
				})
			}
		})
	}
}

func getWorkload(t *testing.T, c client.Client, h *conformanceHarness) client.Object {
	t.Helper()
	const name = "lancelot"
	obj := h.workload(name)
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "ai"}, obj))
	return obj
}
