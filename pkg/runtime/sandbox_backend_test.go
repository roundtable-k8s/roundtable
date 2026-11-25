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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// newSandboxTestScheme builds a scheme without any agent-sandbox types: the
// backend handles Sandboxes as unstructured objects.
func newSandboxTestScheme(t *testing.T) *k8sruntime.Scheme {
	t.Helper()
	s := k8sruntime.NewScheme()
	require.NoError(t, aiv1alpha1.AddToScheme(s))
	require.NoError(t, corev1.AddToScheme(s))
	return s
}

// stubSandboxPodSpecBuilder returns a minimal PodSpec for testing.
func stubSandboxPodSpecBuilder(_ context.Context, knight *aiv1alpha1.Knight) corev1.PodSpec {
	image := knight.Spec.Image
	if image == "" {
		image = "ghcr.io/dapperdivers/openclaw-knight:latest"
	}
	return corev1.PodSpec{
		TerminationGracePeriodSeconds: new(int64(660)),
		Containers: []corev1.Container{
			{
				Name:  "knight",
				Image: image,
			},
		},
	}
}

// existingSandbox builds a stored Sandbox with the given operatingMode and
// optional Ready condition status ("" for none).
func existingSandbox(t *testing.T, mode, ready string) *unstructured.Unstructured {
	t.Helper()
	sb := NewSandboxObject("gawain", "ai")
	require.NoError(t, unstructured.SetNestedSlice(sb.Object, []any{
		map[string]any{"name": "c", "image": "img"},
	}, "spec", "podTemplate", "spec", "containers"))
	if mode != "" {
		require.NoError(t, unstructured.SetNestedField(sb.Object, mode, "spec", "operatingMode"))
	}
	if ready != "" {
		require.NoError(t, unstructured.SetNestedSlice(sb.Object, []any{
			map[string]any{"type": "Ready", "status": ready, "reason": "Test"},
		}, "status", "conditions"))
	}
	return sb
}

func getSandbox(t *testing.T, c client.Client) *unstructured.Unstructured {
	t.Helper()
	sb := NewSandboxObject("", "")
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Name: "gawain", Namespace: "ai"}, sb))
	return sb
}

func nestedStr(t *testing.T, u *unstructured.Unstructured, fields ...string) string {
	t.Helper()
	v, _, err := unstructured.NestedString(u.Object, fields...)
	require.NoError(t, err)
	return v
}

func TestNewSandboxBackend(t *testing.T) {
	s := newSandboxTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()
	b := NewSandboxBackend(c, s, "default-image:latest", stubSandboxPodSpecBuilder)

	assert.NotNil(t, b)
	assert.Equal(t, "default-image:latest", b.DefaultImage)
	assert.NotNil(t, b.BuildPodSpec)
	assert.NotNil(t, b.Client)
	assert.NotNil(t, b.Scheme)
}

func TestSandboxBackend_Reconcile_CreatesSandbox(t *testing.T) {
	ctx := context.Background()
	s := newSandboxTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()
	b := NewSandboxBackend(c, s, "default:latest", stubSandboxPodSpecBuilder)

	knight := newTestKnight("gawain")
	knight.UID = "knight-uid"
	knight.Spec.Skills = []string{"security", "shared"}
	require.NoError(t, b.Reconcile(ctx, knight))

	sb := getSandbox(t, c)
	assert.Equal(t, "agents.x-k8s.io/v1beta1", sb.GetAPIVersion())
	assert.Equal(t, "Sandbox", sb.GetKind())
	assert.Equal(t, SandboxOperatingModeRunning, nestedStr(t, sb, "spec", "operatingMode"))

	wantLabels := map[string]string{
		"app.kubernetes.io/name":       "knight",
		"app.kubernetes.io/instance":   "gawain",
		"app.kubernetes.io/managed-by": "roundtable-operator",
		"roundtable.io/domain":         "security",
	}
	assert.Equal(t, wantLabels, sb.GetLabels())
	podLabels, _, err := unstructured.NestedStringMap(sb.Object, "spec", "podTemplate", "metadata", "labels")
	require.NoError(t, err)
	assert.Equal(t, wantLabels, podLabels)

	podAnnotations, _, err := unstructured.NestedStringMap(sb.Object, "spec", "podTemplate", "metadata", "annotations")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"roundtable.io/model":  knight.Spec.Model,
		"roundtable.io/skills": "security,shared",
		"roundtable.io/domain": "security",
	}, podAnnotations)

	// The pod spec round-trips into the typed form, grace period included.
	podSpecMap, found, err := unstructured.NestedMap(sb.Object, "spec", "podTemplate", "spec")
	require.NoError(t, err)
	require.True(t, found)
	var podSpec corev1.PodSpec
	require.NoError(t, k8sruntime.DefaultUnstructuredConverter.FromUnstructured(podSpecMap, &podSpec))
	assert.Equal(t, "ghcr.io/dapperdivers/openclaw-knight:v1", podSpec.Containers[0].Image)
	require.NotNil(t, podSpec.TerminationGracePeriodSeconds)
	assert.Equal(t, int64(660), *podSpec.TerminationGracePeriodSeconds)

	// v1beta1 has no replicas; the backend creates no Service.
	_, found, _ = unstructured.NestedFieldNoCopy(sb.Object, "spec", "replicas")
	assert.False(t, found, "spec.replicas must not be set")
	_, found, _ = unstructured.NestedFieldNoCopy(sb.Object, "spec", "service")
	assert.False(t, found, "spec.service must not be set")

	owners := sb.GetOwnerReferences()
	require.Len(t, owners, 1)
	assert.Equal(t, "Knight", owners[0].Kind)
	assert.Equal(t, "gawain", owners[0].Name)
	assert.Equal(t, types.UID("knight-uid"), owners[0].UID)
	require.NotNil(t, owners[0].Controller)
	assert.True(t, *owners[0].Controller)
}

func TestSandboxBackend_Reconcile_ResumesSuspendedSandbox(t *testing.T) {
	ctx := context.Background()
	s := newSandboxTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(existingSandbox(t, SandboxOperatingModeSuspended, "")).Build()
	b := NewSandboxBackend(c, s, "default:latest", stubSandboxPodSpecBuilder)

	require.NoError(t, b.Reconcile(ctx, newTestKnight("gawain")))
	assert.Equal(t, SandboxOperatingModeRunning, nestedStr(t, getSandbox(t, c), "spec", "operatingMode"))
}

func TestSandboxBackend_Reconcile_KeepsForeignLabels(t *testing.T) {
	ctx := context.Background()
	s := newSandboxTestScheme(t)
	existing := existingSandbox(t, SandboxOperatingModeRunning, "")
	existing.SetLabels(map[string]string{"team": "red"})
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(existing).Build()
	b := NewSandboxBackend(c, s, "default:latest", stubSandboxPodSpecBuilder)

	require.NoError(t, b.Reconcile(ctx, newTestKnight("gawain")))
	labels := getSandbox(t, c).GetLabels()
	assert.Equal(t, "red", labels["team"])
	assert.Equal(t, "knight", labels["app.kubernetes.io/name"])
}

func TestSandboxBackend_Cleanup_DeletesSandbox(t *testing.T) {
	ctx := context.Background()
	s := newSandboxTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(existingSandbox(t, "", "")).Build()
	b := NewSandboxBackend(c, s, "default:latest", stubSandboxPodSpecBuilder)

	require.NoError(t, b.Cleanup(ctx, newTestKnight("gawain")))

	err := c.Get(ctx, types.NamespacedName{Name: "gawain", Namespace: "ai"}, NewSandboxObject("", ""))
	assert.Error(t, err, "sandbox should be deleted")
}

func TestSandboxBackend_Cleanup_NotFoundIsOK(t *testing.T) {
	ctx := context.Background()
	s := newSandboxTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()
	b := NewSandboxBackend(c, s, "default:latest", stubSandboxPodSpecBuilder)

	assert.NoError(t, b.Cleanup(ctx, newTestKnight("ghost")))
}

func TestSandboxBackend_IsReady(t *testing.T) {
	cases := []struct {
		name  string
		ready string
		want  bool
	}{
		{"Ready=True", "True", true},
		{"Ready=False", "False", false},
		{"Ready=Unknown", "Unknown", false},
		{"no Ready condition", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s := newSandboxTestScheme(t)
			c := fake.NewClientBuilder().WithScheme(s).
				WithObjects(existingSandbox(t, SandboxOperatingModeRunning, tc.ready)).Build()
			b := NewSandboxBackend(c, s, "default:latest", stubSandboxPodSpecBuilder)

			ready, err := b.IsReady(ctx, newTestKnight("gawain"))
			require.NoError(t, err)
			assert.Equal(t, tc.want, ready)
		})
	}
}

func TestSandboxBackend_IsReady_FalseWhenNotFound(t *testing.T) {
	ctx := context.Background()
	s := newSandboxTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()
	b := NewSandboxBackend(c, s, "default:latest", stubSandboxPodSpecBuilder)

	ready, err := b.IsReady(ctx, newTestKnight("ghost"))
	require.NoError(t, err)
	assert.False(t, ready)
}

func TestSandboxBackend_SuspendThenResume(t *testing.T) {
	ctx := context.Background()
	s := newSandboxTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()
	b := NewSandboxBackend(c, s, "default:latest", stubSandboxPodSpecBuilder)
	knight := newTestKnight("gawain")

	require.NoError(t, b.Reconcile(ctx, knight))
	assert.Equal(t, SandboxOperatingModeRunning, nestedStr(t, getSandbox(t, c), "spec", "operatingMode"))

	require.NoError(t, b.Suspend(ctx, knight))
	suspended := getSandbox(t, c)
	assert.Equal(t, SandboxOperatingModeSuspended, nestedStr(t, suspended, "spec", "operatingMode"))
	// Suspending keeps the object and its pod template.
	assert.Equal(t, "knight",
		nestedStr(t, suspended, "spec", "podTemplate", "metadata", "labels", "app.kubernetes.io/name"))

	// Suspend is idempotent.
	require.NoError(t, b.Suspend(ctx, knight))
	assert.Equal(t, suspended.GetResourceVersion(), getSandbox(t, c).GetResourceVersion())

	require.NoError(t, b.Resume(ctx, knight))
	assert.Equal(t, SandboxOperatingModeRunning, nestedStr(t, getSandbox(t, c), "spec", "operatingMode"))
}

func TestSandboxBackend_Suspend_NotFoundIsOK(t *testing.T) {
	ctx := context.Background()
	s := newSandboxTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()
	b := NewSandboxBackend(c, s, "default:latest", stubSandboxPodSpecBuilder)

	assert.NoError(t, b.Suspend(ctx, newTestKnight("ghost")))
}

func TestSandboxBackend_Resume_ErrorsWhenNotFound(t *testing.T) {
	ctx := context.Background()
	s := newSandboxTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()
	b := NewSandboxBackend(c, s, "default:latest", stubSandboxPodSpecBuilder)

	err := b.Resume(ctx, newTestKnight("ghost"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}
