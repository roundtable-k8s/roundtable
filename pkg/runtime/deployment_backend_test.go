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
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// newTestScheme builds a scheme with the types the deployment backend needs.
func newTestScheme(t *testing.T) *k8sruntime.Scheme {
	t.Helper()
	s := k8sruntime.NewScheme()
	require.NoError(t, aiv1alpha1.AddToScheme(s))
	require.NoError(t, appsv1.AddToScheme(s))
	require.NoError(t, corev1.AddToScheme(s))
	return s
}

// stubPodSpecBuilder returns a minimal DeploymentSpec for testing.
func stubPodSpecBuilder(_ context.Context, knight *aiv1alpha1.Knight) appsv1.DeploymentSpec {
	image := knight.Spec.Image
	if image == "" {
		image = "ghcr.io/dapperdivers/openclaw-knight:latest"
	}
	return appsv1.DeploymentSpec{
		Template: corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  "knight",
						Image: image,
					},
				},
			},
		},
	}
}

// newTestKnight creates a minimal Knight CR for testing.
func newTestKnight(name string) *aiv1alpha1.Knight {
	return &aiv1alpha1.Knight{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "ai.roundtable.io/v1alpha1",
			Kind:       "Knight",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "ai",
			UID:       types.UID("test-uid-1234"),
		},
		Spec: aiv1alpha1.KnightSpec{
			Domain: "security",
			Model:  "claude-sonnet-4-20250514",
			Image:  "ghcr.io/dapperdivers/openclaw-knight:v1",
			Skills: []string{"recon", "vuln-scan"},
			NATS: aiv1alpha1.KnightNATS{
				Subjects: []string{"roundtable.tasks.security.>"},
			},
		},
	}
}

func TestNewDeploymentBackend(t *testing.T) {
	s := newTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()
	b := NewDeploymentBackend(c, s, "default-image:latest", stubPodSpecBuilder)

	assert.NotNil(t, b)
	assert.Equal(t, "default-image:latest", b.DefaultImage)
	assert.NotNil(t, b.BuildDeploymentSpec)
	assert.NotNil(t, b.Client)
	assert.NotNil(t, b.Scheme)
}

func TestDeploymentBackend_Reconcile_CreatesDeployment(t *testing.T) {
	ctx := context.Background()
	s := newTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()
	b := NewDeploymentBackend(c, s, "default:latest", stubPodSpecBuilder)

	knight := newTestKnight("percival")

	err := b.Reconcile(ctx, knight)
	require.NoError(t, err)

	// Verify the deployment was created
	deploy := &appsv1.Deployment{}
	err = c.Get(ctx, types.NamespacedName{Name: "percival", Namespace: "ai"}, deploy)
	require.NoError(t, err)

	assert.Equal(t, "percival", deploy.Name)
	assert.Equal(t, "ai", deploy.Namespace)
	assert.Equal(t, int32(1), *deploy.Spec.Replicas)
	assert.Equal(t, appsv1.RecreateDeploymentStrategyType, deploy.Spec.Strategy.Type)
	assert.Equal(t, "security", deploy.Labels["roundtable.io/domain"])
	assert.Equal(t, "knight", deploy.Labels["app.kubernetes.io/name"])
	assert.Equal(t, "ghcr.io/dapperdivers/openclaw-knight:v1", deploy.Spec.Template.Spec.Containers[0].Image)

	// Verify pod annotations
	annos := deploy.Spec.Template.Annotations
	assert.Equal(t, "claude-sonnet-4-20250514", annos["roundtable.io/model"])
	assert.Equal(t, "recon,vuln-scan", annos["roundtable.io/skills"])
	assert.Contains(t, annos, specHashAnnotation)
}

func TestDeploymentBackend_Reconcile_UpdatesOnSpecChange(t *testing.T) {
	ctx := context.Background()
	s := newTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()
	b := NewDeploymentBackend(c, s, "default:latest", stubPodSpecBuilder)

	knight := newTestKnight("percival")

	// First reconcile — creates
	require.NoError(t, b.Reconcile(ctx, knight))

	deploy := &appsv1.Deployment{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "percival", Namespace: "ai"}, deploy))
	originalHash := deploy.Spec.Template.Annotations[specHashAnnotation]
	assert.NotEmpty(t, originalHash)

	// Change the image → different spec hash
	knight.Spec.Image = "ghcr.io/dapperdivers/openclaw-knight:v2"

	require.NoError(t, b.Reconcile(ctx, knight))

	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "percival", Namespace: "ai"}, deploy))
	newHash := deploy.Spec.Template.Annotations[specHashAnnotation]
	assert.NotEqual(t, originalHash, newHash, "spec hash should change when image changes")
	assert.Equal(t, "ghcr.io/dapperdivers/openclaw-knight:v2", deploy.Spec.Template.Spec.Containers[0].Image)
}

func TestDeploymentBackend_Reconcile_SkipsWhenHashUnchanged(t *testing.T) {
	ctx := context.Background()
	s := newTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()
	b := NewDeploymentBackend(c, s, "default:latest", stubPodSpecBuilder)

	knight := newTestKnight("percival")

	require.NoError(t, b.Reconcile(ctx, knight))

	deploy := &appsv1.Deployment{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "percival", Namespace: "ai"}, deploy))
	rv1 := deploy.ResourceVersion

	// Reconcile again with same spec
	require.NoError(t, b.Reconcile(ctx, knight))

	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "percival", Namespace: "ai"}, deploy))
	// Resource version should be the same since nothing changed
	assert.Equal(t, rv1, deploy.ResourceVersion, "resource version should not change when spec is identical")
}

func TestDeploymentBackend_Cleanup_DeletesDeployment(t *testing.T) {
	ctx := context.Background()
	s := newTestScheme(t)
	knight := newTestKnight("percival")

	// Pre-create a deployment
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "percival",
			Namespace: "ai",
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "test"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "img"}}},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(s).WithObjects(deploy).Build()
	b := NewDeploymentBackend(c, s, "default:latest", stubPodSpecBuilder)

	err := b.Cleanup(ctx, knight)
	require.NoError(t, err)

	// Deployment should be gone
	err = c.Get(ctx, types.NamespacedName{Name: "percival", Namespace: "ai"}, &appsv1.Deployment{})
	assert.True(t, err != nil, "deployment should be deleted")
}

func TestDeploymentBackend_Cleanup_NotFoundIsOK(t *testing.T) {
	ctx := context.Background()
	s := newTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()
	b := NewDeploymentBackend(c, s, "default:latest", stubPodSpecBuilder)

	knight := newTestKnight("ghost")
	err := b.Cleanup(ctx, knight)
	assert.NoError(t, err, "cleanup of non-existent deployment should not error")
}

func TestDeploymentBackend_IsReady_TrueWhenAvailable(t *testing.T) {
	ctx := context.Background()
	s := newTestScheme(t)

	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "percival", Namespace: "ai"},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "test"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "img"}}},
			},
		},
		Status: appsv1.DeploymentStatus{
			ReadyReplicas: 1,
		},
	}

	c := fake.NewClientBuilder().WithScheme(s).WithObjects(deploy).WithStatusSubresource(deploy).Build()
	b := NewDeploymentBackend(c, s, "default:latest", stubPodSpecBuilder)

	knight := newTestKnight("percival")
	ready, err := b.IsReady(ctx, knight)
	require.NoError(t, err)
	assert.True(t, ready)
}

func TestDeploymentBackend_IsReady_FalseWhenNotFound(t *testing.T) {
	ctx := context.Background()
	s := newTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()
	b := NewDeploymentBackend(c, s, "default:latest", stubPodSpecBuilder)

	knight := newTestKnight("ghost")
	ready, err := b.IsReady(ctx, knight)
	require.NoError(t, err)
	assert.False(t, ready)
}

func TestDeploymentBackend_IsReady_FalseWhenZeroReplicas(t *testing.T) {
	ctx := context.Background()
	s := newTestScheme(t)

	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "percival", Namespace: "ai"},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "test"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "img"}}},
			},
		},
		Status: appsv1.DeploymentStatus{
			ReadyReplicas: 0,
		},
	}

	c := fake.NewClientBuilder().WithScheme(s).WithObjects(deploy).WithStatusSubresource(deploy).Build()
	b := NewDeploymentBackend(c, s, "default:latest", stubPodSpecBuilder)

	knight := newTestKnight("percival")
	ready, err := b.IsReady(ctx, knight)
	require.NoError(t, err)
	assert.False(t, ready)
}

func TestDeploymentBackend_Suspend_ScalesToZero(t *testing.T) {
	ctx := context.Background()
	s := newTestScheme(t)

	one := int32(1)
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "percival", Namespace: "ai"},
		Spec: appsv1.DeploymentSpec{
			Replicas: &one,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "test"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "img"}}},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(s).WithObjects(deploy).Build()
	b := NewDeploymentBackend(c, s, "default:latest", stubPodSpecBuilder)

	knight := newTestKnight("percival")
	err := b.Suspend(ctx, knight)
	require.NoError(t, err)

	updated := &appsv1.Deployment{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "percival", Namespace: "ai"}, updated))
	assert.Equal(t, int32(0), *updated.Spec.Replicas)
}

func TestDeploymentBackend_Suspend_NotFoundIsOK(t *testing.T) {
	ctx := context.Background()
	s := newTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()
	b := NewDeploymentBackend(c, s, "default:latest", stubPodSpecBuilder)

	knight := newTestKnight("ghost")
	err := b.Suspend(ctx, knight)
	assert.NoError(t, err)
}

func TestDeploymentBackend_Resume_ScalesToOne(t *testing.T) {
	ctx := context.Background()
	s := newTestScheme(t)

	zero := int32(0)
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "percival", Namespace: "ai"},
		Spec: appsv1.DeploymentSpec{
			Replicas: &zero,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "test"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "img"}}},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(s).WithObjects(deploy).Build()
	b := NewDeploymentBackend(c, s, "default:latest", stubPodSpecBuilder)

	knight := newTestKnight("percival")
	err := b.Resume(ctx, knight)
	require.NoError(t, err)

	updated := &appsv1.Deployment{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "percival", Namespace: "ai"}, updated))
	assert.Equal(t, int32(1), *updated.Spec.Replicas)
}

func TestDeploymentBackend_Resume_ErrorsWhenNotFound(t *testing.T) {
	ctx := context.Background()
	s := newTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()
	b := NewDeploymentBackend(c, s, "default:latest", stubPodSpecBuilder)

	knight := newTestKnight("ghost")
	err := b.Resume(ctx, knight)
	assert.Error(t, err, "resume should error when deployment not found")
	assert.Contains(t, err.Error(), "not found")
}

// scaledSpecBuilder wraps stubPodSpecBuilder with fixed replicas and strategy.
func scaledSpecBuilder(replicas int32, strategy appsv1.DeploymentStrategyType) PodSpecBuilder {
	return func(ctx context.Context, knight *aiv1alpha1.Knight) appsv1.DeploymentSpec {
		spec := stubPodSpecBuilder(ctx, knight)
		spec.Replicas = &replicas
		spec.Strategy = appsv1.DeploymentStrategy{Type: strategy}
		return spec
	}
}

func getDeploy(t *testing.T, b *DeploymentBackend, name string) *appsv1.Deployment {
	t.Helper()
	deploy := &appsv1.Deployment{}
	require.NoError(t, b.Client.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "ai"}, deploy))
	return deploy
}

func TestDeploymentBackend_Reconcile_UsesSpecReplicasAndStrategy(t *testing.T) {
	ctx := context.Background()
	s := newTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()
	b := NewDeploymentBackend(c, s, "default:latest", scaledSpecBuilder(3, appsv1.RollingUpdateDeploymentStrategyType))

	require.NoError(t, b.Reconcile(ctx, newTestKnight("tristan")))
	deploy := getDeploy(t, b, "tristan")
	assert.Equal(t, int32(3), *deploy.Spec.Replicas)
	assert.Equal(t, appsv1.RollingUpdateDeploymentStrategyType, deploy.Spec.Strategy.Type)
}

// The strategy is not in the spec hash; it still follows the desired spec
// when the pod template is unchanged (no pod roll needed to switch it).
func TestDeploymentBackend_Reconcile_UpdatesStrategyWithoutTemplateChange(t *testing.T) {
	ctx := context.Background()
	s := newTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()
	knight := newTestKnight("lamorak")

	b := NewDeploymentBackend(c, s, "default:latest", scaledSpecBuilder(1, appsv1.RecreateDeploymentStrategyType))
	require.NoError(t, b.Reconcile(ctx, knight))
	before := getDeploy(t, b, "lamorak")
	assert.Equal(t, appsv1.RecreateDeploymentStrategyType, before.Spec.Strategy.Type)

	b = NewDeploymentBackend(c, s, "default:latest", scaledSpecBuilder(1, appsv1.RollingUpdateDeploymentStrategyType))
	require.NoError(t, b.Reconcile(ctx, knight))
	after := getDeploy(t, b, "lamorak")
	assert.Equal(t, appsv1.RollingUpdateDeploymentStrategyType, after.Spec.Strategy.Type)
	assert.Equal(t, before.Spec.Template.Annotations[specHashAnnotation], after.Spec.Template.Annotations[specHashAnnotation])
}

func TestDeploymentBackend_Reconcile_RestoresReplicasAfterSuspend(t *testing.T) {
	ctx := context.Background()
	s := newTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()
	b := NewDeploymentBackend(c, s, "default:latest", scaledSpecBuilder(2, appsv1.RollingUpdateDeploymentStrategyType))
	knight := newTestKnight("bedivere")

	require.NoError(t, b.Reconcile(ctx, knight))
	require.NoError(t, b.Suspend(ctx, knight))
	assert.Equal(t, int32(0), *getDeploy(t, b, "bedivere").Spec.Replicas)

	// The template is unchanged (same spec hash), but replicas still come back.
	require.NoError(t, b.Reconcile(ctx, knight))
	assert.Equal(t, int32(2), *getDeploy(t, b, "bedivere").Spec.Replicas)
}

func TestDeploymentBackend_Reconcile_LeavesKEDAReplicasAlone(t *testing.T) {
	ctx := context.Background()
	s := newTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()
	b := NewDeploymentBackend(c, s, "default:latest", scaledSpecBuilder(1, appsv1.RollingUpdateDeploymentStrategyType))
	b.Autoscaled = func(context.Context, *aiv1alpha1.Knight) bool { return true }
	knight := newTestKnight("gareth")

	// Created with the initial count.
	require.NoError(t, b.Reconcile(ctx, knight))
	deploy := getDeploy(t, b, "gareth")
	assert.Equal(t, int32(1), *deploy.Spec.Replicas)

	// KEDA scales out; a reconcile (even with a template change) keeps it.
	five := int32(5)
	deploy.Spec.Replicas = &five
	require.NoError(t, c.Update(ctx, deploy))
	knight.Spec.Image = "ghcr.io/dapperdivers/openclaw-knight:v2"
	require.NoError(t, b.Reconcile(ctx, knight))
	deploy = getDeploy(t, b, "gareth")
	assert.Equal(t, int32(5), *deploy.Spec.Replicas)
	assert.Equal(t, "ghcr.io/dapperdivers/openclaw-knight:v2", deploy.Spec.Template.Spec.Containers[0].Image)

	// Autoscaling off again: the operator takes replicas back.
	b.Autoscaled = nil
	require.NoError(t, b.Reconcile(ctx, knight))
	assert.Equal(t, int32(1), *getDeploy(t, b, "gareth").Spec.Replicas)
}

// settle marks a Deployment as fully rolled out; roll marks it mid-rollout.
func setRolloutStatus(t *testing.T, b *DeploymentBackend, name string, settled bool) {
	t.Helper()
	d := getDeploy(t, b, name)
	d.Status = appsv1.DeploymentStatus{Replicas: 1, ReadyReplicas: 1, UpdatedReplicas: 1}
	if !settled {
		d.Status.UpdatedReplicas = 0
		d.Status.UnavailableReplicas = 1
	}
	require.NoError(t, b.Client.Status().Update(context.Background(), d))
}

func bumpedKnight(name string) *aiv1alpha1.Knight {
	k := newTestKnight(name)
	k.Spec.Image = "ghcr.io/dapperdivers/openclaw-knight:v2"
	return k
}

func TestDeploymentBackend_Reconcile_MaxConcurrentRollouts(t *testing.T) {
	ctx := context.Background()
	names := []string{"a", "b", "c"}

	setup := func(t *testing.T, limit int) *DeploymentBackend {
		s := newTestScheme(t)
		c := fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(&appsv1.Deployment{}).Build()
		b := NewDeploymentBackend(c, s, "default:latest", stubPodSpecBuilder)
		b.MaxConcurrentRollouts = limit
		for _, n := range names {
			require.NoError(t, b.Reconcile(ctx, newTestKnight(n)))
			setRolloutStatus(t, b, n, true)
		}
		return b
	}
	image := func(b *DeploymentBackend, n string) string {
		return getDeploy(t, b, n).Spec.Template.Spec.Containers[0].Image
	}

	t.Run("limit 1 rolls one knight at a time", func(t *testing.T) {
		b := setup(t, 1)
		require.NoError(t, b.Reconcile(ctx, bumpedKnight("a")))
		setRolloutStatus(t, b, "a", false)

		for _, n := range []string{"b", "c"} {
			err := b.Reconcile(ctx, bumpedKnight(n))
			require.ErrorIs(t, err, ErrRolloutDeferred)
			assert.Contains(t, image(b, n), ":v1", "deferred knight keeps its template")
		}
		assert.Contains(t, image(b, "a"), ":v2")

		// a finishes: exactly one more may roll.
		setRolloutStatus(t, b, "a", true)
		require.NoError(t, b.Reconcile(ctx, bumpedKnight("b")))
		setRolloutStatus(t, b, "b", false)
		require.ErrorIs(t, b.Reconcile(ctx, bumpedKnight("c")), ErrRolloutDeferred)
		// A knight that is itself mid-rollout is never held.
		require.NoError(t, b.Reconcile(ctx, bumpedKnight("b")))
	})

	t.Run("limit 0 rolls everything at once", func(t *testing.T) {
		b := setup(t, 0)
		for _, n := range names {
			require.NoError(t, b.Reconcile(ctx, bumpedKnight(n)))
			setRolloutStatus(t, b, n, false)
		}
		for _, n := range names {
			assert.Contains(t, image(b, n), ":v2")
		}
	})

	t.Run("scaled to zero knights are not counted", func(t *testing.T) {
		b := setup(t, 1)
		require.NoError(t, b.Reconcile(ctx, bumpedKnight("a")))
		setRolloutStatus(t, b, "a", false)
		zero := int32(0)
		d := getDeploy(t, b, "a")
		d.Spec.Replicas = &zero
		require.NoError(t, b.Client.Update(ctx, d))

		require.NoError(t, b.Reconcile(ctx, bumpedKnight("b")))
	})
}
