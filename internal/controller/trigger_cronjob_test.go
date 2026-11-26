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
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func TestDesiredTriggerCronJobScheduling(t *testing.T) {
	trg := &aiv1alpha1.Trigger{ObjectMeta: metav1.ObjectMeta{Name: "t", Namespace: "ns"}}
	src := &aiv1alpha1.TriggerScheduleSource{Cron: "* * * * *"}
	base := TriggerScheduleConfig{Image: "img", GatewayURL: "http://gw", Audience: "aud"}

	unset := desiredTriggerCronJob(trg, src, base).Spec.JobTemplate.Spec.Template.Spec
	require.Empty(t, unset.ImagePullSecrets)
	require.Empty(t, unset.NodeSelector)
	require.Empty(t, unset.Tolerations)
	require.Empty(t, unset.PriorityClassName)

	cfg := base
	cfg.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "regcred"}}
	cfg.NodeSelector = map[string]string{"pool": "ops"}
	cfg.Tolerations = []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}}
	cfg.PriorityClassName = "high"
	set := desiredTriggerCronJob(trg, src, cfg).Spec.JobTemplate.Spec.Template.Spec
	require.Equal(t, cfg.ImagePullSecrets, set.ImagePullSecrets)
	require.Equal(t, cfg.NodeSelector, set.NodeSelector)
	require.Equal(t, cfg.Tolerations, set.Tolerations)
	require.Equal(t, "high", set.PriorityClassName)
}

func TestTriggerCronJobDriftFields(t *testing.T) {
	t.Run("startingDeadlineSeconds compares nil and set exactly", func(t *testing.T) {
		require.True(t, startingDeadlineEqual(nil, nil))
		require.True(t, startingDeadlineEqual(ptr.To[int64](3600), ptr.To[int64](3600)))
		require.False(t, startingDeadlineEqual(nil, ptr.To[int64](3600)), "clearing must be drift")
		require.False(t, startingDeadlineEqual(ptr.To[int64](3600), nil))
		require.False(t, startingDeadlineEqual(ptr.To[int64](0), ptr.To[int64](3600)))
	})

	t.Run("defaultImagePullPolicy mirrors the API server", func(t *testing.T) {
		for image, want := range map[string]corev1.PullPolicy{
			"operator:test":        corev1.PullIfNotPresent,
			"ghcr.io/x/op:1.2.3":   corev1.PullIfNotPresent,
			"localhost:5000/op:v1": corev1.PullIfNotPresent,
			"op:latest":            corev1.PullAlways,
			"op":                   corev1.PullAlways,
			"localhost:5000/op":    corev1.PullAlways,
			"op:1.0@sha256:abcdef": corev1.PullIfNotPresent,
		} {
			require.Equal(t, want, defaultImagePullPolicy(image), image)
		}
	})

	t.Run("an unset desired pull policy matches only the API server default", func(t *testing.T) {
		img := "op:1.2.3"
		require.True(t, pullPolicyEqual("", "", img))
		require.True(t, pullPolicyEqual("", corev1.PullIfNotPresent, img), "server default is not drift")
		require.False(t, pullPolicyEqual("", corev1.PullAlways, img), "clearing Always must be drift")
		require.True(t, pullPolicyEqual(corev1.PullAlways, corev1.PullAlways, img))
		require.False(t, pullPolicyEqual(corev1.PullAlways, corev1.PullIfNotPresent, img))

		latest := "op:latest"
		require.True(t, pullPolicyEqual("", corev1.PullAlways, latest), "latest's server default is Always")
		require.False(t, pullPolicyEqual("", corev1.PullIfNotPresent, latest))
	})

	t.Run("each operator-set field cleared in the desired spec is drift", func(t *testing.T) {
		trg := &aiv1alpha1.Trigger{ObjectMeta: metav1.ObjectMeta{Name: "t", Namespace: "ns"}}
		src := &aiv1alpha1.TriggerScheduleSource{Cron: "* * * * *", StartingDeadlineSeconds: ptr.To[int64](60)}
		cfg := TriggerScheduleConfig{Image: "op:1.2.3", GatewayURL: "http://gw", Audience: "aud",
			ImagePullPolicy: corev1.PullAlways, PriorityClassName: "high"}
		set := desiredTriggerCronJob(trg, src, cfg)

		clearedSrc := *src
		clearedSrc.StartingDeadlineSeconds = nil
		cleared := desiredTriggerCronJob(trg, &clearedSrc, TriggerScheduleConfig{Image: "op:1.2.3", GatewayURL: "http://gw", Audience: "aud"})

		gotPod := set.Spec.JobTemplate.Spec.Template.Spec
		wantPod := cleared.Spec.JobTemplate.Spec.Template.Spec
		require.False(t, startingDeadlineEqual(cleared.Spec.StartingDeadlineSeconds, set.Spec.StartingDeadlineSeconds))
		require.False(t, pullPolicyEqual(firePullPolicy(wantPod), firePullPolicy(gotPod), firePullImage(wantPod)))
		require.True(t, pullPolicyEqual(firePullPolicy(gotPod), firePullPolicy(gotPod), firePullImage(gotPod)))
		require.False(t, podPlacementEqual(wantPod, gotPod))
	})
}
