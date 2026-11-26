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
