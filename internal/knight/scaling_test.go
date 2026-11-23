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

package knight

import (
	"context"
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func scalingKnight(replicas, maxReplicas *int32, policy string) *aiv1alpha1.Knight {
	k := &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{Name: "galahad"},
		Spec:       aiv1alpha1.KnightSpec{Replicas: replicas, MaxReplicas: maxReplicas},
	}
	if policy != "" {
		k.Spec.Lifecycle = &aiv1alpha1.KnightLifecycle{SuspendPolicy: policy}
	}
	return k
}

func TestRequestedScaling(t *testing.T) {
	cases := []struct {
		name        string
		k           *aiv1alpha1.Knight
		autoscaling bool
		want        Scaling
	}{
		{"defaults", scalingKnight(nil, nil, ""), true, Scaling{MinReplicas: 1, MaxReplicas: 1}},
		{"fixed replicas", scalingKnight(new(int32(3)), nil, ""), true, Scaling{MinReplicas: 3, MaxReplicas: 3}},
		{"maxReplicas 1 is not autoscaling", scalingKnight(nil, new(int32(1)), "never"), true, Scaling{MinReplicas: 1, MaxReplicas: 1}},
		{"maxReplicas", scalingKnight(new(int32(2)), new(int32(6)), ""), true, Scaling{Autoscaled: true, MinReplicas: 2, MaxReplicas: 6}},
		{"auto scales to zero", scalingKnight(nil, nil, "auto"), true, Scaling{Autoscaled: true, MinReplicas: 0, MaxReplicas: 1}},
		{"auto with maxReplicas", scalingKnight(new(int32(2)), new(int32(4)), "auto"), true, Scaling{Autoscaled: true, MinReplicas: 0, MaxReplicas: 4}},
		{"no KEDA falls back to replicas", scalingKnight(new(int32(2)), new(int32(6)), "auto"), false, Scaling{MinReplicas: 2, MaxReplicas: 2}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := RequestedScaling(tc.k, tc.autoscaling); got != tc.want {
				t.Errorf("RequestedScaling = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestScalingInitialAndCap(t *testing.T) {
	s := Scaling{Autoscaled: true, MinReplicas: 0, MaxReplicas: 4}
	if s.InitialReplicas() != 1 {
		t.Errorf("scale-to-zero knight must start with one pod to create its consumer, got %d", s.InitialReplicas())
	}
	if got := (Scaling{MinReplicas: 3, MaxReplicas: 3}).InitialReplicas(); got != 3 {
		t.Errorf("fixed InitialReplicas = %d, want 3", got)
	}
	if got := (Scaling{Autoscaled: true, MinReplicas: 2, MaxReplicas: 6}).CapReplicas(1); got.MinReplicas != 1 || got.MaxReplicas != 1 {
		t.Errorf("CapReplicas(1) = %+v", got)
	}
	if got := (Scaling{MinReplicas: 3, MaxReplicas: 3}).CapReplicas(0); got.MaxReplicas != 1 {
		t.Errorf("CapReplicas(0) must keep one pod, got %+v", got)
	}
	if got := s.CapReplicas(10); got != s {
		t.Errorf("CapReplicas above max changed the plan: %+v", got)
	}
}

func TestCooldownSeconds(t *testing.T) {
	k := scalingKnight(nil, nil, "auto")
	if got := CooldownSeconds(k); got != 300 {
		t.Errorf("default cooldown = %d, want 300", got)
	}
	k.Spec.Lifecycle.IdleTimeoutSeconds = 1800
	if got := CooldownSeconds(k); got != 1800 {
		t.Errorf("idleTimeoutSeconds 1800 cooldown = %d, want 1800", got)
	}
}

func TestAllotPlanReplicas(t *testing.T) {
	cases := []struct {
		name    string
		cap     int32
		demands []PlanDemand
		want    map[string]int32
	}{
		{
			name:    "floor of one pod each even over the cap",
			cap:     2,
			demands: []PlanDemand{{"b", 3, 2}, {"a", 3, 2}},
			want:    map[string]int32{"a": 1, "b": 1},
		},
		{
			name:    "round robin in name order",
			cap:     10,
			demands: []PlanDemand{{"b", 5, 2}, {"a", 5, 2}},
			want:    map[string]int32{"a": 3, "b": 2},
		},
		{
			name:    "spare capacity goes to who still wants it",
			cap:     10,
			demands: []PlanDemand{{"a", 1, 1}, {"b", 9, 1}},
			want:    map[string]int32{"a": 1, "b": 9},
		},
		{
			name:    "big concurrency doesn't block a smaller knight",
			cap:     7,
			demands: []PlanDemand{{"a", 3, 4}, {"b", 4, 1}},
			want:    map[string]int32{"a": 1, "b": 3},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := AllotPlanReplicas(tc.cap, tc.demands); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("AllotPlanReplicas = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestTaskConsumerName(t *testing.T) {
	if got := TaskConsumerName(&aiv1alpha1.Knight{ObjectMeta: metav1.ObjectMeta{Name: "rt-operator"}}); got != "Rt-operator-consumer" {
		t.Errorf("TaskConsumerName = %q", got)
	}
}

func TestWithWorkspaceEphemeral(t *testing.T) {
	k := &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{Name: "galahad"},
		Spec: aiv1alpha1.KnightSpec{
			Workspace: &aiv1alpha1.KnightWorkspace{Mode: aiv1alpha1.WorkspaceModeEphemeral, Size: new(resource.MustParse("2Gi"))},
		},
	}
	spec := NewPodBuilder(k, "img").WithWorkspace().Build(context.Background())
	var data *corev1.Volume
	for i := range spec.Volumes {
		if spec.Volumes[i].Name == "data" {
			data = &spec.Volumes[i]
		}
	}
	if data == nil || data.EmptyDir == nil || data.PersistentVolumeClaim != nil {
		t.Fatalf("data volume = %+v, want an emptyDir", data)
	}
	if data.EmptyDir.SizeLimit == nil || data.EmptyDir.SizeLimit.String() != "2Gi" {
		t.Errorf("sizeLimit = %v, want 2Gi", data.EmptyDir.SizeLimit)
	}
	found := false
	for _, e := range spec.Containers[0].Env {
		if e.Name == "KNIGHT_WORKSPACE_MODE" && e.Value == "ephemeral" {
			found = true
		}
	}
	if !found {
		t.Error("KNIGHT_WORKSPACE_MODE=ephemeral not set")
	}

	k.Spec.Workspace.Mode = aiv1alpha1.WorkspaceModePVC
	spec = NewPodBuilder(k, "img").WithWorkspace().Build(context.Background())
	if spec.Volumes[0].PersistentVolumeClaim == nil || spec.Volumes[0].PersistentVolumeClaim.ClaimName != "galahad" {
		t.Errorf("pvc mode volume = %+v", spec.Volumes[0])
	}
}

func TestScaleDownStabilizationSeconds(t *testing.T) {
	k := &aiv1alpha1.Knight{}
	if got := ScaleDownStabilizationSeconds(k); got != 600 {
		t.Fatalf("default = %d, want 600", got)
	}
	k.Spec.Lifecycle = &aiv1alpha1.KnightLifecycle{}
	if got := ScaleDownStabilizationSeconds(k); got != 600 {
		t.Fatalf("unset = %d, want 600", got)
	}
	v := int32(120)
	k.Spec.Lifecycle.ScaleDownStabilizationSeconds = &v
	if got := ScaleDownStabilizationSeconds(k); got != 120 {
		t.Fatalf("set = %d, want 120", got)
	}
	zero := int32(0)
	k.Spec.Lifecycle.ScaleDownStabilizationSeconds = &zero
	if got := ScaleDownStabilizationSeconds(k); got != 0 {
		t.Fatalf("zero = %d, want 0", got)
	}
}
