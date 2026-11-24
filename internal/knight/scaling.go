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
	"sort"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// SuspendPolicyAuto is the lifecycle.suspendPolicy value that scales a knight
// to zero when idle.
const SuspendPolicyAuto = "auto"

// DefaultCooldown is the ScaledObject cooldownPeriod when lifecycle.idleTimeoutSeconds
// is unset or invalid (KEDA's own default).
const DefaultCooldown = 5 * time.Minute

// Scaling is the resolved replica plan for a knight.
type Scaling struct {
	// Autoscaled is true when a KEDA ScaledObject owns the replica count.
	Autoscaled bool
	// MinReplicas is the fixed replica count, or KEDA minReplicaCount.
	MinReplicas int32
	// MaxReplicas is KEDA maxReplicaCount; equal to MinReplicas when not
	// autoscaled.
	MaxReplicas int32
}

// MultiReplica reports whether the knight can run more than one pod.
func (s Scaling) MultiReplica() bool {
	return s.MaxReplicas > 1
}

// InitialReplicas is the replica count for a newly created Deployment. An
// autoscaled knight that may scale to zero still starts with one pod: the
// knight runtime creates its durable consumer on startup, and KEDA can't
// measure the lag of a consumer that doesn't exist yet.
func (s Scaling) InitialReplicas() int32 {
	if s.Autoscaled && s.MinReplicas < 1 {
		return 1
	}
	return s.MinReplicas
}

// EphemeralWorkspace reports whether the knight uses a per-pod emptyDir
// workspace instead of a PVC.
func EphemeralWorkspace(k *aiv1alpha1.Knight) bool {
	return k.Spec.Workspace != nil && k.Spec.Workspace.Mode == aiv1alpha1.WorkspaceModeEphemeral
}

// SpecReplicas returns spec.replicas, defaulting to 1.
func SpecReplicas(k *aiv1alpha1.Knight) int32 {
	if k.Spec.Replicas != nil && *k.Spec.Replicas > 0 {
		return *k.Spec.Replicas
	}
	return 1
}

// ScaleToZero reports whether lifecycle.suspendPolicy is auto.
func ScaleToZero(k *aiv1alpha1.Knight) bool {
	return k.Spec.Lifecycle != nil && k.Spec.Lifecycle.SuspendPolicy == SuspendPolicyAuto
}

// AutoscalingRequested reports whether the spec asks for a KEDA ScaledObject:
// lifecycle.suspendPolicy auto, or maxReplicas above 1.
func AutoscalingRequested(k *aiv1alpha1.Knight) bool {
	return ScaleToZero(k) || (k.Spec.MaxReplicas != nil && *k.Spec.MaxReplicas > 1)
}

// RequestedScaling resolves the spec into a replica plan. autoscaling says
// whether a ScaledObject can actually be used (KEDA served and a runtime that
// supports it); without it the knight runs spec.replicas pods.
func RequestedScaling(k *aiv1alpha1.Knight, autoscaling bool) Scaling {
	replicas := SpecReplicas(k)
	if !autoscaling || !AutoscalingRequested(k) {
		return Scaling{MinReplicas: replicas, MaxReplicas: replicas}
	}
	maxReplicas := replicas
	if k.Spec.MaxReplicas != nil && *k.Spec.MaxReplicas > maxReplicas {
		maxReplicas = *k.Spec.MaxReplicas
	}
	minReplicas := replicas
	if ScaleToZero(k) {
		minReplicas = 0
	}
	return Scaling{Autoscaled: true, MinReplicas: minReplicas, MaxReplicas: maxReplicas}
}

// CapReplicas lowers a replica plan so it never runs more than allowed pods.
// allowed < 1 is treated as 1: a running knight always keeps one pod.
func (s Scaling) CapReplicas(allowed int32) Scaling {
	if allowed < 1 {
		allowed = 1
	}
	if s.MaxReplicas > allowed {
		s.MaxReplicas = allowed
	}
	if s.MinReplicas > s.MaxReplicas {
		s.MinReplicas = s.MaxReplicas
	}
	return s
}

// CooldownSeconds returns the ScaledObject cooldownPeriod:
// lifecycle.idleTimeoutSeconds when positive, DefaultCooldown otherwise.
func CooldownSeconds(k *aiv1alpha1.Knight) int64 {
	if k.Spec.Lifecycle != nil && k.Spec.Lifecycle.IdleTimeoutSeconds > 0 {
		return int64(k.Spec.Lifecycle.IdleTimeoutSeconds)
	}
	return int64(DefaultCooldown / time.Second)
}

// DefaultScaleDownStabilizationSeconds is the HPA scale-down stabilization
// window when lifecycle.scaleDownStabilizationSeconds is unset.
const DefaultScaleDownStabilizationSeconds int64 = 600

// ScaleDownStabilizationSeconds returns the HPA scale-down stabilization
// window: lifecycle.scaleDownStabilizationSeconds when set, else the default.
func ScaleDownStabilizationSeconds(k *aiv1alpha1.Knight) int64 {
	if k.Spec.Lifecycle != nil && k.Spec.Lifecycle.ScaleDownStabilizationSeconds != nil {
		return int64(*k.Spec.Lifecycle.ScaleDownStabilizationSeconds)
	}
	return DefaultScaleDownStabilizationSeconds
}

// Concurrency returns spec.concurrency, defaulting to 1 when unset.
func Concurrency(k *aiv1alpha1.Knight) int32 {
	if k.Spec.Concurrency > 0 {
		return k.Spec.Concurrency
	}
	return 1
}

// PlanDemand is one plan-backed knight's request for capacity.
type PlanDemand struct {
	// Name of the knight.
	Name string
	// Want is the number of pods the knight would run without a cap
	// (maxReplicas when autoscaled, replicas otherwise).
	Want int32
	// Concurrency is the knight's tasks per pod.
	Concurrency int32
}

// AllotPlanReplicas splits a table's plan-task cap across its plan-backed
// knights and returns the pod count each one may run. Every knight gets one
// pod (even if that alone exceeds the cap); remaining capacity is then handed
// out one pod at a time, round-robin in name order, to knights that want more
// and whose concurrency still fits. The result is deterministic for a given
// set of demands, so every knight's reconcile computes the same split.
func AllotPlanReplicas(capTasks int32, demands []PlanDemand) map[string]int32 {
	sorted := append([]PlanDemand(nil), demands...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })

	allot := make(map[string]int32, len(sorted))
	used := int32(0)
	for _, d := range sorted {
		allot[d.Name] = 1
		used += max(d.Concurrency, 1)
	}
	for progress := true; progress; {
		progress = false
		for _, d := range sorted {
			c := max(d.Concurrency, 1)
			if allot[d.Name] < d.Want && used+c <= capTasks {
				allot[d.Name]++
				used += c
				progress = true
			}
		}
	}
	return allot
}

// TaskConsumerName is the durable consumer the knight runtimes create on the
// tasks stream: "${KNIGHT_NAME}-consumer", where KNIGHT_NAME is the
// capitalised knight name the operator sets (see PodBuilder.Build).
func TaskConsumerName(k *aiv1alpha1.Knight) string {
	return natspkg.KnightTaskConsumerName(k.Name)
}

// DrainGraceBufferSeconds is added to taskTimeoutSeconds to form the pod's
// terminationGracePeriodSeconds, covering result publishing and shutdown.
const DrainGraceBufferSeconds int64 = 60

// TerminationGracePeriod returns taskTimeoutSeconds + 60 so a terminating pod
// can finish its in-flight task. The API default (120s) applies when unset.
func TerminationGracePeriod(k *aiv1alpha1.Knight) int64 {
	t := int64(k.Spec.TaskTimeoutSeconds)
	if t <= 0 {
		t = 120
	}
	return t + DrainGraceBufferSeconds
}

// SharedWorkspace reports whether more than one pod can mount the workspace
// at once: ephemeral, or an existingClaim (ReadWriteMany when replicas or
// maxReplicas exceed 1, which the Knight CEL rules enforce). Single-replica
// existingClaim knights are treated as ReadWriteOnce, the safe assumption.
func SharedWorkspace(k *aiv1alpha1.Knight) bool {
	if EphemeralWorkspace(k) {
		return true
	}
	if k.Spec.Workspace == nil || k.Spec.Workspace.ExistingClaim == "" {
		return false
	}
	return SpecReplicas(k) > 1 || (k.Spec.MaxReplicas != nil && *k.Spec.MaxReplicas > 1)
}

// RolloutStrategy picks the Deployment strategy: a zero-downtime rolling
// update (surge 1, unavailable 0) when the workspace is shareable, otherwise
// Recreate because a ReadWriteOnce PVC cannot be mounted by two pods.
func RolloutStrategy(k *aiv1alpha1.Knight) appsv1.DeploymentStrategy {
	if !SharedWorkspace(k) {
		return appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType}
	}
	surge, unavailable := intstr.FromInt32(1), intstr.FromInt32(0)
	return appsv1.DeploymentStrategy{
		Type:          appsv1.RollingUpdateDeploymentStrategyType,
		RollingUpdate: &appsv1.RollingUpdateDeployment{MaxSurge: &surge, MaxUnavailable: &unavailable},
	}
}
