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
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func pvcVolume(name, claim string) corev1.Volume {
	return corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{
		PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim},
	}}
}

func emptyDirVolume(name string) corev1.Volume {
	return corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}
}

var (
	rwo  = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	rwop = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOncePod}
	rwx  = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
	rox  = []corev1.PersistentVolumeAccessMode{corev1.ReadOnlyMany}
)

func TestRolloutStrategySingleReplica(t *testing.T) {
	single := scalingKnight(nil, nil, "")
	tests := []struct {
		name     string
		volumes  []corev1.Volume
		modes    ClaimAccessModes
		recreate string // substring of the reason; "" means RollingUpdate
	}{
		{name: "no volumes"},
		{name: "no claims", volumes: []corev1.Volume{emptyDirVolume("data"), emptyDirVolume("tmp")}},
		{
			name:    "RWX claims",
			volumes: []corev1.Volume{pvcVolume("data", "ws"), pvcVolume("memory", "mem")},
			modes:   ClaimAccessModes{"ws": rwx, "mem": rwx},
		},
		{
			name:    "RWX and ROX claims",
			volumes: []corev1.Volume{pvcVolume("data", "ws"), pvcVolume("vault", "v")},
			modes:   ClaimAccessModes{"ws": rwx, "v": rox},
		},
		{
			name:    "claim allowing RWO and RWX",
			volumes: []corev1.Volume{pvcVolume("data", "ws")},
			modes:   ClaimAccessModes{"ws": {corev1.ReadWriteOnce, corev1.ReadWriteMany}},
		},
		{
			name:     "an RWO claim",
			volumes:  []corev1.Volume{pvcVolume("memory", "mem"), pvcVolume("data", "ws")},
			modes:    ClaimAccessModes{"mem": rwx, "ws": rwo},
			recreate: `volume "data": PVC "ws" is ReadWriteOnce`,
		},
		{
			name:     "an RWOP claim",
			volumes:  []corev1.Volume{pvcVolume("data", "ws")},
			modes:    ClaimAccessModes{"ws": rwop},
			recreate: `PVC "ws" is ReadWriteOncePod`,
		},
		{
			name:     "unknown claim",
			volumes:  []corev1.Volume{pvcVolume("data", "ws"), pvcVolume("memory", "missing")},
			modes:    ClaimAccessModes{"ws": rwx},
			recreate: `volume "memory": access modes of PVC "missing" are unknown`,
		},
		{
			name:     "claim without access modes",
			volumes:  []corev1.Volume{pvcVolume("data", "ws")},
			modes:    ClaimAccessModes{"ws": nil},
			recreate: "unknown",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := &corev1.PodSpec{Volumes: tt.volumes}
			strategy, reason := RolloutStrategy(single, spec, tt.modes)
			if tt.recreate != "" {
				if strategy.Type != appsv1.RecreateDeploymentStrategyType || strategy.RollingUpdate != nil {
					t.Fatalf("strategy = %+v, want Recreate", strategy)
				}
				if !strings.Contains(reason, tt.recreate) {
					t.Fatalf("reason = %q, want it to contain %q", reason, tt.recreate)
				}
				return
			}
			if reason != "" {
				t.Fatalf("reason = %q, want none", reason)
			}
			assertZeroDowntime(t, strategy)
		})
	}
}

// assertZeroDowntime checks the strategy never takes the old pod down before
// its replacement is Ready: one extra pod may be created (maxSurge 1) and no
// pod may be unavailable (maxUnavailable 0), so the Deployment controller
// only scales the old ReplicaSet down once the new pod counts as available.
func assertZeroDowntime(t *testing.T, s appsv1.DeploymentStrategy) {
	t.Helper()
	if s.Type != appsv1.RollingUpdateDeploymentStrategyType || s.RollingUpdate == nil {
		t.Fatalf("strategy = %+v, want RollingUpdate", s)
	}
	if got := s.RollingUpdate.MaxUnavailable.IntValue(); got != 0 {
		t.Fatalf("maxUnavailable = %d, want 0", got)
	}
	if got := s.RollingUpdate.MaxSurge.IntValue(); got != 1 {
		t.Fatalf("maxSurge = %d, want 1", got)
	}
}

// Knights that can run more than one pod keep the workspace rule, whatever
// the claims' access modes.
func TestRolloutStrategyMultiReplicaUnchanged(t *testing.T) {
	spec := &corev1.PodSpec{Volumes: []corev1.Volume{pvcVolume("data", "ws")}}

	k := scalingKnight(new(int32(2)), nil, "")
	k.Spec.Workspace = &aiv1alpha1.KnightWorkspace{ExistingClaim: "ws"}
	strategy, reason := RolloutStrategy(k, spec, nil)
	if reason != "" {
		t.Fatalf("existingClaim with replicas 2: reason = %q, want RollingUpdate", reason)
	}
	assertZeroDowntime(t, strategy)

	k = scalingKnight(nil, new(int32(3)), "")
	k.Spec.Workspace = &aiv1alpha1.KnightWorkspace{Mode: aiv1alpha1.WorkspaceModeEphemeral}
	if _, reason := RolloutStrategy(k, &corev1.PodSpec{}, nil); reason != "" {
		t.Fatalf("ephemeral with maxReplicas 3: reason = %q, want RollingUpdate", reason)
	}

	k = scalingKnight(new(int32(2)), nil, "")
	strategy, reason = RolloutStrategy(k, spec, ClaimAccessModes{"ws": rwx})
	if strategy.Type != appsv1.RecreateDeploymentStrategyType || reason == "" {
		t.Fatalf("operator workspace PVC with replicas 2: strategy = %+v reason %q, want Recreate", strategy, reason)
	}
}

func TestPodClaims(t *testing.T) {
	spec := &corev1.PodSpec{Volumes: []corev1.Volume{
		pvcVolume("data", "ws"), emptyDirVolume("tmp"), pvcVolume("memory", "mem"),
		{Name: "eph", VolumeSource: corev1.VolumeSource{Ephemeral: &corev1.EphemeralVolumeSource{}}},
	}}
	got := PodClaims(spec)
	if strings.Join(got, ",") != "ws,mem" {
		t.Fatalf("PodClaims = %v, want [ws mem]", got)
	}
}
