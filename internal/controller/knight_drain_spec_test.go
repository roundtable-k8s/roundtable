package controller

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
)

// The sandbox backend is handed BuildDrainablePodSpec, so its pod template
// must carry the same drain grace period as Deployment knights.
func TestBuildDrainablePodSpecSetsGracePeriod(t *testing.T) {
	k := &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{Name: "sbx", Namespace: "default"},
		Spec:       aiv1alpha1.KnightSpec{Domain: "security", TaskTimeoutSeconds: 600},
	}
	r := &KnightReconciler{Client: fake.NewClientBuilder().Build(), DefaultImage: "ghcr.io/roundtable-k8s/claude-knight:sha"}
	spec := r.BuildDrainablePodSpec(context.Background(), k)
	want := knightpkg.TerminationGracePeriod(k)
	if spec.TerminationGracePeriodSeconds == nil || *spec.TerminationGracePeriodSeconds != want {
		t.Fatalf("TerminationGracePeriodSeconds = %v, want %d", spec.TerminationGracePeriodSeconds, want)
	}
	if want <= 30 {
		t.Fatalf("grace %d should exceed API default 30", want)
	}
}
