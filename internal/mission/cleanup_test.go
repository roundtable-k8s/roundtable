package mission

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func TestShouldDeleteResources(t *testing.T) {
	tests := []struct {
		name   string
		policy string
		phase  aiv1alpha1.MissionPhase
		want   bool
	}{
		{"Delete always deletes", "Delete", aiv1alpha1.MissionPhaseSucceeded, true},
		{"Retain on success", "Retain", aiv1alpha1.MissionPhaseSucceeded, false},
		{"Retain on failure", "Retain", aiv1alpha1.MissionPhaseFailed, false},
		{"OnSuccess deletes on success", "OnSuccess", aiv1alpha1.MissionPhaseSucceeded, true},
		{"OnSuccess retains on failure", "OnSuccess", aiv1alpha1.MissionPhaseFailed, false},
		{"OnFailure deletes on failure", "OnFailure", aiv1alpha1.MissionPhaseFailed, true},
		{"OnFailure retains on success", "OnFailure", aiv1alpha1.MissionPhaseSucceeded, false},
		{"empty defaults to delete", "", aiv1alpha1.MissionPhaseSucceeded, true},
		{"unknown defaults to delete", "SomethingElse", aiv1alpha1.MissionPhaseFailed, true},
	}
	c := &Cleaner{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &aiv1alpha1.Mission{
				Spec:   aiv1alpha1.MissionSpec{CleanupPolicy: tt.policy},
				Status: aiv1alpha1.MissionStatus{Phase: tt.phase},
			}
			if got := c.ShouldDeleteResources(m); got != tt.want {
				t.Errorf("ShouldDeleteResources() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDeleteEphemeralKnightsSkipsNonEphemeral(t *testing.T) {
	scheme := testScheme(t)
	eph := &aiv1alpha1.Knight{ObjectMeta: metav1.ObjectMeta{Name: "m1-eph", Namespace: "ns"}}
	shared := &aiv1alpha1.Knight{ObjectMeta: metav1.ObjectMeta{Name: "m1-shared", Namespace: "ns"}}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(eph, shared).Build()
	c := &Cleaner{Client: cl}
	m := &aiv1alpha1.Mission{
		ObjectMeta: metav1.ObjectMeta{Name: "m1", Namespace: "ns"},
		Status: aiv1alpha1.MissionStatus{KnightStatuses: []aiv1alpha1.MissionKnightStatus{
			{Name: "eph", Ephemeral: true},
			{Name: "shared"},
			{Name: "gone", Ephemeral: true}, // already deleted: must not error
		}},
	}
	if err := c.DeleteEphemeralKnights(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	list := &aiv1alpha1.KnightList{}
	if err := cl.List(t.Context(), list); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 || list.Items[0].Name != "m1-shared" {
		t.Errorf("expected only m1-shared to remain, got %v", list.Items)
	}
}

func TestNATSCleanupWithoutProviderIsNoop(t *testing.T) {
	c := &Cleaner{}
	m := &aiv1alpha1.Mission{Status: aiv1alpha1.MissionStatus{NATSTasksStream: "t", NATSResultsStream: "r"}}
	if err := c.DeleteNATSConsumers(context.Background(), m); err != nil {
		t.Errorf("DeleteNATSConsumers: %v", err)
	}
	if err := c.DeleteNATSStreams(context.Background(), m); err != nil {
		t.Errorf("DeleteNATSStreams: %v", err)
	}
	if err := c.StoreResultsToKV(t.Context(), m); err == nil {
		t.Error("StoreResultsToKV without NATS should return an error")
	}
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := aiv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func clientKey(ns, name string) types.NamespacedName {
	return types.NamespacedName{Namespace: ns, Name: name}
}
