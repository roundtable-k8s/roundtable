package controller

import (
	"testing"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Two chains that have no run trigger yet must not share the commentOnce
// marker on one issue: the marker falls back to the Chain name.
func TestRunMarkerKey_EmptyTriggerUsesChainName(t *testing.T) {
	a := &aiv1alpha1.Chain{ObjectMeta: metav1.ObjectMeta{Name: "plan-a"}}
	b := &aiv1alpha1.Chain{ObjectMeta: metav1.ObjectMeta{Name: "plan-b"}}
	if ka, kb := runMarkerKey(a), runMarkerKey(b); ka == kb {
		t.Fatalf("empty-run chains share marker key %q", ka)
	}
	if got := runMarkerKey(a); got != "plan-a" {
		t.Errorf("runMarkerKey(empty trigger) = %q, want chain name plan-a", got)
	}
	a.Status.LastRunTrigger = "run-42"
	if got := runMarkerKey(a); got != "run-42" {
		t.Errorf("runMarkerKey(trigger set) = %q, want run-42", got)
	}
}
