package mission

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func TestTable(t *testing.T) {
	m := &aiv1alpha1.Mission{ObjectMeta: metav1.ObjectMeta{Name: "m"}}
	if got := Table(m); got != "none" {
		t.Errorf("no table: got %q, want none", got)
	}
	m.Status.RoundTableName = "msn-eph"
	if got := Table(m); got != "msn-eph" {
		t.Errorf("ephemeral table: got %q, want msn-eph", got)
	}
	m.Spec.RoundTableRef = aiv1alpha1.LocalRef("rt")
	if got := Table(m); got != "rt" {
		t.Errorf("roundTableRef wins: got %q, want rt", got)
	}
}
