package controller

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	rtmetrics "github.com/roundtable-k8s/roundtable/pkg/metrics"
)

// The gauges below are written by two kinds of reconcile (RoundTable for
// "table=<name>", Knight/Mission for "table=none"). Two reconciles running at
// once can briefly leave an out-of-date count; that is acceptable for a gauge
// and the next reconcile corrects it.

// listTables returns every RoundTable, or false after logging the failure.
func listTables(ctx context.Context, c client.Reader) ([]aiv1alpha1.RoundTable, bool) {
	tables := &aiv1alpha1.RoundTableList{}
	if err := c.List(ctx, tables); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list RoundTables for unowned metrics")
		return nil, false
	}
	return tables.Items, true
}

// knightHasTable reports whether some RoundTable counts the knight: it
// decides membership like the RoundTable reconciler (roundTableIncludes), and
// a table's own planner knight is that table's too.
func knightHasTable(tables []aiv1alpha1.RoundTable, k *aiv1alpha1.Knight) bool {
	owner := metav1.GetControllerOf(k)
	for i := range tables {
		rt := &tables[i]
		if roundTableIncludes(rt, k) {
			return true
		}
		if owner != nil && owner.Kind == "RoundTable" && owner.Name == rt.Name && rt.Namespace == k.Namespace {
			return true
		}
	}
	return false
}

// updateUnownedKnightMetrics recomputes roundtable_knights{table="none"} from
// the knights no RoundTable counts. The series is reset first so phases and
// knights that are gone disappear instead of going stale. Call it from every
// Knight reconcile, including the not-found path.
func updateUnownedKnightMetrics(ctx context.Context, c client.Reader) {
	tables, ok := listTables(ctx, c)
	if !ok {
		return
	}
	knights := &aiv1alpha1.KnightList{}
	if err := c.List(ctx, knights); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list knights for unowned metrics")
		return
	}
	phases := map[string]int{}
	for i := range knights.Items {
		if !knightHasTable(tables, &knights.Items[i]) {
			phases[string(knights.Items[i].Status.Phase)]++
		}
	}
	rtmetrics.Knights.DeletePartialMatch(prometheus.Labels{"table": noTableLabel})
	for phase, n := range phases {
		rtmetrics.Knights.WithLabelValues(noTableLabel, phase).Set(float64(n))
	}
}

// updateUnownedMissionMetrics is the Mission counterpart: missions whose
// table (roundTableRef, else the ephemeral table in status) is empty or does
// not exist.
func updateUnownedMissionMetrics(ctx context.Context, c client.Reader) {
	tables, ok := listTables(ctx, c)
	if !ok {
		return
	}
	missions := &aiv1alpha1.MissionList{}
	if err := c.List(ctx, missions); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list missions for unowned metrics")
		return
	}
	phases := map[string]int{}
	for i := range missions.Items {
		m := &missions.Items[i]
		found := false
		if name := missionTable(m); name != "" {
			for j := range tables {
				if tables[j].Name == name && tables[j].Namespace == m.Namespace {
					found = true
					break
				}
			}
		}
		if !found {
			phases[string(m.Status.Phase)]++
		}
	}
	rtmetrics.Missions.DeletePartialMatch(prometheus.Labels{"table": noTableLabel})
	for phase, n := range phases {
		rtmetrics.Missions.WithLabelValues(noTableLabel, phase).Set(float64(n))
	}
}
