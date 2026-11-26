package controller

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
	rtmetrics "github.com/roundtable-k8s/roundtable/pkg/metrics"
)

// noTableMetricLabel is the table label value for knights and missions that
// belong to no RoundTable (same convention as the other table-scoped signals).
const noTableMetricLabel = "none"

// updateUnownedKnightMetrics recomputes roundtable_knights{table="none"} from
// the knights that carry no table label. The series is reset first so phases
// and knights that are gone disappear instead of going stale. Call it from
// every Knight reconcile, including the not-found path.
func updateUnownedKnightMetrics(ctx context.Context, c client.Reader) {
	knights := &aiv1alpha1.KnightList{}
	if err := c.List(ctx, knights); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list knights for unowned metrics")
		return
	}
	phases := map[string]int{}
	for i := range knights.Items {
		if knightpkg.KnightTable(&knights.Items[i]) == "" {
			phases[string(knights.Items[i].Status.Phase)]++
		}
	}
	rtmetrics.Knights.DeletePartialMatch(prometheus.Labels{"table": noTableMetricLabel})
	for phase, n := range phases {
		rtmetrics.Knights.WithLabelValues(noTableMetricLabel, phase).Set(float64(n))
	}
}

// updateUnownedMissionMetrics is the Mission counterpart: missions with no
// roundTableRef and no ephemeral table in status.
func updateUnownedMissionMetrics(ctx context.Context, c client.Reader) {
	missions := &aiv1alpha1.MissionList{}
	if err := c.List(ctx, missions); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list missions for unowned metrics")
		return
	}
	phases := map[string]int{}
	for i := range missions.Items {
		if missionTable(&missions.Items[i]) == "" {
			phases[string(missions.Items[i].Status.Phase)]++
		}
	}
	rtmetrics.Missions.DeletePartialMatch(prometheus.Labels{"table": noTableMetricLabel})
	for phase, n := range phases {
		rtmetrics.Missions.WithLabelValues(noTableMetricLabel, phase).Set(float64(n))
	}
}
