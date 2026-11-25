package controller

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/roundtable-k8s/roundtable/pkg/metrics"
)

func TestDeleteTriggerLastFiredDropsOnlyThatTrigger(t *testing.T) {
	metrics.TriggerLastFiredTimestampSeconds.Reset()
	metrics.TriggerLastFiredTimestampSeconds.WithLabelValues("t1", "gone").Set(1)
	metrics.TriggerLastFiredTimestampSeconds.WithLabelValues("t1", "kept").Set(2)

	deleteTriggerLastFired("gone")

	if n := testutil.CollectAndCount(metrics.TriggerLastFiredTimestampSeconds); n != 1 {
		t.Fatalf("series after delete = %d, want 1", n)
	}
	if v := testutil.ToFloat64(metrics.TriggerLastFiredTimestampSeconds.WithLabelValues("t1", "kept")); v != 2 {
		t.Fatalf("kept series = %v, want 2", v)
	}
}
