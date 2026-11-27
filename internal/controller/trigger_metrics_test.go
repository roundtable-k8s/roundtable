package controller

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/roundtable-k8s/roundtable/pkg/metrics"
)

func TestDeleteTriggerLastFiredDropsOnlyThatTrigger(t *testing.T) {
	metrics.TriggerLastFiredTimestampSeconds.Reset()
	metrics.TriggerLastFiredTimestampSeconds.WithLabelValues("ns1", "t1", "gone").Set(1)
	metrics.TriggerLastFiredTimestampSeconds.WithLabelValues("ns1", "t1", "kept").Set(2)
	// Same name in another namespace must survive.
	metrics.TriggerLastFiredTimestampSeconds.WithLabelValues("ns2", "t1", "gone").Set(3)

	deleteTriggerLastFired("ns1", "gone")

	if n := testutil.CollectAndCount(metrics.TriggerLastFiredTimestampSeconds); n != 2 {
		t.Fatalf("series after delete = %d, want 2", n)
	}
	if v := testutil.ToFloat64(metrics.TriggerLastFiredTimestampSeconds.WithLabelValues("ns1", "t1", "kept")); v != 2 {
		t.Fatalf("kept series = %v, want 2", v)
	}
	if v := testutil.ToFloat64(metrics.TriggerLastFiredTimestampSeconds.WithLabelValues("ns2", "t1", "gone")); v != 3 {
		t.Fatalf("other-namespace series = %v, want 3", v)
	}
}
