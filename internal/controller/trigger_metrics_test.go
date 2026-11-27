package controller

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/roundtable-k8s/roundtable/pkg/metrics"
)

func TestDeleteTriggerLastFiredDropsOnlyThatTrigger(t *testing.T) {
	// Unique label values keep this independent of other tests sharing the
	// global vector (no Reset, which is order-dependent under -shuffle).
	metrics.TriggerLastFiredTimestampSeconds.WithLabelValues("del-ns1", "del-t1", "gone").Set(1)
	metrics.TriggerLastFiredTimestampSeconds.WithLabelValues("del-ns1", "del-t1", "kept").Set(2)
	// Same name in another namespace must survive.
	metrics.TriggerLastFiredTimestampSeconds.WithLabelValues("del-ns2", "del-t1", "gone").Set(3)

	before := testutil.CollectAndCount(metrics.TriggerLastFiredTimestampSeconds)

	deleteTriggerLastFired("del-ns1", "gone")

	if n := testutil.CollectAndCount(metrics.TriggerLastFiredTimestampSeconds); n != before-1 {
		t.Fatalf("series after delete = %d, want %d", n, before-1)
	}
	if v := testutil.ToFloat64(metrics.TriggerLastFiredTimestampSeconds.WithLabelValues("del-ns1", "del-t1", "kept")); v != 2 {
		t.Fatalf("kept series = %v, want 2", v)
	}
	if v := testutil.ToFloat64(metrics.TriggerLastFiredTimestampSeconds.WithLabelValues("del-ns2", "del-t1", "gone")); v != 3 {
		t.Fatalf("other-namespace series = %v, want 3", v)
	}
}
