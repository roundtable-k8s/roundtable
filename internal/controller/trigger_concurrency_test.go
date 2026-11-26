package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// The CRD's CEL rules refuse Replace with a knight or mission target on
// apply; reconcileSource still reports it as not Ready for objects that were
// stored before the rule existed. The check runs before any API call.
func TestReconcileSourceReplaceNotReady(t *testing.T) {
	targets := map[string]aiv1alpha1.TriggerTarget{
		"knight":  {Knight: &aiv1alpha1.TriggerKnightTarget{}},
		"mission": {Mission: &aiv1alpha1.TriggerMissionTarget{}},
	}
	for kind, target := range targets {
		r := &TriggerReconciler{
			consumers: newTriggerConsumers(logf.Log),
			pollers:   newTriggerPollers(logf.Log),
		}
		trg := &aiv1alpha1.Trigger{
			ObjectMeta: metav1.ObjectMeta{Name: "t", Namespace: "ns"},
			Spec: aiv1alpha1.TriggerSpec{
				Source: aiv1alpha1.TriggerSource{Schedule: &aiv1alpha1.TriggerScheduleSource{Cron: "* * * * *"}},
				Target: target,
			},
		}
		trg.Spec.Concurrency.Policy = aiv1alpha1.TriggerConcurrencyReplace
		_, cond, err := r.reconcileSource(context.Background(), trg, time.Now())
		if err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
		if cond.Status != metav1.ConditionFalse || cond.Reason != aiv1alpha1.ReasonTriggerInvalidSpec {
			t.Errorf("%s: condition = %s/%s, want False/InvalidSpec", kind, cond.Status, cond.Reason)
		}
		if want := "spec.concurrency.policy Replace is not supported with target." + kind; !strings.Contains(cond.Message, want) {
			t.Errorf("%s: message %q lacks %q", kind, cond.Message, want)
		}
	}
}
