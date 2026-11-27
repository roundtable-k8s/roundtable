package controller

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func TestKnightChangedForTable(t *testing.T) {
	base := func() *aiv1alpha1.Knight {
		return &aiv1alpha1.Knight{ObjectMeta: metav1.ObjectMeta{Name: "k", Generation: 1, Labels: map[string]string{"a": "b"}}}
	}
	mut := func(f func(k *aiv1alpha1.Knight)) *aiv1alpha1.Knight { k := base(); f(k); return k }
	cond := func(status metav1.ConditionStatus, reason string) func(k *aiv1alpha1.Knight) {
		return func(k *aiv1alpha1.Knight) {
			meta.SetStatusCondition(&k.Status.Conditions, metav1.Condition{
				Type: aiv1alpha1.ConditionKnightAvailable, Status: status, Reason: reason})
		}
	}
	now := metav1.NewTime(time.Now())
	tests := []struct {
		name string
		old  *aiv1alpha1.Knight
		new  *aiv1alpha1.Knight
		want bool
	}{
		{"spec change (generation)", base(), mut(func(k *aiv1alpha1.Knight) { k.Generation = 2 }), true},
		{"label change", base(), mut(func(k *aiv1alpha1.Knight) { k.Labels = map[string]string{"a": "c"} }), true},
		{"phase change", base(), mut(func(k *aiv1alpha1.Knight) { k.Status.Phase = aiv1alpha1.KnightPhaseSuspended }), true},
		{"ready change", base(), mut(func(k *aiv1alpha1.Knight) { k.Status.Ready = true }), true},
		{"readyReplicas change", base(), mut(func(k *aiv1alpha1.Knight) { k.Status.ReadyReplicas = 1 }), true},
		{"model change", base(), mut(func(k *aiv1alpha1.Knight) { k.Status.Model = "m" }), true},
		{"available appears", base(), mut(cond(metav1.ConditionTrue, "Ready")), true},
		{"available reason change", mut(cond(metav1.ConditionFalse, "Waking")), mut(cond(metav1.ConditionFalse, "ScaledToZero")), true},
		{"available status change", mut(cond(metav1.ConditionFalse, "X")), mut(cond(metav1.ConditionTrue, "X")), true},
		{"ignored status fields", base(), mut(func(k *aiv1alpha1.Knight) {
			k.Status.TasksCompleted = 5
			k.Status.TotalCost = "1.00"
			k.Status.LastTaskAt = &now
		}), false},
		{"available message/time only", mut(cond(metav1.ConditionTrue, "Ready")), mut(func(k *aiv1alpha1.Knight) {
			cond(metav1.ConditionTrue, "Ready")(k)
			k.Status.Conditions[0].Message = "other"
			k.Status.Conditions[0].LastTransitionTime = now
		}), false},
		{"no change", base(), base(), false},
	}
	p := knightChangedForTable()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := p.Update(event.UpdateEvent{ObjectOld: tc.old, ObjectNew: tc.new}); got != tc.want {
				t.Errorf("Update = %v, want %v", got, tc.want)
			}
		})
	}
	if !p.Create(event.CreateEvent{Object: base()}) {
		t.Error("create must pass")
	}
	if !p.Delete(event.DeleteEvent{Object: base()}) {
		t.Error("delete must pass")
	}
}

func TestReconcileDeletedTableClearsSkillsQueriedAt(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := aiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	r := &RoundTableReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
		Scheme: scheme,
		skillsQueriedAt: map[string]time.Time{
			"ns/gone": time.Now(),
			"ns/kept": time.Now(),
		},
	}
	req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "gone"}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.skillsQueriedAt["ns/gone"]; ok {
		t.Error("deleted table's skillsQueriedAt entry not cleared")
	}
	if _, ok := r.skillsQueriedAt["ns/kept"]; !ok {
		t.Error("other table's entry must remain")
	}
}
