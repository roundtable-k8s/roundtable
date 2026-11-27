package controller

import (
	"context"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// A status write that loses an optimistic-lock race must end the reconcile as
// a quiet requeue: no error returned (so no ERROR line, no counted reconcile
// error) and a re-read on the next pass.
func TestRoundTableReconcileStatusConflictRequeuesQuietly(t *testing.T) {
	s := k8sruntime.NewScheme()
	if err := aiv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	rt := &aiv1alpha1.RoundTable{
		ObjectMeta: metav1.ObjectMeta{Name: "rt", Namespace: "default"},
		Spec:       aiv1alpha1.RoundTableSpec{Suspend: true},
	}
	conflicts := 0
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(rt).WithStatusSubresource(rt).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(_ context.Context, _ client.Client, _ string, obj client.Object, _ ...client.SubResourceUpdateOption) error {
				conflicts++
				return apierrors.NewConflict(schema.GroupResource{Group: "ai.roundtable.io", Resource: "roundtables"}, obj.GetName(), nil)
			},
		}).Build()
	r := &RoundTableReconciler{Client: c, Scheme: s, Recorder: record.NewFakeRecorder(10)}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "rt"}})
	if err != nil {
		t.Fatalf("conflict must not surface as a reconcile error, got %v", err)
	}
	if conflicts == 0 {
		t.Fatal("status write was never attempted")
	}
	if res.RequeueAfter != RequeueFast {
		t.Fatalf("want RequeueAfter=%v, got %+v", RequeueFast, res)
	}
}

func TestQuietConflictPassesOtherErrorsThrough(t *testing.T) {
	want := apierrors.NewBadRequest("nope")
	if _, err := quietConflict(context.Background(), ctrl.Result{}, want); err != want {
		t.Fatalf("non-conflict error must pass through, got %v", err)
	}
}

// The Knight reconcile wraps its status write the same way: a conflict on
// the status Update is a quiet requeue, not an error.
func TestKnightReconcileStatusConflictRequeuesQuietly(t *testing.T) {
	s := k8sruntime.NewScheme()
	if err := aiv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	k := &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{Name: "kay", Namespace: "default", Finalizers: []string{knightFinalizer}},
		Spec:       aiv1alpha1.KnightSpec{Suspend: true},
	}
	conflicts := 0
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(k).WithStatusSubresource(k).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(_ context.Context, _ client.Client, _ string, obj client.Object, _ ...client.SubResourceUpdateOption) error {
				conflicts++
				return apierrors.NewConflict(schema.GroupResource{Group: "ai.roundtable.io", Resource: "knights"}, obj.GetName(), nil)
			},
		}).Build()
	r := &KnightReconciler{Client: c, APIReader: c, Scheme: s, Recorder: record.NewFakeRecorder(10)}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "kay"}})
	if err != nil {
		t.Fatalf("conflict must not surface as a reconcile error, got %v", err)
	}
	if conflicts == 0 {
		t.Fatal("status write was never attempted")
	}
	if res.RequeueAfter != RequeueFast {
		t.Fatalf("want RequeueAfter=%v, got %+v", RequeueFast, res)
	}
}
