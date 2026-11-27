package controller

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
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
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
	rtmetrics "github.com/roundtable-k8s/roundtable/pkg/metrics"
)

func TestDeleteKnightWakeMetric(t *testing.T) {
	k := &aiv1alpha1.Knight{ObjectMeta: metav1.ObjectMeta{Name: "wake-gone", Namespace: "wake-ns"}}
	keep := &aiv1alpha1.Knight{ObjectMeta: metav1.ObjectMeta{Name: "wake-kept", Namespace: "wake-ns"}}
	for _, kn := range []*aiv1alpha1.Knight{k, keep} {
		rtmetrics.KnightWakeSeconds.WithLabelValues(knightpkg.KnightTable(kn), kn.Name, kn.Namespace).Observe(3)
	}
	deleteKnightWakeMetric(k)
	if rtmetrics.KnightWakeSeconds.DeleteLabelValues(knightpkg.KnightTable(k), k.Name, k.Namespace) {
		t.Error("wake series of the deleted knight still exists")
	}
	if !rtmetrics.KnightWakeSeconds.DeleteLabelValues(knightpkg.KnightTable(keep), keep.Name, keep.Namespace) {
		t.Error("wake series of another knight was deleted")
	}
}

func TestRoundTableNotFoundDeletesTableMetrics(t *testing.T) {
	s := k8sruntime.NewScheme()
	if err := aiv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	rtmetrics.Knights.WithLabelValues("gone-table", "Ready").Set(2)
	rtmetrics.Missions.WithLabelValues("gone-table", "Active").Set(1)
	rtmetrics.TableCostUSD.WithLabelValues("gone-table").Set(4)
	rtmetrics.TableCostUSD.WithLabelValues("other-table").Set(1)
	rtmetrics.DeadLettersOpen.WithLabelValues("gone-table", "k1").Set(3)
	rtmetrics.KnightConsumerReady.WithLabelValues("gone-table", "k1").Set(1)
	rtmetrics.KnightIsolated.WithLabelValues("gone-table", "k1", "Isolated").Set(1)
	rtmetrics.DeadLettersOpen.WithLabelValues("other-table", "k1").Set(3)

	r := &RoundTableReconciler{Client: fake.NewClientBuilder().WithScheme(s).Build(), Scheme: s, Recorder: record.NewFakeRecorder(10)}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "gone-table"}}); err != nil {
		t.Fatal(err)
	}
	if rtmetrics.Knights.DeleteLabelValues("gone-table", "Ready") ||
		rtmetrics.Missions.DeleteLabelValues("gone-table", "Active") ||
		rtmetrics.TableCostUSD.DeleteLabelValues("gone-table") {
		t.Error("series of the deleted table still exist")
	}
	if rtmetrics.DeadLettersOpen.DeleteLabelValues("gone-table", "k1") ||
		rtmetrics.KnightConsumerReady.DeleteLabelValues("gone-table", "k1") ||
		rtmetrics.KnightIsolated.DeleteLabelValues("gone-table", "k1", "Isolated") {
		t.Error("dead-letter, consumer or isolation series of the deleted table still exist")
	}
	if !rtmetrics.DeadLettersOpen.DeleteLabelValues("other-table", "k1") {
		t.Error("dead-letter series of another table were deleted")
	}
	if !rtmetrics.TableCostUSD.DeleteLabelValues("other-table") {
		t.Error("series of another table were deleted")
	}
}

// A status-update conflict on the terminal write must not count the run;
// the retry that persists it counts it exactly once.
func TestChainRunCountedOnceAcrossStatusConflict(t *testing.T) {
	ctx := context.Background()
	s := k8sruntime.NewScheme()
	if err := aiv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	chain := &aiv1alpha1.Chain{
		ObjectMeta: metav1.ObjectMeta{Name: "conflict-chain", Namespace: "default"},
		Spec:       aiv1alpha1.ChainSpec{RoundTableRef: aiv1alpha1.LocalRef("conflict-table")},
		Status: aiv1alpha1.ChainStatus{
			Phase: aiv1alpha1.ChainPhaseRunning,
			StepStatuses: []aiv1alpha1.ChainStepStatus{
				{Name: "a", Phase: aiv1alpha1.ChainStepPhaseSucceeded, TaskID: "t1"},
			},
		},
	}
	conflicts := 1
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(chain).WithStatusSubresource(chain).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if conflicts > 0 {
					conflicts--
					return apierrors.NewConflict(schema.GroupResource{Group: "ai.roundtable.io", Resource: "chains"}, obj.GetName(), nil)
				}
				return cl.SubResource(sub).Update(ctx, obj, opts...)
			},
		}).Build()
	r := &ChainReconciler{Client: c, Recorder: record.NewFakeRecorder(10)}

	attempt := func() {
		fresh := &aiv1alpha1.Chain{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(chain), fresh); err != nil {
			t.Fatal(err)
		}
		baseRV := fresh.ResourceVersion
		actx, pending := withPendingRunMetrics(ctx)
		if _, err := r.finishRun(actx, fresh, &chainRun{specMap: map[string]*aiv1alpha1.ChainStep{}}); err != nil {
			t.Fatal(err)
		}
		pending.flushIfPersisted(actx, r, fresh, baseRV)
	}
	count := func() float64 {
		return testutil.ToFloat64(rtmetrics.ChainRunsTotal.WithLabelValues("conflict-table", "conflict-chain", "succeeded"))
	}

	attempt()
	if got := count(); got != 0 {
		t.Fatalf("run counted %v times after a conflicting write, want 0", got)
	}
	attempt()
	if got := count(); got != 1 {
		t.Fatalf("run counted %v times after the persisted write, want 1", got)
	}
}

// Creating a RoundTable that a Mission already names moves the Mission from
// table="none" to the table without waiting for the Mission's own reconcile.
func TestRoundTableCreationRecountsMissions(t *testing.T) {
	ctx := context.Background()
	s := k8sruntime.NewScheme()
	if err := aiv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	m := &aiv1alpha1.Mission{
		ObjectMeta: metav1.ObjectMeta{Name: "late-m", Namespace: "late-ns"},
		Spec:       aiv1alpha1.MissionSpec{RoundTableRef: aiv1alpha1.LocalRef("late-table")},
		Status:     aiv1alpha1.MissionStatus{Phase: aiv1alpha1.MissionPhaseActive},
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(m).WithStatusSubresource(&aiv1alpha1.RoundTable{}).Build()
	r := &RoundTableReconciler{Client: c, Scheme: s, Recorder: record.NewFakeRecorder(50)}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "late-ns", Name: "late-table"}}

	// The table does not exist yet: the mission is counted under "none".
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(rtmetrics.Missions.WithLabelValues("none", "Active")); got != 1 {
		t.Fatalf("none/Active = %v, want 1", got)
	}

	rt := &aiv1alpha1.RoundTable{ObjectMeta: metav1.ObjectMeta{Name: "late-table", Namespace: "late-ns"}}
	if err := c.Create(ctx, rt); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(rtmetrics.Missions.WithLabelValues("none", "Active")); got != 0 {
		t.Errorf("none/Active = %v after table creation, want 0", got)
	}
	if got := testutil.ToFloat64(rtmetrics.Missions.WithLabelValues("late-table", "Active")); got != 1 {
		t.Errorf("late-table/Active = %v, want 1", got)
	}
}
