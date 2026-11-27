package controller

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	nodev1 "k8s.io/api/node/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
)

func statusTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = aiv1alpha1.AddToScheme(s)
	return s
}

func newParkedKnight(model string) *aiv1alpha1.Knight {
	rc := "gvisor"
	return &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{Name: "parked", Namespace: "default", Finalizers: []string{knightFinalizer}},
		Spec: aiv1alpha1.KnightSpec{
			Model: model, RuntimeClassName: &rc,
			NATS: aiv1alpha1.KnightNATS{Stream: "rt_tasks", URL: "nats://nats.test:4222", Subjects: []string{"t.>"}},
		},
		Status: aiv1alpha1.KnightStatus{Phase: aiv1alpha1.KnightPhaseProvisioning},
	}
}

func reconcileByName(t *testing.T, r *KnightReconciler, name string) reconcile.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: "default"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	return res
}

func TestParkedKnightPicksUpNewModel(t *testing.T) {
	s := statusTestScheme()
	k := newParkedKnight("model-a")
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(k).WithStatusSubresource(k).Build()
	r := &KnightReconciler{Client: c, Scheme: s, Recorder: record.NewFakeRecorder(50)}

	reconcileByName(t, r, "parked")
	got := &aiv1alpha1.Knight{}
	_ = c.Get(context.Background(), clientKey(k), got)
	if got.Status.Model != "model-a" || availReason(got) != aiv1alpha1.ReasonRuntimeClassNotFound {
		t.Fatalf("model=%q reason=%q, want model-a/RuntimeClassNotFound", got.Status.Model, availReason(got))
	}

	got.Spec.Model = "model-b"
	if err := c.Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	reconcileByName(t, r, "parked")
	_ = c.Get(context.Background(), clientKey(k), got)
	if got.Status.Model != "model-b" {
		t.Errorf("status.model = %q while parked, want model-b", got.Status.Model)
	}
}

func TestSuspendedKnightDropsStaleRuntimeAvailableFalse(t *testing.T) {
	s := statusTestScheme()
	k := newParkedKnight("m")
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(k).WithStatusSubresource(k).Build()
	r := &KnightReconciler{Client: c, Scheme: s, Recorder: record.NewFakeRecorder(50)}
	reconcileByName(t, r, "parked")

	got := &aiv1alpha1.Knight{}
	_ = c.Get(context.Background(), clientKey(k), got)
	if rc := meta.FindStatusCondition(got.Status.Conditions, aiv1alpha1.ConditionKnightRuntimeAvailable); rc == nil || rc.Status != metav1.ConditionFalse {
		t.Fatalf("precondition: RuntimeAvailable = %+v, want False", rc)
	}
	got.Spec.Suspend = true
	if err := c.Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	reconcileByName(t, r, "parked")
	_ = c.Get(context.Background(), clientKey(k), got)
	if got.Status.Phase != aiv1alpha1.KnightPhaseSuspended {
		t.Fatalf("phase = %q, want Suspended", got.Status.Phase)
	}
	if rc := meta.FindStatusCondition(got.Status.Conditions, aiv1alpha1.ConditionKnightRuntimeAvailable); rc == nil || rc.Status != metav1.ConditionTrue {
		t.Errorf("RuntimeAvailable = %+v after suspend, want True", rc)
	}
}

func TestWakingPastCapChangesReason(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := base
	s := statusTestScheme()
	zero := int32(0)
	k := &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{Name: "stuck", Namespace: "default"},
		Spec:       aiv1alpha1.KnightSpec{NATS: aiv1alpha1.KnightNATS{Stream: "rt_tasks"}},
	}
	d := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "stuck", Namespace: "default"},
		Spec:       appsv1.DeploymentSpec{Replicas: &zero},
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(k, d).WithStatusSubresource(k, d).Build()
	r := &KnightReconciler{Client: c, Scheme: s, Recorder: record.NewFakeRecorder(50),
		PendingSource: &fakePending{n: 2}, Now: func() time.Time { return clock }}
	update := func(now time.Time) {
		t.Helper()
		clock = now
		if err := r.updateStatus(context.Background(), k, knightpkg.Scaling{Autoscaled: true}, nil); err != nil {
			t.Fatal(err)
		}
	}
	update(base)
	if availReason(k) != aiv1alpha1.ReasonKnightWaking {
		t.Fatalf("reason = %q, want Waking", availReason(k))
	}
	update(base.Add(knightWakeTimeout - time.Second))
	if availReason(k) != aiv1alpha1.ReasonKnightWaking {
		t.Fatalf("reason = %q before the cap, want Waking", availReason(k))
	}
	update(base.Add(knightWakeTimeout + time.Second))
	if availReason(k) != aiv1alpha1.ReasonKnightWakeTimeout || k.Status.Phase != aiv1alpha1.KnightPhaseDegraded {
		t.Fatalf("reason=%q phase=%q past the cap, want WakeTimeout/Degraded", availReason(k), k.Status.Phase)
	}
	// Stays timed out while tasks are still queued (no flapping back to Waking).
	update(base.Add(knightWakeTimeout + time.Minute))
	if availReason(k) != aiv1alpha1.ReasonKnightWakeTimeout {
		t.Fatalf("reason = %q, want WakeTimeout to stick", availReason(k))
	}
}

func TestWakingKnightRequeuesEvery5s(t *testing.T) {
	s := statusTestScheme()
	zero := int32(0)
	k := &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{Name: "waker", Namespace: "default", Finalizers: []string{knightFinalizer}},
		Spec: aiv1alpha1.KnightSpec{NATS: aiv1alpha1.KnightNATS{Stream: "rt_tasks", URL: "nats://nats.test:4222", Subjects: []string{"t.>"}},
			Lifecycle: &aiv1alpha1.KnightLifecycle{SuspendPolicy: knightpkg.SuspendPolicyAuto}},
	}
	d := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "waker", Namespace: "default"},
		Spec:       appsv1.DeploymentSpec{Replicas: &zero},
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(k, d).WithStatusSubresource(k, d).Build()
	r := &KnightReconciler{Client: c, Scheme: s, Recorder: record.NewFakeRecorder(100), PendingSource: &fakePending{n: 3}, KEDAEnabled: true}
	res := reconcileByName(t, r, "waker")
	got := &aiv1alpha1.Knight{}
	_ = c.Get(context.Background(), clientKey(k), got)
	if availReason(got) != aiv1alpha1.ReasonKnightWaking {
		t.Fatalf("reason = %q, want Waking", availReason(got))
	}
	if res.RequeueAfter != wakingRequeue {
		t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, wakingRequeue)
	}
}

func TestKnightFinalizerRemovalUsesPatch(t *testing.T) {
	s := statusTestScheme()
	now := metav1.Now()
	k := newParkedKnight("m")
	k.DeletionTimestamp = &now
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(k).WithStatusSubresource(k).Build()
	r := &KnightReconciler{Client: c, Scheme: s, Recorder: record.NewFakeRecorder(10)}
	reconcileByName(t, r, "parked")
	got := &aiv1alpha1.Knight{}
	err := c.Get(context.Background(), clientKey(k), got)
	if err == nil && controllerutil.ContainsFinalizer(got, knightFinalizer) {
		t.Errorf("finalizer not removed")
	}
}

func TestRuntimeClassKnightRequests(t *testing.T) {
	s := statusTestScheme()
	gv, other := "gvisor", "kata"
	a := &aiv1alpha1.Knight{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "default"}, Spec: aiv1alpha1.KnightSpec{RuntimeClassName: &gv}}
	b := &aiv1alpha1.Knight{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "default"}, Spec: aiv1alpha1.KnightSpec{RuntimeClassName: &other}}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(a, b).Build()
	r := &KnightReconciler{Client: c, Scheme: s}
	reqs := r.runtimeClassKnightRequests(context.Background(), &nodev1.RuntimeClass{ObjectMeta: metav1.ObjectMeta{Name: "gvisor"}})
	if len(reqs) != 1 || reqs[0].Name != "a" {
		t.Errorf("requests = %v, want only knight a", reqs)
	}
}

func clientKey(k *aiv1alpha1.Knight) types.NamespacedName {
	return types.NamespacedName{Name: k.Name, Namespace: k.Namespace}
}

type countingPending struct {
	n     uint64
	calls int
}

func (f *countingPending) ConsumerPending(context.Context, string, string) (uint64, error) {
	f.calls++
	return f.n, nil
}

func TestWakeTimeoutSticksAtOneReplicaAndBacksOff(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := base
	s := statusTestScheme()
	zero, one := int32(0), int32(1)
	k := &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{Name: "stuck", Namespace: "default"},
		Spec:       aiv1alpha1.KnightSpec{NATS: aiv1alpha1.KnightNATS{Stream: "rt_tasks"}},
	}
	d := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "stuck", Namespace: "default"},
		Spec:       appsv1.DeploymentSpec{Replicas: &zero},
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(k, d).WithStatusSubresource(k, d).Build()
	src := &countingPending{n: 2}
	r := &KnightReconciler{Client: c, Scheme: s, Recorder: record.NewFakeRecorder(50),
		PendingSource: src, Now: func() time.Time { return clock }}
	update := func(now time.Time) {
		t.Helper()
		clock = now
		if err := r.updateStatus(context.Background(), k, knightpkg.Scaling{Autoscaled: true}, nil); err != nil {
			t.Fatal(err)
		}
	}
	update(base)
	update(base.Add(knightWakeTimeout + time.Second))
	if availReason(k) != aiv1alpha1.ReasonKnightWakeTimeout {
		t.Fatalf("reason = %q, want WakeTimeout", availReason(k))
	}
	// KEDA scales to 1 but the pod never gets ready.
	d.Spec.Replicas = &one
	if err := c.Update(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	t0 := base.Add(knightWakeTimeout + 2*time.Second)
	update(t0)
	if availReason(k) != aiv1alpha1.ReasonKnightWakeTimeout || k.Status.Phase != aiv1alpha1.KnightPhaseDegraded {
		t.Fatalf("reason=%q phase=%q at 1 replica, want WakeTimeout/Degraded", availReason(k), k.Status.Phase)
	}
	if got := r.wakeBackoffDelay(client.ObjectKeyFromObject(k)); got <= wakingRequeue || got > time.Minute {
		t.Errorf("backoff delay = %v, want in (%v, 1m]", got, wakingRequeue)
	}
	// Inside the backoff window there is no pending lookup and the reason sticks.
	calls := src.calls
	update(t0.Add(time.Second))
	if src.calls != calls {
		t.Errorf("pending lookups grew %d -> %d inside the backoff window", calls, src.calls)
	}
	if availReason(k) != aiv1alpha1.ReasonKnightWakeTimeout {
		t.Errorf("reason = %q inside backoff, want WakeTimeout", availReason(k))
	}
	// Delay grows to the bound and stays there.
	var last time.Duration
	now := t0
	for range 6 {
		now = now.Add(2 * time.Minute)
		update(now)
		last = r.wakeBackoffDelay(client.ObjectKeyFromObject(k))
	}
	if last != time.Minute {
		t.Errorf("final backoff = %v, want 1m bound", last)
	}
}

func TestParkedModelChangeRaisesNoSecondEvent(t *testing.T) {
	s := statusTestScheme()
	k := newParkedKnight("model-a")
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(k).WithStatusSubresource(k).Build()
	rec := record.NewFakeRecorder(50)
	r := &KnightReconciler{Client: c, Scheme: s, Recorder: rec}
	reconcileByName(t, r, "parked")
	drain := func() (n int) {
		for {
			select {
			case <-rec.Events:
				n++
			default:
				return
			}
		}
	}
	if n := drain(); n != 1 {
		t.Fatalf("events after first park = %d, want 1", n)
	}
	got := &aiv1alpha1.Knight{}
	_ = c.Get(context.Background(), clientKey(k), got)
	got.Spec.Model = "model-b"
	got.Generation++
	if err := c.Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	reconcileByName(t, r, "parked")
	_ = c.Get(context.Background(), clientKey(k), got)
	if got.Status.Model != "model-b" {
		t.Errorf("status.model = %q, want model-b", got.Status.Model)
	}
	if n := drain(); n != 0 {
		t.Errorf("events after model change = %d, want 0", n)
	}
}
