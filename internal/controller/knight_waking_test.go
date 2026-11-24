package controller

import (
	"context"
	"errors"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
)

type fakePending struct {
	n        uint64
	err      error
	stream   string
	consumer string
}

func (f *fakePending) ConsumerPending(stream, consumer string) (uint64, error) {
	f.stream, f.consumer = stream, consumer
	return f.n, f.err
}

func wakingStatus(t *testing.T, replicas, ready int32, prior aiv1alpha1.KnightPhase, src ConsumerPendingSource) *aiv1alpha1.Knight {
	t.Helper()
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = aiv1alpha1.AddToScheme(s)
	k := &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{Name: "waker", Namespace: "default"},
		Spec:       aiv1alpha1.KnightSpec{NATS: aiv1alpha1.KnightNATS{Stream: "rt_tasks"}},
		Status:     aiv1alpha1.KnightStatus{Phase: prior},
	}
	d := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "waker", Namespace: "default"},
		Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
		Status:     appsv1.DeploymentStatus{Replicas: replicas, ReadyReplicas: ready},
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(k, d).WithStatusSubresource(k).Build()
	r := &KnightReconciler{Client: c, Scheme: s, Recorder: record.NewFakeRecorder(10), PendingSource: src}
	if err := r.updateStatus(context.Background(), k, knightpkg.Scaling{Autoscaled: true}, nil); err != nil {
		t.Fatalf("updateStatus: %v", err)
	}
	return k
}

func availReason(k *aiv1alpha1.Knight) string {
	c := meta.FindStatusCondition(k.Status.Conditions, aiv1alpha1.ConditionKnightAvailable)
	if c == nil {
		return ""
	}
	return c.Reason
}

func TestKnightWakingStatus(t *testing.T) {
	tests := []struct {
		name      string
		replicas  int32
		ready     int32
		prior     aiv1alpha1.KnightPhase
		pending   uint64
		err       error
		wantPhase aiv1alpha1.KnightPhase
		want      string
	}{
		{"zero replicas, nothing pending", 0, 0, "", 0, nil, aiv1alpha1.KnightPhaseSuspended, aiv1alpha1.ReasonKnightScaledToZero},
		{"zero replicas, tasks pending", 0, 0, "", 3, nil, aiv1alpha1.KnightPhaseSuspended, aiv1alpha1.ReasonKnightWaking},
		{"zero replicas, lookup fails", 0, 0, "", 3, errors.New("boom"), aiv1alpha1.KnightPhaseSuspended, aiv1alpha1.ReasonKnightScaledToZero},
		{"pod starting after sleep, tasks pending", 1, 0, aiv1alpha1.KnightPhaseSuspended, 1, nil, aiv1alpha1.KnightPhaseSuspended, aiv1alpha1.ReasonKnightWaking},
		{"pod starting after sleep, queue drained", 1, 0, aiv1alpha1.KnightPhaseSuspended, 0, nil, aiv1alpha1.KnightPhaseSuspended, aiv1alpha1.ReasonKnightScaledToZero},
		{"first provisioning is not waking", 1, 0, aiv1alpha1.KnightPhaseProvisioning, 5, nil, aiv1alpha1.KnightPhaseProvisioning, aiv1alpha1.ReasonKnightProvisioning},
		{"one ready replica", 1, 1, aiv1alpha1.KnightPhaseSuspended, 5, nil, aiv1alpha1.KnightPhaseReady, aiv1alpha1.ReasonKnightReady},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := &fakePending{n: tt.pending, err: tt.err}
			k := wakingStatus(t, tt.replicas, tt.ready, tt.prior, src)
			if got := availReason(k); got != tt.want {
				t.Errorf("reason = %q, want %q", got, tt.want)
			}
			if k.Status.Phase != tt.wantPhase {
				t.Errorf("phase = %q, want %q", k.Status.Phase, tt.wantPhase)
			}
		})
	}
}

func TestKnightWakingReadsCapitalisedConsumer(t *testing.T) {
	src := &fakePending{n: 1}
	wakingStatus(t, 0, 0, "", src)
	if src.stream != "rt_tasks" || src.consumer != "Waker-consumer" {
		t.Errorf("looked up %s/%s, want rt_tasks/Waker-consumer", src.stream, src.consumer)
	}
}
