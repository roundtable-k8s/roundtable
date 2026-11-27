/*
Copyright 2026 dapperdivers.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/cli-utils/pkg/kstatus/status"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
)

func knightKstatusOf(t *testing.T, k *aiv1alpha1.Knight) status.Status {
	t.Helper()
	m, err := k8sruntime.DefaultUnstructuredConverter.ToUnstructured(k)
	if err != nil {
		t.Fatal(err)
	}
	u := &unstructured.Unstructured{Object: m}
	u.SetAPIVersion(aiv1alpha1.GroupVersion.String())
	u.SetKind("Knight")
	res, err := status.Compute(u)
	if err != nil {
		t.Fatal(err)
	}
	return res.Status
}

func knightKstatusScheme(t *testing.T) *k8sruntime.Scheme {
	t.Helper()
	s := k8sruntime.NewScheme()
	for _, add := range []func(*k8sruntime.Scheme) error{corev1.AddToScheme, appsv1.AddToScheme, aiv1alpha1.AddToScheme} {
		if err := add(s); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func kstatusTestKnight(generation, observed int64) *aiv1alpha1.Knight {
	return &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{Name: "k", Namespace: "default", Generation: generation, Finalizers: []string{knightFinalizer}},
		Spec: aiv1alpha1.KnightSpec{
			Domain: "ops", Model: "claude-sonnet-4",
			NATS: aiv1alpha1.KnightNATS{Stream: "s", Subjects: []string{"a.>"}},
		},
		Status: aiv1alpha1.KnightStatus{Phase: aiv1alpha1.KnightPhaseReady, ObservedGeneration: observed},
	}
}

// TestKnightKstatus checks what Helm's --wait and Flux health checks see: a
// Knight whose spec the operator has accepted is Current whatever phase it
// reaches (Provisioning, Ready, scaled to zero, waking, Degraded), including
// right after a chart upgrade bumped metadata.generation.
func TestKnightKstatus(t *testing.T) {
	var one, zero int32 = 1, 0
	cases := []struct {
		name      string
		replicas  int32
		ready     int32
		autoscale bool
		pending   uint64
		reconErr  error
		wantPhase aiv1alpha1.KnightPhase
		wantAvail string
	}{
		{name: "provisioning", replicas: one, ready: 0, wantPhase: aiv1alpha1.KnightPhaseProvisioning, wantAvail: aiv1alpha1.ReasonKnightProvisioning},
		{name: "ready", replicas: one, ready: 1, wantPhase: aiv1alpha1.KnightPhaseReady},
		{name: "scaled to zero", replicas: zero, autoscale: true, wantPhase: aiv1alpha1.KnightPhaseSuspended, wantAvail: aiv1alpha1.ReasonKnightScaledToZero},
		{name: "waking", replicas: zero, autoscale: true, pending: 1, wantPhase: aiv1alpha1.KnightPhaseSuspended, wantAvail: aiv1alpha1.ReasonKnightWaking},
		{name: "reconcile error", replicas: one, reconErr: errors.New("boom"), wantPhase: aiv1alpha1.KnightPhaseDegraded, wantAvail: aiv1alpha1.ReasonKnightReconcileError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := knightKstatusScheme(t)
			k := kstatusTestKnight(2, 1)
			if got := knightKstatusOf(t, k); got != status.InProgressStatus {
				t.Fatalf("unobserved spec: kstatus = %s, want InProgress", got)
			}
			d := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Name: "k", Namespace: "default"},
				Spec:       appsv1.DeploymentSpec{Replicas: &tc.replicas},
				Status:     appsv1.DeploymentStatus{Replicas: tc.replicas, ReadyReplicas: tc.ready},
			}
			c := fake.NewClientBuilder().WithScheme(s).WithObjects(k, d).WithStatusSubresource(k).Build()
			r := &KnightReconciler{Client: c, Scheme: s, Recorder: record.NewFakeRecorder(100), PendingSource: &fakePending{n: tc.pending}}
			if err := r.updateStatus(context.Background(), k, knightpkg.Scaling{Autoscaled: tc.autoscale}, tc.reconErr); err != nil {
				t.Fatalf("updateStatus: %v", err)
			}
			assertKnightCurrent(t, c, tc.wantPhase, tc.wantAvail)
		})
	}
}

// TestKnightKstatusReconcileError runs the full Reconcile in an environment
// where it fails (no NATS). The Knight ends Degraded, but the spec was
// accepted, so it must still read Current.
func TestKnightKstatusReconcileError(t *testing.T) {
	s := knightKstatusScheme(t)
	k := kstatusTestKnight(2, 1)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(k).WithStatusSubresource(k).Build()
	r := &KnightReconciler{Client: c, Scheme: s, Recorder: record.NewFakeRecorder(100), DefaultImage: "ghcr.io/roundtable-k8s/claude-knight:test"}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "k", Namespace: "default"}}); err == nil {
		t.Fatal("expected the reconcile to fail without NATS")
	}
	assertKnightCurrent(t, c, aiv1alpha1.KnightPhaseDegraded, aiv1alpha1.ReasonKnightReconcileError)
}

// TestKnightKstatusSuspended runs the full Reconcile of a suspended Knight.
func TestKnightKstatusSuspended(t *testing.T) {
	s := knightKstatusScheme(t)
	k := kstatusTestKnight(2, 1)
	k.Spec.Suspend = true
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(k).WithStatusSubresource(k).Build()
	r := &KnightReconciler{Client: c, Scheme: s, Recorder: record.NewFakeRecorder(100), DefaultImage: "ghcr.io/roundtable-k8s/claude-knight:test"}
	_, _ = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "k", Namespace: "default"}})
	assertKnightCurrent(t, c, aiv1alpha1.KnightPhaseSuspended, "")
}

func assertKnightCurrent(t *testing.T, c client.Client, wantPhase aiv1alpha1.KnightPhase, wantAvail string) {
	t.Helper()
	got := &aiv1alpha1.Knight{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "k", Namespace: "default"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != wantPhase {
		t.Fatalf("phase = %s, want %s", got.Status.Phase, wantPhase)
	}
	if wantAvail != "" && availReason(got) != wantAvail {
		t.Errorf("Available reason = %q, want %q", availReason(got), wantAvail)
	}
	if got.Status.ObservedGeneration != got.Generation {
		t.Errorf("observedGeneration = %d, want %d", got.Status.ObservedGeneration, got.Generation)
	}
	if s := knightKstatusOf(t, got); s != status.CurrentStatus {
		t.Errorf("phase %s: kstatus = %s, want Current", got.Status.Phase, s)
	}
	for _, cond := range got.Status.Conditions {
		if cond.Type == "Reconciling" || cond.Type == "Stalled" {
			t.Errorf("unexpected kstatus condition %s", cond.Type)
		}
	}
}
