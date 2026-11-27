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
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
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

// TestKnightKstatus checks what Helm's --wait and Flux health checks see: a
// Knight whose spec the operator has accepted is Current whatever its phase
// (provisioning, ready, degraded, suspended), including right after a chart
// upgrade bumped metadata.generation.
func TestKnightKstatus(t *testing.T) {
	cases := []struct {
		name  string
		build func(k *aiv1alpha1.Knight)
		// deploy, when set, is created alongside the Knight.
		deploy *appsv1.Deployment
	}{
		{name: "provisioning", build: func(*aiv1alpha1.Knight) {}},
		{name: "ready", build: func(*aiv1alpha1.Knight) {}, deploy: &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "k", Namespace: "default"},
			Status:     appsv1.DeploymentStatus{Replicas: 1, ReadyReplicas: 1},
		}},
		{name: "suspended", build: func(k *aiv1alpha1.Knight) { k.Spec.Suspend = true }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := k8sruntime.NewScheme()
			for _, add := range []func(*k8sruntime.Scheme) error{corev1.AddToScheme, appsv1.AddToScheme, aiv1alpha1.AddToScheme} {
				if err := add(s); err != nil {
					t.Fatal(err)
				}
			}
			k := &aiv1alpha1.Knight{
				ObjectMeta: metav1.ObjectMeta{Name: "k", Namespace: "default", Generation: 2, Finalizers: []string{knightFinalizer}},
				Spec: aiv1alpha1.KnightSpec{
					Domain: "ops", Model: "claude-sonnet-4",
					NATS: aiv1alpha1.KnightNATS{Stream: "s", Subjects: []string{"a.>"}},
				},
				Status: aiv1alpha1.KnightStatus{Phase: aiv1alpha1.KnightPhaseReady, ObservedGeneration: 1},
			}
			tc.build(k)
			if got := knightKstatusOf(t, k); got != status.InProgressStatus {
				t.Fatalf("unobserved spec: kstatus = %s, want InProgress", got)
			}
			b := fake.NewClientBuilder().WithScheme(s).WithObjects(k.DeepCopy()).WithStatusSubresource(&aiv1alpha1.Knight{})
			if tc.deploy != nil {
				b = b.WithObjects(tc.deploy)
			}
			c := b.Build()
			r := &KnightReconciler{Client: c, Scheme: s, Recorder: record.NewFakeRecorder(100), DefaultImage: "ghcr.io/roundtable-k8s/claude-knight:test"}
			key := types.NamespacedName{Name: "k", Namespace: "default"}
			// A reconcile may report an error (no NATS here); the spec is
			// still accepted, so the status must not stay InProgress.
			_, _ = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
			got := &aiv1alpha1.Knight{}
			if err := c.Get(context.Background(), key, got); err != nil {
				t.Fatal(err)
			}
			if got.Status.ObservedGeneration != got.Generation {
				t.Errorf("observedGeneration = %d, want %d", got.Status.ObservedGeneration, got.Generation)
			}
			if s := knightKstatusOf(t, got); s != status.CurrentStatus {
				t.Errorf("after reconcile (phase %s): kstatus = %s, want Current", got.Status.Phase, s)
			}
			for _, cond := range got.Status.Conditions {
				if cond.Type == "Reconciling" || cond.Type == "Stalled" {
					t.Errorf("unexpected kstatus condition %s", cond.Type)
				}
			}
		})
	}
}
