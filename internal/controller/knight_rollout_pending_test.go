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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

var _ = Describe("Knight RolloutPending condition", func() {
	ctx := context.Background()
	names := []string{"rollgate-a", "rollgate-b"}
	// Own namespace: the rollout gate counts every knight Deployment in the
	// namespace, and envtest has no Deployment controller, so other tests'
	// Deployments in "default" would all look mid-rollout.
	const ns = "rollout-pending"

	nn := func(n string) types.NamespacedName { return types.NamespacedName{Name: n, Namespace: ns} }
	pending := func(n string) *metav1.Condition {
		k := &aiv1alpha1.Knight{}
		ExpectWithOffset(1, k8sClient.Get(ctx, nn(n), k)).To(Succeed())
		return meta.FindStatusCondition(k.Status.Conditions, aiv1alpha1.ConditionKnightRolloutPending)
	}
	setRolling := func(n string, rolling bool) {
		d := &appsv1.Deployment{}
		ExpectWithOffset(1, k8sClient.Get(ctx, nn(n), d)).To(Succeed())
		d.Status = appsv1.DeploymentStatus{
			Replicas: 1, ReadyReplicas: 1, UpdatedReplicas: 1, ObservedGeneration: d.Generation,
		}
		if rolling {
			d.Status.UpdatedReplicas = 0
			d.Status.UnavailableReplicas = 1
		}
		ExpectWithOffset(1, k8sClient.Status().Update(ctx, d)).To(Succeed())
	}
	setImage := func(n, image string) {
		k := &aiv1alpha1.Knight{}
		ExpectWithOffset(1, k8sClient.Get(ctx, nn(n), k)).To(Succeed())
		k.Spec.Image = image
		ExpectWithOffset(1, k8sClient.Update(ctx, k)).To(Succeed())
	}

	It("is set while deferred, and removed once the knight rolls or is suspended", func() {
		r := &KnightReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
			Recorder: record.NewFakeRecorder(1000), MaxConcurrentRollouts: 1,
		}
		reconcileKnight := func(n string) {
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn(n)})
			ExpectWithOffset(1, err).NotTo(HaveOccurred())
		}
		if err := k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
			Expect(apierrors.IsAlreadyExists(err)).To(BeTrue())
		}
		for _, n := range names {
			k := &aiv1alpha1.Knight{
				ObjectMeta: metav1.ObjectMeta{Name: n, Namespace: ns},
				Spec: aiv1alpha1.KnightSpec{
					Domain: "security", Skills: []string{"security"}, Image: "example.com/knight:v1",
					NATS: aiv1alpha1.KnightNATS{
						URL: "nats://nats.test:4222", Subjects: []string{"test.tasks.security.>"},
						Stream: "test_tasks", ResultsStream: "test_results", MaxDeliver: 1,
					},
				},
			}
			Expect(k8sClient.Create(ctx, k)).To(Succeed())
			DeferCleanup(func() {
				k := &aiv1alpha1.Knight{}
				if k8sClient.Get(ctx, nn(n), k) == nil {
					k.Finalizers = nil
					_ = k8sClient.Update(ctx, k)
					_ = k8sClient.Delete(ctx, k)
				}
			})
			reconcileKnight(n) // finalizer
			reconcileKnight(n) // resources
			setRolling(n, false)
		}

		By("rolling the first knight")
		setImage(names[0], "example.com/knight:v2")
		reconcileKnight(names[0])
		setRolling(names[0], true)
		Expect(pending(names[0])).To(BeNil())

		By("deferring the second while the first is mid-rollout")
		setImage(names[1], "example.com/knight:v2")
		reconcileKnight(names[1])
		c := pending(names[1])
		Expect(c).NotTo(BeNil())
		Expect(c.Status).To(Equal(metav1.ConditionTrue))
		Expect(c.Reason).To(Equal(aiv1alpha1.ReasonWaitingForOtherRollouts))
		d := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, nn(names[1]), d)).To(Succeed())
		Expect(d.Spec.Template.Spec.Containers[0].Image).To(Equal("example.com/knight:v1"))

		By("removing the condition once the first settles and the second rolls")
		setRolling(names[0], false)
		reconcileKnight(names[1])
		Expect(pending(names[1])).To(BeNil())

		By("clearing it on suspend while deferred")
		setRolling(names[1], true)
		setImage(names[0], "example.com/knight:v3")
		reconcileKnight(names[0])
		Expect(pending(names[0])).NotTo(BeNil())
		k := &aiv1alpha1.Knight{}
		Expect(k8sClient.Get(ctx, nn(names[0]), k)).To(Succeed())
		k.Spec.Suspend = true
		Expect(k8sClient.Update(ctx, k)).To(Succeed())
		reconcileKnight(names[0])
		Expect(pending(names[0])).To(BeNil())
	})
})

// The whenIdle maximum wait counts from the RolloutPending transition time:
// it must stay put while the knight keeps waiting for the same reason and
// restart when the reason changes.
func TestSetRolloutPending_TransitionTime(t *testing.T) {
	k := &aiv1alpha1.Knight{}
	t0 := time.Date(2026, 11, 27, 6, 0, 0, 0, time.UTC)
	since := func() time.Time {
		c := meta.FindStatusCondition(k.Status.Conditions, aiv1alpha1.ConditionKnightRolloutPending)
		if c == nil {
			t.Fatal("RolloutPending not set")
		}
		return c.LastTransitionTime.Time
	}

	setRolloutPending(k, aiv1alpha1.ReasonWaitingForOtherRollouts, t0, "others")
	if !since().Equal(t0) {
		t.Fatalf("transition time %v, want %v", since(), t0)
	}

	t1 := t0.Add(time.Minute)
	setRolloutPending(k, aiv1alpha1.ReasonWaitingForIdle, t1, "idle")
	if !since().Equal(t1) {
		t.Fatalf("reason change kept transition time %v, want %v", since(), t1)
	}

	setRolloutPending(k, aiv1alpha1.ReasonWaitingForIdle, t1.Add(time.Minute), "idle")
	if !since().Equal(t1) {
		t.Fatalf("same reason moved transition time to %v, want %v", since(), t1)
	}
	c := meta.FindStatusCondition(k.Status.Conditions, aiv1alpha1.ConditionKnightRolloutPending)
	if c.Reason != aiv1alpha1.ReasonWaitingForIdle || c.Message != "idle" {
		t.Fatalf("condition %+v", c)
	}
}
