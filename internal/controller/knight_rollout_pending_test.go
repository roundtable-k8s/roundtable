package controller

import (
	"context"

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
