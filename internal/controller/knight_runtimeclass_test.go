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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	rtruntime "github.com/roundtable-k8s/roundtable/pkg/runtime"
)

var _ = Describe("Knight runtimeClassName", func() {
	ctx := context.Background()
	const name = "rc-knight"
	var reconciler *KnightReconciler

	newKnight := func(rc string) *aiv1alpha1.Knight {
		return &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: aiv1alpha1.KnightSpec{
				Domain: "code", Skills: []string{"shared"}, RuntimeClassName: &rc,
				NATS: aiv1alpha1.KnightNATS{
					URL: "nats://nats.test:4222", Subjects: []string{"rc.tasks.code.>"},
					Stream: "rc_tasks", ResultsStream: "rc_results",
				},
			},
		}
	}
	key := types.NamespacedName{Name: name, Namespace: "default"}

	BeforeEach(func() {
		reconciler = &KnightReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: record.NewFakeRecorder(100)}
	})

	AfterEach(func() {
		k := &aiv1alpha1.Knight{}
		if err := k8sClient.Get(ctx, key, k); err == nil {
			k.Finalizers = nil
			_ = k8sClient.Update(ctx, k)
			_ = k8sClient.Delete(ctx, k)
		}
		_ = k8sClient.Delete(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}})
		_ = k8sClient.Delete(ctx, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}})
		_ = k8sClient.Delete(ctx, &nodev1.RuntimeClass{ObjectMeta: metav1.ObjectMeta{Name: "gvisor-test"}})
	})

	It("reports RuntimeClassNotFound until the RuntimeClass exists", func() {
		Expect(k8sClient.Create(ctx, newKnight("gvisor-test"))).To(Succeed())
		req := reconcile.Request{NamespacedName: key}
		_, _ = reconciler.Reconcile(ctx, req)
		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		k := &aiv1alpha1.Knight{}
		Expect(k8sClient.Get(ctx, key, k)).To(Succeed())
		c := meta.FindStatusCondition(k.Status.Conditions, aiv1alpha1.ConditionKnightRuntimeAvailable)
		Expect(c).NotTo(BeNil())
		Expect(c.Status).To(Equal(metav1.ConditionFalse))
		Expect(c.Reason).To(Equal(aiv1alpha1.ReasonRuntimeClassNotFound))
		Expect(c.Message).To(ContainSubstring("gvisor-test"))
		Expect(k8sClient.Get(ctx, key, &appsv1.Deployment{})).NotTo(Succeed())

		Expect(k8sClient.Create(ctx, &nodev1.RuntimeClass{
			ObjectMeta: metav1.ObjectMeta{Name: "gvisor-test"}, Handler: "runsc",
		})).To(Succeed())
		Eventually(func(g Gomega) {
			_, err := reconciler.Reconcile(ctx, req)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(k8sClient.Get(ctx, key, k)).To(Succeed())
			c := meta.FindStatusCondition(k.Status.Conditions, aiv1alpha1.ConditionKnightRuntimeAvailable)
			g.Expect(c).NotTo(BeNil())
			g.Expect(c.Status).To(Equal(metav1.ConditionTrue))
		}, 10*time.Second, 250*time.Millisecond).Should(Succeed())

		d := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, key, d)).To(Succeed())
		Expect(d.Spec.Template.Spec.RuntimeClassName).To(HaveValue(Equal("gvisor-test")))
	})

	It("lets a knight with a missing RuntimeClass be suspended", func() {
		Expect(k8sClient.Create(ctx, newKnight("gvisor-test"))).To(Succeed())
		req := reconcile.Request{NamespacedName: key}
		_, _ = reconciler.Reconcile(ctx, req)
		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		k := &aiv1alpha1.Knight{}
		Expect(k8sClient.Get(ctx, key, k)).To(Succeed())
		k.Spec.Suspend = true
		Expect(k8sClient.Update(ctx, k)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Get(ctx, key, k)).To(Succeed())
		Expect(k.Status.Phase).To(Equal(aiv1alpha1.KnightPhaseSuspended))
	})

	It("puts the RuntimeClass on both the Deployment and the Sandbox pod templates", func() {
		k := newKnight("gvisor")

		ds := reconciler.BuildDeploymentSpec(ctx, k)
		Expect(ds.Template.Spec.RuntimeClassName).To(HaveValue(Equal("gvisor")))

		s := k8sruntime.NewScheme()
		Expect(aiv1alpha1.AddToScheme(s)).To(Succeed())
		c := fake.NewClientBuilder().WithScheme(s).Build()
		backend := rtruntime.NewSandboxBackend(c, s, "", reconciler.BuildPodSpec)
		Expect(backend.Reconcile(ctx, k)).To(Succeed())
		sb := rtruntime.NewSandboxObject("", "")
		Expect(c.Get(ctx, client.ObjectKeyFromObject(k), sb)).To(Succeed())
		Expect(sandboxPodSpec(sb).RuntimeClassName).To(HaveValue(Equal("gvisor")))
	})
})
