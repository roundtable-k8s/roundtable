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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

var _ = Describe("Knight vault settings survive reconcile", func() {
	ctx := context.Background()
	const name = "vault-knight"
	key := types.NamespacedName{Name: name, Namespace: "default"}
	var reconciler *KnightReconciler

	newKnight := func(v *aiv1alpha1.KnightVault) *aiv1alpha1.Knight {
		return &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: aiv1alpha1.KnightSpec{
				Domain: "code", Skills: []string{"shared"}, Vault: v,
				NATS: aiv1alpha1.KnightNATS{
					URL: "nats://nats.test:4222", Subjects: []string{"vault.tasks.code.>"},
					Stream: "vault_tasks", ResultsStream: "vault_results",
				},
			},
		}
	}

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
	})

	reconcileTwice := func() {
		req := reconcile.Request{NamespacedName: key}
		_, _ = reconciler.Reconcile(ctx, req)
		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
	}

	vaultMounts := func(d *appsv1.Deployment) []corev1.VolumeMount {
		var out []corev1.VolumeMount
		for _, m := range d.Spec.Template.Spec.Containers[0].VolumeMounts {
			if m.Name == "vault" {
				out = append(out, m)
			}
		}
		return out
	}

	It("keeps readOnly: false", func() {
		Expect(k8sClient.Create(ctx, newKnight(&aiv1alpha1.KnightVault{ClaimName: "x", ReadOnly: new(false)}))).To(Succeed())
		reconcileTwice()

		k := &aiv1alpha1.Knight{}
		Expect(k8sClient.Get(ctx, key, k)).To(Succeed())
		Expect(k.Finalizers).To(ContainElement(knightFinalizer))
		Expect(k.Spec.Vault.ReadOnly).To(HaveValue(BeFalse()))
		Expect(k.Spec.Vault.WritablePaths).To(BeEmpty())

		d := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, key, d)).To(Succeed())
		m := vaultMounts(d)
		Expect(m).To(HaveLen(1))
		Expect(m[0].MountPath).To(Equal("/vault"))
		Expect(m[0].ReadOnly).To(BeFalse())
	})

	It("keeps readOnly: true with an empty writablePaths", func() {
		Expect(k8sClient.Create(ctx, newKnight(&aiv1alpha1.KnightVault{
			ClaimName: "x", ReadOnly: new(true), WritablePaths: []string{},
		}))).To(Succeed())
		reconcileTwice()

		k := &aiv1alpha1.Knight{}
		Expect(k8sClient.Get(ctx, key, k)).To(Succeed())
		Expect(k.Spec.Vault.WritablePaths).To(BeEmpty())

		d := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, key, d)).To(Succeed())
		m := vaultMounts(d)
		Expect(m).To(HaveLen(1))
		Expect(m[0].ReadOnly).To(BeTrue())
	})
})
