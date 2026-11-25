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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

var _ = Describe("Knight memory volume (#388)", func() {
	ctx := context.Background()
	const name = "memory-knight"
	key := types.NamespacedName{Name: name, Namespace: "default"}
	memKey := types.NamespacedName{Name: name + "-memory", Namespace: "default"}
	var reconciler *KnightReconciler

	newKnight := func(m *aiv1alpha1.KnightMemory, maxReplicas int32) *aiv1alpha1.Knight {
		return &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: aiv1alpha1.KnightSpec{
				Domain: "code", Skills: []string{"shared"}, Memory: m,
				MaxReplicas: new(maxReplicas),
				Workspace:   &aiv1alpha1.KnightWorkspace{Mode: aiv1alpha1.WorkspaceModeEphemeral},
				NATS: aiv1alpha1.KnightNATS{
					URL: "nats://nats.test:4222", Subjects: []string{"mem.tasks.code.>"},
					Stream: "mem_tasks", ResultsStream: "mem_results",
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
		for _, n := range []string{name, memKey.Name, "table-memory"} {
			pvc := &corev1.PersistentVolumeClaim{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: n, Namespace: "default"}, pvc); err != nil {
				continue
			}
			// No PVC protection controller in envtest: drop its finalizer.
			pvc.Finalizers = nil
			_ = k8sClient.Update(ctx, pvc)
			_ = k8sClient.Delete(ctx, pvc)
		}
		Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(ctx, memKey, &corev1.PersistentVolumeClaim{}))
		}).Should(BeTrue())
	})

	reconcileTwice := func() {
		req := reconcile.Request{NamespacedName: key}
		_, _ = reconciler.Reconcile(ctx, req)
		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
	}

	memoryMounts := func(d *appsv1.Deployment) []corev1.VolumeMount {
		var out []corev1.VolumeMount
		for _, m := range d.Spec.Template.Spec.Containers[0].VolumeMounts {
			if m.Name == "memory" {
				out = append(out, m)
			}
		}
		return out
	}
	memoryClaim := func(d *appsv1.Deployment) string {
		for _, v := range d.Spec.Template.Spec.Volumes {
			if v.Name == "memory" && v.PersistentVolumeClaim != nil {
				return v.PersistentVolumeClaim.ClaimName
			}
		}
		return ""
	}

	It("creates <knight>-memory ReadWriteMany with the requested size, owned by the Knight, and mounts it", func() {
		size := resource.MustParse("3Gi")
		Expect(k8sClient.Create(ctx, newKnight(&aiv1alpha1.KnightMemory{Size: &size, StorageClassName: "cephfs"}, 3))).To(Succeed())
		reconcileTwice()

		k := &aiv1alpha1.Knight{}
		Expect(k8sClient.Get(ctx, key, k)).To(Succeed())

		pvc := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, memKey, pvc)).To(Succeed())
		Expect(pvc.Spec.AccessModes).To(ConsistOf(corev1.ReadWriteMany))
		Expect(pvc.Spec.Resources.Requests.Storage().Cmp(size)).To(Equal(0))
		Expect(pvc.Spec.StorageClassName).To(HaveValue(Equal("cephfs")))
		owner := metav1.GetControllerOf(pvc)
		Expect(owner).NotTo(BeNil())
		Expect(owner.UID).To(Equal(k.UID))

		// The ephemeral workspace still gets no PVC.
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, key, &corev1.PersistentVolumeClaim{}))).To(BeTrue())

		d := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, key, d)).To(Succeed())
		Expect(memoryClaim(d)).To(Equal(memKey.Name))
		Expect(memoryMounts(d)).To(ConsistOf(corev1.VolumeMount{Name: "memory", MountPath: "/memory/" + name}))
		env := map[string]string{}
		for _, e := range d.Spec.Template.Spec.Containers[0].Env {
			env[e.Name] = e.Value
		}
		Expect(env).To(HaveKeyWithValue("KNIGHT_MEMORY_DIR", "/memory/"+name))
		Expect(env).To(HaveKeyWithValue("KNIGHT_MEMORY_ROOT", "/memory"))

		// Not bound yet: no MemoryShared condition.
		Expect(meta.FindStatusCondition(k.Status.Conditions, aiv1alpha1.ConditionKnightMemoryShared)).To(BeNil())
	})

	It("defaults the memory PVC to 1Gi", func() {
		Expect(k8sClient.Create(ctx, newKnight(&aiv1alpha1.KnightMemory{}, 1))).To(Succeed())
		reconcileTwice()

		pvc := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, memKey, pvc)).To(Succeed())
		Expect(pvc.Spec.Resources.Requests.Storage().Cmp(resource.MustParse("1Gi"))).To(Equal(0))
		Expect(pvc.Spec.StorageClassName).To(BeNil())
	})

	It("reports MemoryShared=False when a multi-pod knight's memory PVC binds without ReadWriteMany", func() {
		Expect(k8sClient.Create(ctx, newKnight(&aiv1alpha1.KnightMemory{}, 3))).To(Succeed())
		reconcileTwice()

		bind := func(modes ...corev1.PersistentVolumeAccessMode) {
			pvc := &corev1.PersistentVolumeClaim{}
			Expect(k8sClient.Get(ctx, memKey, pvc)).To(Succeed())
			pvc.Status.Phase = corev1.ClaimBound
			pvc.Status.AccessModes = modes
			Expect(k8sClient.Status().Update(ctx, pvc)).To(Succeed())
			reconcileTwice()
		}
		condition := func() *metav1.Condition {
			k := &aiv1alpha1.Knight{}
			Expect(k8sClient.Get(ctx, key, k)).To(Succeed())
			return meta.FindStatusCondition(k.Status.Conditions, aiv1alpha1.ConditionKnightMemoryShared)
		}

		bind(corev1.ReadWriteOnce)
		c := condition()
		Expect(c).NotTo(BeNil())
		Expect(c.Status).To(Equal(metav1.ConditionFalse))
		Expect(c.Reason).To(Equal(aiv1alpha1.ReasonMemoryNotRWX))

		bind(corev1.ReadWriteMany)
		c = condition()
		Expect(c).NotTo(BeNil())
		Expect(c.Status).To(Equal(metav1.ConditionTrue))
		Expect(c.Reason).To(Equal(aiv1alpha1.ReasonMemoryRWX))
	})

	It("sets no MemoryShared condition on a single-pod knight", func() {
		Expect(k8sClient.Create(ctx, newKnight(&aiv1alpha1.KnightMemory{}, 1))).To(Succeed())
		reconcileTwice()

		pvc := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, memKey, pvc)).To(Succeed())
		pvc.Status.Phase = corev1.ClaimBound
		pvc.Status.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
		Expect(k8sClient.Status().Update(ctx, pvc)).To(Succeed())
		reconcileTwice()

		k := &aiv1alpha1.Knight{}
		Expect(k8sClient.Get(ctx, key, k)).To(Succeed())
		Expect(meta.FindStatusCondition(k.Status.Conditions, aiv1alpha1.ConditionKnightMemoryShared)).To(BeNil())
	})

	It("mounts an existingClaim without creating, owning or deleting it", func() {
		shared := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "table-memory", Namespace: "default"},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
				},
			},
		}
		Expect(k8sClient.Create(ctx, shared)).To(Succeed())
		Expect(k8sClient.Create(ctx, newKnight(&aiv1alpha1.KnightMemory{ExistingClaim: "table-memory"}, 3))).To(Succeed())
		reconcileTwice()

		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, memKey, &corev1.PersistentVolumeClaim{}))).To(BeTrue())
		got := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "table-memory", Namespace: "default"}, got)).To(Succeed())
		Expect(got.OwnerReferences).To(BeEmpty())
		Expect(got.DeletionTimestamp).To(BeNil())

		d := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, key, d)).To(Succeed())
		Expect(memoryClaim(d)).To(Equal("table-memory"))
		Expect(memoryMounts(d)).To(ConsistOf(
			corev1.VolumeMount{Name: "memory", MountPath: "/memory", ReadOnly: true},
			corev1.VolumeMount{Name: "memory", MountPath: "/memory/" + name, SubPath: name},
		))
	})

	It("refuses a <knight>-memory PVC controlled by another object", func() {
		other := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "memory-owner", Namespace: "default"}}
		Expect(k8sClient.Create(ctx, other)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, other) })
		foreign := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: memKey.Name, Namespace: "default",
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "v1", Kind: "ConfigMap", Name: other.Name, UID: other.UID, Controller: new(true),
				}},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
				},
			},
		}
		Expect(k8sClient.Create(ctx, foreign)).To(Succeed())
		Expect(k8sClient.Create(ctx, newKnight(&aiv1alpha1.KnightMemory{}, 1))).To(Succeed())

		req := reconcile.Request{NamespacedName: key}
		_, _ = reconciler.Reconcile(ctx, req)
		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).To(MatchError(ContainSubstring("not this Knight")))
	})
})
