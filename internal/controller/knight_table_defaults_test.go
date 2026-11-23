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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

var _ = Describe("Knight inherits RoundTable defaults", func() {
	ctx := context.Background()
	const rtName = "defaults-fleet"
	names := []string{"dflt-inherit", "dflt-own", "dflt-nolabel"}

	var reconciler *KnightReconciler

	newKnight := func(name string, labels map[string]string) *aiv1alpha1.Knight {
		return &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: labels},
			Spec: aiv1alpha1.KnightSpec{
				Domain: "code",
				Skills: []string{"shared"},
				NATS: aiv1alpha1.KnightNATS{
					URL:           "nats://nats.test:4222",
					Subjects:      []string{"df.tasks.code.>"},
					Stream:        "df_tasks",
					ResultsStream: "df_results",
				},
			},
		}
	}

	reconcileTwice := func(name string) {
		req := reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: "default"}}
		_, _ = reconciler.Reconcile(ctx, req)
		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
	}

	getDeployment := func(name string) *appsv1.Deployment {
		d := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, d)).To(Succeed())
		return d
	}

	BeforeEach(func() {
		reconciler = &KnightReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: record.NewFakeRecorder(100)}
		size := resource.MustParse("20Gi")
		rt := &aiv1alpha1.RoundTable{
			ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: "default"},
			Spec: aiv1alpha1.RoundTableSpec{
				NATS: aiv1alpha1.RoundTableNATS{SubjectPrefix: "df", TasksStream: "df_tasks", ResultsStream: "df_results"},
				Defaults: &aiv1alpha1.RoundTableDefaults{
					Resources: &corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:              resource.MustParse("1"),
							corev1.ResourceEphemeralStorage: resource.MustParse("4Gi"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:              resource.MustParse("4"),
							corev1.ResourceEphemeralStorage: resource.MustParse("20Gi"),
						},
					},
					Workspace: &aiv1alpha1.KnightWorkspace{Size: &size},
				},
			},
		}
		Expect(k8sClient.Create(ctx, rt)).To(Succeed())
	})

	AfterEach(func() {
		for _, name := range names {
			k := &aiv1alpha1.Knight{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, k); err == nil {
				k.Finalizers = nil
				_ = k8sClient.Update(ctx, k)
				_ = k8sClient.Delete(ctx, k)
			}
			_ = k8sClient.Delete(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}})
			_ = k8sClient.Delete(ctx, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}})
		}
		_ = k8sClient.Delete(ctx, &aiv1alpha1.RoundTable{ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: "default"}})
	})

	It("gives a labelled knight the table's resources and workspace, without touching its spec", func() {
		Expect(k8sClient.Create(ctx, newKnight("dflt-inherit", map[string]string{aiv1alpha1.LabelRoundTable: rtName}))).To(Succeed())
		reconcileTwice("dflt-inherit")

		res := getDeployment("dflt-inherit").Spec.Template.Spec.Containers[0].Resources
		Expect(res.Limits.StorageEphemeral().String()).To(Equal("20Gi"))
		Expect(res.Requests.StorageEphemeral().String()).To(Equal("4Gi"))
		Expect(res.Limits.Cpu().String()).To(Equal("4"))

		pvc := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "dflt-inherit", Namespace: "default"}, pvc)).To(Succeed())
		Expect(pvc.Spec.Resources.Requests.Storage().String()).To(Equal("20Gi"))

		k := &aiv1alpha1.Knight{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "dflt-inherit", Namespace: "default"}, k)).To(Succeed())
		Expect(k.Spec.Resources).To(BeNil())
		Expect(k.Spec.Workspace).To(BeNil())
	})

	It("keeps the knight's own resources and workspace", func() {
		k := newKnight("dflt-own", map[string]string{aiv1alpha1.LabelRoundTable: rtName})
		k.Spec.Resources = &corev1.ResourceRequirements{
			Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")},
		}
		size := resource.MustParse("3Gi")
		k.Spec.Workspace = &aiv1alpha1.KnightWorkspace{Size: &size}
		Expect(k8sClient.Create(ctx, k)).To(Succeed())
		reconcileTwice("dflt-own")

		res := getDeployment("dflt-own").Spec.Template.Spec.Containers[0].Resources
		Expect(res.Limits.Memory().String()).To(Equal("1Gi"))
		Expect(res.Limits).NotTo(HaveKey(corev1.ResourceEphemeralStorage))
		Expect(res.Limits).NotTo(HaveKey(corev1.ResourceCPU))

		pvc := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "dflt-own", Namespace: "default"}, pvc)).To(Succeed())
		Expect(pvc.Spec.Resources.Requests.Storage().String()).To(Equal("3Gi"))
	})

	It("leaves a knight without the table label on the operator default", func() {
		Expect(k8sClient.Create(ctx, newKnight("dflt-nolabel", nil))).To(Succeed())
		reconcileTwice("dflt-nolabel")
		res := getDeployment("dflt-nolabel").Spec.Template.Spec.Containers[0].Resources
		Expect(res.Limits).To(BeEmpty())
		Expect(res.Requests.Memory().String()).To(Equal("256Mi"))
	})
})
