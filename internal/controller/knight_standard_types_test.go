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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// Standard Kubernetes types in the Knight CRD (#232): the API server applies
// the CRD defaults and validation, and the controller carries the values to
// the Deployment and PVC.
var _ = Describe("Knight standard types", func() {
	ctx := context.Background()

	newKnight := func(name string) *aiv1alpha1.Knight {
		return &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: aiv1alpha1.KnightSpec{
				Domain: "security",
				Skills: []string{"shared"},
				NATS:   aiv1alpha1.KnightNATS{Subjects: []string{"test.tasks.security.>"}},
			},
		}
	}
	cleanup := func(k *aiv1alpha1.Knight) {
		_ = k8sClient.Delete(ctx, k)
	}

	It("applies resources, workspace.size and arsenal periodSeconds to the pod", func() {
		k := newKnight("std-types-knight")
		k.Spec.Resources = &corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("250m"),
				corev1.ResourceMemory: resource.MustParse("512Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("2"),
				corev1.ResourceMemory: resource.MustParse("4Gi"),
			},
		}
		k.Spec.Workspace = &aiv1alpha1.KnightWorkspace{Size: new(resource.MustParse("10Gi"))}
		k.Spec.Arsenal = &aiv1alpha1.KnightArsenal{Repo: "https://github.com/example/skills", PeriodSeconds: 60}
		Expect(k8sClient.Create(ctx, k)).To(Succeed())
		DeferCleanup(cleanup, k)

		By("leaving taskTimeoutSeconds unset in the API (the operator defaults it, #372)")
		stored := &aiv1alpha1.Knight{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(k), stored)).To(Succeed())
		Expect(stored.Spec.TaskTimeoutSeconds).To(BeZero())

		r := &KnightReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: record.NewFakeRecorder(100)}
		req := reconcile.Request{NamespacedName: types.NamespacedName{Name: k.Name, Namespace: k.Namespace}}
		_, _ = r.Reconcile(ctx, req)
		_, err := r.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())

		deploy := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, req.NamespacedName, deploy)).To(Succeed())
		var app, gitSync *corev1.Container
		for i := range deploy.Spec.Template.Spec.Containers {
			c := &deploy.Spec.Template.Spec.Containers[i]
			switch c.Name {
			case "app":
				app = c
			case "git-sync":
				gitSync = c
			}
		}
		Expect(app).NotTo(BeNil())
		Expect(app.Resources.Requests.Cpu().String()).To(Equal("250m"))
		Expect(app.Resources.Requests.Memory().String()).To(Equal("512Mi"))
		Expect(app.Resources.Limits.Cpu().String()).To(Equal("2"))
		Expect(app.Resources.Limits.Memory().String()).To(Equal("4Gi"))
		Expect(app.Env).To(ContainElement(corev1.EnvVar{Name: "TASK_TIMEOUT_MS", Value: "120000"}))

		Expect(gitSync).NotTo(BeNil())
		Expect(gitSync.Env).To(ContainElement(corev1.EnvVar{Name: "GITSYNC_PERIOD", Value: "60s"}))

		pvc := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, req.NamespacedName, pvc)).To(Succeed())
		Expect(pvc.Spec.Resources.Requests.Storage().String()).To(Equal("10Gi"))
	})

	It("rejects a tokenSecretRef without a key", func() {
		// Unstructured, so the request really omits key (the typed client
		// would send key: "").
		u := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": aiv1alpha1.GroupVersion.String(),
			"kind":       "Knight",
			"metadata":   map[string]any{"name": "std-types-nokey", "namespace": "default"},
			"spec": map[string]any{
				"domain": "security",
				"skills": []any{"shared"},
				"nats":   map[string]any{"subjects": []any{"test.tasks.security.>"}},
				"github": map[string]any{"tokenSecretRef": map[string]any{"name": "gh"}},
			},
		}}
		err := k8sClient.Create(ctx, u)
		if err == nil {
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, u) })
		}
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("spec.github.tokenSecretRef.key"))
	})
})
