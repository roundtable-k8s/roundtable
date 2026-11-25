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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	rtruntime "github.com/roundtable-k8s/roundtable/pkg/runtime"
)

// Regression tests for #186: the operator must start and reconcile
// Deployment-backed knights when agents.x-k8s.io/v1beta1 Sandbox is not
// served. The suite installs only config/crd/bases, so no agent-sandbox CRDs.

func optionalSandboxKnight(name, runtime string) *aiv1alpha1.Knight {
	return &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: aiv1alpha1.KnightSpec{
			Runtime: runtime,
			Domain:  "test",
			Model:   "claude-sonnet-4-20250514",
			Skills:  []string{"shared"},
			NATS: aiv1alpha1.KnightNATS{
				URL:           "nats://nats.test:4222",
				Subjects:      []string{"test.tasks.>"},
				Stream:        "test_tasks",
				ResultsStream: "test_results",
			},
		},
	}
}

func newOptionalSandboxManager(syncTimeout time.Duration) ctrl.Manager {
	s := k8sruntime.NewScheme()
	Expect(clientgoscheme.AddToScheme(s)).To(Succeed())
	Expect(aiv1alpha1.AddToScheme(s)).To(Succeed())
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 s,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Controller: config.Controller{
			CacheSyncTimeout:   syncTimeout,
			SkipNameValidation: new(true),
		},
	})
	Expect(err).NotTo(HaveOccurred())
	return mgr
}

func newOptionalSandboxReconciler(mgr ctrl.Manager, sandboxEnabled bool) *KnightReconciler {
	r := &KnightReconciler{
		Client:         mgr.GetClient(),
		Scheme:         mgr.GetScheme(),
		Recorder:       record.NewFakeRecorder(100),
		DefaultImage:   "test:latest",
		SandboxEnabled: sandboxEnabled,
	}
	deploymentBackend := rtruntime.NewDeploymentBackend(mgr.GetClient(), mgr.GetScheme(), r.DefaultImage, r.BuildDeploymentSpec)
	r.RuntimeBackend = deploymentBackend
	r.RuntimeBackends = map[string]rtruntime.RuntimeBackend{"deployment": deploymentBackend}
	return r
}

var _ = Describe("Optional sandbox backend (#186)", func() {
	It("detects that the Sandbox API is not served", func() {
		dc, err := discovery.NewDiscoveryClientForConfig(cfg)
		Expect(err).NotTo(HaveOccurred())
		enabled, _, err := rtruntime.ResolveSandboxBackend(rtruntime.SandboxModeAuto, dc)
		Expect(err).NotTo(HaveOccurred())
		Expect(enabled).To(BeFalse())
	})

	It("reproduces the crash when Sandbox is watched but not served", func() {
		mgr := newOptionalSandboxManager(3 * time.Second)
		Expect(newOptionalSandboxReconciler(mgr, true).SetupWithManager(mgr)).To(Succeed())

		mgrCtx, mgrCancel := context.WithTimeout(ctx, time.Minute)
		defer mgrCancel()
		err := mgr.Start(mgrCtx)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("timed out waiting for cache to be synced"))
	})

	It("starts without the Sandbox API and reconciles knights", func() {
		mgr := newOptionalSandboxManager(10 * time.Second)
		Expect(newOptionalSandboxReconciler(mgr, false).SetupWithManager(mgr)).To(Succeed())

		mgrCtx, mgrCancel := context.WithCancel(ctx)
		mgrErr := make(chan error, 1)
		go func() {
			defer GinkgoRecover()
			mgrErr <- mgr.Start(mgrCtx)
		}()
		defer func() {
			mgrCancel()
			Eventually(mgrErr, time.Minute).Should(Receive(BeNil()))
		}()

		syncCtx, syncCancel := context.WithTimeout(ctx, 30*time.Second)
		defer syncCancel()
		Expect(mgr.GetCache().WaitForCacheSync(syncCtx)).To(BeTrue())

		By("reconciling a Deployment-backed knight")
		dep := optionalSandboxKnight("optional-sandbox-deploy", "deployment")
		Expect(k8sClient.Create(ctx, dep)).To(Succeed())
		depKey := types.NamespacedName{Name: dep.Name, Namespace: dep.Namespace}
		Eventually(func() error {
			return k8sClient.Get(ctx, depKey, &appsv1.Deployment{})
		}, 30*time.Second, 250*time.Millisecond).Should(Succeed())
		Eventually(func(g Gomega) {
			k := &aiv1alpha1.Knight{}
			g.Expect(k8sClient.Get(ctx, depKey, k)).To(Succeed())
			c := meta.FindStatusCondition(k.Status.Conditions, aiv1alpha1.ConditionKnightRuntimeAvailable)
			g.Expect(c).NotTo(BeNil())
			g.Expect(c.Status).To(Equal(metav1.ConditionTrue))
		}, 30*time.Second, 250*time.Millisecond).Should(Succeed())

		By("parking a sandbox knight with RuntimeAvailable=False")
		sb := optionalSandboxKnight("optional-sandbox-sandbox", "sandbox")
		Expect(k8sClient.Create(ctx, sb)).To(Succeed())
		sbKey := types.NamespacedName{Name: sb.Name, Namespace: sb.Namespace}
		Eventually(func(g Gomega) {
			k := &aiv1alpha1.Knight{}
			g.Expect(k8sClient.Get(ctx, sbKey, k)).To(Succeed())
			c := meta.FindStatusCondition(k.Status.Conditions, aiv1alpha1.ConditionKnightRuntimeAvailable)
			g.Expect(c).NotTo(BeNil())
			g.Expect(c.Status).To(Equal(metav1.ConditionFalse))
			g.Expect(c.Reason).To(Equal(aiv1alpha1.ReasonSandboxUnavailable))
			g.Expect(k.Status.Phase).To(Equal(aiv1alpha1.KnightPhaseDegraded))
		}, 30*time.Second, 250*time.Millisecond).Should(Succeed())
		Consistently(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(ctx, sbKey, &appsv1.Deployment{}))
		}, 2*time.Second, 250*time.Millisecond).Should(BeTrue())

		By("the manager is still running")
		Consistently(mgrErr, 2*time.Second).ShouldNot(Receive())

		// Delete while the manager runs so the finalizer is removed.
		for _, k := range []*aiv1alpha1.Knight{dep, sb} {
			Expect(k8sClient.Delete(ctx, k)).To(Succeed())
			key := types.NamespacedName{Name: k.Name, Namespace: k.Namespace}
			Eventually(func() bool {
				return apierrors.IsNotFound(k8sClient.Get(ctx, key, &aiv1alpha1.Knight{}))
			}, 30*time.Second, 250*time.Millisecond).Should(BeTrue())
		}
		_ = k8sClient.Delete(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: dep.Name, Namespace: dep.Namespace}})
	})

	It("sets RuntimeAvailable=False without touching an existing Deployment", func() {
		r := &KnightReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: record.NewFakeRecorder(100),
		}
		knight := optionalSandboxKnight("optional-sandbox-unit", "sandbox")
		Expect(k8sClient.Create(ctx, knight)).To(Succeed())
		key := types.NamespacedName{Name: knight.Name, Namespace: knight.Namespace}
		DeferCleanup(func() {
			k := &aiv1alpha1.Knight{}
			if err := k8sClient.Get(ctx, key, k); err == nil {
				k.Finalizers = nil
				_ = k8sClient.Update(ctx, k)
				_ = k8sClient.Delete(ctx, k)
			}
			_ = k8sClient.Delete(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}})
		})

		// A Deployment left over from when the knight ran on the deployment
		// runtime must survive: deleting it would take the knight down for good.
		deploy := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "t"}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "t"}},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "t", Image: "t:latest"}}},
				},
			},
		}
		Expect(k8sClient.Create(ctx, deploy)).To(Succeed())

		res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequeueAfter).To(Equal(RequeueVerySlow))

		k := &aiv1alpha1.Knight{}
		Expect(k8sClient.Get(ctx, key, k)).To(Succeed())
		c := meta.FindStatusCondition(k.Status.Conditions, aiv1alpha1.ConditionKnightRuntimeAvailable)
		Expect(c).NotTo(BeNil())
		Expect(c.Status).To(Equal(metav1.ConditionFalse))
		Expect(c.Reason).To(Equal(aiv1alpha1.ReasonSandboxUnavailable))
		Expect(c.Message).To(ContainSubstring("agents.x-k8s.io/v1beta1"))
		avail := meta.FindStatusCondition(k.Status.Conditions, aiv1alpha1.ConditionKnightAvailable)
		Expect(avail).NotTo(BeNil())
		Expect(avail.Status).To(Equal(metav1.ConditionFalse))
		Expect(k.Status.Phase).To(Equal(aiv1alpha1.KnightPhaseDegraded))
		Expect(k.Status.Ready).To(BeFalse())

		Expect(k8sClient.Get(ctx, key, &appsv1.Deployment{})).To(Succeed())

		By("a second reconcile is a no-op for status")
		rv := k.ResourceVersion
		_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, key, k)).To(Succeed())
		Expect(k.ResourceVersion).To(Equal(rv))
	})
})

// sandboxPodSpec decodes spec.podTemplate.spec of an unstructured Sandbox.
func sandboxPodSpec(sb *unstructured.Unstructured) corev1.PodSpec {
	m, found, err := unstructured.NestedMap(sb.Object, "spec", "podTemplate", "spec")
	Expect(err).NotTo(HaveOccurred())
	Expect(found).To(BeTrue())
	var spec corev1.PodSpec
	Expect(k8sruntime.DefaultUnstructuredConverter.FromUnstructured(m, &spec)).To(Succeed())
	return spec
}
