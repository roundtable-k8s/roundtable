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

package sandboxenvtest

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	rtruntime "github.com/roundtable-k8s/roundtable/pkg/runtime"
)

const (
	timeout  = 30 * time.Second
	interval = 250 * time.Millisecond
)

func newSandboxKnight(name string) *aiv1alpha1.Knight {
	return &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: aiv1alpha1.KnightSpec{
			Runtime: "sandbox",
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

func getSandbox(key types.NamespacedName) (*unstructured.Unstructured, error) {
	sb := rtruntime.NewSandboxObject(key.Name, key.Namespace)
	err := k8sClient.Get(ctx, key, sb)
	return sb, err
}

func operatingMode(key types.NamespacedName) (string, error) {
	sb, err := getSandbox(key)
	if err != nil {
		return "", err
	}
	mode, _, _ := unstructured.NestedString(sb.Object, "spec", "operatingMode")
	return mode, nil
}

func knightPhase(key types.NamespacedName) (aiv1alpha1.KnightPhase, error) {
	k := &aiv1alpha1.Knight{}
	if err := k8sClient.Get(ctx, key, k); err != nil {
		return "", err
	}
	return k.Status.Phase, nil
}

// createKnight creates the knight and deletes it (letting the finalizer run
// while the manager is still up) when the spec ends.
func createKnight(k *aiv1alpha1.Knight) types.NamespacedName {
	Expect(k8sClient.Create(ctx, k)).To(Succeed())
	key := types.NamespacedName{Name: k.Name, Namespace: k.Namespace}
	DeferCleanup(func() {
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, k))).To(Succeed())
		Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(ctx, key, &aiv1alpha1.Knight{}))
		}, timeout, interval).Should(BeTrue())
	})
	return key
}

var _ = Describe("Sandbox knights against the agent-sandbox v1beta1 CRDs", func() {
	It("creates a Sandbox the API server accepts, owned by the Knight, with the knight pod labels", func() {
		knight := newSandboxKnight("sbx-create")
		key := createKnight(knight)

		var sb *unstructured.Unstructured
		Eventually(func() error {
			var err error
			sb, err = getSandbox(key)
			return err
		}, timeout, interval).Should(Succeed())

		Expect(sb.GetOwnerReferences()).To(HaveLen(1))
		owner := sb.GetOwnerReferences()[0]
		Expect(owner.Kind).To(Equal("Knight"))
		Expect(owner.Name).To(Equal(knight.Name))
		Expect(owner.Controller).NotTo(BeNil())
		Expect(*owner.Controller).To(BeTrue())

		labels, found, err := unstructured.NestedStringMap(sb.Object, "spec", "podTemplate", "metadata", "labels")
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue())
		Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/name", "knight"))
		Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/instance", knight.Name))
		Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/managed-by", "roundtable-operator"))
		Expect(labels).To(HaveKeyWithValue("roundtable.io/domain", "test"))

		// The API server prunes unknown fields, so a misspelled field in the
		// backend shows up here as a missing value.
		Expect(operatingMode(key)).To(Equal(rtruntime.SandboxOperatingModeRunning))

		containers, found, err := unstructured.NestedSlice(sb.Object, "spec", "podTemplate", "spec", "containers")
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue())
		Expect(containers).NotTo(BeEmpty())

		// No Deployment for a sandbox knight.
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, key, &appsv1.Deployment{}))).To(BeTrue())
	})

	It("carries the memory mounts in the Sandbox pod template (#388)", func() {
		knight := newSandboxKnight("sbx-memory")
		knight.Spec.Memory = &aiv1alpha1.KnightMemory{ExistingClaim: "table-memory"}
		key := createKnight(knight)

		var mounts []any
		Eventually(func() ([]any, error) {
			sb, err := getSandbox(key)
			if err != nil {
				return nil, err
			}
			containers, _, err := unstructured.NestedSlice(sb.Object, "spec", "podTemplate", "spec", "containers")
			if err != nil || len(containers) == 0 {
				return nil, err
			}
			mounts, _, err = unstructured.NestedSlice(containers[0].(map[string]any), "volumeMounts")
			return mounts, err
		}, timeout, interval).ShouldNot(BeEmpty())

		var memory []map[string]any
		for _, m := range mounts {
			if mm := m.(map[string]any); mm["name"] == "memory" {
				memory = append(memory, mm)
			}
		}
		Expect(memory).To(ConsistOf(
			map[string]any{"name": "memory", "mountPath": "/memory", "readOnly": true},
			map[string]any{"name": "memory", "mountPath": "/memory/sbx-memory", "subPath": "sbx-memory"},
		))
	})

	It("maps spec.suspend to operatingMode Suspended/Running and the Suspended phase", func() {
		knight := newSandboxKnight("sbx-suspend")
		key := createKnight(knight)
		Eventually(func() (string, error) { return operatingMode(key) }, timeout, interval).
			Should(Equal(rtruntime.SandboxOperatingModeRunning))

		By("suspending the knight")
		Eventually(func() error {
			k := &aiv1alpha1.Knight{}
			if err := k8sClient.Get(ctx, key, k); err != nil {
				return err
			}
			k.Spec.Suspend = true
			return k8sClient.Update(ctx, k)
		}, timeout, interval).Should(Succeed())
		Eventually(func() (string, error) { return operatingMode(key) }, timeout, interval).
			Should(Equal(rtruntime.SandboxOperatingModeSuspended))
		Eventually(func() (aiv1alpha1.KnightPhase, error) { return knightPhase(key) }, timeout, interval).
			Should(Equal(aiv1alpha1.KnightPhaseSuspended))

		By("clearing suspend")
		Eventually(func() error {
			k := &aiv1alpha1.Knight{}
			if err := k8sClient.Get(ctx, key, k); err != nil {
				return err
			}
			k.Spec.Suspend = false
			return k8sClient.Update(ctx, k)
		}, timeout, interval).Should(Succeed())
		Eventually(func() (string, error) { return operatingMode(key) }, timeout, interval).
			Should(Equal(rtruntime.SandboxOperatingModeRunning))
		Eventually(func() (aiv1alpha1.KnightPhase, error) { return knightPhase(key) }, timeout, interval).
			ShouldNot(Equal(aiv1alpha1.KnightPhaseSuspended))
	})

	It("reports the Knight Ready once the Sandbox has Ready=True", func() {
		knight := newSandboxKnight("sbx-ready")
		key := createKnight(knight)
		Eventually(func() error { _, err := getSandbox(key); return err }, timeout, interval).Should(Succeed())
		Consistently(func() (aiv1alpha1.KnightPhase, error) { return knightPhase(key) }, 2*time.Second, interval).
			ShouldNot(Equal(aiv1alpha1.KnightPhaseReady))

		Eventually(func() error {
			sb, err := getSandbox(key)
			if err != nil {
				return err
			}
			Expect(unstructured.SetNestedSlice(sb.Object, []any{map[string]any{
				"type":               "Ready",
				"status":             "True",
				"reason":             "DependenciesReady",
				"message":            "Pod is Ready",
				"lastTransitionTime": metav1.Now().UTC().Format(time.RFC3339),
			}}, "status", "conditions")).To(Succeed())
			return k8sClient.Status().Update(ctx, sb)
		}, timeout, interval).Should(Succeed())

		Eventually(func(g Gomega) {
			k := &aiv1alpha1.Knight{}
			g.Expect(k8sClient.Get(ctx, key, k)).To(Succeed())
			g.Expect(k.Status.Phase).To(Equal(aiv1alpha1.KnightPhaseReady))
			g.Expect(k.Status.Ready).To(BeTrue())
		}, timeout, interval).Should(Succeed())
	})

	It("swaps the Sandbox and the Deployment when spec.runtime changes", func() {
		knight := newSandboxKnight("sbx-swap")
		key := createKnight(knight)
		Eventually(func() error { _, err := getSandbox(key); return err }, timeout, interval).Should(Succeed())

		setRuntime := func(runtime string) {
			Eventually(func() error {
				k := &aiv1alpha1.Knight{}
				if err := k8sClient.Get(ctx, key, k); err != nil {
					return err
				}
				k.Spec.Runtime = runtime
				return k8sClient.Update(ctx, k)
			}, timeout, interval).Should(Succeed())
		}

		By("sandbox -> deployment")
		setRuntime("deployment")
		Eventually(func() error { return k8sClient.Get(ctx, key, &appsv1.Deployment{}) }, timeout, interval).
			Should(Succeed())
		Eventually(func() bool {
			_, err := getSandbox(key)
			return apierrors.IsNotFound(err)
		}, timeout, interval).Should(BeTrue())

		By("deployment -> sandbox")
		setRuntime("sandbox")
		Eventually(func() error { _, err := getSandbox(key); return err }, timeout, interval).Should(Succeed())
		Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(ctx, key, &appsv1.Deployment{}))
		}, timeout, interval).Should(BeTrue())
	})
})
