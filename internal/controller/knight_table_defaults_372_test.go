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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
)

// RoundTable spec.defaults (model, image, taskTimeoutSeconds, concurrency)
// and spec.vault reach the table's knights (#372).
var _ = Describe("RoundTable defaults inheritance (#372)", func() {
	const ns = "tabledefaults"
	var table *aiv1alpha1.RoundTable
	var knights []*aiv1alpha1.Knight

	BeforeEach(func() {
		err := k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})
		if err != nil {
			Expect(apierrors.IsAlreadyExists(err)).To(BeTrue())
		}
		table = &aiv1alpha1.RoundTable{
			ObjectMeta: metav1.ObjectMeta{Name: "inherit", Namespace: ns},
			Spec: aiv1alpha1.RoundTableSpec{
				Ephemeral: true,
				NATS: aiv1alpha1.RoundTableNATS{
					SubjectPrefix: "rt_test", TasksStream: "rt_test_tasks", ResultsStream: "rt_test_results",
				},
				Defaults: &aiv1alpha1.RoundTableDefaults{
					Model:              "table-model",
					Image:              "ghcr.io/example/table-knight:1",
					TaskTimeoutSeconds: 600,
					Concurrency:        4,
				},
				Vault: &aiv1alpha1.KnightVault{ClaimName: "table-vault"},
			},
		}
		Expect(k8sClient.Create(ctx, table)).To(Succeed())
		knights = nil
	})
	AfterEach(func() {
		for _, k := range knights {
			deleteKnight(k)
		}
		_ = k8sClient.Delete(ctx, table)
	})

	member := func(name string, mutate func(*aiv1alpha1.KnightSpec)) *aiv1alpha1.Knight {
		k := scalingKnight(ns, name, func(s *aiv1alpha1.KnightSpec) {
			s.Model = ""
			if mutate != nil {
				mutate(s)
			}
		})
		k.Labels = map[string]string{aiv1alpha1.LabelRoundTable: table.Name}
		Expect(k8sClient.Create(ctx, k)).To(Succeed())
		knights = append(knights, k)
		return k
	}
	appEnv := func(k *aiv1alpha1.Knight) map[string]string {
		out := map[string]string{}
		for _, c := range getKnightDeployment(k).Spec.Template.Spec.Containers {
			if c.Name != "app" {
				continue
			}
			for _, e := range c.Env {
				out[e.Name] = e.Value
			}
		}
		return out
	}
	appImage := func(k *aiv1alpha1.Knight) string {
		for _, c := range getKnightDeployment(k).Spec.Template.Spec.Containers {
			if c.Name == "app" {
				return c.Image
			}
		}
		return ""
	}
	vaultClaim := func(k *aiv1alpha1.Knight) string {
		for _, v := range getKnightDeployment(k).Spec.Template.Spec.Volumes {
			if v.Name == "vault" && v.PersistentVolumeClaim != nil {
				return v.PersistentVolumeClaim.ClaimName
			}
		}
		return ""
	}

	It("stores no defaults on a Knight that leaves the fields unset", func() {
		k := member("td-stored", nil)
		got := getKnight(k)
		Expect(got.Spec.Model).To(BeEmpty())
		Expect(got.Spec.Concurrency).To(BeZero())
		Expect(got.Spec.TaskTimeoutSeconds).To(BeZero())
	})

	It("runs the table's defaults and reports the effective model", func() {
		k := member("td-inherit", nil)
		r := newScalingReconciler(false)
		reconcileKnight(r, k)

		Expect(getKnight(k).Status.Model).To(Equal("table-model"))
		env := appEnv(k)
		Expect(env["KNIGHT_MODEL"]).To(Equal("table-model"))
		Expect(env["MAX_CONCURRENT_TASKS"]).To(Equal("4"))
		Expect(env["TASK_TIMEOUT_MS"]).To(Equal("600000"))
		Expect(appImage(k)).To(Equal("ghcr.io/example/table-knight:1"))
		Expect(vaultClaim(k)).To(Equal("table-vault"))
		Expect(*getKnightDeployment(k).Spec.Template.Spec.TerminationGracePeriodSeconds).To(Equal(int64(660)))
		// The in-memory defaults are never written back to the Knight.
		Expect(getKnight(k).Spec.Model).To(BeEmpty())

		By("rolling the knight when a table default changes")
		Expect(r.roundTableKnightRequests(ctx, table)).To(ContainElement(
			HaveField("NamespacedName", types.NamespacedName{Name: k.Name, Namespace: ns})))
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: table.Name, Namespace: ns}, table)).To(Succeed())
		table.Spec.Defaults.Model = "new-table-model"
		table.Spec.Defaults.Concurrency = 3
		Expect(k8sClient.Update(ctx, table)).To(Succeed())
		reconcileKnight(r, k)
		Expect(appEnv(k)["KNIGHT_MODEL"]).To(Equal("new-table-model"))
		Expect(appEnv(k)["MAX_CONCURRENT_TASKS"]).To(Equal("3"))
		Expect(getKnight(k).Status.Model).To(Equal("new-table-model"))
	})

	It("keeps the knight's own values over the table's", func() {
		k := member("td-own", func(s *aiv1alpha1.KnightSpec) {
			s.Model = "knight-model"
			s.Image = "ghcr.io/example/own:2"
			s.Concurrency = 1
			s.TaskTimeoutSeconds = 45
			s.Vault = &aiv1alpha1.KnightVault{ClaimName: "own-vault"}
		})
		reconcileKnight(newScalingReconciler(false), k)

		Expect(getKnight(k).Status.Model).To(Equal("knight-model"))
		env := appEnv(k)
		Expect(env["KNIGHT_MODEL"]).To(Equal("knight-model"))
		Expect(env["MAX_CONCURRENT_TASKS"]).To(Equal("1"))
		Expect(env["TASK_TIMEOUT_MS"]).To(Equal("45000"))
		Expect(appImage(k)).To(Equal("ghcr.io/example/own:2"))
		Expect(vaultClaim(k)).To(Equal("own-vault"))
	})

	It("falls back to the operator defaults outside a table", func() {
		k := scalingKnight(ns, "td-none", func(s *aiv1alpha1.KnightSpec) { s.Model = "" })
		Expect(k8sClient.Create(ctx, k)).To(Succeed())
		knights = append(knights, k)
		reconcileKnight(newScalingReconciler(false), k)

		Expect(getKnight(k).Status.Model).To(Equal(knightpkg.DefaultModel))
		env := appEnv(k)
		Expect(env["MAX_CONCURRENT_TASKS"]).To(Equal("2"))
		Expect(env["TASK_TIMEOUT_MS"]).To(Equal("120000"))
		Expect(appImage(k)).To(Equal("ghcr.io/roundtable-k8s/pi-knight:test"))
		Expect(vaultClaim(k)).To(BeEmpty())
	})
})
