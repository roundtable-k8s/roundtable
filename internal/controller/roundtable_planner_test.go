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
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
)

// Table-owned planner knights (#566).
var _ = Describe("RoundTable planner", func() {
	const namespace = "default"
	ctx := context.Background()

	var r *RoundTableReconciler
	var created []string

	newTable := func(name, prefix string, planner *aiv1alpha1.RoundTablePlanner) *aiv1alpha1.RoundTable {
		rt := &aiv1alpha1.RoundTable{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: aiv1alpha1.RoundTableSpec{
				NATS: aiv1alpha1.RoundTableNATS{
					URL:           "nats://nats.example:4222",
					SubjectPrefix: prefix, TasksStream: prefix + "_tasks", ResultsStream: prefix + "_results",
				},
				// Only the table's own knights, so other tests' knights in the
				// namespace do not take part.
				KnightSelector: &metav1.LabelSelector{MatchLabels: map[string]string{aiv1alpha1.LabelRoundTable: name}},
				Planner:        planner,
			},
		}
		Expect(k8sClient.Create(ctx, rt)).To(Succeed())
		created = append(created, name)
		return rt
	}
	reconcileTable := func(name string) {
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: namespace}})
		Expect(err).NotTo(HaveOccurred())
	}
	getKnight := func(name string) (*aiv1alpha1.Knight, error) {
		k := &aiv1alpha1.Knight{}
		return k, k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, k)
	}
	getTable := func(name string) *aiv1alpha1.RoundTable {
		rt := &aiv1alpha1.RoundTable{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, rt)).To(Succeed())
		return rt
	}

	BeforeEach(func() {
		created = nil
		r = &RoundTableReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: record.NewFakeRecorder(100),
			PlannerDefaults: PlannerDefaults{
				Model:    "install-model",
				Arsenals: []aiv1alpha1.KnightArsenal{{Name: "platform", Repo: "https://example.com/arsenal.git"}},
				Prompt:   &aiv1alpha1.KnightPrompt{Instructions: "plan well"},
			},
		}
	})

	AfterEach(func() {
		// envtest runs no garbage collector: delete owned planners by hand.
		for _, name := range created {
			_ = k8sClient.Delete(ctx, &aiv1alpha1.Knight{ObjectMeta: metav1.ObjectMeta{Name: name + "-planner", Namespace: namespace}})
			_ = k8sClient.Delete(ctx, &aiv1alpha1.RoundTable{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}})
		}
	})

	It("gives a new table its own planner on the table's prefix and streams", func() {
		rt := newTable("plan-a", "plan-a", nil)
		reconcileTable("plan-a")

		k, err := getKnight("plan-a-planner")
		Expect(err).NotTo(HaveOccurred())
		Expect(metav1.IsControlledBy(k, rt)).To(BeTrue(), "planner must be owned by the table")
		Expect(k.Labels).To(HaveKeyWithValue(aiv1alpha1.LabelRoundTable, "plan-a"))
		Expect(k.Labels).To(HaveKeyWithValue(aiv1alpha1.LabelRole, "planner"))
		Expect(k.Spec.Domain).To(Equal("planner"))
		Expect(k.Spec.NATS.Subjects).To(Equal([]string{"plan-a.tasks.planner.>"}))
		Expect(k.Spec.NATS.Stream).To(Equal("plan-a_tasks"))
		Expect(k.Spec.NATS.ResultsStream).To(Equal("plan-a_results"))
		Expect(k.Spec.NATS.URL).To(Equal("nats://nats.example:4222"))
		Expect(k.Spec.Lifecycle).NotTo(BeNil())
		Expect(k.Spec.Lifecycle.SuspendPolicy).To(Equal(knightpkg.SuspendPolicyAuto))
		// No table defaults: the install-wide planner defaults apply.
		Expect(k.Spec.Model).To(Equal("install-model"))
		Expect(k.Spec.Arsenals).To(HaveLen(1))
		Expect(k.Spec.Prompt).NotTo(BeNil())
		Expect(k.Spec.Prompt.Instructions).To(Equal("plan well"))
		Expect(k.Spec.TaskTimeoutSeconds).To(Equal(int32(600)))
		Expect(k.Spec.Card).NotTo(BeNil())
		Expect(k.Spec.Card.Description).To(ContainSubstring("plan-a"))

		st := getTable("plan-a")
		Expect(st.Status.PlannerKnight).To(Equal("plan-a-planner"))
		Expect(st.Status.Knights).To(ContainElement(HaveField("Name", "plan-a-planner")))

		By("reconciling again without changes")
		rv := k.ResourceVersion
		reconcileTable("plan-a")
		k, err = getKnight("plan-a-planner")
		Expect(err).NotTo(HaveOccurred())
		Expect(k.ResourceVersion).To(Equal(rv), "an unchanged planner must not be rewritten")
	})

	It("lets spec.planner and the table defaults win over the install-wide defaults", func() {
		rt := newTable("plan-b", "plan-b", &aiv1alpha1.RoundTablePlanner{
			Resources: &corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}},
		})
		rt.Spec.Defaults = &aiv1alpha1.RoundTableDefaults{Model: "table-model"}
		Expect(k8sClient.Update(ctx, rt)).To(Succeed())
		reconcileTable("plan-b")

		k, err := getKnight("plan-b-planner")
		Expect(err).NotTo(HaveOccurred())
		// Left empty so the knight inherits spec.defaults.model.
		Expect(k.Spec.Model).To(BeEmpty())
		Expect(k.Spec.Resources).NotTo(BeNil())
		Expect(knightpkg.WithTableDefaults(ctx, k8sClient, k).Spec.Model).To(Equal("table-model"))

		By("setting spec.planner.model")
		rt = getTable("plan-b")
		rt.Spec.Planner.Model = "planner-model"
		Expect(k8sClient.Update(ctx, rt)).To(Succeed())
		reconcileTable("plan-b")
		k, err = getKnight("plan-b-planner")
		Expect(err).NotTo(HaveOccurred())
		Expect(k.Spec.Model).To(Equal("planner-model"))
	})

	It("uses knightRef and creates no knight", func() {
		newTable("plan-c", "plan-c", &aiv1alpha1.RoundTablePlanner{KnightRef: aiv1alpha1.LocalRef("my-planner")})
		reconcileTable("plan-c")

		_, err := getKnight("plan-c-planner")
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "no planner knight should be created with knightRef")
		Expect(getTable("plan-c").Status.PlannerKnight).To(Equal("my-planner"))
	})

	It("removes the owned planner when disabled", func() {
		newTable("plan-d", "plan-d", nil)
		reconcileTable("plan-d")
		_, err := getKnight("plan-d-planner")
		Expect(err).NotTo(HaveOccurred())

		rt := getTable("plan-d")
		rt.Spec.Planner = &aiv1alpha1.RoundTablePlanner{Enabled: new(bool)}
		Expect(k8sClient.Update(ctx, rt)).To(Succeed())
		reconcileTable("plan-d")

		_, err = getKnight("plan-d-planner")
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "disabled planner should be deleted")
		Expect(getTable("plan-d").Status.PlannerKnight).To(BeEmpty())
	})

	It("does not adopt a knight of the same name it does not own", func() {
		Expect(k8sClient.Create(ctx, &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{Name: "plan-e-planner", Namespace: namespace},
			Spec: aiv1alpha1.KnightSpec{
				Domain: "ops", Model: "hand-made", Skills: []string{"ops"},
				NATS: aiv1alpha1.KnightNATS{Subjects: []string{"x.tasks.ops.>"}},
			},
		})).To(Succeed())
		newTable("plan-e", "plan-e", nil)
		reconcileTable("plan-e")

		k, err := getKnight("plan-e-planner")
		Expect(err).NotTo(HaveOccurred())
		Expect(k.Spec.Model).To(Equal("hand-made"))
		Expect(k.OwnerReferences).To(BeEmpty())
		Expect(r.Recorder.(*record.FakeRecorder).Events).To(Receive(ContainSubstring("PlannerFailed")))
	})

	It("gives an ephemeral table no planner", func() {
		rt := newTable("plan-f", "plan-f", nil)
		rt.Spec.Ephemeral = true
		Expect(k8sClient.Update(ctx, rt)).To(Succeed())
		reconcileTable("plan-f")
		_, err := getKnight("plan-f-planner")
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
	})
})

var _ = Describe("PlannerDefaultsFromEnv", func() {
	It("reads the chart's JSON over the built-in defaults", func() {
		GinkgoT().Setenv(EnvPlannerDefaults, `{"model":"m","arsenals":[{"name":"a","repo":"r"}],"taskTimeoutSeconds":900}`)
		d, err := PlannerDefaultsFromEnv()
		Expect(err).NotTo(HaveOccurred())
		Expect(d.Model).To(Equal("m"))
		Expect(d.Arsenals).To(HaveLen(1))
		Expect(d.TaskTimeoutSeconds).To(Equal(int32(900)))
		Expect(d.Concurrency).To(Equal(int32(1)))
		Expect(d.Skills).To(Equal([]string{"operator", "shared"}))
	})

	It("rejects invalid JSON", func() {
		GinkgoT().Setenv(EnvPlannerDefaults, `{`)
		_, err := PlannerDefaultsFromEnv()
		Expect(err).To(HaveOccurred())
	})
})
