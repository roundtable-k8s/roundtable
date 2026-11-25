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
package knight

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

var _ = Describe("ApplyTableDefaults (#372)", func() {
	ctx := context.Background()

	newKnight := func(label string, mutate func(*aiv1alpha1.KnightSpec)) *aiv1alpha1.Knight {
		k := &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{
				Name: "td-knight", Namespace: "default",
				Labels: map[string]string{label: "td-table"},
			},
			Spec: aiv1alpha1.KnightSpec{
				Domain: "d", Skills: []string{"s"},
				NATS: aiv1alpha1.KnightNATS{URL: "nats://x:4222", Stream: "s", ResultsStream: "r", Subjects: []string{"a.>"}},
			},
		}
		if mutate != nil {
			mutate(&k.Spec)
		}
		return k
	}
	tableVault := &aiv1alpha1.KnightVault{ClaimName: "table-vault", ReadOnly: new(true), WritablePaths: []string{"Briefings/"}}
	table := func() *aiv1alpha1.RoundTable {
		return &aiv1alpha1.RoundTable{
			ObjectMeta: metav1.ObjectMeta{Name: "td-table", Namespace: "default"},
			Spec: aiv1alpha1.RoundTableSpec{
				Defaults: &aiv1alpha1.RoundTableDefaults{
					Model:              "table-model",
					Image:              "ghcr.io/example/table-knight:1",
					TaskTimeoutSeconds: 600,
					Concurrency:        4,
				},
				Vault: tableVault.DeepCopy(),
			},
		}
	}
	reader := func(objs ...client.Object) client.Reader {
		s := runtime.NewScheme()
		Expect(aiv1alpha1.AddToScheme(s)).To(Succeed())
		return fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
	}
	env := func(k *aiv1alpha1.Knight) map[string]string {
		out := map[string]string{}
		for _, e := range NewPodBuilder(k, "default-img").Build(ctx).Containers[0].Env {
			out[e.Name] = e.Value
		}
		return out
	}

	It("inherits model, image, taskTimeoutSeconds, concurrency and vault when the knight leaves them unset", func() {
		k := newKnight(aiv1alpha1.LabelRoundTable, nil)
		ApplyTableDefaults(ctx, reader(table()), k)

		Expect(k.Spec.Model).To(Equal("table-model"))
		Expect(k.Spec.Image).To(Equal("ghcr.io/example/table-knight:1"))
		Expect(k.Spec.TaskTimeoutSeconds).To(Equal(int32(600)))
		Expect(k.Spec.Concurrency).To(Equal(int32(4)))
		Expect(k.Spec.Vault).To(Equal(tableVault))

		pod := NewPodBuilder(k, "default-img").WithVault().Build(ctx)
		Expect(pod.Containers[0].Image).To(Equal("ghcr.io/example/table-knight:1"))
		Expect(pod.Volumes).To(ContainElement(HaveField("VolumeSource.PersistentVolumeClaim",
			HaveValue(HaveField("ClaimName", "table-vault")))))
		e := env(k)
		Expect(e["KNIGHT_MODEL"]).To(Equal("table-model"))
		Expect(e["MAX_CONCURRENT_TASKS"]).To(Equal("4"))
		Expect(e["TASK_TIMEOUT_MS"]).To(Equal("600000"))
		Expect(TerminationGracePeriod(k)).To(Equal(int64(660)))
	})

	It("keeps every value the knight sets", func() {
		own := &aiv1alpha1.KnightVault{ClaimName: "own-vault"}
		k := newKnight(aiv1alpha1.LabelRoundTable, func(s *aiv1alpha1.KnightSpec) {
			s.Model = "knight-model"
			s.Image = "ghcr.io/example/own:2"
			s.TaskTimeoutSeconds = 45
			s.Concurrency = 1
			s.Vault = own.DeepCopy()
		})
		ApplyTableDefaults(ctx, reader(table()), k)

		Expect(k.Spec.Model).To(Equal("knight-model"))
		Expect(k.Spec.Image).To(Equal("ghcr.io/example/own:2"))
		Expect(k.Spec.TaskTimeoutSeconds).To(Equal(int32(45)))
		Expect(k.Spec.Concurrency).To(Equal(int32(1)))
		Expect(k.Spec.Vault).To(Equal(own))
	})

	DescribeTable("overrides one field and inherits the rest",
		func(mutate func(*aiv1alpha1.KnightSpec), check func(*aiv1alpha1.Knight)) {
			k := newKnight(aiv1alpha1.LabelRoundTable, mutate)
			ApplyTableDefaults(ctx, reader(table()), k)
			check(k)
		},
		Entry("model", func(s *aiv1alpha1.KnightSpec) { s.Model = "mine" }, func(k *aiv1alpha1.Knight) {
			Expect(k.Spec.Model).To(Equal("mine"))
			Expect(k.Spec.Concurrency).To(Equal(int32(4)))
		}),
		Entry("image", func(s *aiv1alpha1.KnightSpec) { s.Image = "mine:1" }, func(k *aiv1alpha1.Knight) {
			Expect(k.Spec.Image).To(Equal("mine:1"))
			Expect(k.Spec.Model).To(Equal("table-model"))
		}),
		Entry("taskTimeoutSeconds", func(s *aiv1alpha1.KnightSpec) { s.TaskTimeoutSeconds = 90 }, func(k *aiv1alpha1.Knight) {
			Expect(k.Spec.TaskTimeoutSeconds).To(Equal(int32(90)))
			Expect(k.Spec.Image).To(Equal("ghcr.io/example/table-knight:1"))
		}),
		Entry("concurrency", func(s *aiv1alpha1.KnightSpec) { s.Concurrency = 7 }, func(k *aiv1alpha1.Knight) {
			Expect(k.Spec.Concurrency).To(Equal(int32(7)))
			Expect(k.Spec.TaskTimeoutSeconds).To(Equal(int32(600)))
		}),
		Entry("vault", func(s *aiv1alpha1.KnightSpec) { s.Vault = &aiv1alpha1.KnightVault{ClaimName: "mine"} }, func(k *aiv1alpha1.Knight) {
			Expect(k.Spec.Vault.ClaimName).To(Equal("mine"))
			Expect(k.Spec.Model).To(Equal("table-model"))
		}),
	)

	Describe("egress", func() {
		tableEgress := func() *aiv1alpha1.KnightEgress {
			return &aiv1alpha1.KnightEgress{AllowedHosts: []string{"github.com", "*.githubusercontent.com"}, AllowedPorts: []int32{443}}
		}
		withEgress := func() *aiv1alpha1.RoundTable {
			rt := table()
			rt.Spec.Defaults.Egress = tableEgress()
			return rt
		}

		It("inherits the table's egress when the knight leaves it unset", func() {
			k := newKnight(aiv1alpha1.LabelRoundTable, nil)
			ApplyTableDefaults(ctx, reader(withEgress()), k)
			Expect(k.Spec.Egress).To(Equal(tableEgress()))
		})

		It("inherits egress from a table whose only default is egress", func() {
			rt := table()
			rt.Spec.Vault = nil
			rt.Spec.Defaults = &aiv1alpha1.RoundTableDefaults{Egress: tableEgress()}
			k := newKnight(aiv1alpha1.LabelRoundTable, func(s *aiv1alpha1.KnightSpec) {
				s.Model = "m"
				s.Image = "i"
				s.TaskTimeoutSeconds = 1
				s.Concurrency = 1
				s.Resources = &corev1.ResourceRequirements{}
				s.Workspace = &aiv1alpha1.KnightWorkspace{}
				s.RuntimeClassName = new(string)
				s.Vault = &aiv1alpha1.KnightVault{ClaimName: "v"}
			})
			ApplyTableDefaults(ctx, reader(rt), k)
			Expect(k.Spec.Egress).To(Equal(tableEgress()))
		})

		It("keeps the knight's own egress without merging", func() {
			own := &aiv1alpha1.KnightEgress{AllowedHosts: []string{"api.anthropic.com"}}
			k := newKnight(aiv1alpha1.LabelRoundTable, func(s *aiv1alpha1.KnightSpec) { s.Egress = own.DeepCopy() })
			ApplyTableDefaults(ctx, reader(withEgress()), k)
			Expect(k.Spec.Egress).To(Equal(own))
		})

		It("keeps an explicit empty allowlist (DNS and NATS only)", func() {
			k := newKnight(aiv1alpha1.LabelRoundTable, func(s *aiv1alpha1.KnightSpec) { s.Egress = &aiv1alpha1.KnightEgress{} })
			ApplyTableDefaults(ctx, reader(withEgress()), k)
			Expect(k.Spec.Egress).To(Equal(&aiv1alpha1.KnightEgress{}))
		})

		It("does not share the table's egress with the knight", func() {
			rt := withEgress()
			k := newKnight(aiv1alpha1.LabelRoundTable, nil)
			ApplyTableDefaults(ctx, reader(rt), k)
			k.Spec.Egress.AllowedHosts[0] = "changed.example"
			Expect(rt.Spec.Defaults.Egress.AllowedHosts[0]).To(Equal("github.com"))
		})

		It("leaves egress unset without a table default", func() {
			k := newKnight(aiv1alpha1.LabelRoundTable, nil)
			ApplyTableDefaults(ctx, reader(table()), k)
			Expect(k.Spec.Egress).To(BeNil())
		})
	})

	It("finds the table through the legacy ai.roundtable.io/table label", func() {
		k := newKnight(LegacyTableLabel, nil)
		ApplyTableDefaults(ctx, reader(table()), k)
		Expect(k.Spec.Model).To(Equal("table-model"))
		Expect(k.Spec.Vault).To(Equal(tableVault))
	})

	It("applies the operator defaults without a table", func() {
		k := newKnight(aiv1alpha1.LabelRoundTable, nil)
		ApplyTableDefaults(ctx, reader(), k)

		Expect(k.Spec.Model).To(Equal(DefaultModel))
		Expect(k.Spec.Concurrency).To(Equal(DefaultConcurrency))
		Expect(k.Spec.TaskTimeoutSeconds).To(Equal(DefaultTaskTimeoutSeconds))
		Expect(k.Spec.Image).To(BeEmpty())
		Expect(k.Spec.Vault).To(BeNil())
		Expect(EffectiveImage(k, "default-img")).To(Equal("default-img"))
		e := env(k)
		Expect(e["KNIGHT_MODEL"]).To(Equal(DefaultModel))
		Expect(e["MAX_CONCURRENT_TASKS"]).To(Equal("2"))
		Expect(e["TASK_TIMEOUT_MS"]).To(Equal("120000"))
	})

	It("applies the operator defaults to fields the table leaves unset", func() {
		rt := table()
		rt.Spec.Defaults = &aiv1alpha1.RoundTableDefaults{Image: "table-img:1"}
		rt.Spec.Vault = nil
		k := newKnight(aiv1alpha1.LabelRoundTable, nil)
		ApplyTableDefaults(ctx, reader(rt), k)

		Expect(k.Spec.Image).To(Equal("table-img:1"))
		Expect(k.Spec.Model).To(Equal(DefaultModel))
		Expect(k.Spec.Concurrency).To(Equal(DefaultConcurrency))
		Expect(k.Spec.TaskTimeoutSeconds).To(Equal(DefaultTaskTimeoutSeconds))
		Expect(k.Spec.Vault).To(BeNil())
	})

	It("inherits only the vault from a table without spec.defaults", func() {
		rt := table()
		rt.Spec.Defaults = nil
		k := newKnight(aiv1alpha1.LabelRoundTable, nil)
		ApplyTableDefaults(ctx, reader(rt), k)
		Expect(k.Spec.Vault).To(Equal(tableVault))
		Expect(k.Spec.Model).To(Equal(DefaultModel))
	})

	It("does not share the table's vault with the knight", func() {
		rt := table()
		k := newKnight(aiv1alpha1.LabelRoundTable, nil)
		ApplyTableDefaults(ctx, reader(rt), k)
		k.Spec.Vault.WritablePaths[0] = "changed/"
		Expect(rt.Spec.Vault.WritablePaths).To(Equal([]string{"Briefings/"}))
	})

	It("WithTableDefaults returns a defaulted copy and leaves the knight alone", func() {
		k := newKnight(aiv1alpha1.LabelRoundTable, nil)
		out := WithTableDefaults(ctx, reader(table()), k)
		Expect(out.Spec.Model).To(Equal("table-model"))
		Expect(Concurrency(out)).To(Equal(int32(4)))
		Expect(k.Spec.Model).To(BeEmpty())
		Expect(k.Spec.Concurrency).To(BeZero())
		Expect(k.Spec.Vault).To(BeNil())
	})

	It("rolls the pod when a table default changes", func() {
		rt := table()
		k1 := WithTableDefaults(ctx, reader(rt), newKnight(aiv1alpha1.LabelRoundTable, nil))
		rt.Spec.Defaults.Model = "new-model"
		k2 := WithTableDefaults(ctx, reader(rt), newKnight(aiv1alpha1.LabelRoundTable, nil))
		spec := func(k *aiv1alpha1.Knight) corev1.PodSpec { return NewPodBuilder(k, "img").Build(ctx) }
		Expect(spec(k1)).NotTo(Equal(spec(k2)))
	})
})
