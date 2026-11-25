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
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

var _ = Describe("Pod scheduling fields", func() {
	ctx := context.Background()

	type sched struct {
		NodeSelector map[string]string
		Affinity     *corev1.Affinity
		Tolerations  []corev1.Toleration
		Spread       []corev1.TopologySpreadConstraint
		Priority     string
	}
	mk := func(tag string) sched {
		return sched{
			NodeSelector: map[string]string{"node-size": tag},
			Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "zone", Operator: corev1.NodeSelectorOpIn, Values: []string{tag}},
					}}},
				},
			}},
			Tolerations: []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: tag, Effect: corev1.TaintEffectNoSchedule}},
			Spread: []corev1.TopologySpreadConstraint{{
				MaxSkew: 1, TopologyKey: "kubernetes.io/hostname", WhenUnsatisfiable: corev1.ScheduleAnyway,
				LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": tag}},
			}},
			Priority: "pc-" + tag,
		}
	}
	knight := func(s *sched) *aiv1alpha1.Knight {
		k := &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{
				Name: "sched", Namespace: "default",
				Labels: map[string]string{aiv1alpha1.LabelRoundTable: "sched-table"},
			},
			Spec: aiv1alpha1.KnightSpec{
				Domain: "d",
				NATS:   aiv1alpha1.KnightNATS{URL: "nats://x:4222", Stream: "s", ResultsStream: "r", Subjects: []string{"a.>"}},
			},
		}
		if s != nil {
			k.Spec.NodeSelector, k.Spec.Affinity, k.Spec.Tolerations = s.NodeSelector, s.Affinity, s.Tolerations
			k.Spec.TopologySpreadConstraints, k.Spec.PriorityClassName = s.Spread, s.Priority
		}
		return k
	}
	table := func(s *sched) *aiv1alpha1.RoundTable {
		d := &aiv1alpha1.RoundTableDefaults{Concurrency: 1}
		if s != nil {
			d.NodeSelector, d.Affinity, d.Tolerations = s.NodeSelector, s.Affinity, s.Tolerations
			d.TopologySpreadConstraints, d.PriorityClassName = s.Spread, s.Priority
		}
		return &aiv1alpha1.RoundTable{
			ObjectMeta: metav1.ObjectMeta{Name: "sched-table", Namespace: "default"},
			Spec:       aiv1alpha1.RoundTableSpec{Defaults: d},
		}
	}
	build := func(k *aiv1alpha1.Knight, rt *aiv1alpha1.RoundTable) corev1.PodSpec {
		s := runtime.NewScheme()
		Expect(aiv1alpha1.AddToScheme(s)).To(Succeed())
		ApplyTableDefaults(ctx, fake.NewClientBuilder().WithScheme(s).WithObjects(rt).Build(), k)
		return NewPodBuilder(k, "img").Build(ctx)
	}
	expect := func(p corev1.PodSpec, s sched) {
		Expect(p.NodeSelector).To(Equal(s.NodeSelector))
		Expect(p.Affinity).To(Equal(s.Affinity))
		Expect(p.Tolerations).To(Equal(s.Tolerations))
		Expect(p.TopologySpreadConstraints).To(Equal(s.Spread))
		Expect(p.PriorityClassName).To(Equal(s.Priority))
	}

	It("copies the knight's fields onto the pod unchanged", func() {
		own := mk("own")
		expect(build(knight(&own), table(nil)), own)
	})
	It("inherits the table defaults when the knight leaves them unset", func() {
		def := mk("table")
		expect(build(knight(nil), table(&def)), def)
	})
	It("prefers the knight's own value, per field, over the table default", func() {
		own, def := mk("own"), mk("table")
		k := knight(nil)
		k.Spec.NodeSelector = own.NodeSelector
		p := build(k, table(&def))
		Expect(p.NodeSelector).To(Equal(own.NodeSelector))
		Expect(p.Tolerations).To(Equal(def.Tolerations))
		Expect(p.PriorityClassName).To(Equal(def.Priority))
	})
	It("leaves the pod unscheduled-constrained when nothing is set", func() {
		p := build(knight(nil), table(nil))
		Expect(p.NodeSelector).To(BeEmpty())
		Expect(p.Affinity).To(BeNil())
		Expect(p.Tolerations).To(BeEmpty())
		Expect(p.TopologySpreadConstraints).To(BeEmpty())
		Expect(p.PriorityClassName).To(BeEmpty())
	})
})
