package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	rtmetrics "github.com/roundtable-k8s/roundtable/pkg/metrics"
)

var _ = Describe("Unowned knight and mission metrics", func() {
	knight := func(name string, lbls map[string]string) *aiv1alpha1.Knight {
		return &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: lbls},
			Status:     aiv1alpha1.KnightStatus{Phase: aiv1alpha1.KnightPhaseReady},
		}
	}

	It("counts only knights and missions no RoundTable counts under table=none", func() {
		ctx := context.Background()
		s := k8sruntime.NewScheme()
		Expect(aiv1alpha1.AddToScheme(s)).To(Succeed())

		all := &aiv1alpha1.RoundTable{ObjectMeta: metav1.ObjectMeta{Name: "all", Namespace: "default"}}
		fleet := &aiv1alpha1.RoundTable{
			ObjectMeta: metav1.ObjectMeta{Name: "fleet", Namespace: "other"},
			Spec: aiv1alpha1.RoundTableSpec{KnightSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"roundtable.ai.roundtable.io/fleet": "a"},
			}},
		}
		// "all" has no selector: it takes every knight in "default", so only
		// knights in other namespaces / ephemeral knights can be table-less.
		stray := knight("stray", nil)
		stray.Namespace = "elsewhere"
		ephemeral := knight("eph", map[string]string{aiv1alpha1.LabelEphemeral: "true", aiv1alpha1.LabelRoundTable: "gone"})
		// Selected by a non-table label in a namespace with a selector table.
		selected := knight("selected", map[string]string{"roundtable.ai.roundtable.io/fleet": "a"})
		selected.Namespace = "other"
		unselected := knight("unselected", nil)
		unselected.Namespace = "other"
		// Planner owned by a table whose selector does not match it.
		planner := knight("planner", nil)
		planner.Namespace = "other"
		planner.OwnerReferences = []metav1.OwnerReference{{APIVersion: aiv1alpha1.GroupVersion.String(), Kind: "RoundTable", Name: "fleet", Controller: new(true)}}
		inDefault := knight("in-default", nil)
		// Labelled with a table that does not exist, in a namespace without tables.
		ghost := knight("ghost", map[string]string{aiv1alpha1.LabelRoundTable: "missing"})
		ghost.Namespace = "elsewhere"

		missionOf := func(name, table string) *aiv1alpha1.Mission {
			m := &aiv1alpha1.Mission{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
				Status:     aiv1alpha1.MissionStatus{Phase: aiv1alpha1.MissionPhaseActive},
			}
			if table != "" {
				m.Spec.RoundTableRef = &corev1.LocalObjectReference{Name: table}
			}
			return m
		}
		looseM := missionOf("lm", "")
		c := fake.NewClientBuilder().WithScheme(s).WithObjects(
			all, fleet, stray, ephemeral, selected, unselected, planner, inDefault, ghost,
			missionOf("om", "all"), missionOf("missing-ref", "nope"), looseM,
		).Build()

		updateUnownedKnightMetrics(ctx, c)
		updateUnownedMissionMetrics(ctx, c)
		// Table-less: stray, eph (ephemeral, default ns), unselected, ghost.
		Expect(testutil.ToFloat64(rtmetrics.Knights.WithLabelValues("none", "Ready"))).To(Equal(4.0))
		// Table-less: lm and the mission naming a missing table.
		Expect(testutil.ToFloat64(rtmetrics.Missions.WithLabelValues("none", "Active"))).To(Equal(2.0))

		Expect(c.Delete(ctx, looseM)).To(Succeed())
		Expect(c.Delete(ctx, stray)).To(Succeed())
		updateUnownedKnightMetrics(ctx, c)
		updateUnownedMissionMetrics(ctx, c)
		Expect(testutil.ToFloat64(rtmetrics.Knights.WithLabelValues("none", "Ready"))).To(Equal(3.0))
		Expect(testutil.ToFloat64(rtmetrics.Missions.WithLabelValues("none", "Active"))).To(Equal(1.0))
	})
})
