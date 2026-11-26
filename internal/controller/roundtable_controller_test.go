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

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	rtmetrics "github.com/roundtable-k8s/roundtable/pkg/metrics"
)

var _ = Describe("RoundTable metrics", func() {
	It("counts knights and missions per table and phase", func() {
		ctx := context.Background()
		s := k8sruntime.NewScheme()
		Expect(aiv1alpha1.AddToScheme(s)).To(Succeed())
		mission := &aiv1alpha1.Mission{
			ObjectMeta: metav1.ObjectMeta{Name: "m1", Namespace: "default"},
			Spec:       aiv1alpha1.MissionSpec{RoundTableRef: &corev1.LocalObjectReference{Name: "metrics-table"}},
			Status:     aiv1alpha1.MissionStatus{Phase: aiv1alpha1.MissionPhaseActive},
		}
		other := &aiv1alpha1.Mission{
			ObjectMeta: metav1.ObjectMeta{Name: "m2", Namespace: "default"},
			Spec:       aiv1alpha1.MissionSpec{RoundTableRef: &corev1.LocalObjectReference{Name: "other"}},
			Status:     aiv1alpha1.MissionStatus{Phase: aiv1alpha1.MissionPhaseActive},
		}
		r := &RoundTableReconciler{Client: fake.NewClientBuilder().WithScheme(s).WithObjects(mission, other).Build()}
		rt := &aiv1alpha1.RoundTable{ObjectMeta: metav1.ObjectMeta{Name: "metrics-table", Namespace: "default"}}
		ready := aiv1alpha1.Knight{Status: aiv1alpha1.KnightStatus{Phase: aiv1alpha1.KnightPhaseReady}}
		degraded := aiv1alpha1.Knight{Status: aiv1alpha1.KnightStatus{Phase: aiv1alpha1.KnightPhaseDegraded}}

		r.updateMetrics(ctx, rt, []aiv1alpha1.Knight{ready, ready, degraded}, 2.5)
		Expect(testutil.ToFloat64(rtmetrics.Knights.WithLabelValues("metrics-table", "Ready"))).To(Equal(2.0))
		Expect(testutil.ToFloat64(rtmetrics.Knights.WithLabelValues("metrics-table", "Degraded"))).To(Equal(1.0))
		Expect(testutil.ToFloat64(rtmetrics.Missions.WithLabelValues("metrics-table", "Active"))).To(Equal(1.0))
		Expect(testutil.ToFloat64(rtmetrics.TableCostUSD.WithLabelValues("metrics-table"))).To(Equal(2.5))

		// A phase with no members left must disappear, not go stale.
		r.updateMetrics(ctx, rt, []aiv1alpha1.Knight{ready}, 2.5)
		Expect(testutil.ToFloat64(rtmetrics.Knights.WithLabelValues("metrics-table", "Ready"))).To(Equal(1.0))
		Expect(rtmetrics.Knights.DeleteLabelValues("metrics-table", "Degraded")).To(BeFalse(), "stale Degraded series")
	})
})

var _ = Describe("RoundTable usage totals", func() {
	It("sums chain and mission cost and chain runs for the table", func() {
		ctx := context.Background()
		s := k8sruntime.NewScheme()
		Expect(aiv1alpha1.AddToScheme(s)).To(Succeed())
		ref := func(n string) *corev1.LocalObjectReference { return &corev1.LocalObjectReference{Name: n} }
		chain := func(name, table, cost string, runs int64) *aiv1alpha1.Chain {
			return &aiv1alpha1.Chain{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
				Spec:       aiv1alpha1.ChainSpec{RoundTableRef: ref(table)},
				Status:     aiv1alpha1.ChainStatus{CostUSD: cost, RunsCompleted: runs},
			}
		}
		missionChain := chain("mc", "", "0.5000", 1)
		missionChain.Spec.RoundTableRef = nil
		missionChain.Spec.MissionRef = ref("m1")
		mission := &aiv1alpha1.Mission{
			ObjectMeta: metav1.ObjectMeta{Name: "m1", Namespace: "default"},
			Spec:       aiv1alpha1.MissionSpec{RoundTableRef: ref("usage-table")},
			Status:     aiv1alpha1.MissionStatus{TotalCost: "0.5000"},
		}
		r := &RoundTableReconciler{Client: fake.NewClientBuilder().WithScheme(s).WithObjects(
			chain("a", "usage-table", "0.2500", 3), chain("b", "usage-table", "1.0000", 2),
			chain("x", "other", "9.0000", 7), missionChain, mission).Build()}
		rt := &aiv1alpha1.RoundTable{ObjectMeta: metav1.ObjectMeta{Name: "usage-table", Namespace: "default"}}

		tasks, cost, err := r.tableUsage(ctx, rt)
		Expect(err).NotTo(HaveOccurred())
		Expect(tasks).To(Equal(int64(6)))
		Expect(cost).To(BeNumerically("~", 1.75, 1e-9))
	})
})

var _ = Describe("RoundTable health with knights at rest", func() {
	const (
		namespace = "default"
		table     = "idle-rt"
	)
	member := map[string]string{"idle-test": table}

	reconcileTable := func() *aiv1alpha1.RoundTable {
		r := &RoundTableReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: record.NewFakeRecorder(100)}
		key := types.NamespacedName{Name: table, Namespace: namespace}
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())
		rt := &aiv1alpha1.RoundTable{}
		Expect(k8sClient.Get(ctx, key, rt)).To(Succeed())
		return rt
	}

	// newKnight creates a member knight and writes its status: phase plus an
	// Available condition with the given reason ("" for none).
	newKnight := func(name string, suspend bool, phase aiv1alpha1.KnightPhase, ready bool, reason string) {
		k := &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: member},
			Spec: aiv1alpha1.KnightSpec{
				Domain:  "ops",
				Model:   "model-a",
				Skills:  []string{"ops"},
				Suspend: suspend,
				NATS:    aiv1alpha1.KnightNATS{Subjects: []string{"idle.tasks.ops.>"}},
			},
		}
		Expect(k8sClient.Create(ctx, k)).To(Succeed())
		k.Status.Phase = phase
		k.Status.Ready = ready
		if reason != "" {
			status := metav1.ConditionFalse
			if ready {
				status = metav1.ConditionTrue
			}
			meta.SetStatusCondition(&k.Status.Conditions, metav1.Condition{
				Type: aiv1alpha1.ConditionKnightAvailable, Status: status, Reason: reason, Message: "test",
			})
		}
		Expect(k8sClient.Status().Update(ctx, k)).To(Succeed())
	}

	BeforeEach(func() {
		Expect(k8sClient.Create(ctx, &aiv1alpha1.RoundTable{
			ObjectMeta: metav1.ObjectMeta{Name: table, Namespace: namespace},
			Spec: aiv1alpha1.RoundTableSpec{
				NATS:           aiv1alpha1.RoundTableNATS{SubjectPrefix: "idle", TasksStream: "idle_tasks", ResultsStream: "idle_results"},
				KnightSelector: &metav1.LabelSelector{MatchLabels: member},
				// No owned planner: these specs count exactly the knights they create.
				Planner: &aiv1alpha1.RoundTablePlanner{Enabled: new(false)},
			},
		})).To(Succeed())
	})

	AfterEach(func() {
		Expect(k8sClient.DeleteAllOf(ctx, &aiv1alpha1.Knight{},
			client.InNamespace(namespace), client.MatchingLabels(member))).To(Succeed())
		Expect(k8sClient.Delete(ctx, &aiv1alpha1.RoundTable{
			ObjectMeta: metav1.ObjectMeta{Name: table, Namespace: namespace},
		})).To(Succeed())
	})

	It("reports Ready when every knight is scaled to zero", func() {
		for _, name := range []string{"idle-a", "idle-b", "idle-c"} {
			newKnight(name, false, aiv1alpha1.KnightPhaseSuspended, false, aiv1alpha1.ReasonKnightScaledToZero)
		}
		rt := reconcileTable()
		Expect(rt.Status.Phase).To(Equal(aiv1alpha1.RoundTablePhaseReady))
		Expect(rt.Status.KnightsTotal).To(Equal(int32(3)))
		Expect(rt.Status.KnightsReady).To(BeZero())
		Expect(rt.Status.KnightsScaledToZero).To(Equal(int32(3)))
		Expect(rt.Status.KnightsNotReady).To(BeZero())
		cond := meta.FindStatusCondition(rt.Status.Conditions, aiv1alpha1.ConditionRoundTableAvailable)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		Expect(cond.Message).To(Equal("3 knights: 0 ready, 3 scaled to zero, 0 suspended, 0 not ready"))
	})

	It("counts ready, waking, user-suspended knights as healthy and a crashed one as Degraded", func() {
		newKnight("idle-ready", false, aiv1alpha1.KnightPhaseReady, true, aiv1alpha1.ReasonKnightReady)
		newKnight("idle-zero", false, aiv1alpha1.KnightPhaseSuspended, false, aiv1alpha1.ReasonKnightScaledToZero)
		newKnight("idle-waking", false, aiv1alpha1.KnightPhaseSuspended, false, aiv1alpha1.ReasonKnightWaking)
		newKnight("idle-off", true, aiv1alpha1.KnightPhaseSuspended, false, aiv1alpha1.ReasonKnightSuspended)

		rt := reconcileTable()
		Expect(rt.Status.Phase).To(Equal(aiv1alpha1.RoundTablePhaseReady))
		Expect(rt.Status.KnightsReady).To(Equal(int32(1)))
		Expect(rt.Status.KnightsScaledToZero).To(Equal(int32(2)))
		Expect(rt.Status.KnightsSuspended).To(Equal(int32(1)))

		By("a knight that should run crashing")
		newKnight("idle-crashed", false, aiv1alpha1.KnightPhaseDegraded, false, aiv1alpha1.ReasonKnightReconcileError)
		rt = reconcileTable()
		Expect(rt.Status.Phase).To(Equal(aiv1alpha1.RoundTablePhaseDegraded))
		Expect(rt.Status.KnightsNotReady).To(Equal(int32(1)))
		cond := meta.FindStatusCondition(rt.Status.Conditions, aiv1alpha1.ConditionRoundTableAvailable)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal(aiv1alpha1.ReasonKnightsDegraded))
		Expect(cond.Message).To(Equal("5 knights: 1 ready, 2 scaled to zero, 1 suspended, 1 not ready"))
	})
})

var _ = Describe("RoundTable deprecated policies", func() {
	table := func(ephemeral bool, p *aiv1alpha1.RoundTablePolicies) *aiv1alpha1.RoundTable {
		return &aiv1alpha1.RoundTable{
			ObjectMeta: metav1.ObjectMeta{Generation: 1},
			Spec:       aiv1alpha1.RoundTableSpec{Ephemeral: ephemeral, Policies: p},
		}
	}
	run := func(rt *aiv1alpha1.RoundTable) (*metav1.Condition, *record.FakeRecorder) {
		rec := record.NewFakeRecorder(10)
		r := &RoundTableReconciler{Recorder: rec}
		r.setDeprecatedFieldsCondition(rt)
		r.setDeprecatedFieldsCondition(rt) // repeated reconcile, same generation
		return meta.FindStatusCondition(rt.Status.Conditions, aiv1alpha1.ConditionRoundTableDeprecatedFieldsUsed), rec
	}

	DescribeTable("reports each deprecated field on a standing table",
		func(p *aiv1alpha1.RoundTablePolicies, field string) {
			c, rec := run(table(false, p))
			Expect(c).NotTo(BeNil())
			Expect(c.Status).To(Equal(metav1.ConditionTrue))
			Expect(c.Message).To(ContainSubstring(field))
			Expect(countEvents(rec, aiv1alpha1.ReasonRoundTableDeprecatedFieldsUsed)).To(Equal(1))
		},
		Entry("costResetSchedule", &aiv1alpha1.RoundTablePolicies{CostResetSchedule: "0 0 1 * *"}, "policies.costResetSchedule"),
	)

	It("is silent when only enforced policies are set", func() {
		c, rec := run(table(false, &aiv1alpha1.RoundTablePolicies{MaxMissions: 2, MaxConcurrentPlanTasks: 3, CostBudgetUSD: "5"}))
		Expect(c).To(BeNil())
		Expect(drainEvents(rec)).To(BeEmpty())
	})

	It("is silent on an ephemeral table", func() {
		c, rec := run(table(true, &aiv1alpha1.RoundTablePolicies{CostResetSchedule: "0 0 1 * *"}))
		Expect(c).To(BeNil())
		Expect(drainEvents(rec)).To(BeEmpty())
	})

	It("clears the condition when the fields are removed", func() {
		rt := table(false, &aiv1alpha1.RoundTablePolicies{CostResetSchedule: "0 0 1 * *"})
		c, _ := run(rt)
		Expect(c).NotTo(BeNil())
		rt.Spec.Policies.CostResetSchedule = "" //nolint:staticcheck // exercising the deprecated field
		rt.Generation = 2
		c, _ = run(rt)
		Expect(c).To(BeNil())
	})
})
