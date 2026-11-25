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
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

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
