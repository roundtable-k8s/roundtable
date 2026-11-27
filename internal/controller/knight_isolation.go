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
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
	rtmetrics "github.com/roundtable-k8s/roundtable/pkg/metrics"
)

// runtimeClassSource is where a knight's effective RuntimeClass came from.
type runtimeClassSource int

const (
	runtimeClassFromKnight runtimeClassSource = iota
	runtimeClassFromTable
	runtimeClassFromOperator
)

// isolationOptOutHint tells the user how to opt a single knight out.
const isolationOptOutHint = `or opt this knight out with spec.isolation.runtime: none and a spec.isolation.reason`

// runtimeClassNotFoundMessage names the missing class, where it came from and
// the ways out.
func runtimeClassNotFoundMessage(name string, src runtimeClassSource) string {
	var where string
	switch src {
	case runtimeClassFromKnight:
		where = "change spec.runtimeClassName"
	case runtimeClassFromTable:
		where = "change the RoundTable's spec.defaults.runtimeClassName (or set spec.runtimeClassName on the knight)"
	default:
		where = "set spec.runtimeClassName on the knight (or the RoundTable's spec.defaults.runtimeClassName), " +
			"or turn the operator default off with the chart value isolation.runtimeClassName: \"\""
	}
	return fmt.Sprintf("RuntimeClass %q does not exist; create it (install the runtime), %s, %s",
		name, where, isolationOptOutHint)
}

// setIsolated sets the Isolated condition and the isolation metric, and
// reports whether the condition changed.
func (r *KnightReconciler) setIsolated(k *aiv1alpha1.Knight, status metav1.ConditionStatus, reason, msg string) bool {
	setIsolatedMetric(k, reason)
	return meta.SetStatusCondition(&k.Status.Conditions, metav1.Condition{
		Type:               aiv1alpha1.ConditionKnightIsolated,
		Status:             status,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: k.Generation,
	})
}

// reconcileIsolated derives the Isolated condition from the knight's
// effective RuntimeClass, its opt-out and the EgressRestricted condition. Call
// it after ApplyIsolationDefaults and reconcileEgress.
func (r *KnightReconciler) reconcileIsolated(k *aiv1alpha1.Knight) {
	status, reason, msg := isolationState(k)
	r.setIsolated(k, status, reason, msg)
}

// isolationState returns the Isolated condition for the knight.
func isolationState(k *aiv1alpha1.Knight) (metav1.ConditionStatus, string, string) {
	iso := k.Spec.Isolation
	if iso.RuntimeOptedOut() || iso.EgressOptedOut() {
		var what []string
		if iso.RuntimeOptedOut() {
			what = append(what, "runtime: none")
		}
		if iso.EgressOptedOut() {
			what = append(what, "egress: unrestricted")
		}
		return metav1.ConditionFalse, aiv1alpha1.ReasonIsolationOptedOut,
			fmt.Sprintf("Knight opts out of isolation (%s): %s", strings.Join(what, ", "), iso.Reason)
	}
	rc := runtimeClassOf(k)
	if rc == "" {
		return metav1.ConditionFalse, aiv1alpha1.ReasonIsolationDisabled,
			"No RuntimeClass applies (the operator default is off and neither the knight nor its table sets runtimeClassName); " +
				"the knight runs on the cluster's default runtime"
	}
	egress := meta.FindStatusCondition(k.Status.Conditions, aiv1alpha1.ConditionKnightEgressRestricted)
	switch {
	case egress == nil:
		return metav1.ConditionFalse, aiv1alpha1.ReasonIsolationDisabled,
			fmt.Sprintf("RuntimeClass %q applies, but no egress policy (the operator's default egress mode is off and "+
				"neither the knight nor its table sets egress)", rc)
	case egress.Status == metav1.ConditionTrue:
		return metav1.ConditionTrue, aiv1alpha1.ReasonIsolated,
			fmt.Sprintf("RuntimeClass %q applies; %s", rc, egress.Message)
	case egress.Reason == aiv1alpha1.ReasonFQDNPolicyUnavailable:
		return metav1.ConditionFalse, aiv1alpha1.ReasonFQDNPolicyUnavailable, egress.Message
	default:
		return metav1.ConditionFalse, egress.Reason, egress.Message
	}
}

// isolatedMetricLabels selects the knight's series of the isolation metric.
func isolatedMetricLabels(k *aiv1alpha1.Knight) prometheus.Labels {
	return prometheus.Labels{"table": knightpkg.KnightTable(k), "knight": k.Name}
}

// setIsolatedMetric sets the knight's isolation metric to 1 for reason only.
func setIsolatedMetric(k *aiv1alpha1.Knight, reason string) {
	labels := isolatedMetricLabels(k)
	rtmetrics.KnightIsolated.DeletePartialMatch(labels)
	labels["reason"] = reason
	rtmetrics.KnightIsolated.With(labels).Set(1)
}

// deleteIsolatedMetric drops the knight's isolation metric series.
func deleteIsolatedMetric(k *aiv1alpha1.Knight) {
	rtmetrics.KnightIsolated.DeletePartialMatch(isolatedMetricLabels(k))
}

// deleteKnightWakeMetric drops the knight's wake-latency histogram series,
// whatever table label they were observed under.
func deleteKnightWakeMetric(k *aiv1alpha1.Knight) {
	rtmetrics.KnightWakeSeconds.DeletePartialMatch(prometheus.Labels{"knight": k.Name, "namespace": k.Namespace})
}
