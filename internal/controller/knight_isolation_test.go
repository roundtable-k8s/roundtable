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
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
	missionpkg "github.com/roundtable-k8s/roundtable/internal/mission"
	rtmetrics "github.com/roundtable-k8s/roundtable/pkg/metrics"
)

func isolatedCond(k *aiv1alpha1.Knight) *metav1.Condition {
	return meta.FindStatusCondition(getKnight(k).Status.Conditions, aiv1alpha1.ConditionKnightIsolated)
}

func isolatedMetric(k *aiv1alpha1.Knight, reason string) float64 {
	return testutil.ToFloat64(rtmetrics.KnightIsolated.WithLabelValues(knightpkg.KnightTable(k), k.Name, reason))
}

var _ = Describe("Knight isolation defaults (#387)", func() {
	isolationReconciler := func() *KnightReconciler {
		r := newScalingReconciler(false)
		r.Isolation = knightpkg.DefaultIsolationSettings()
		return r
	}

	It("parks a knight without opt-out while the gvisor RuntimeClass is missing", func() {
		k := egressKnight("iso-default", nil)
		Expect(k8sClient.Create(ctx, k)).To(Succeed())
		DeferCleanup(deleteKnight, k)
		DeferCleanup(func() {
			_ = k8sClient.Delete(ctx, &nodev1.RuntimeClass{ObjectMeta: metav1.ObjectMeta{Name: "gvisor"}})
		})
		r := isolationReconciler()
		reconcileKnight(r, k)

		c := isolatedCond(k)
		Expect(c).NotTo(BeNil())
		Expect(c.Status).To(Equal(metav1.ConditionFalse))
		Expect(c.Reason).To(Equal(aiv1alpha1.ReasonRuntimeClassNotFound))
		Expect(c.Message).To(ContainSubstring(`"gvisor"`))
		Expect(c.Message).To(ContainSubstring("spec.isolation.runtime: none"))
		Expect(c.Message).To(ContainSubstring(`isolation.runtimeClassName: ""`))
		rc := meta.FindStatusCondition(getKnight(k).Status.Conditions, aiv1alpha1.ConditionKnightRuntimeAvailable)
		Expect(rc.Reason).To(Equal(aiv1alpha1.ReasonRuntimeClassNotFound))
		Expect(getKnight(k).Status.Phase).To(Equal(aiv1alpha1.KnightPhaseDegraded))
		Expect(isolatedMetric(k, aiv1alpha1.ReasonRuntimeClassNotFound)).To(Equal(1.0))

		Expect(k8sClient.Create(ctx, &nodev1.RuntimeClass{
			ObjectMeta: metav1.ObjectMeta{Name: "gvisor"}, Handler: "runsc",
		})).To(Succeed())
		reconcileKnight(r, k)

		// No Cilium in this envtest: the default-deny fallback NetworkPolicy
		// can't enforce hostnames, so the knight is not fully isolated.
		c = isolatedCond(k)
		Expect(c.Reason).To(Equal(aiv1alpha1.ReasonFQDNPolicyUnavailable))
		rc = meta.FindStatusCondition(getKnight(k).Status.Conditions, aiv1alpha1.ConditionKnightRuntimeAvailable)
		Expect(rc.Status).To(Equal(metav1.ConditionTrue))
		Expect(getKnightDeployment(k).Spec.Template.Spec.RuntimeClassName).To(HaveValue(Equal("gvisor")))
		_, err := getEgressNetPol(k)
		Expect(err).NotTo(HaveOccurred(), "default-deny renders a policy without spec.egress")
		Expect(isolatedMetric(k, aiv1alpha1.ReasonRuntimeClassNotFound)).To(Equal(0.0))
		Expect(isolatedMetric(k, aiv1alpha1.ReasonFQDNPolicyUnavailable)).To(Equal(1.0))
	})

	It("requires a reason to opt out", func() {
		for _, iso := range []*aiv1alpha1.KnightIsolation{
			{Runtime: "none"},
			{Egress: "unrestricted"},
			{Runtime: "none", Reason: "too short"},
		} {
			k := egressKnight("iso-noreason", nil)
			k.Spec.Isolation = iso
			err := k8sClient.Create(ctx, k)
			Expect(err).To(HaveOccurred(), "%+v", *iso)
			Expect(err.Error()).To(ContainSubstring("reason"))
		}
		bad := egressKnight("iso-badvalue", nil)
		bad.Spec.Isolation = &aiv1alpha1.KnightIsolation{Runtime: "off", Reason: "a long enough reason"}
		Expect(apierrors.IsInvalid(k8sClient.Create(ctx, bad))).To(BeTrue())

		ok := egressKnight("iso-defaults-ok", nil)
		ok.Spec.Isolation = &aiv1alpha1.KnightIsolation{Runtime: "default", Egress: "default"}
		Expect(k8sClient.Create(ctx, ok)).To(Succeed())
		DeferCleanup(deleteKnight, ok)
	})

	It("honours an explicit opt-out with a reason", func() {
		k := egressKnight("iso-optout", nil)
		k.Spec.Isolation = &aiv1alpha1.KnightIsolation{
			Runtime: "none", Egress: "unrestricted", Reason: "needs ptrace for the debugger skill",
		}
		Expect(k8sClient.Create(ctx, k)).To(Succeed())
		DeferCleanup(deleteKnight, k)
		reconcileKnight(isolationReconciler(), k)

		c := isolatedCond(k)
		Expect(c.Status).To(Equal(metav1.ConditionFalse))
		Expect(c.Reason).To(Equal(aiv1alpha1.ReasonIsolationOptedOut))
		Expect(c.Message).To(ContainSubstring("needs ptrace for the debugger skill"))
		Expect(getKnightDeployment(k).Spec.Template.Spec.RuntimeClassName).To(BeNil())
		_, err := getEgressNetPol(k)
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
		Expect(isolatedMetric(k, aiv1alpha1.ReasonIsolationOptedOut)).To(Equal(1.0))
	})

	It("changes nothing with the install-wide off switch", func() {
		k := egressKnight("iso-off", nil)
		Expect(k8sClient.Create(ctx, k)).To(Succeed())
		DeferCleanup(deleteKnight, k)
		r := newScalingReconciler(false)
		r.Isolation = knightpkg.IsolationSettings{BaseHosts: knightpkg.DefaultEgressBaseHosts}
		reconcileKnight(r, k)

		Expect(getKnightDeployment(k).Spec.Template.Spec.RuntimeClassName).To(BeNil())
		_, err := getEgressNetPol(k)
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
		Expect(isolatedCond(k).Reason).To(Equal(aiv1alpha1.ReasonIsolationDisabled))
	})
})

func TestIsolationState(t *testing.T) {
	gv := "gvisor"
	k := &aiv1alpha1.Knight{Spec: aiv1alpha1.KnightSpec{RuntimeClassName: &gv}}
	setEgress := func(status metav1.ConditionStatus, reason string) {
		meta.SetStatusCondition(&k.Status.Conditions, metav1.Condition{
			Type: aiv1alpha1.ConditionKnightEgressRestricted, Status: status, Reason: reason, Message: "m",
		})
	}

	setEgress(metav1.ConditionTrue, aiv1alpha1.ReasonFQDNPolicyApplied)
	status, reason, _ := isolationState(k)
	assert.Equal(t, metav1.ConditionTrue, status)
	assert.Equal(t, aiv1alpha1.ReasonIsolated, reason)

	setEgress(metav1.ConditionFalse, aiv1alpha1.ReasonFQDNPolicyUnavailable)
	_, reason, _ = isolationState(k)
	assert.Equal(t, aiv1alpha1.ReasonFQDNPolicyUnavailable, reason)

	setEgress(metav1.ConditionFalse, aiv1alpha1.ReasonEgressPolicyFailed)
	_, reason, _ = isolationState(k)
	assert.Equal(t, aiv1alpha1.ReasonEgressPolicyFailed, reason)

	meta.RemoveStatusCondition(&k.Status.Conditions, aiv1alpha1.ConditionKnightEgressRestricted)
	_, reason, _ = isolationState(k)
	assert.Equal(t, aiv1alpha1.ReasonIsolationDisabled, reason)

	k.Spec.Isolation = &aiv1alpha1.KnightIsolation{Egress: "unrestricted", Reason: "crawls arbitrary sites"}
	status, reason, msg := isolationState(k)
	assert.Equal(t, metav1.ConditionFalse, status)
	assert.Equal(t, aiv1alpha1.ReasonIsolationOptedOut, reason)
	assert.Contains(t, msg, "egress: unrestricted")
	assert.Contains(t, msg, "crawls arbitrary sites")

	k.Spec.Isolation = nil
	k.Spec.RuntimeClassName = nil
	_, reason, _ = isolationState(k)
	assert.Equal(t, aiv1alpha1.ReasonIsolationDisabled, reason)
}

// TestDefaultDenyPolicyHasBaseline: a knight with no fields set gets a
// deny-by-default policy with the baseline under the operator defaults.
func TestDefaultDenyPolicyHasBaseline(t *testing.T) {
	k := testEgressKnight("")
	k.Spec.Egress = nil
	k.Spec.Arsenals = []aiv1alpha1.KnightArsenal{{Repo: "https://github.com/roundtable-k8s/arsenal"}}
	r := &KnightReconciler{Isolation: knightpkg.DefaultIsolationSettings()}
	knightpkg.ApplyIsolationDefaults(k, r.Isolation)
	allow := r.egressAllowlistOf(k)
	require.NotNil(t, allow)

	cnp := RenderCiliumEgressPolicy(k, *allow, missionpkg.DefaultNetPolSettings())
	egress, _, _ := unstructured.NestedSlice(cnp.Object, "spec", "egress")
	// DNS, NATS, kube-apiserver, base and arsenal hosts on 443; no allowlist rule.
	require.Len(t, egress, 4)
	assert.Equal(t, map[string]any{"toEntities": []any{"kube-apiserver"}}, egress[2])
	assert.Equal(t, ciliumFQDNRule([]string{"api.anthropic.com", "github.com", "openrouter.ai"}, []int32{443}), egress[3])
}

var _ = Describe("Knight arsenal OCI validation", func() {
	const digest = "registry.example.com/arsenal@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	It("requires a digest-pinned reference and excludes a custom repo", func() {
		bad := egressKnight("arsenal-oci-nodigest", nil)
		bad.Spec.Arsenals = []aiv1alpha1.KnightArsenal{{OCI: &aiv1alpha1.KnightArsenalOCI{Reference: "registry.example.com/arsenal:latest"}}}
		err := k8sClient.Create(ctx, bad)
		Expect(apierrors.IsInvalid(err)).To(BeTrue())
		Expect(err.Error()).To(ContainSubstring("sha256"))

		both := egressKnight("arsenal-oci-repo", nil)
		both.Spec.Arsenals = []aiv1alpha1.KnightArsenal{{Repo: "https://github.com/x/y", OCI: &aiv1alpha1.KnightArsenalOCI{Reference: digest}}}
		err = k8sClient.Create(ctx, both)
		Expect(apierrors.IsInvalid(err)).To(BeTrue())
		Expect(err.Error()).To(ContainSubstring("mutually exclusive"))

		ok := egressKnight("arsenal-oci-ok", nil)
		ok.Spec.Arsenals = []aiv1alpha1.KnightArsenal{{OCI: &aiv1alpha1.KnightArsenalOCI{Reference: digest}}}
		Expect(k8sClient.Create(ctx, ok)).To(Succeed())
		Expect(ok.Spec.Arsenals[0].OCI.PullPolicy).To(Equal(corev1.PullIfNotPresent))
	})
})
