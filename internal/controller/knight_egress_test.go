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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	missionpkg "github.com/roundtable-k8s/roundtable/internal/mission"
	rtruntime "github.com/roundtable-k8s/roundtable/pkg/runtime"
)

// ciliumNetworkPolicyCRD is a minimal stand-in for Cilium's CRD: enough for
// the API server to serve cilium.io/v2 CiliumNetworkPolicy.
func ciliumNetworkPolicyCRD() *apiextensionsv1.CustomResourceDefinition {
	preserve := true
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "ciliumnetworkpolicies.cilium.io"},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "cilium.io",
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural: "ciliumnetworkpolicies", Singular: "ciliumnetworkpolicy",
				Kind: "CiliumNetworkPolicy", ListKind: "CiliumNetworkPolicyList",
			},
			Scope: apiextensionsv1.NamespaceScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name: "v2", Served: true, Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Type:                   "object",
						XPreserveUnknownFields: &preserve,
					},
				},
			}},
		},
	}
}

const egressTestNATSURL = "nats://nats.database.svc.cluster.local:4222"

func egressKnight(name string, egress *aiv1alpha1.KnightEgress) *aiv1alpha1.Knight {
	return scalingKnight("default", name, func(s *aiv1alpha1.KnightSpec) {
		ephemeral(s)
		s.NATS.URL = egressTestNATSURL
		s.Egress = egress
	})
}

func getCNP(k *aiv1alpha1.Knight) (*unstructured.Unstructured, error) {
	cnp := &unstructured.Unstructured{}
	cnp.SetGroupVersionKind(rtruntime.CiliumNetworkPolicyGVK)
	err := k8sClient.Get(ctx, types.NamespacedName{Name: "knight-" + k.Name + "-egress", Namespace: k.Namespace}, cnp)
	return cnp, err
}

func getEgressNetPol(k *aiv1alpha1.Knight) (*networkingv1.NetworkPolicy, error) {
	np := &networkingv1.NetworkPolicy{}
	err := k8sClient.Get(ctx, types.NamespacedName{Name: "knight-" + k.Name + "-egress", Namespace: k.Namespace}, np)
	return np, err
}

func removeEgress(k *aiv1alpha1.Knight) {
	got := getKnight(k)
	got.Spec.Egress = nil
	Expect(k8sClient.Update(ctx, got)).To(Succeed())
}

var _ = Describe("Knight egress allowlist (#348)", func() {
	allow := &aiv1alpha1.KnightEgress{AllowedHosts: []string{"api.anthropic.com", "*.github.com"}}

	It("renders nothing for a knight without spec.egress", func() {
		k := egressKnight("egress-none", nil)
		Expect(k8sClient.Create(ctx, k)).To(Succeed())
		DeferCleanup(deleteKnight, k)
		reconcileKnight(newScalingReconciler(false), k)

		_, err := getEgressNetPol(k)
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
		Expect(meta.FindStatusCondition(getKnight(k).Status.Conditions, aiv1alpha1.ConditionKnightEgressRestricted)).To(BeNil())
	})

	It("defaults allowedPorts to 443 and rejects invalid hosts", func() {
		k := egressKnight("egress-defaults", &aiv1alpha1.KnightEgress{AllowedHosts: []string{"api.anthropic.com"}})
		Expect(k8sClient.Create(ctx, k)).To(Succeed())
		DeferCleanup(deleteKnight, k)
		Expect(getKnight(k).Spec.Egress.AllowedPorts).To(Equal([]int32{443}))

		for _, host := range []string{"https://api.anthropic.com", "api.anthropic.com/v1", "a.*.com", "*", "Upper.com"} {
			bad := egressKnight("egress-bad", &aiv1alpha1.KnightEgress{AllowedHosts: []string{host}})
			Expect(k8sClient.Create(ctx, bad)).NotTo(Succeed(), host)
		}
	})

	It("falls back to a NetworkPolicy without the Cilium API", func() {
		k := egressKnight("egress-netpol", allow)
		Expect(k8sClient.Create(ctx, k)).To(Succeed())
		DeferCleanup(deleteKnight, k)
		r := newScalingReconciler(false)
		reconcileKnight(r, k)

		np, err := getEgressNetPol(k)
		Expect(err).NotTo(HaveOccurred())
		Expect(metav1.IsControlledBy(np, getKnight(k))).To(BeTrue())
		Expect(np.Spec.PodSelector.MatchLabels).To(Equal(map[string]string{
			"app.kubernetes.io/name": "knight", "app.kubernetes.io/instance": k.Name,
		}))
		Expect(np.Spec.PolicyTypes).To(Equal([]networkingv1.PolicyType{networkingv1.PolicyTypeEgress}))
		Expect(np.Spec.Egress).To(HaveLen(3))
		// DNS, NATS in its namespace, then 443 to any destination.
		Expect(np.Spec.Egress[0].To[0].NamespaceSelector.MatchLabels).To(HaveKeyWithValue("kubernetes.io/metadata.name", "kube-system"))
		Expect(np.Spec.Egress[1].To[0].NamespaceSelector.MatchLabels).To(HaveKeyWithValue("kubernetes.io/metadata.name", "database"))
		Expect(np.Spec.Egress[1].Ports[0].Port.IntValue()).To(Equal(4222))
		Expect(np.Spec.Egress[2].To).To(BeEmpty())
		Expect(np.Spec.Egress[2].Ports).To(HaveLen(1))
		Expect(*np.Spec.Egress[2].Ports[0].Port).To(Equal(intstr.FromInt32(443)))

		cond := meta.FindStatusCondition(getKnight(k).Status.Conditions, aiv1alpha1.ConditionKnightEgressRestricted)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal(aiv1alpha1.ReasonFQDNPolicyUnavailable))

		removeEgress(k)
		reconcileKnight(r, k)
		_, err = getEgressNetPol(k)
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
		Expect(meta.FindStatusCondition(getKnight(k).Status.Conditions, aiv1alpha1.ConditionKnightEgressRestricted)).To(BeNil())
	})

	Describe("with Cilium", Ordered, func() {
		BeforeAll(func() {
			_, err := envtest.InstallCRDs(cfg, envtest.CRDInstallOptions{
				CRDs: []*apiextensionsv1.CustomResourceDefinition{ciliumNetworkPolicyCRD()},
			})
			Expect(err).NotTo(HaveOccurred())
		})

		ciliumReconciler := func() *KnightReconciler {
			r := newScalingReconciler(false)
			r.CiliumEnabled = true
			return r
		}

		It("detects the CiliumNetworkPolicy API through discovery", func() {
			served, err := rtruntime.CiliumNetworkPolicyAPIServed(discoveryFor())
			Expect(err).NotTo(HaveOccurred())
			Expect(served).To(BeTrue())
		})

		It("renders a CiliumNetworkPolicy and deletes it when egress is removed", func() {
			k := egressKnight("egress-cilium", allow)
			Expect(k8sClient.Create(ctx, k)).To(Succeed())
			DeferCleanup(deleteKnight, k)
			r := ciliumReconciler()

			// A fallback NetworkPolicy from before Cilium was installed is replaced.
			fallback := RenderEgressNetworkPolicy(getKnight(k), EgressAllowlist{}, missionpkg.DefaultNetPolSettings())
			setControllerRef(fallback, getKnight(k))
			Expect(k8sClient.Create(ctx, fallback)).To(Succeed())

			reconcileKnight(r, k)

			cnp, err := getCNP(k)
			Expect(err).NotTo(HaveOccurred())
			Expect(metav1.IsControlledBy(cnp, getKnight(k))).To(BeTrue())
			sel, _, _ := unstructured.NestedStringMap(cnp.Object, "spec", "endpointSelector", "matchLabels")
			Expect(sel).To(Equal(map[string]string{"app.kubernetes.io/name": "knight", "app.kubernetes.io/instance": k.Name}))

			egress, _, _ := unstructured.NestedSlice(cnp.Object, "spec", "egress")
			Expect(egress).To(HaveLen(3))
			Expect(egress[0]).To(Equal(map[string]any{
				"toEndpoints": []any{map[string]any{"matchLabels": map[string]any{
					"k8s:io.kubernetes.pod.namespace": "kube-system", "k8s-app": "kube-dns",
				}}},
				"toPorts": []any{map[string]any{
					"ports": []any{map[string]any{"port": "53", "protocol": "ANY"}},
					"rules": map[string]any{"dns": []any{map[string]any{"matchPattern": "*"}}},
				}},
			}))
			Expect(egress[1]).To(Equal(map[string]any{
				"toEndpoints": []any{map[string]any{"matchLabels": map[string]any{
					"k8s:io.kubernetes.pod.namespace": "database",
				}}},
				"toPorts": []any{map[string]any{"ports": []any{map[string]any{"port": "4222", "protocol": "TCP"}}}},
			}))
			Expect(egress[2]).To(Equal(map[string]any{
				"toFQDNs": []any{
					map[string]any{"matchPattern": "*.github.com"},
					map[string]any{"matchName": "api.anthropic.com"},
				},
				"toPorts": []any{map[string]any{"ports": []any{map[string]any{"port": "443", "protocol": "TCP"}}}},
			}))

			_, err = getEgressNetPol(k)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())

			cond := meta.FindStatusCondition(getKnight(k).Status.Conditions, aiv1alpha1.ConditionKnightEgressRestricted)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionTrue))
			Expect(cond.Reason).To(Equal(aiv1alpha1.ReasonFQDNPolicyApplied))

			removeEgress(k)
			reconcileKnight(r, k)
			_, err = getCNP(k)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
			Expect(meta.FindStatusCondition(getKnight(k).Status.Conditions, aiv1alpha1.ConditionKnightEgressRestricted)).To(BeNil())
		})

		It("renders nothing for a knight without spec.egress", func() {
			k := egressKnight("egress-cilium-none", nil)
			Expect(k8sClient.Create(ctx, k)).To(Succeed())
			DeferCleanup(deleteKnight, k)
			reconcileKnight(ciliumReconciler(), k)
			_, err := getCNP(k)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})
})

// setControllerRef makes k the controller of obj.
func setControllerRef(obj metav1.Object, k *aiv1alpha1.Knight) {
	obj.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(k, aiv1alpha1.GroupVersion.WithKind("Knight"))})
}

func testEgressKnight(runtime string) *aiv1alpha1.Knight {
	return &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{Name: "scout", Namespace: "roundtable"},
		Spec: aiv1alpha1.KnightSpec{
			Runtime: runtime,
			Domain:  "research",
			NATS:    aiv1alpha1.KnightNATS{URL: egressTestNATSURL},
			Egress:  &aiv1alpha1.KnightEgress{AllowedHosts: []string{"api.anthropic.com", "*.github.com"}},
		},
	}
}

func TestEgressPolicySameForBothRuntimes(t *testing.T) {
	s := missionpkg.DefaultNetPolSettings()
	dep, sbx := testEgressKnight("deployment"), testEgressKnight("sandbox")

	depCNP := RenderCiliumEgressPolicy(dep, *egressAllowlistOf(dep), s)
	sbxCNP := RenderCiliumEgressPolicy(sbx, *egressAllowlistOf(sbx), s)
	assert.Equal(t, depCNP.Object, sbxCNP.Object)
	sel, _, _ := unstructured.NestedStringMap(sbxCNP.Object, "spec", "endpointSelector", "matchLabels")
	assert.Equal(t, map[string]string{"app.kubernetes.io/name": "knight", "app.kubernetes.io/instance": "scout"}, sel)
	assert.Equal(t, "knight-scout-egress", sbxCNP.GetName())

	depNP := RenderEgressNetworkPolicy(dep, *egressAllowlistOf(dep), s)
	sbxNP := RenderEgressNetworkPolicy(sbx, *egressAllowlistOf(sbx), s)
	assert.Equal(t, depNP, sbxNP)
	assert.Equal(t, sel, sbxNP.Spec.PodSelector.MatchLabels)
}

func TestRenderEgressBaselineAndPorts(t *testing.T) {
	k := testEgressKnight("")
	s := missionpkg.NetPolSettings{
		DNSNamespace:  "dns",
		DNSPodLabels:  map[string]string{"app": "coredns"},
		NATSPodLabels: map[string]string{"app.kubernetes.io/name": "nats"},
	}

	// Empty allowlist (the #387 baseline): DNS and NATS only.
	cnp := RenderCiliumEgressPolicy(k, EgressAllowlist{}, s)
	egress, _, _ := unstructured.NestedSlice(cnp.Object, "spec", "egress")
	require.Len(t, egress, 2)
	dnsSel, _, _ := unstructured.NestedStringMap(egress[0].(map[string]any)["toEndpoints"].([]any)[0].(map[string]any), "matchLabels")
	assert.Equal(t, map[string]string{"k8s:io.kubernetes.pod.namespace": "dns", "app": "coredns"}, dnsSel)
	natsSel, _, _ := unstructured.NestedStringMap(egress[1].(map[string]any)["toEndpoints"].([]any)[0].(map[string]any), "matchLabels")
	assert.Equal(t, map[string]string{"k8s:io.kubernetes.pod.namespace": "database", "app.kubernetes.io/name": "nats"}, natsSel)
	np := RenderEgressNetworkPolicy(k, EgressAllowlist{}, s)
	assert.Len(t, np.Spec.Egress, 2)

	// Explicit ports, deduplicated and sorted; hosts lower-cased.
	allow := EgressAllowlist{AllowedHosts: []string{"Example.com", "example.com"}, AllowedPorts: []int32{8443, 443, 8443}}
	cnp = RenderCiliumEgressPolicy(k, allow, s)
	egress, _, _ = unstructured.NestedSlice(cnp.Object, "spec", "egress")
	require.Len(t, egress, 3)
	assert.Equal(t, map[string]any{
		"toFQDNs": []any{map[string]any{"matchName": "example.com"}},
		"toPorts": []any{map[string]any{"ports": []any{
			map[string]any{"port": "443", "protocol": "TCP"},
			map[string]any{"port": "8443", "protocol": "TCP"},
		}}},
	}, egress[2])
	np = RenderEgressNetworkPolicy(k, allow, s)
	require.Len(t, np.Spec.Egress, 3)
	require.Len(t, np.Spec.Egress[2].Ports, 2)
	assert.Equal(t, corev1.ProtocolTCP, *np.Spec.Egress[2].Ports[1].Protocol)
	assert.Equal(t, int32(8443), np.Spec.Egress[2].Ports[1].Port.IntVal)

	// NATS outside the cluster: the port to any destination; multiple URLs
	// to the same server collapse into one rule.
	k.Spec.NATS.URL = "nats://nats.example.com:4222,nats://nats2.example.com:4222"
	cnp = RenderCiliumEgressPolicy(k, EgressAllowlist{}, s)
	egress, _, _ = unstructured.NestedSlice(cnp.Object, "spec", "egress")
	require.Len(t, egress, 2)
	assert.Equal(t, []any{"all"}, egress[1].(map[string]any)["toEntities"])
	np = RenderEgressNetworkPolicy(k, EgressAllowlist{}, s)
	require.Len(t, np.Spec.Egress, 2)
	assert.Empty(t, np.Spec.Egress[1].To)
}
