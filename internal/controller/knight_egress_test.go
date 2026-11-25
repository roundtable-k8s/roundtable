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
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
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
		// DNS, NATS in its namespace, then 443 (and the API server's 6443) to any destination.
		Expect(np.Spec.Egress[0].To[0].NamespaceSelector.MatchLabels).To(HaveKeyWithValue("kubernetes.io/metadata.name", "kube-system"))
		Expect(np.Spec.Egress[1].To[0].NamespaceSelector.MatchLabels).To(HaveKeyWithValue("kubernetes.io/metadata.name", "database"))
		Expect(np.Spec.Egress[1].Ports[0].Port.IntValue()).To(Equal(4222))
		Expect(np.Spec.Egress[2].To).To(BeEmpty())
		Expect(np.Spec.Egress[2].Ports).To(HaveLen(2))
		Expect(*np.Spec.Egress[2].Ports[0].Port).To(Equal(intstr.FromInt32(443)))
		Expect(*np.Spec.Egress[2].Ports[1].Port).To(Equal(intstr.FromInt32(6443)))

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
			Expect(egress).To(HaveLen(4))
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
			Expect(egress[2]).To(Equal(map[string]any{"toEntities": []any{"kube-apiserver"}}))
			Expect(egress[3]).To(Equal(map[string]any{
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

	depCNP := RenderCiliumEgressPolicy(dep, *(&KnightReconciler{}).egressAllowlistOf(dep), s)
	sbxCNP := RenderCiliumEgressPolicy(sbx, *(&KnightReconciler{}).egressAllowlistOf(sbx), s)
	assert.Equal(t, depCNP.Object, sbxCNP.Object)
	sel, _, _ := unstructured.NestedStringMap(sbxCNP.Object, "spec", "endpointSelector", "matchLabels")
	assert.Equal(t, map[string]string{"app.kubernetes.io/name": "knight", "app.kubernetes.io/instance": "scout"}, sel)
	assert.Equal(t, "knight-scout-egress", sbxCNP.GetName())

	depNP := RenderEgressNetworkPolicy(dep, *(&KnightReconciler{}).egressAllowlistOf(dep), s)
	sbxNP := RenderEgressNetworkPolicy(sbx, *(&KnightReconciler{}).egressAllowlistOf(sbx), s)
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

	// Empty allowlist (the #387 baseline): DNS, NATS and the Kubernetes API.
	cnp := RenderCiliumEgressPolicy(k, EgressAllowlist{}, s)
	egress, _, _ := unstructured.NestedSlice(cnp.Object, "spec", "egress")
	require.Len(t, egress, 3)
	dnsSel, _, _ := unstructured.NestedStringMap(egress[0].(map[string]any)["toEndpoints"].([]any)[0].(map[string]any), "matchLabels")
	assert.Equal(t, map[string]string{"k8s:io.kubernetes.pod.namespace": "dns", "app": "coredns"}, dnsSel)
	natsSel, _, _ := unstructured.NestedStringMap(egress[1].(map[string]any)["toEndpoints"].([]any)[0].(map[string]any), "matchLabels")
	assert.Equal(t, map[string]string{"k8s:io.kubernetes.pod.namespace": "database", "app.kubernetes.io/name": "nats"}, natsSel)
	assert.Equal(t, map[string]any{"toEntities": []any{"kube-apiserver"}}, egress[2])
	np := RenderEgressNetworkPolicy(k, EgressAllowlist{}, s)
	require.Len(t, np.Spec.Egress, 3)
	assert.Equal(t, []int32{443, 6443}, npPorts(np.Spec.Egress[2]))

	// Explicit ports, deduplicated and sorted; hosts lower-cased.
	allow := EgressAllowlist{AllowedHosts: []string{"Example.com", "example.com"}, AllowedPorts: []int32{8443, 443, 8443}}
	cnp = RenderCiliumEgressPolicy(k, allow, s)
	egress, _, _ = unstructured.NestedSlice(cnp.Object, "spec", "egress")
	require.Len(t, egress, 4)
	assert.Equal(t, map[string]any{
		"toFQDNs": []any{map[string]any{"matchName": "example.com"}},
		"toPorts": []any{map[string]any{"ports": []any{
			map[string]any{"port": "443", "protocol": "TCP"},
			map[string]any{"port": "8443", "protocol": "TCP"},
		}}},
	}, egress[3])
	np = RenderEgressNetworkPolicy(k, allow, s)
	require.Len(t, np.Spec.Egress, 3)
	assert.Equal(t, corev1.ProtocolTCP, *np.Spec.Egress[2].Ports[2].Protocol)
	assert.Equal(t, []int32{443, 6443, 8443}, npPorts(np.Spec.Egress[2]))

	// NATS outside the cluster: the port to any destination; multiple URLs
	// to the same server collapse into one rule.
	k.Spec.NATS.URL = "nats://nats.example.com:4222,nats://nats2.example.com:4222"
	cnp = RenderCiliumEgressPolicy(k, EgressAllowlist{}, s)
	egress, _, _ = unstructured.NestedSlice(cnp.Object, "spec", "egress")
	require.Len(t, egress, 3)
	assert.Equal(t, []any{"all"}, egress[1].(map[string]any)["toEntities"])
	np = RenderEgressNetworkPolicy(k, EgressAllowlist{}, s)
	require.Len(t, np.Spec.Egress, 3)
	assert.Empty(t, np.Spec.Egress[1].To)
}

// npPorts lists a NetworkPolicy egress rule's ports.
func npPorts(r networkingv1.NetworkPolicyEgressRule) []int32 {
	out := make([]int32, 0, len(r.Ports))
	for _, p := range r.Ports {
		out = append(out, p.Port.IntVal)
	}
	return out
}

func TestRenderEgressIsolationBaseline(t *testing.T) {
	k := testEgressKnight("")
	s := missionpkg.DefaultNetPolSettings()
	allow := EgressAllowlist{
		AllowedHosts: []string{"example.com"},
		BaseHosts:    []string{"OpenRouter.ai", "api.anthropic.com", "openrouter.ai"},
		GitHosts: []knightpkg.GitHost{
			{Host: "github.com", Port: 443},
			{Host: "git.example.org", Port: 22},
		},
	}
	cnp := RenderCiliumEgressPolicy(k, allow, s)
	egress, _, _ := unstructured.NestedSlice(cnp.Object, "spec", "egress")
	// DNS, NATS, kube-apiserver, port 22 hosts, port 443 hosts, allowlist.
	require.Len(t, egress, 6)
	assert.Equal(t, map[string]any{"toEntities": []any{"kube-apiserver"}}, egress[2])
	assert.Equal(t, ciliumFQDNRule([]string{"git.example.org"}, []int32{22}), egress[3])
	assert.Equal(t, ciliumFQDNRule([]string{"api.anthropic.com", "github.com", "openrouter.ai"}, []int32{443}), egress[4])
	assert.Equal(t, ciliumFQDNRule([]string{"example.com"}, []int32{443}), egress[5])
	assert.Equal(t, 5, allow.hostCount())

	np := RenderEgressNetworkPolicy(k, allow, s)
	require.Len(t, np.Spec.Egress, 3)
	assert.Equal(t, []int32{22, 443, 6443}, npPorts(np.Spec.Egress[2]))
}

// ─── spec.egress.allowedServices (#517) ─────────────────────────────────────

func TestRenderEgressAllowedServices(t *testing.T) {
	k := testEgressKnight("")
	s := missionpkg.DefaultNetPolSettings()
	allow := EgressAllowlist{Services: []EgressServicePeer{{
		Namespace: "selfhosted", Name: "searxng",
		Selector: map[string]string{"app.kubernetes.io/name": "searxng"},
		Ports:    []EgressServicePort{{Port: intstr.FromInt32(8080), Protocol: corev1.ProtocolTCP}},
	}}}

	cnp := RenderCiliumEgressPolicy(k, allow, s)
	egress, _, _ := unstructured.NestedSlice(cnp.Object, "spec", "egress")
	// DNS, NATS, kube-apiserver, then the Service; no FQDN rule without hosts.
	require.Len(t, egress, 4)
	assert.Equal(t, map[string]any{
		"toEndpoints": []any{map[string]any{"matchLabels": map[string]any{
			"k8s:io.kubernetes.pod.namespace": "selfhosted", "app.kubernetes.io/name": "searxng",
		}}},
		"toPorts": []any{map[string]any{"ports": []any{map[string]any{"port": "8080", "protocol": "TCP"}}}},
	}, egress[3])

	np := RenderEgressNetworkPolicy(k, allow, s)
	require.Len(t, np.Spec.Egress, 4)
	// The Service port is not opened to any destination.
	assert.Equal(t, []int32{443, 6443}, npPorts(np.Spec.Egress[2]))
	rule := np.Spec.Egress[3]
	require.Len(t, rule.To, 1)
	assert.Equal(t, map[string]string{"kubernetes.io/metadata.name": "selfhosted"}, rule.To[0].NamespaceSelector.MatchLabels)
	assert.Equal(t, map[string]string{"app.kubernetes.io/name": "searxng"}, rule.To[0].PodSelector.MatchLabels)
	assert.Equal(t, []int32{8080}, npPorts(rule))
	assert.Equal(t, corev1.ProtocolTCP, *rule.Ports[0].Protocol)
}

func TestServiceTargetPorts(t *testing.T) {
	svc := &corev1.Service{Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{
		{Name: "http", Port: 80, TargetPort: intstr.FromInt32(8080), Protocol: corev1.ProtocolTCP},
		{Name: "web", Port: 8000, TargetPort: intstr.FromString("web")},
		{Name: "plain", Port: 8123},
		{Name: "dns", Port: 53, TargetPort: intstr.FromInt32(5353), Protocol: corev1.ProtocolUDP},
		{Name: "dns-tcp", Port: 53, TargetPort: intstr.FromInt32(5353), Protocol: corev1.ProtocolTCP},
	}}}
	got := serviceTargetPorts(svc, []int32{8000, 80, 8123, 53, 9999, 80})
	assert.Equal(t, []EgressServicePort{
		{Port: intstr.FromInt32(5353), Protocol: corev1.ProtocolTCP},
		{Port: intstr.FromInt32(5353), Protocol: corev1.ProtocolUDP},
		{Port: intstr.FromInt32(8080), Protocol: corev1.ProtocolTCP},
		{Port: intstr.FromInt32(8123), Protocol: corev1.ProtocolTCP},
		// Not a Service port: allowed as a pod port as-is.
		{Port: intstr.FromInt32(9999), Protocol: corev1.ProtocolTCP},
		{Port: intstr.FromString("web"), Protocol: corev1.ProtocolTCP},
	}, got)

	// Only the listed ports: port 80 does not open 8000 or 8123.
	assert.Equal(t, []EgressServicePort{{Port: intstr.FromInt32(8080), Protocol: corev1.ProtocolTCP}},
		serviceTargetPorts(svc, []int32{80}))

	// Rendered named ports keep the name.
	rule := ciliumServiceRule(EgressServicePeer{Namespace: "ha", Selector: map[string]string{"a": "b"},
		Ports: []EgressServicePort{{Port: intstr.FromString("web"), Protocol: corev1.ProtocolTCP}}})
	assert.Equal(t, []any{map[string]any{"ports": []any{map[string]any{"port": "web", "protocol": "TCP"}}}}, rule["toPorts"])
}

func TestEgressServiceRefs(t *testing.T) {
	k := testEgressKnight("")
	k.Spec.Egress.AllowedServices = []aiv1alpha1.EgressService{
		{Name: "searxng", Namespace: "selfhosted", Ports: []int32{8080}},
		{Name: "miniflux", Ports: []int32{8080}},
		{Name: "searxng", Namespace: "selfhosted", Ports: []int32{80, 8080}},
	}
	assert.Equal(t, []egressServiceRef{
		{Namespace: "roundtable", Name: "miniflux", Ports: []int32{8080}},
		{Namespace: "selfhosted", Name: "searxng", Ports: []int32{80, 8080}},
	}, egressServiceRefs(k))
	k.Spec.Egress = nil
	assert.Empty(t, egressServiceRefs(k))
}

func TestInClusterHostWarnings(t *testing.T) {
	got := inClusterHostWarnings([]string{"api.anthropic.com", "searxng.selfhosted.svc.cluster.local", "hass.home.svc", "svc.example.com"})
	require.Len(t, got, 2)
	assert.Contains(t, got[0], `"hass.home.svc"`)
	assert.Contains(t, got[0], "allowedServices")
	assert.Contains(t, got[1], `"searxng.selfhosted.svc.cluster.local"`)
	assert.Empty(t, inClusterHostWarnings(nil))
}

func TestEgressServiceChangedPredicate(t *testing.T) {
	old := &corev1.Service{Spec: corev1.ServiceSpec{
		Selector: map[string]string{"app": "a"},
		Ports:    []corev1.ServicePort{{Port: 80}},
	}}
	upd := func(mut func(*corev1.Service)) bool {
		n := old.DeepCopy()
		mut(n)
		return egressServiceChanged.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: n})
	}
	assert.False(t, upd(func(s *corev1.Service) { s.Labels = map[string]string{"x": "y"} }))
	assert.False(t, upd(func(s *corev1.Service) { s.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: "1.2.3.4"}} }))
	assert.True(t, upd(func(s *corev1.Service) { s.Spec.Selector["app"] = "b" }))
	assert.True(t, upd(func(s *corev1.Service) { s.Spec.Ports[0].TargetPort = intstr.FromInt32(8080) }))
	assert.True(t, egressServiceChanged.Create(event.CreateEvent{Object: old}))
	assert.True(t, egressServiceChanged.Delete(event.DeleteEvent{Object: old}))
}

var _ = Describe("Knight egress allowedServices (#517)", Ordered, func() {
	BeforeAll(func() {
		_, err := envtest.InstallCRDs(cfg, envtest.CRDInstallOptions{
			CRDs: []*apiextensionsv1.CustomResourceDefinition{ciliumNetworkPolicyCRD()},
		})
		Expect(err).NotTo(HaveOccurred())
	})

	ciliumReconciler := func() (*KnightReconciler, *record.FakeRecorder) {
		r := newScalingReconciler(false)
		r.CiliumEnabled = true
		rec := record.NewFakeRecorder(100)
		r.Recorder = rec
		return r, rec
	}
	newService := func(name string, selector map[string]string) *corev1.Service {
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: corev1.ServiceSpec{
				Selector: selector,
				Ports:    []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromInt32(8080)}},
			},
		}
		Expect(k8sClient.Create(ctx, svc)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, svc) })
		return svc
	}
	serviceRules := func(k *aiv1alpha1.Knight) []any {
		cnp, err := getCNP(k)
		Expect(err).NotTo(HaveOccurred())
		egress, _, _ := unstructured.NestedSlice(cnp.Object, "spec", "egress")
		var out []any
		for _, e := range egress {
			eps, ok := e.(map[string]any)["toEndpoints"].([]any)
			if !ok {
				continue
			}
			labels := eps[0].(map[string]any)["matchLabels"].(map[string]any)
			if labels["k8s:io.kubernetes.pod.namespace"] == "default" {
				out = append(out, e)
			}
		}
		return out
	}
	drain := func(rec *record.FakeRecorder) []string {
		var out []string
		for len(rec.Events) > 0 {
			out = append(out, <-rec.Events)
		}
		return out
	}

	It("rejects an allowed Service without ports", func() {
		bad := egressKnight("egress-svc-bad", &aiv1alpha1.KnightEgress{
			AllowedServices: []aiv1alpha1.EgressService{{Name: "searxng"}},
		})
		Expect(k8sClient.Create(ctx, bad)).NotTo(Succeed())
		bad = egressKnight("egress-svc-bad", &aiv1alpha1.KnightEgress{
			AllowedServices: []aiv1alpha1.EgressService{{Name: "searxng", Ports: []int32{1, 2, 3, 4, 5, 6, 7, 8, 9}}},
		})
		Expect(k8sClient.Create(ctx, bad)).NotTo(Succeed())
	})

	It("renders the Service's pods on its target ports, reports a missing Service and re-renders on a selector change", func() {
		newService("searxng", map[string]string{"app": "searxng"})
		k := egressKnight("egress-svc", &aiv1alpha1.KnightEgress{
			AllowedHosts: []string{"searxng.default.svc.cluster.local"},
			AllowedServices: []aiv1alpha1.EgressService{
				{Name: "searxng", Namespace: "default", Ports: []int32{80}},
				{Name: "miniflux", Ports: []int32{8080}},
			},
		})
		Expect(k8sClient.Create(ctx, k)).To(Succeed())
		DeferCleanup(deleteKnight, k)
		r, rec := ciliumReconciler()
		reconcileKnight(r, k)

		Expect(serviceRules(k)).To(Equal([]any{map[string]any{
			"toEndpoints": []any{map[string]any{"matchLabels": map[string]any{
				"k8s:io.kubernetes.pod.namespace": "default", "app": "searxng",
			}}},
			"toPorts": []any{map[string]any{"ports": []any{map[string]any{"port": "8080", "protocol": "TCP"}}}},
		}}))
		cond := meta.FindStatusCondition(getKnight(k).Status.Conditions, aiv1alpha1.ConditionKnightEgressRestricted)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		Expect(cond.Reason).To(Equal(aiv1alpha1.ReasonEgressServiceNotFound))
		Expect(cond.Message).To(ContainSubstring("1 Service(s)"))
		Expect(cond.Message).To(ContainSubstring("allowed Service default/miniflux not found"))
		Expect(cond.Message).To(ContainSubstring(`allowedHosts entry "searxng.default.svc.cluster.local" is an in-cluster name`))
		events := drain(rec)
		Expect(events).To(ContainElement(ContainSubstring("Warning ServiceNotFound allowed Service default/miniflux not found")))
		Expect(events).To(ContainElement(ContainSubstring("Warning InClusterAllowedHost")))

		// Unchanged: no repeated warnings.
		reconcileKnight(r, k)
		Expect(drain(rec)).NotTo(ContainElement(ContainSubstring("ServiceNotFound")))

		// The Service watch maps a selector change to the knight, which re-renders.
		svc := &corev1.Service{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "searxng", Namespace: "default"}, svc)).To(Succeed())
		svc.Spec.Selector = map[string]string{"app.kubernetes.io/name": "searxng"}
		Expect(k8sClient.Update(ctx, svc)).To(Succeed())
		Expect(r.serviceKnightRequests(ctx, svc)).To(ContainElement(
			reconcile.Request{NamespacedName: types.NamespacedName{Name: k.Name, Namespace: k.Namespace}}))
		reconcileKnight(r, k)
		rules := serviceRules(k)
		Expect(rules).To(HaveLen(1))
		Expect(rules[0].(map[string]any)["toEndpoints"]).To(Equal([]any{map[string]any{"matchLabels": map[string]any{
			"k8s:io.kubernetes.pod.namespace": "default", "app.kubernetes.io/name": "searxng",
		}}}))

		// The missing Service appears: its rule is added and the condition clears.
		mf := newService("miniflux", map[string]string{"app": "miniflux"})
		Expect(r.serviceKnightRequests(ctx, mf)).To(HaveLen(1))
		reconcileKnight(r, k)
		Expect(serviceRules(k)).To(HaveLen(2))
		cond = meta.FindStatusCondition(getKnight(k).Status.Conditions, aiv1alpha1.ConditionKnightEgressRestricted)
		Expect(cond.Reason).To(Equal(aiv1alpha1.ReasonFQDNPolicyApplied))
		Expect(cond.Message).To(ContainSubstring("2 Service(s)"))
		Expect(cond.Message).NotTo(ContainSubstring("not found"))

		// A Service without a selector is left out.
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(mf), mf)).To(Succeed())
		mf.Spec.Selector = nil
		Expect(k8sClient.Update(ctx, mf)).To(Succeed())
		reconcileKnight(r, k)
		Expect(serviceRules(k)).To(HaveLen(1))
		cond = meta.FindStatusCondition(getKnight(k).Status.Conditions, aiv1alpha1.ConditionKnightEgressRestricted)
		Expect(cond.Reason).To(Equal(aiv1alpha1.ReasonEgressServiceNotFound))
		Expect(cond.Message).To(ContainSubstring("allowed Service default/miniflux has no pod selector"))
	})

	It("maps a Service to knights using it through their table's defaults", func() {
		svc := newService("hass", map[string]string{"app": "hass"})
		rt := &aiv1alpha1.RoundTable{
			ObjectMeta: metav1.ObjectMeta{Name: "egress-svc-table", Namespace: "default"},
			Spec: aiv1alpha1.RoundTableSpec{
				NATS: aiv1alpha1.RoundTableNATS{URL: egressTestNATSURL, SubjectPrefix: "egress-svc"},
				Defaults: &aiv1alpha1.RoundTableDefaults{Egress: &aiv1alpha1.KnightEgress{
					AllowedServices: []aiv1alpha1.EgressService{{Name: "hass", Ports: []int32{80}}},
				}},
			},
		}
		Expect(k8sClient.Create(ctx, rt)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, rt) })

		member := egressKnight("egress-svc-member", nil)
		member.Labels = map[string]string{aiv1alpha1.LabelRoundTable: rt.Name}
		Expect(k8sClient.Create(ctx, member)).To(Succeed())
		DeferCleanup(deleteKnight, member)
		own := egressKnight("egress-svc-own", &aiv1alpha1.KnightEgress{})
		own.Labels = map[string]string{aiv1alpha1.LabelRoundTable: rt.Name}
		Expect(k8sClient.Create(ctx, own)).To(Succeed())
		DeferCleanup(deleteKnight, own)

		r, _ := ciliumReconciler()
		Expect(r.serviceKnightRequests(ctx, svc)).To(Equal([]reconcile.Request{
			{NamespacedName: types.NamespacedName{Name: member.Name, Namespace: member.Namespace}},
		}))
		reconcileKnight(r, member)
		Expect(serviceRules(member)).To(HaveLen(1))
	})
})
