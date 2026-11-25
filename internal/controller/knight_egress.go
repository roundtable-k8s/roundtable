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
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
	missionpkg "github.com/roundtable-k8s/roundtable/internal/mission"
	rtruntime "github.com/roundtable-k8s/roundtable/pkg/runtime"
)

// +kubebuilder:rbac:groups=cilium.io,resources=ciliumnetworkpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete

// defaultEgressPort is the port allowed to the allowed hosts when
// spec.egress.allowedPorts is empty.
const defaultEgressPort int32 = 443

// ciliumNamespaceLabel is the label Cilium puts on every endpoint with the
// endpoint's namespace; toEndpoints selectors use it to reach other namespaces.
const ciliumNamespaceLabel = "k8s:io.kubernetes.pod.namespace"

// egressPolicyName is the name of the knight's egress policy (both kinds).
func egressPolicyName(k *aiv1alpha1.Knight) string {
	return "knight-" + k.Name + "-egress"
}

// knightPodSelectorLabels select a knight's pods on either runtime backend.
// They are pod labels only, with no reference to the Deployment or Sandbox.
func knightPodSelectorLabels(k *aiv1alpha1.Knight) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":     "knight",
		"app.kubernetes.io/instance": k.Name,
	}
}

// kubeAPIFallbackPorts are the ports the fallback NetworkPolicy allows to any
// destination for the Kubernetes API: a NetworkPolicy can't select the API
// server, and the kubernetes Service (443) is usually DNATed to 6443.
var kubeAPIFallbackPorts = []int32{443, 6443}

// EgressAllowlist is the input to the egress policy renderers. Every policy
// allows the baseline: DNS, NATS, the Kubernetes API, BaseHosts (on 443) and
// GitHosts (on their ports). AllowedHosts are allowed on AllowedPorts on top.
type EgressAllowlist struct {
	AllowedHosts []string
	AllowedPorts []int32
	// BaseHosts are the operator's base hosts (KNIGHT_EGRESS_BASE_HOSTS).
	BaseHosts []string
	// GitHosts are the hosts of the knight's arsenal repos.
	GitHosts []knightpkg.GitHost
}

// egressAllowlistOf returns the knight's allowlist with the baseline, or nil
// without spec.egress. Call it after ApplyIsolationDefaults, which fills
// spec.egress under default-deny and clears it for isolation.egress:
// unrestricted.
func (r *KnightReconciler) egressAllowlistOf(k *aiv1alpha1.Knight) *EgressAllowlist {
	if k.Spec.Egress == nil {
		return nil
	}
	return &EgressAllowlist{
		AllowedHosts: k.Spec.Egress.AllowedHosts,
		AllowedPorts: k.Spec.Egress.AllowedPorts,
		BaseHosts:    r.Isolation.BaseHosts,
		GitHosts:     knightpkg.ArsenalGitHosts(k),
	}
}

// baselineHostsByPort returns the base hosts (on 443) and git hosts (on their
// ports) grouped by port, each list lower-cased, sorted and deduplicated.
func (a EgressAllowlist) baselineHostsByPort() map[int32][]string {
	out := map[int32][]string{}
	for _, h := range a.BaseHosts {
		if h = strings.ToLower(strings.TrimSpace(h)); h != "" {
			out[defaultEgressPort] = append(out[defaultEgressPort], h)
		}
	}
	for _, g := range a.GitHosts {
		if g.Host != "" && g.Port > 0 {
			out[g.Port] = append(out[g.Port], strings.ToLower(g.Host))
		}
	}
	for p, hosts := range out {
		slices.Sort(hosts)
		out[p] = slices.Compact(hosts)
	}
	return out
}

// hostCount is the number of distinct hosts the policy allows by name.
func (a EgressAllowlist) hostCount() int {
	seen := map[string]bool{}
	for _, h := range a.hosts() {
		seen[h] = true
	}
	for _, hosts := range a.baselineHostsByPort() {
		for _, h := range hosts {
			seen[h] = true
		}
	}
	return len(seen)
}

// ports returns the allowed ports, sorted and deduplicated, defaulting to 443.
func (a EgressAllowlist) ports() []int32 {
	if len(a.AllowedPorts) == 0 {
		return []int32{defaultEgressPort}
	}
	out := slices.Clone(a.AllowedPorts)
	slices.Sort(out)
	return slices.Compact(out)
}

// hosts returns the allowed hosts, lower-cased, sorted and deduplicated.
func (a EgressAllowlist) hosts() []string {
	out := make([]string, 0, len(a.AllowedHosts))
	for _, h := range a.AllowedHosts {
		if h = strings.ToLower(strings.TrimSpace(h)); h != "" {
			out = append(out, h)
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// natsURLs splits a (possibly comma-separated) NATS URL list.
func natsURLs(raw string) []string {
	var out []string
	for u := range strings.SplitSeq(raw, ",") {
		if u = strings.TrimSpace(u); u != "" {
			out = append(out, u)
		}
	}
	if len(out) == 0 {
		// No URL: NATSNamespaceAndPort falls back to the default port.
		out = []string{""}
	}
	return out
}

// egressNetPolSettings returns the DNS/NATS peer settings, falling back to
// the defaults when the reconciler was built without them.
func (r *KnightReconciler) egressNetPolSettings() missionpkg.NetPolSettings {
	if r.EgressNetPol == nil {
		return missionpkg.DefaultNetPolSettings()
	}
	return *r.EgressNetPol
}

// RenderCiliumEgressPolicy renders the knight's CiliumNetworkPolicy: egress
// is allowed to (a) the cluster DNS with a DNS proxy rule (needed for FQDN
// tracking), (b) the knight's NATS server, (c) the Kubernetes API server,
// (d) the base and arsenal git hosts and (e) each allowed host on the allowed
// ports. Everything else is denied. The endpoint selector uses the
// knight's pod labels, so it covers both runtime backends.
func RenderCiliumEgressPolicy(k *aiv1alpha1.Knight, allow EgressAllowlist, s missionpkg.NetPolSettings) *unstructured.Unstructured {
	egress := []any{
		map[string]any{
			"toEndpoints": []any{map[string]any{"matchLabels": stringMap(ciliumPeerLabels(dnsNamespace(s), s.DNSPodLabels))}},
			"toPorts": []any{map[string]any{
				"ports": []any{map[string]any{"port": "53", "protocol": "ANY"}},
				"rules": map[string]any{"dns": []any{map[string]any{"matchPattern": "*"}}},
			}},
		},
	}
	egress = append(egress, ciliumNATSRules(k.Spec.NATS.URL, s)...)
	egress = append(egress, map[string]any{"toEntities": []any{"kube-apiserver"}})
	baseline := allow.baselineHostsByPort()
	for _, p := range slices.Sorted(maps.Keys(baseline)) {
		egress = append(egress, ciliumFQDNRule(baseline[p], []int32{p}))
	}
	if hosts := allow.hosts(); len(hosts) > 0 {
		egress = append(egress, ciliumFQDNRule(hosts, allow.ports()))
	}

	cnp := newCiliumEgressPolicy(k)
	cnp.SetLabels(knightLabels(k))
	cnp.Object["spec"] = map[string]any{
		"endpointSelector": map[string]any{"matchLabels": stringMap(knightPodSelectorLabels(k))},
		"egress":           egress,
	}
	return cnp
}

// ciliumFQDNRule allows TCP on ports to hosts (exact names, or "*." patterns).
func ciliumFQDNRule(hosts []string, ports []int32) map[string]any {
	fqdns := make([]any, 0, len(hosts))
	for _, h := range hosts {
		if strings.HasPrefix(h, "*.") {
			fqdns = append(fqdns, map[string]any{"matchPattern": h})
		} else {
			fqdns = append(fqdns, map[string]any{"matchName": h})
		}
	}
	toPorts := make([]any, 0, len(ports))
	for _, p := range ports {
		toPorts = append(toPorts, map[string]any{"port": strconv.Itoa(int(p)), "protocol": "TCP"})
	}
	return map[string]any{
		"toFQDNs": fqdns,
		"toPorts": []any{map[string]any{"ports": toPorts}},
	}
}

// ciliumNATSRules allows TCP to each NATS server: to the NATS pods when the
// host is a cluster service name, otherwise to any destination on the port
// (the same trade-off as the mission NetworkPolicy).
func ciliumNATSRules(rawURL string, s missionpkg.NetPolSettings) []any {
	var rules []any
	seen := map[string]bool{}
	for _, u := range natsURLs(rawURL) {
		ns, port, inCluster := missionpkg.NATSNamespaceAndPort(u)
		key := fmt.Sprintf("%s/%d/%t", ns, port, inCluster)
		if seen[key] {
			continue
		}
		seen[key] = true
		rule := map[string]any{
			"toPorts": []any{map[string]any{
				"ports": []any{map[string]any{"port": strconv.Itoa(port), "protocol": "TCP"}},
			}},
		}
		if inCluster {
			rule["toEndpoints"] = []any{map[string]any{"matchLabels": stringMap(ciliumPeerLabels(ns, s.NATSPodLabels))}}
		} else {
			rule["toEntities"] = []any{"all"}
		}
		rules = append(rules, rule)
	}
	return rules
}

// RenderEgressNetworkPolicy renders the fallback NetworkPolicy used when the
// CiliumNetworkPolicy API isn't served. It can't express hostnames or the
// API server, so it allows DNS, NATS and, to any destination, the Kubernetes
// API ports (443, 6443), the baseline hosts' ports and, when hosts are
// allowed, the allowed ports.
func RenderEgressNetworkPolicy(k *aiv1alpha1.Knight, allow EgressAllowlist, s missionpkg.NetPolSettings) *networkingv1.NetworkPolicy {
	rules := []networkingv1.NetworkPolicyEgressRule{missionpkg.DNSEgressRule(s)}
	seen := map[string]bool{}
	for _, u := range natsURLs(k.Spec.NATS.URL) {
		ns, port, inCluster := missionpkg.NATSNamespaceAndPort(u)
		key := fmt.Sprintf("%s/%d/%t", ns, port, inCluster)
		rule, _ := missionpkg.NATSEgressRule(u, s.NATSPodLabels)
		if seen[key] {
			continue
		}
		seen[key] = true
		rules = append(rules, rule)
	}
	allowed := slices.Clone(kubeAPIFallbackPorts)
	allowed = slices.AppendSeq(allowed, maps.Keys(allow.baselineHostsByPort()))
	if len(allow.hosts()) > 0 {
		allowed = append(allowed, allow.ports()...)
	}
	slices.Sort(allowed)
	allowed = slices.Compact(allowed)
	ports := make([]networkingv1.NetworkPolicyPort, 0, len(allowed))
	for _, p := range allowed {
		ports = append(ports, networkingv1.NetworkPolicyPort{
			Protocol: ptr.To(corev1.ProtocolTCP),
			Port:     new(intstr.FromInt32(p)),
		})
	}
	rules = append(rules, networkingv1.NetworkPolicyEgressRule{Ports: ports})
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      egressPolicyName(k),
			Namespace: k.Namespace,
			Labels:    knightLabels(k),
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: knightPodSelectorLabels(k)},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress},
			Egress:      rules,
		},
	}
}

func dnsNamespace(s missionpkg.NetPolSettings) string {
	if s.DNSNamespace != "" {
		return s.DNSNamespace
	}
	return missionpkg.DefaultNetPolSettings().DNSNamespace
}

// ciliumPeerLabels selects pods in namespace, narrowed by podLabels.
func ciliumPeerLabels(namespace string, podLabels map[string]string) map[string]string {
	out := maps.Clone(podLabels)
	if out == nil {
		out = map[string]string{}
	}
	out[ciliumNamespaceLabel] = namespace
	return out
}

// stringMap converts labels to the map type unstructured content expects.
func stringMap(m map[string]string) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func newCiliumEgressPolicy(k *aiv1alpha1.Knight) *unstructured.Unstructured {
	cnp := &unstructured.Unstructured{}
	cnp.SetGroupVersionKind(rtruntime.CiliumNetworkPolicyGVK)
	cnp.SetName(egressPolicyName(k))
	cnp.SetNamespace(k.Namespace)
	return cnp
}

// reconcileEgress renders the knight's egress policy from spec.egress (after
// ApplyIsolationDefaults), or removes it when spec.egress is unset, and sets
// EgressRestricted.
func (r *KnightReconciler) reconcileEgress(ctx context.Context, k *aiv1alpha1.Knight) error {
	allow := r.egressAllowlistOf(k)
	if allow == nil {
		meta.RemoveStatusCondition(&k.Status.Conditions, aiv1alpha1.ConditionKnightEgressRestricted)
		return r.deleteEgressPolicies(ctx, k)
	}
	err := r.applyEgressPolicy(ctx, k, *allow)
	cond := metav1.Condition{
		Type:               aiv1alpha1.ConditionKnightEgressRestricted,
		ObservedGeneration: k.Generation,
	}
	switch {
	case err != nil:
		cond.Status = metav1.ConditionFalse
		cond.Reason = aiv1alpha1.ReasonEgressPolicyFailed
		cond.Message = err.Error()
	case r.CiliumEnabled:
		cond.Status = metav1.ConditionTrue
		cond.Reason = aiv1alpha1.ReasonFQDNPolicyApplied
		cond.Message = fmt.Sprintf("CiliumNetworkPolicy %s limits egress to DNS, NATS, the Kubernetes API and %d host(s)",
			egressPolicyName(k), allow.hostCount())
	default:
		cond.Status = metav1.ConditionFalse
		cond.Reason = aiv1alpha1.ReasonFQDNPolicyUnavailable
		cond.Message = fmt.Sprintf("%s CiliumNetworkPolicy is not served, so hostnames can't be enforced; "+
			"NetworkPolicy %s allows DNS, NATS and the allowed ports to any destination",
			rtruntime.CiliumNetworkPolicyGVK.GroupVersion(), egressPolicyName(k))
	}
	meta.SetStatusCondition(&k.Status.Conditions, cond)
	return err
}

// applyEgressPolicy creates or updates the CiliumNetworkPolicy when Cilium is
// served (removing a fallback NetworkPolicy left from before), otherwise the
// fallback NetworkPolicy.
func (r *KnightReconciler) applyEgressPolicy(ctx context.Context, k *aiv1alpha1.Knight, allow EgressAllowlist) error {
	s := r.egressNetPolSettings()
	if r.CiliumEnabled {
		desired := RenderCiliumEgressPolicy(k, allow, s)
		cnp := newCiliumEgressPolicy(k)
		op, err := controllerutil.CreateOrUpdate(ctx, r.Client, cnp, func() error {
			if err := controllerutil.SetControllerReference(k, cnp, r.Scheme); err != nil {
				return err
			}
			cnp.SetLabels(desired.GetLabels())
			cnp.Object["spec"] = desired.Object["spec"]
			return nil
		})
		if err != nil {
			return fmt.Errorf("CiliumNetworkPolicy reconcile failed: %w", err)
		}
		if op != controllerutil.OperationResultNone {
			logf.FromContext(ctx).Info("Egress CiliumNetworkPolicy reconciled", "operation", op, "name", cnp.GetName())
		}
		return r.deleteOwned(ctx, k, &networkingv1.NetworkPolicy{}, "NetworkPolicy")
	}

	desired := RenderEgressNetworkPolicy(k, allow, s)
	np := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, np, func() error {
		if err := controllerutil.SetControllerReference(k, np, r.Scheme); err != nil {
			return err
		}
		np.Labels = desired.Labels
		np.Spec = desired.Spec
		return nil
	})
	if err != nil {
		return fmt.Errorf("egress NetworkPolicy reconcile failed: %w", err)
	}
	if op != controllerutil.OperationResultNone {
		logf.FromContext(ctx).Info("Egress NetworkPolicy reconciled", "operation", op, "name", np.Name)
	}
	return nil
}

// deleteEgressPolicies removes the knight's egress policies of either kind.
func (r *KnightReconciler) deleteEgressPolicies(ctx context.Context, k *aiv1alpha1.Knight) error {
	if r.CiliumEnabled {
		if err := r.deleteOwned(ctx, k, newCiliumEgressPolicy(k), rtruntime.CiliumNetworkPolicyGVK.Kind); err != nil {
			return err
		}
	}
	return r.deleteOwned(ctx, k, &networkingv1.NetworkPolicy{}, "NetworkPolicy")
}

// deleteOwned deletes the knight's egress policy into obj if the knight
// controls it.
func (r *KnightReconciler) deleteOwned(ctx context.Context, k *aiv1alpha1.Knight, obj client.Object, kind string) error {
	key := types.NamespacedName{Name: egressPolicyName(k), Namespace: k.Namespace}
	if err := r.Get(ctx, key, obj); err != nil {
		if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
			return nil
		}
		return fmt.Errorf("failed to get egress policy %s: %w", key.Name, err)
	}
	if !metav1.IsControlledBy(obj, k) {
		return nil
	}
	if err := r.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete egress policy %s: %w", key.Name, err)
	}
	logf.FromContext(ctx).Info("Egress policy deleted", "name", key.Name, "kind", kind)
	return nil
}
