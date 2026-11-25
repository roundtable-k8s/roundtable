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
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
	missionpkg "github.com/roundtable-k8s/roundtable/internal/mission"
	rtruntime "github.com/roundtable-k8s/roundtable/pkg/runtime"
)

// +kubebuilder:rbac:groups=cilium.io,resources=ciliumnetworkpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get
// +kubebuilder:rbac:groups=discovery.k8s.io,resources=endpointslices,verbs=get;list;watch

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
// GitHosts (on their ports). AllowedHosts are allowed on top, each on its own
// ports or, without any, on AllowedPorts, and each of Services on its own ports.
type EgressAllowlist struct {
	AllowedHosts []aiv1alpha1.EgressHost
	AllowedPorts []int32
	// BaseHosts are the operator's base hosts (KNIGHT_EGRESS_BASE_HOSTS).
	BaseHosts []string
	// GitHosts are the hosts of the knight's arsenal repos.
	GitHosts []knightpkg.GitHost
	// Services are the allowed in-cluster Services that exist and have a pod
	// selector, resolved by resolveEgressServices.
	Services []EgressServicePeer
}

// EgressServicePeer is an allowed in-cluster Service resolved to its pods.
type EgressServicePeer struct {
	Namespace string
	Name      string
	// Selector is the Service's pod selector (never empty).
	Selector map[string]string
	// Ports are the pod ports to allow (the listed ports' targetPorts). For a
	// host-network Service they are resolved port numbers.
	Ports []EgressServicePort
	// HostNetwork is true when the Service's ready endpoints are pods on the
	// host network. They have no Cilium endpoint identity, so the Service is
	// allowed as traffic to the nodes (Cilium: toEntities host and
	// remote-node) on Ports, not as a pod selector.
	HostNetwork bool
	// Addresses are the ready endpoint addresses of a host-network Service
	// (the /32 or /128 ipBlocks of the fallback NetworkPolicy).
	Addresses []string
}

// EgressServicePort is a pod port (number or name) with its protocol.
type EgressServicePort struct {
	Port     intstr.IntOrString
	Protocol corev1.Protocol
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
		if name := strings.ToLower(strings.TrimSpace(h.Host)); name != "" {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// hostPorts returns each allowed host (lower-cased, sorted, deduplicated)
// with its ports: the union of the ports of its entries, where an entry
// without ports uses AllowedPorts (default 443).
func (a EgressAllowlist) hostPorts() map[string][]int32 {
	out := map[string][]int32{}
	for _, h := range a.AllowedHosts {
		name := strings.ToLower(strings.TrimSpace(h.Host))
		if name == "" {
			continue
		}
		ports := h.Ports
		if len(ports) == 0 {
			ports = a.ports()
		}
		out[name] = append(out[name], ports...)
	}
	for name, ports := range out {
		slices.Sort(ports)
		out[name] = slices.Compact(ports)
	}
	return out
}

// hostGroup is a set of allowed hosts that share the same ports.
type hostGroup struct {
	Hosts []string
	Ports []int32
}

// hostGroups groups the allowed hosts by port set, sorted by ports, each
// group's hosts sorted. Without per-host ports it is a single group.
func (a EgressAllowlist) hostGroups() []hostGroup {
	byPorts := map[string]*hostGroup{}
	for name, ports := range a.hostPorts() {
		key := fmt.Sprint(ports)
		g, ok := byPorts[key]
		if !ok {
			g = &hostGroup{Ports: ports}
			byPorts[key] = g
		}
		g.Hosts = append(g.Hosts, name)
	}
	out := make([]hostGroup, 0, len(byPorts))
	for _, g := range byPorts {
		slices.Sort(g.Hosts)
		out = append(out, *g)
	}
	slices.SortFunc(out, func(x, y hostGroup) int { return slices.Compare(x.Ports, y.Ports) })
	return out
}

// hasHostPorts reports whether any allowedHosts entry has its own ports.
func (a EgressAllowlist) hasHostPorts() bool {
	return slices.ContainsFunc(a.AllowedHosts, func(h aiv1alpha1.EgressHost) bool { return len(h.Ports) > 0 })
}

// allHostPorts is the union of the ports of all allowed hosts.
func (a EgressAllowlist) allHostPorts() []int32 {
	out := make([]int32, 0, len(a.AllowedHosts))
	for _, ports := range a.hostPorts() {
		out = append(out, ports...)
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
// (d) the base and arsenal git hosts and (e) each allowed host on its own
// ports or the allowed ports (one rule per distinct port set). Everything else is denied. The endpoint selector uses the
// knight's pod labels, so it covers both runtime backends.
func RenderCiliumEgressPolicy(k *aiv1alpha1.Knight, allow EgressAllowlist, s missionpkg.NetPolSettings) *unstructured.Unstructured {
	egress := []any{ //nolint:prealloc // the rule count depends on several optional sources
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
	for _, g := range allow.hostGroups() {
		egress = append(egress, ciliumFQDNRule(g.Hosts, g.Ports))
	}
	for _, svc := range allow.Services {
		egress = append(egress, ciliumServiceRule(svc))
	}

	cnp := newCiliumEgressPolicy(k)
	cnp.SetLabels(knightLabels(k))
	cnp.Object["spec"] = map[string]any{
		"endpointSelector": map[string]any{"matchLabels": stringMap(knightPodSelectorLabels(k))},
		"egress":           egress,
	}
	return cnp
}

// ciliumServiceRule allows the Service's pods (its namespace and pod
// selector) on the Service's allowed pod ports; a host-network Service is
// allowed as the host and remote-node entities on those ports.
func ciliumServiceRule(svc EgressServicePeer) map[string]any {
	ports := make([]any, 0, len(svc.Ports))
	for _, p := range svc.Ports {
		ports = append(ports, map[string]any{"port": p.Port.String(), "protocol": string(p.Protocol)})
	}
	if svc.HostNetwork {
		return map[string]any{
			"toEntities": []any{"host", "remote-node"},
			"toPorts":    []any{map[string]any{"ports": ports}},
		}
	}
	return map[string]any{
		"toEndpoints": []any{map[string]any{"matchLabels": stringMap(ciliumPeerLabels(svc.Namespace, svc.Selector))}},
		"toPorts":     []any{map[string]any{"ports": ports}},
	}
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
// allowed, the allowed ports. Allowed Services get a namespace and pod
// selector rule on their ports.
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
	allowed = append(allowed, allow.allHostPorts()...)
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
	for _, svc := range allow.Services {
		rules = append(rules, serviceEgressRule(svc))
	}
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

// serviceEgressRule allows the Service's pods on its allowed pod ports, or a
// host-network Service's endpoint addresses (as /32 or /128 ipBlocks) on them.
func serviceEgressRule(svc EgressServicePeer) networkingv1.NetworkPolicyEgressRule {
	ports := make([]networkingv1.NetworkPolicyPort, 0, len(svc.Ports))
	for _, p := range svc.Ports {
		ports = append(ports, networkingv1.NetworkPolicyPort{Protocol: new(p.Protocol), Port: new(p.Port)})
	}
	if svc.HostNetwork {
		rule := networkingv1.NetworkPolicyEgressRule{Ports: ports}
		for _, a := range svc.Addresses {
			bits := 32
			if strings.Contains(a, ":") {
				bits = 128
			}
			rule.To = append(rule.To, networkingv1.NetworkPolicyPeer{
				IPBlock: &networkingv1.IPBlock{CIDR: fmt.Sprintf("%s/%d", a, bits)},
			})
		}
		return rule
	}
	return networkingv1.NetworkPolicyEgressRule{
		To: []networkingv1.NetworkPolicyPeer{{
			NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{corev1.LabelMetadataName: svc.Namespace}},
			PodSelector:       &metav1.LabelSelector{MatchLabels: maps.Clone(svc.Selector)},
		}},
		Ports: ports,
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
// EgressRestricted. Allowed Services that are missing or have no selector,
// and in-cluster names under allowedHosts, are reported in the condition
// message and as Warning events.
func (r *KnightReconciler) reconcileEgress(ctx context.Context, k *aiv1alpha1.Knight) error {
	allow := r.egressAllowlistOf(k)
	if allow == nil {
		meta.RemoveStatusCondition(&k.Status.Conditions, aiv1alpha1.ConditionKnightEgressRestricted)
		return r.deleteEgressPolicies(ctx, k)
	}
	// Fail closed: render the policy from the valid entries only, so a bad
	// entry never leaves the knight without a policy; report the dropped ones.
	// An EgressAllowlist holds decoded hosts, so the raw entries are parsed here.
	hosts, dropped := aiv1alpha1.ParseEgressHosts(k.Spec.Egress.AllowedHosts)
	allow.AllowedHosts = hosts
	var err error
	var services []EgressServicePeer
	var missing []string
	services, missing, err = r.resolveEgressServices(ctx, k)
	if err == nil {
		allow.Services = services
		err = r.applyEgressPolicy(ctx, k, *allow)
	}
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
		if len(missing) > 0 {
			cond.Reason = aiv1alpha1.ReasonEgressServiceNotFound
		}
		cond.Message = fmt.Sprintf("CiliumNetworkPolicy %s limits egress to DNS, NATS, the Kubernetes API and %d host(s)",
			egressPolicyName(k), allow.hostCount())
		if n := len(allow.Services); n > 0 {
			cond.Message += fmt.Sprintf(" and %d Service(s)", n)
		}
	default:
		cond.Status = metav1.ConditionFalse
		cond.Reason = aiv1alpha1.ReasonFQDNPolicyUnavailable
		cond.Message = fmt.Sprintf("%s CiliumNetworkPolicy is not served, so hostnames can't be enforced; "+
			"NetworkPolicy %s allows DNS, NATS, the Kubernetes API (ports 443 and 6443) and the allowed ports to any destination",
			rtruntime.CiliumNetworkPolicyGVK.GroupVersion(), egressPolicyName(k))
		if allow.hasHostPorts() {
			cond.Message += "; per-host ports are not enforced, so the union of all host ports is allowed to any destination"
		}
		if n := len(allow.Services); n > 0 {
			cond.Message += fmt.Sprintf(", and %d Service(s) on their ports", n)
		}
	}
	if err == nil {
		hostWarnings := inClusterHostWarnings(allow.AllowedHosts)
		for _, w := range slices.Concat(missing, hostWarnings, hostNetworkNotes(allow.Services)) {
			cond.Message += "; " + w
		}
		prev := meta.FindStatusCondition(k.Status.Conditions, aiv1alpha1.ConditionKnightEgressRestricted)
		if r.Recorder != nil && (prev == nil || prev.Message != cond.Message) {
			for _, w := range missing {
				r.Recorder.Event(k, corev1.EventTypeWarning, aiv1alpha1.ReasonEgressServiceNotFound, w)
			}
			for _, w := range hostWarnings {
				r.Recorder.Event(k, corev1.EventTypeWarning, eventReasonInClusterHost, w)
			}
		}
	}
	if err == nil && len(dropped) > 0 {
		err = fmt.Errorf("%d allowedHosts entr(ies) invalid and not allowed: %w", len(dropped), errors.Join(dropped...))
		cond.Status = metav1.ConditionFalse
		cond.Reason = aiv1alpha1.ReasonEgressPolicyFailed
		cond.Message = fmt.Sprintf("%s; the policy was rendered from the valid entries only (%s)", err.Error(), cond.Message)
	}
	meta.SetStatusCondition(&k.Status.Conditions, cond)
	return err
}

// eventReasonInClusterHost is the Warning event reason for an allowedHosts
// entry that names an in-cluster Service.
const eventReasonInClusterHost = "InClusterAllowedHost"

// inClusterHostWarnings returns a warning for each allowedHosts entry that is
// an in-cluster name (ending in .svc or .cluster.local): it resolves to a
// ClusterIP, which FQDN rules never match.
func inClusterHostWarnings(hosts []aiv1alpha1.EgressHost) []string {
	var out []string
	for _, h := range (EgressAllowlist{AllowedHosts: hosts}).hosts() {
		if strings.HasSuffix(h, ".svc") || strings.HasSuffix(h, ".cluster.local") {
			out = append(out, fmt.Sprintf("allowedHosts entry %q is an in-cluster name, which hostname rules never match; "+
				"use allowedServices instead", h))
		}
	}
	return out
}

// egressServiceRef is a Service named in spec.egress.allowedServices, with
// the union of its listed ports.
type egressServiceRef struct {
	Namespace string
	Name      string
	Ports     []int32
}

// egressServiceRefs returns the Services in the knight's spec.egress (empty
// namespace = the knight's), merged by namespace/name and sorted.
func egressServiceRefs(k *aiv1alpha1.Knight) []egressServiceRef {
	if k.Spec.Egress == nil {
		return nil
	}
	byKey := map[types.NamespacedName][]int32{}
	for _, s := range k.Spec.Egress.AllowedServices {
		ns := s.Namespace
		if ns == "" {
			ns = k.Namespace
		}
		key := types.NamespacedName{Namespace: ns, Name: s.Name}
		byKey[key] = append(byKey[key], s.Ports...)
	}
	out := make([]egressServiceRef, 0, len(byKey))
	for key, ports := range byKey {
		slices.Sort(ports)
		out = append(out, egressServiceRef{Namespace: key.Namespace, Name: key.Name, Ports: slices.Compact(ports)})
	}
	slices.SortFunc(out, func(a, b egressServiceRef) int {
		return strings.Compare(a.Namespace+"/"+a.Name, b.Namespace+"/"+b.Name)
	})
	return out
}

// resolveEgressServices reads the knight's allowed Services. Services that
// exist and have a pod selector are returned as peers; the others are
// described in missing and left out of the policy.
func (r *KnightReconciler) resolveEgressServices(ctx context.Context, k *aiv1alpha1.Knight) (
	peers []EgressServicePeer, missing []string, err error) {
	for _, ref := range egressServiceRefs(k) {
		svc := &corev1.Service{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}, svc); err != nil {
			if apierrors.IsNotFound(err) {
				missing = append(missing, fmt.Sprintf("allowed Service %s/%s not found; its egress rule is left out",
					ref.Namespace, ref.Name))
				continue
			}
			return nil, nil, fmt.Errorf("failed to get allowed Service %s/%s: %w", ref.Namespace, ref.Name, err)
		}
		if len(svc.Spec.Selector) == 0 {
			missing = append(missing, fmt.Sprintf("allowed Service %s/%s has no pod selector; its egress rule is left out",
				ref.Namespace, ref.Name))
			continue
		}
		peer := EgressServicePeer{
			Namespace: ref.Namespace,
			Name:      ref.Name,
			Selector:  maps.Clone(svc.Spec.Selector),
			Ports:     serviceTargetPorts(svc, ref.Ports),
		}
		hostNet, addrs, slicePorts, err := r.hostNetworkEndpoints(ctx, svc)
		if err != nil {
			return nil, nil, err
		}
		if hostNet {
			if ports := hostNetworkPorts(svc, ref.Ports, slicePorts); len(ports) > 0 {
				peer.HostNetwork = true
				peer.Addresses = addrs
				peer.Ports = ports
			} else {
				// A host-network rule with no ports would allow every port on
				// the nodes: keep the pod selector rule instead.
				missing = append(missing, fmt.Sprintf("allowed Service %s/%s uses the host network but its "+
					"target ports could not be resolved to numbers; its egress rule stays a pod selector rule "+
					"and will not reach host-network pods", ref.Namespace, ref.Name))
			}
		}
		peers = append(peers, peer)
	}
	return peers, missing, nil
}

// hostNetworkEndpoints reads the Service's EndpointSlices and reports whether
// its ready endpoints are pods on the host network, with their addresses
// (sorted, deduplicated) and the slices' ports. A Service with no ready
// endpoint, or with any endpoint that is an ordinary pod, is not host-network:
// the pod selector rule still matches the ordinary pods.
func (r *KnightReconciler) hostNetworkEndpoints(ctx context.Context, svc *corev1.Service) (
	hostNet bool, addrs []string, ports []discoveryv1.EndpointPort, err error) {
	slices_ := &discoveryv1.EndpointSliceList{}
	if err := r.List(ctx, slices_, client.InNamespace(svc.Namespace),
		client.MatchingLabels{discoveryv1.LabelServiceName: svc.Name}); err != nil {
		return false, nil, nil, fmt.Errorf("failed to list EndpointSlices of allowed Service %s/%s: %w",
			svc.Namespace, svc.Name, err)
	}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	podHost := map[types.NamespacedName]bool{}
	ready := 0
	for _, es := range slices_.Items {
		for _, ep := range es.Endpoints {
			if ep.Conditions.Ready != nil && !*ep.Conditions.Ready {
				continue
			}
			if ep.TargetRef == nil || ep.TargetRef.Kind != "Pod" {
				return false, nil, nil, nil
			}
			key := types.NamespacedName{Namespace: cmp.Or(ep.TargetRef.Namespace, svc.Namespace), Name: ep.TargetRef.Name}
			isHost, seen := podHost[key]
			if !seen {
				pod := &corev1.Pod{}
				if err := reader.Get(ctx, key, pod); err != nil {
					if apierrors.IsNotFound(err) {
						continue // gone since the slice was written
					}
					return false, nil, nil, fmt.Errorf("failed to get pod %s of allowed Service %s/%s: %w",
						key, svc.Namespace, svc.Name, err)
				}
				isHost = pod.Spec.HostNetwork
				podHost[key] = isHost
			}
			if !isHost {
				return false, nil, nil, nil
			}
			ready++
			addrs = append(addrs, ep.Addresses...)
		}
		ports = append(ports, es.Ports...)
	}
	if ready == 0 {
		return false, nil, nil, nil
	}
	slices.Sort(addrs)
	return true, slices.Compact(addrs), ports, nil
}

// hostNetworkPorts returns the numeric ports to allow on the nodes for a
// host-network Service: each listed Service port resolved to the port number
// in the EndpointSlices (which resolves a named targetPort), else the
// targetPort when numeric, else the Service port itself.
func hostNetworkPorts(svc *corev1.Service, listed []int32, slicePorts []discoveryv1.EndpointPort) []EgressServicePort {
	var out []EgressServicePort
	for _, p := range listed {
		matched := false
		for _, sp := range svc.Spec.Ports {
			if sp.Port != p {
				continue
			}
			matched = true
			proto := sp.Protocol
			if proto == "" {
				proto = corev1.ProtocolTCP
			}
			num := int32(0)
			for _, ep := range slicePorts {
				epProto := corev1.ProtocolTCP
				if ep.Protocol != nil {
					epProto = *ep.Protocol
				}
				if ep.Port != nil && ptr.Deref(ep.Name, "") == sp.Name && epProto == proto {
					num = *ep.Port
					break
				}
			}
			if num == 0 && sp.TargetPort.Type == intstr.Int && sp.TargetPort.IntVal > 0 {
				num = sp.TargetPort.IntVal
			}
			if num == 0 && sp.TargetPort.Type == intstr.Int {
				num = sp.Port
			}
			if num == 0 {
				continue // unresolved named port: nothing to allow
			}
			out = append(out, EgressServicePort{Port: intstr.FromInt32(num), Protocol: proto})
		}
		if !matched {
			out = append(out, EgressServicePort{Port: intstr.FromInt32(p), Protocol: corev1.ProtocolTCP})
		}
	}
	slices.SortFunc(out, func(a, b EgressServicePort) int {
		if c := int(a.Port.IntVal) - int(b.Port.IntVal); c != 0 {
			return c
		}
		return strings.Compare(string(a.Protocol), string(b.Protocol))
	})
	return slices.Compact(out)
}

// hostNetworkNotes describes each host-network Service in the status message:
// the rule is wider than one Service, so the reader should know.
func hostNetworkNotes(services []EgressServicePeer) []string {
	var out []string
	for _, svc := range services {
		if !svc.HostNetwork {
			continue
		}
		ports := make([]string, 0, len(svc.Ports))
		for _, p := range svc.Ports {
			ports = append(ports, p.Port.String())
		}
		slices.Sort(ports)
		ports = slices.Compact(ports)
		out = append(out, fmt.Sprintf("Service %s/%s uses the host network: reaches every node on port %s",
			svc.Namespace, svc.Name, strings.Join(ports, ", ")))
	}
	return out
}

// endpointSliceKnightRequests maps an EndpointSlice to the knights that
// allow its Service.
func (r *KnightReconciler) endpointSliceKnightRequests(ctx context.Context, obj client.Object) []reconcile.Request {
	name := obj.GetLabels()[discoveryv1.LabelServiceName]
	if name == "" {
		return nil
	}
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: obj.GetNamespace()}}
	return r.serviceKnightRequests(ctx, svc)
}

// egressEndpointSliceChanged passes EndpointSlice creates and deletes, and
// updates that change the endpoints or ports a host-network rule is rendered from.
var egressEndpointSliceChanged = predicate.Funcs{
	UpdateFunc: func(e event.UpdateEvent) bool {
		oldES, ok1 := e.ObjectOld.(*discoveryv1.EndpointSlice)
		newES, ok2 := e.ObjectNew.(*discoveryv1.EndpointSlice)
		if !ok1 || !ok2 {
			return true
		}
		return !equality.Semantic.DeepEqual(oldES.Endpoints, newES.Endpoints) ||
			!equality.Semantic.DeepEqual(oldES.Ports, newES.Ports)
	},
}

// serviceTargetPorts maps the listed Service ports to the pod ports a policy
// must allow: each matching Service port's targetPort (a number or a named
// port; unset means the port itself) with its protocol. A listed port the
// Service does not expose is allowed as a TCP pod port as-is. The result is
// deduplicated and sorted (numbers first).
func serviceTargetPorts(svc *corev1.Service, listed []int32) []EgressServicePort {
	var out []EgressServicePort
	for _, p := range listed {
		matched := false
		for _, sp := range svc.Spec.Ports {
			if sp.Port != p {
				continue
			}
			matched = true
			target := sp.TargetPort
			if target.Type == intstr.Int && target.IntVal == 0 || target.Type == intstr.String && target.StrVal == "" {
				target = intstr.FromInt32(sp.Port)
			}
			proto := sp.Protocol
			if proto == "" {
				proto = corev1.ProtocolTCP
			}
			out = append(out, EgressServicePort{Port: target, Protocol: proto})
		}
		if !matched {
			out = append(out, EgressServicePort{Port: intstr.FromInt32(p), Protocol: corev1.ProtocolTCP})
		}
	}
	slices.SortFunc(out, func(a, b EgressServicePort) int {
		if a.Port.Type != b.Port.Type {
			return int(a.Port.Type) - int(b.Port.Type)
		}
		if c := int(a.Port.IntVal) - int(b.Port.IntVal); c != 0 {
			return c
		}
		if c := strings.Compare(a.Port.StrVal, b.Port.StrVal); c != 0 {
			return c
		}
		return strings.Compare(string(a.Protocol), string(b.Protocol))
	})
	return slices.Compact(out)
}

// serviceKnightRequests maps a Service to the knights whose effective egress
// allowlist (their own, or their table's default) names it.
func (r *KnightReconciler) serviceKnightRequests(ctx context.Context, obj client.Object) []reconcile.Request {
	knights := &aiv1alpha1.KnightList{}
	if err := r.List(ctx, knights); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list knights for allowed Service requeue")
		return nil
	}
	var reqs []reconcile.Request
	for i := range knights.Items {
		k := knightpkg.WithTableDefaults(ctx, r.Client, &knights.Items[i])
		if k.Spec.Isolation.EgressOptedOut() {
			continue
		}
		for _, ref := range egressServiceRefs(k) {
			if ref.Namespace == obj.GetNamespace() && ref.Name == obj.GetName() {
				reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(k)})
				break
			}
		}
	}
	return reqs
}

// egressServiceChanged passes Service creates and deletes, and updates that
// change what an egress rule is rendered from (the selector or the ports).
var egressServiceChanged = predicate.Funcs{
	UpdateFunc: func(e event.UpdateEvent) bool {
		oldSvc, ok1 := e.ObjectOld.(*corev1.Service)
		newSvc, ok2 := e.ObjectNew.(*corev1.Service)
		if !ok1 || !ok2 {
			return true
		}
		return !equality.Semantic.DeepEqual(oldSvc.Spec.Selector, newSvc.Spec.Selector) ||
			!equality.Semantic.DeepEqual(oldSvc.Spec.Ports, newSvc.Spec.Ports)
	},
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
