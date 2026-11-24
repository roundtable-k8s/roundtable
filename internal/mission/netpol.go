package mission

import (
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
)

// Operator env settings for the mission NetworkPolicy (set by the chart's
// missionNetworkPolicy values).
const (
	EnvNetPolEnabled       = "MISSION_NETPOL_ENABLED"
	EnvNetPolNATSPodLabels = "MISSION_NETPOL_NATS_POD_LABELS"
	EnvNetPolDNSNamespace  = "MISSION_NETPOL_DNS_NAMESPACE"
	EnvNetPolDNSPodLabels  = "MISSION_NETPOL_DNS_POD_LABELS"

	defaultNATSPort     = 4222
	defaultDNSNamespace = "kube-system"
)

// NetPolSettings configures the NATS and DNS egress peers of generated
// NetworkPolicies.
type NetPolSettings struct {
	// Enabled controls whether the mission NetworkPolicy is created at all.
	Enabled bool
	// NATSPodLabels optionally narrows the NATS peer to matching pods.
	NATSPodLabels map[string]string
	// DNSNamespace is the namespace of the cluster DNS pods.
	DNSNamespace string
	// DNSPodLabels selects the cluster DNS pods.
	DNSPodLabels map[string]string
}

// DefaultNetPolSettings returns the settings used when no env is set.
func DefaultNetPolSettings() NetPolSettings {
	return NetPolSettings{
		Enabled:      true,
		DNSNamespace: defaultDNSNamespace,
		DNSPodLabels: map[string]string{"k8s-app": "kube-dns"},
	}
}

// NetPolSettingsFromEnv reads the MISSION_NETPOL_* env vars over the defaults.
func NetPolSettingsFromEnv() NetPolSettings {
	s := DefaultNetPolSettings()
	if v := strings.TrimSpace(os.Getenv(EnvNetPolEnabled)); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			s.Enabled = b
		}
	}
	if v := os.Getenv(EnvNetPolNATSPodLabels); strings.TrimSpace(v) != "" {
		s.NATSPodLabels = ParseLabelList(v)
	}
	if v := strings.TrimSpace(os.Getenv(EnvNetPolDNSNamespace)); v != "" {
		s.DNSNamespace = v
	}
	if v := os.Getenv(EnvNetPolDNSPodLabels); strings.TrimSpace(v) != "" {
		s.DNSPodLabels = ParseLabelList(v)
	}
	return s
}

// ParseLabelList parses "k=v,k=v" into a map; malformed entries are skipped.
func ParseLabelList(s string) map[string]string {
	out := map[string]string{}
	for part := range strings.SplitSeq(s, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		k = strings.TrimSpace(k)
		if !ok || k == "" {
			continue
		}
		out[k] = strings.TrimSpace(v)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// NATSNamespaceAndPort extracts the port (default 4222) and, for hosts of the
// form <svc>.<namespace>.svc[.cluster.local], the namespace from a NATS URL.
// inCluster is false when the host is not in that form.
func NATSNamespaceAndPort(natsURL string) (namespace string, port int, inCluster bool) {
	port = defaultNATSPort
	u, err := url.Parse(natsURL)
	if err != nil || u.Hostname() == "" {
		return "", port, false
	}
	if p := u.Port(); p != "" {
		if n, err := strconv.Atoi(p); err == nil && n > 0 && n < 65536 {
			port = n
		}
	}
	host := strings.TrimSuffix(u.Hostname(), ".")
	if net.ParseIP(host) != nil {
		return "", port, false
	}
	parts := strings.Split(host, ".")
	if len(parts) >= 3 && parts[2] == "svc" &&
		(len(parts) == 3 || (len(parts) == 5 && parts[3] == "cluster" && parts[4] == "local")) &&
		parts[0] != "" && parts[1] != "" {
		return parts[1], port, true
	}
	return "", port, false
}

// NATSEgressRule returns the egress rule allowing a knight to reach NATS at
// natsURL. When the host is not a cluster service name the rule allows the
// port to any destination (no To peers) and inCluster is false.
func NATSEgressRule(natsURL string, podLabels map[string]string) (rule networkingv1.NetworkPolicyEgressRule, inCluster bool) {
	ns, port, inCluster := NATSNamespaceAndPort(natsURL)
	natsPort := intstr.FromInt32(int32(port)) //nolint:gosec // validated range
	rule.Ports = []networkingv1.NetworkPolicyPort{{
		Protocol: ptr.To(corev1.ProtocolTCP),
		Port:     &natsPort,
	}}
	if inCluster {
		rule.To = []networkingv1.NetworkPolicyPeer{NATSPeer(ns, podLabels)}
	}
	return rule, inCluster
}

// NATSPeer returns the peer for the NATS pods in namespace, optionally
// narrowed by pod labels.
func NATSPeer(namespace string, podLabels map[string]string) networkingv1.NetworkPolicyPeer {
	peer := networkingv1.NetworkPolicyPeer{
		NamespaceSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"kubernetes.io/metadata.name": namespace},
		},
	}
	if len(podLabels) > 0 {
		peer.PodSelector = &metav1.LabelSelector{MatchLabels: podLabels}
	}
	return peer
}

// DNSPeer returns the peer for the cluster DNS pods.
func DNSPeer(s NetPolSettings) networkingv1.NetworkPolicyPeer {
	ns := s.DNSNamespace
	if ns == "" {
		ns = defaultDNSNamespace
	}
	peer := networkingv1.NetworkPolicyPeer{
		NamespaceSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"kubernetes.io/metadata.name": ns},
		},
	}
	if len(s.DNSPodLabels) > 0 {
		peer.PodSelector = &metav1.LabelSelector{MatchLabels: s.DNSPodLabels}
	}
	return peer
}

// DNSEgressRule returns the egress rule allowing DNS (UDP and TCP 53).
func DNSEgressRule(s NetPolSettings) networkingv1.NetworkPolicyEgressRule {
	return networkingv1.NetworkPolicyEgressRule{
		To: []networkingv1.NetworkPolicyPeer{DNSPeer(s)},
		Ports: []networkingv1.NetworkPolicyPort{
			{Protocol: ptr.To(corev1.ProtocolUDP), Port: new(intstr.FromInt32(53))},
			{Protocol: ptr.To(corev1.ProtocolTCP), Port: new(intstr.FromInt32(53))},
		},
	}
}
