package mission

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
)

func TestNATSNamespaceAndPort(t *testing.T) {
	tests := []struct {
		name       string
		url        string
		wantNS     string
		wantPort   int
		wantInClus bool
	}{
		{"short service name", "nats://nats.nats.svc:4222", "nats", 4222, true},
		{"fully qualified", "nats://nats.nats.svc.cluster.local:4222", "nats", 4222, true},
		{"fqdn with trailing dot", "nats://nats.nats.svc.cluster.local.", "nats", 4222, true},
		{"svc with trailing dot", "nats://nats.nats.svc.", "nats", 4222, true},
		{"no port uses default", "nats://nats.nats.svc", "nats", 4222, true},
		{"max valid port", "nats://nats.nats.svc:65535", "nats", 65535, true},
		{"port zero ignored", "nats://nats.nats.svc:0", "nats", 4222, true},
		{"IPv4 host is not in cluster", "nats://10.0.0.5:4222", "", 4222, false},
		{"IPv6 host is not in cluster", "nats://[::1]:4222", "", 4222, false},
		{"external host", "nats://broker.example.com:4222", "", 4222, false},
		{"external host with svc label", "nats://foo.bar.svc.example.com:4222", "", 4222, false},
		{"external host with cluster.local suffix mismatch", "nats://nats.nats.svc.cluster.example", "", 4222, false},
		{"too many labels", "nats://nats.nats.svc.cluster.local.x", "", 4222, false},
		{"missing namespace", "nats://nats.svc:4222", "", 4222, false},
		{"not svc", "nats://nats.nats.pod:4222", "", 4222, false},
		{"empty service label", "nats://.nats.svc:4222", "", 4222, false},
		{"empty namespace label", "nats://nats..svc:4222", "", 4222, false},
		{"port zero on external host", "nats://host:0", "", 4222, false},
		{"port out of range on external host", "nats://host:70000", "", 4222, false},
		{"userinfo stripped", "nats://user:pass@nats.nats.svc:4222", "nats", 4222, true},
		{"empty url", "", "", 4222, false},
		{"malformed url", "://bad", "", 4222, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns, port, inCluster := NATSNamespaceAndPort(tt.url)
			if ns != tt.wantNS || port != tt.wantPort || inCluster != tt.wantInClus {
				t.Errorf("NATSNamespaceAndPort(%q) = (%q, %d, %v), want (%q, %d, %v)",
					tt.url, ns, port, inCluster, tt.wantNS, tt.wantPort, tt.wantInClus)
			}
		})
	}
}

func TestNATSEgressRule(t *testing.T) {
	natsPort := intstr.FromInt32(4222)
	tcp := []networkingv1.NetworkPolicyPort{{Protocol: ptr.To(corev1.ProtocolTCP), Port: &natsPort}}
	labels := map[string]string{"app": "nats"}

	t.Run("in cluster with pod labels", func(t *testing.T) {
		rule, inCluster := NATSEgressRule("nats://nats.nats.svc:4222", labels)
		if !inCluster {
			t.Fatal("inCluster = false, want true")
		}
		if !reflect.DeepEqual(rule.Ports, tcp) {
			t.Errorf("Ports = %+v, want %+v", rule.Ports, tcp)
		}
		want := []networkingv1.NetworkPolicyPeer{NATSPeer("nats", labels)}
		if !reflect.DeepEqual(rule.To, want) {
			t.Errorf("To = %+v, want %+v", rule.To, want)
		}
	})

	t.Run("in cluster without pod labels", func(t *testing.T) {
		rule, inCluster := NATSEgressRule("nats://nats.nats.svc:4222", nil)
		if !inCluster {
			t.Fatal("inCluster = false, want true")
		}
		if len(rule.To) != 1 || rule.To[0].PodSelector != nil {
			t.Errorf("To = %+v, want one peer with no PodSelector", rule.To)
		}
	})

	t.Run("external host opens port to any destination", func(t *testing.T) {
		rule, inCluster := NATSEgressRule("nats://broker.example.com:4222", labels)
		if inCluster {
			t.Fatal("inCluster = true, want false")
		}
		if rule.To != nil {
			t.Errorf("To = %+v, want nil (any destination)", rule.To)
		}
		if !reflect.DeepEqual(rule.Ports, tcp) {
			t.Errorf("Ports = %+v, want %+v", rule.Ports, tcp)
		}
	})

	t.Run("custom port is used for the rule", func(t *testing.T) {
		rule, _ := NATSEgressRule("nats://nats.nats.svc:5222", nil)
		p := intstr.FromInt32(5222)
		want := []networkingv1.NetworkPolicyPort{{Protocol: ptr.To(corev1.ProtocolTCP), Port: &p}}
		if !reflect.DeepEqual(rule.Ports, want) {
			t.Errorf("Ports = %+v, want %+v", rule.Ports, want)
		}
	})
}

func TestNATSPeer(t *testing.T) {
	nsSelector := &metav1.LabelSelector{
		MatchLabels: map[string]string{"kubernetes.io/metadata.name": "nats"},
	}
	tests := []struct {
		name   string
		labels map[string]string
		want   networkingv1.NetworkPolicyPeer
	}{
		{
			name:   "namespace only when no labels",
			labels: nil,
			want:   networkingv1.NetworkPolicyPeer{NamespaceSelector: nsSelector},
		},
		{
			name:   "empty labels map adds no pod selector",
			labels: map[string]string{},
			want:   networkingv1.NetworkPolicyPeer{NamespaceSelector: nsSelector},
		},
		{
			name:   "pod labels narrow the peer",
			labels: map[string]string{"app": "nats"},
			want: networkingv1.NetworkPolicyPeer{
				NamespaceSelector: nsSelector,
				PodSelector:       &metav1.LabelSelector{MatchLabels: map[string]string{"app": "nats"}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NATSPeer("nats", tt.labels); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NATSPeer = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestDNSPeer(t *testing.T) {
	kubeDNS := &metav1.LabelSelector{MatchLabels: map[string]string{"k8s-app": "kube-dns"}}
	tests := []struct {
		name string
		s    NetPolSettings
		want networkingv1.NetworkPolicyPeer
	}{
		{
			name: "defaults select kube-dns in kube-system",
			s:    DefaultNetPolSettings(),
			want: networkingv1.NetworkPolicyPeer{
				NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": "kube-system"}},
				PodSelector:       kubeDNS,
			},
		},
		{
			name: "empty namespace falls back to kube-system",
			s:    NetPolSettings{DNSPodLabels: map[string]string{"k8s-app": "kube-dns"}},
			want: networkingv1.NetworkPolicyPeer{
				NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": "kube-system"}},
				PodSelector:       kubeDNS,
			},
		},
		{
			name: "custom namespace and no pod labels",
			s:    NetPolSettings{DNSNamespace: "dns"},
			want: networkingv1.NetworkPolicyPeer{
				NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": "dns"}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DNSPeer(tt.s); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DNSPeer = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestDNSEgressRule(t *testing.T) {
	s := DefaultNetPolSettings()
	rule := DNSEgressRule(s)
	if want := []networkingv1.NetworkPolicyPeer{DNSPeer(s)}; !reflect.DeepEqual(rule.To, want) {
		t.Errorf("To = %+v, want %+v", rule.To, want)
	}
	p53 := intstr.FromInt32(53)
	want := []networkingv1.NetworkPolicyPort{
		{Protocol: ptr.To(corev1.ProtocolUDP), Port: &p53},
		{Protocol: ptr.To(corev1.ProtocolTCP), Port: &p53},
	}
	if !reflect.DeepEqual(rule.Ports, want) {
		t.Errorf("Ports = %+v, want UDP and TCP 53", rule.Ports)
	}
}

func TestParseLabelList(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want map[string]string
	}{
		{"empty", "", nil},
		{"whitespace only", " , ", nil},
		{"single pair", "app=nats", map[string]string{"app": "nats"}},
		{"spaces around parts", " a = b , c=d ", map[string]string{"a": "b", "c": "d"}},
		{"duplicate key keeps last", "a=1,a=2", map[string]string{"a": "2"}},
		{"no equals sign is skipped", "novalue,a=b", map[string]string{"a": "b"}},
		{"empty key is skipped", "=v,a=b", map[string]string{"a": "b"}},
		{"only malformed entries yields nil", "novalue,=v", nil},
		{"empty value is kept", "k=", map[string]string{"k": ""}},
		{"value may contain equals", "a=b=c", map[string]string{"a": "b=c"}},
		{"empty entries are skipped", ",,a=b,,", map[string]string{"a": "b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ParseLabelList(tt.in); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseLabelList(%q) = %#v, want %#v", tt.in, got, tt.want)
			}
		})
	}
}

func TestDefaultNetPolSettings(t *testing.T) {
	got := DefaultNetPolSettings()
	want := NetPolSettings{
		Enabled:      true,
		DNSNamespace: "kube-system",
		DNSPodLabels: map[string]string{"k8s-app": "kube-dns"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("DefaultNetPolSettings() = %#v, want %#v", got, want)
	}
}

func TestNetPolSettingsFromEnv(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want func(s *NetPolSettings)
	}{
		{
			name: "unset env keeps defaults",
			env:  map[string]string{},
			want: func(*NetPolSettings) {},
		},
		{
			name: "enabled false",
			env:  map[string]string{EnvNetPolEnabled: "false"},
			want: func(s *NetPolSettings) { s.Enabled = false },
		},
		{
			name: "enabled accepts whitespace and 0",
			env:  map[string]string{EnvNetPolEnabled: " 0 "},
			want: func(s *NetPolSettings) { s.Enabled = false },
		},
		{
			name: "unparseable enabled keeps default",
			env:  map[string]string{EnvNetPolEnabled: "bogus"},
			want: func(*NetPolSettings) {},
		},
		{
			name: "nats labels set",
			env:  map[string]string{EnvNetPolNATSPodLabels: "app=nats"},
			want: func(s *NetPolSettings) { s.NATSPodLabels = map[string]string{"app": "nats"} },
		},
		{
			name: "whitespace-only nats labels stay unset",
			env:  map[string]string{EnvNetPolNATSPodLabels: "   "},
			want: func(*NetPolSettings) {},
		},
		{
			name: "dns namespace is trimmed",
			env:  map[string]string{EnvNetPolDNSNamespace: "  dns-ns  "},
			want: func(s *NetPolSettings) { s.DNSNamespace = "dns-ns" },
		},
		{
			name: "dns labels replace the default",
			env:  map[string]string{EnvNetPolDNSPodLabels: "k8s-app=coredns"},
			want: func(s *NetPolSettings) { s.DNSPodLabels = map[string]string{"k8s-app": "coredns"} },
		},
		{
			// Pins current behaviour: a DNS label value made only of malformed
			// entries replaces the default selector with nil, so the DNS peer
			// has no pod selector and matches every pod in the DNS namespace.
			// Tracked in #751; this case must change when #751 lands.
			name: "malformed dns labels clear the default selector",
			env:  map[string]string{EnvNetPolDNSPodLabels: "=x"},
			want: func(s *NetPolSettings) { s.DNSPodLabels = nil },
		},
		{
			name: "whitespace-only dns labels keep the default",
			env:  map[string]string{EnvNetPolDNSPodLabels: " "},
			want: func(*NetPolSettings) {},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, k := range []string{EnvNetPolEnabled, EnvNetPolNATSPodLabels, EnvNetPolDNSNamespace, EnvNetPolDNSPodLabels} {
				t.Setenv(k, "")
			}
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			want := DefaultNetPolSettings()
			tt.want(&want)
			if got := NetPolSettingsFromEnv(); !reflect.DeepEqual(got, want) {
				t.Errorf("NetPolSettingsFromEnv() = %#v, want %#v", got, want)
			}
		})
	}
}
