package mission

import (
	"context"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
)

// ─── buildEphemeralKnight ───────────────────────────────────────────────────

func ephemeralFixtures() (*aiv1alpha1.Mission, aiv1alpha1.MissionKnight, *aiv1alpha1.RoundTable) {
	rt := &aiv1alpha1.RoundTable{
		ObjectMeta: metav1.ObjectMeta{Name: "personal", Namespace: "roundtable"},
		Spec: aiv1alpha1.RoundTableSpec{
			NATS: aiv1alpha1.RoundTableNATS{
				URL:           "nats://nats.database.svc:4222",
				SubjectPrefix: "fleet-a",
				TasksStream:   "fleet_a_tasks",
				ResultsStream: "fleet_a_results",
			},
			KnightTemplates: map[string]aiv1alpha1.KnightSpec{
				"base": {
					Model:  "openrouter/deepseek/deepseek-v3.2",
					Domain: "general",
				},
			},
		},
	}
	mission := &aiv1alpha1.Mission{
		ObjectMeta: metav1.ObjectMeta{Name: "itertest", Namespace: "roundtable"},
		Spec:       aiv1alpha1.MissionSpec{RoundTableRef: aiv1alpha1.LocalRef("personal")},
	}
	mk := aiv1alpha1.MissionKnight{
		Name:         "haiku-writer",
		Ephemeral:    true,
		TemplateName: "base",
		SpecOverrides: &aiv1alpha1.KnightSpecOverrides{
			Domain: "creative",
		},
	}
	return mission, mk, rt
}

func TestBuildEphemeralKnightSubscribesToExactSubject(t *testing.T) {
	mission, mk, rt := ephemeralFixtures()
	a := &KnightAssembler{}

	knight, err := a.buildEphemeralKnight(mission, mk, rt)
	if err != nil {
		t.Fatalf("buildEphemeralKnight: %v", err)
	}

	want := "fleet-a.tasks.creative.itertest-haiku-writer"
	if len(knight.Spec.NATS.Subjects) != 1 || knight.Spec.NATS.Subjects[0] != want {
		t.Errorf("subjects = %v, want [%s] (a domain wildcard replays other missions' retained tasks)",
			knight.Spec.NATS.Subjects, want)
	}
}

func TestBuildEphemeralKnightInjectsRoundTableAndMissionSecrets(t *testing.T) {
	mission, mk, rt := ephemeralFixtures()
	rt.Spec.Secrets = []corev1.LocalObjectReference{{Name: "roundtable-secret"}}
	mission.Spec.Secrets = []corev1.LocalObjectReference{{Name: "mission-extra"}}
	a := &KnightAssembler{}

	knight, err := a.buildEphemeralKnight(mission, mk, rt)
	if err != nil {
		t.Fatalf("buildEphemeralKnight: %v", err)
	}

	var got []string
	for _, ef := range knight.Spec.EnvFrom {
		if ef.SecretRef != nil {
			got = append(got, ef.SecretRef.Name)
		}
	}
	want := []string{"roundtable-secret", "mission-extra"}
	if len(got) != len(want) {
		t.Fatalf("envFrom secrets = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("envFrom[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestBuildEphemeralKnightDedupesSecrets(t *testing.T) {
	mission, mk, rt := ephemeralFixtures()
	// Template already carries the secret, and RT + mission list it again.
	tmpl := rt.Spec.KnightTemplates["base"]
	tmpl.EnvFrom = []corev1.EnvFromSource{{
		SecretRef: &corev1.SecretEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: "roundtable-secret"},
		},
	}}
	rt.Spec.KnightTemplates["base"] = tmpl
	rt.Spec.Secrets = []corev1.LocalObjectReference{{Name: "roundtable-secret"}}
	mission.Spec.Secrets = []corev1.LocalObjectReference{{Name: "roundtable-secret"}}
	a := &KnightAssembler{}

	knight, err := a.buildEphemeralKnight(mission, mk, rt)
	if err != nil {
		t.Fatalf("buildEphemeralKnight: %v", err)
	}

	count := 0
	for _, ef := range knight.Spec.EnvFrom {
		if ef.SecretRef != nil && ef.SecretRef.Name == "roundtable-secret" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("roundtable-secret referenced %d times in envFrom, want 1", count)
	}
}

// ─── GitHub token inheritance ───────────────────────────────────────────────

func TestBuildEphemeralKnightInheritsGitHubTokenSecret(t *testing.T) {
	mission, mk, rt := ephemeralFixtures()
	rt.Spec.GitHub = &aiv1alpha1.GitHubConfig{
		TokenSecretRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: testGitHubTokenSecret}, Key: "token"},
	}
	a := &KnightAssembler{}

	knight, err := a.buildEphemeralKnight(mission, mk, rt)
	if err != nil {
		t.Fatalf("buildEphemeralKnight: %v", err)
	}
	if knight.Spec.GitHub == nil || knight.Spec.GitHub.TokenSecretRef == nil ||
		knight.Spec.GitHub.TokenSecretRef.Name != testGitHubTokenSecret {
		t.Fatalf("github = %+v, want tokenSecretRef roundtable-github-token", knight.Spec.GitHub)
	}
	// Must be a copy, not an alias of the table's spec.
	knight.Spec.GitHub.TokenSecretRef.Name = "mutated"
	if rt.Spec.GitHub.TokenSecretRef.Name != testGitHubTokenSecret {
		t.Error("knight spec aliases the RoundTable's github config")
	}
}

func TestBuildEphemeralKnightKeepsTemplateGitHubTokenSecret(t *testing.T) {
	mission, mk, rt := ephemeralFixtures()
	rt.Spec.GitHub = &aiv1alpha1.GitHubConfig{
		TokenSecretRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "table-token"}},
	}
	tmpl := rt.Spec.KnightTemplates["base"]
	tmpl.GitHub = &aiv1alpha1.GitHubConfig{
		TokenSecretRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "template-token"}},
	}
	rt.Spec.KnightTemplates["base"] = tmpl
	a := &KnightAssembler{}

	knight, err := a.buildEphemeralKnight(mission, mk, rt)
	if err != nil {
		t.Fatalf("buildEphemeralKnight: %v", err)
	}
	if got := knight.Spec.GitHub.TokenSecretRef.Name; got != "template-token" {
		t.Errorf("tokenSecretRef = %q, want template-token", got)
	}
}

func TestBuildEphemeralRoundTableInheritsParentGitHub(t *testing.T) {
	mission, _, parent := ephemeralFixtures()
	parent.Spec.GitHub = &aiv1alpha1.GitHubConfig{
		TokenSecretRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: testGitHubTokenSecret}, Key: "token"},
	}
	s := k8sruntime.NewScheme()
	if err := aiv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	a := &KnightAssembler{Client: fake.NewClientBuilder().WithScheme(s).WithObjects(parent).Build()}

	rt := a.BuildEphemeralRoundTable(context.Background(), mission, "msn-itertest", "msn-itertest", "t", "r")
	if rt.Spec.GitHub == nil || rt.Spec.GitHub.TokenSecretRef == nil ||
		rt.Spec.GitHub.TokenSecretRef.Name != testGitHubTokenSecret {
		t.Fatalf("ephemeral table github = %+v, want parent's tokenSecretRef", rt.Spec.GitHub)
	}
}

const testGitHubTokenSecret = "roundtable-github-token"

// ─── mission NetworkPolicy ──────────────────────────────────────────────────

func netpolMission() *aiv1alpha1.Mission {
	return &aiv1alpha1.Mission{
		ObjectMeta: metav1.ObjectMeta{Name: "m1", Namespace: "roundtable", UID: "uid-1"},
	}
}

func buildNetpol(t *testing.T, natsURL string, s NetPolSettings) *networkingv1.NetworkPolicy {
	t.Helper()
	a := &KnightAssembler{}
	p, _ := a.buildMissionNetworkPolicy(netpolMission(), "mission-m1-isolation", natsURL, s)
	if len(p.Spec.Egress) != 3 {
		t.Fatalf("expected 3 egress rules, got %d", len(p.Spec.Egress))
	}
	return p
}

func TestMissionNetworkPolicyNATSFromURL(t *testing.T) {
	p := buildNetpol(t, "nats://nats.messaging.svc:4333", DefaultNetPolSettings())
	r := p.Spec.Egress[0]
	if len(r.To) != 1 || r.To[0].NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] != "messaging" {
		t.Fatalf("unexpected NATS peer: %+v", r.To)
	}
	if r.To[0].PodSelector != nil {
		t.Errorf("default settings must not set a NATS pod selector")
	}
	if r.Ports[0].Port.IntValue() != 4333 {
		t.Errorf("port = %v, want 4333", r.Ports[0].Port)
	}
}

func TestMissionNetworkPolicyNATSPodLabelsAndShortForm(t *testing.T) {
	s := DefaultNetPolSettings()
	s.NATSPodLabels = map[string]string{"app.kubernetes.io/name": "nats"}
	p := buildNetpol(t, "nats://nats.database.svc:4222", s)
	r := p.Spec.Egress[0]
	if r.To[0].NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] != "database" ||
		r.To[0].PodSelector.MatchLabels["app.kubernetes.io/name"] != "nats" ||
		r.Ports[0].Port.IntValue() != 4222 {
		t.Fatalf("not equivalent to previous hardcoded rule: %+v", r)
	}
}

func TestMissionNetworkPolicyNATSNonClusterHost(t *testing.T) {
	for _, u := range []string{"nats://nats.example.com:4223", "nats://10.0.0.5", "nats://nats:4222"} {
		p := buildNetpol(t, u, DefaultNetPolSettings())
		if len(p.Spec.Egress[0].To) != 0 {
			t.Errorf("%s: expected port-only rule, got peers %+v", u, p.Spec.Egress[0].To)
		}
	}
	a := &KnightAssembler{}
	_, inCluster := a.buildMissionNetworkPolicy(netpolMission(), "x", "nats://nats.example.com:4223", DefaultNetPolSettings())
	if inCluster {
		t.Error("expected inCluster=false")
	}
	if got := buildNetpol(t, "nats://10.0.0.5", DefaultNetPolSettings()).Spec.Egress[0].Ports[0].Port.IntValue(); got != 4222 {
		t.Errorf("default port = %d, want 4222", got)
	}
}

func TestMissionNetworkPolicyDNSFromEnv(t *testing.T) {
	t.Setenv(EnvNetPolDNSNamespace, "dns-system")
	t.Setenv(EnvNetPolDNSPodLabels, "app=coredns, tier=dns")
	t.Setenv(EnvNetPolNATSPodLabels, "a=b")
	s := NetPolSettingsFromEnv()
	p := buildNetpol(t, "nats://nats.database.svc:4222", s)
	dns := p.Spec.Egress[1].To[0]
	if dns.NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] != "dns-system" ||
		dns.PodSelector.MatchLabels["app"] != "coredns" || dns.PodSelector.MatchLabels["tier"] != "dns" {
		t.Fatalf("unexpected DNS peer: %+v", dns)
	}
	if p.Spec.Egress[0].To[0].PodSelector.MatchLabels["a"] != "b" {
		t.Errorf("NATS pod labels not applied")
	}
}

func TestNetPolSettingsDefaults(t *testing.T) {
	s := NetPolSettingsFromEnv()
	if !s.Enabled || s.DNSNamespace != "kube-system" || s.DNSPodLabels["k8s-app"] != "kube-dns" || s.NATSPodLabels != nil {
		t.Fatalf("unexpected defaults: %+v", s)
	}
}

func TestEnsureMissionNetworkPolicyDisabled(t *testing.T) {
	t.Setenv(EnvNetPolEnabled, "false")
	scheme := k8sruntime.NewScheme()
	_ = aiv1alpha1.AddToScheme(scheme)
	_ = networkingv1.AddToScheme(scheme)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	a := &KnightAssembler{Client: c}
	if err := a.EnsureMissionNetworkPolicy(context.Background(), netpolMission()); err != nil {
		t.Fatal(err)
	}
	list := &networkingv1.NetworkPolicyList{}
	if err := c.List(context.Background(), list); err != nil || len(list.Items) != 0 {
		t.Fatalf("expected no policy, got %d (err %v)", len(list.Items), err)
	}
}

func TestEnsureMissionNetworkPolicyUsesMissionNATSURL(t *testing.T) {
	scheme := k8sruntime.NewScheme()
	_ = aiv1alpha1.AddToScheme(scheme)
	_ = networkingv1.AddToScheme(scheme)
	m := netpolMission()
	m.Spec.RoundTableTemplate = &aiv1alpha1.MissionRoundTableTemplate{NATSURL: "nats://nats.messaging.svc:4333"}
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	a := &KnightAssembler{Client: c}
	if err := a.EnsureMissionNetworkPolicy(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	p := &networkingv1.NetworkPolicy{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "roundtable", Name: "mission-m1-isolation"}, p); err != nil {
		t.Fatal(err)
	}
	if p.Spec.Egress[0].To[0].NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] != "messaging" {
		t.Fatalf("unexpected: %+v", p.Spec.Egress[0])
	}
}

// ─── isolation defaults (#387) ──────────────────────────────────────────────

// An ephemeral mission knight is a Knight like any other: under the operator
// defaults it gets gvisor and a default-deny egress allowlist, which the
// Knight controller renders with the same baseline (DNS, NATS, Kubernetes
// API, base hosts, arsenal hosts).
func TestEphemeralKnightGetsIsolationDefaults(t *testing.T) {
	mission, mk, rt := ephemeralFixtures()
	a := &KnightAssembler{EgressDeny: true}
	k, err := a.buildEphemeralKnight(mission, mk, rt)
	if err != nil {
		t.Fatalf("buildEphemeralKnight: %v", err)
	}
	knightpkg.ApplyIsolationDefaults(k, knightpkg.DefaultIsolationSettings())
	if k.Spec.RuntimeClassName == nil || *k.Spec.RuntimeClassName != "gvisor" {
		t.Fatalf("runtimeClassName = %v, want gvisor", k.Spec.RuntimeClassName)
	}
	if k.Spec.Egress == nil || len(k.Spec.Egress.AllowedHosts) != 0 {
		t.Fatalf("egress = %+v, want an empty default-deny allowlist", k.Spec.Egress)
	}
	if k.Spec.NATS.URL != rt.Spec.NATS.URL {
		t.Fatalf("NATS URL = %q, want the table's (the policy's NATS peer)", k.Spec.NATS.URL)
	}
}

// A mission knight gets its template's allowedServices (#517).
func TestEphemeralKnightAllowedServicesFromTemplate(t *testing.T) {
	mission, mk, rt := ephemeralFixtures()
	tpl := rt.Spec.KnightTemplates["base"]
	want := []aiv1alpha1.EgressService{{Name: "searxng", Namespace: "selfhosted", Ports: []int32{8080}}}
	tpl.Egress = &aiv1alpha1.KnightEgress{AllowedServices: want}
	rt.Spec.KnightTemplates["base"] = tpl
	k, err := (&KnightAssembler{EgressDeny: true}).buildEphemeralKnight(mission, mk, rt)
	if err != nil {
		t.Fatalf("buildEphemeralKnight: %v", err)
	}
	knightpkg.ApplyIsolationDefaults(k, knightpkg.DefaultIsolationSettings())
	if k.Spec.Egress == nil || !reflect.DeepEqual(k.Spec.Egress.AllowedServices, want) {
		t.Fatalf("egress = %+v, want allowedServices %+v", k.Spec.Egress, want)
	}
	k.Spec.Egress.AllowedServices[0].Ports[0] = 1
	if rt.Spec.KnightTemplates["base"].Egress.AllowedServices[0].Ports[0] != 8080 {
		t.Fatal("the mission knight shares the template's allowedServices")
	}
}

// Only the template (or inline spec) can opt a mission knight out; spec
// overrides cannot.
func TestEphemeralKnightOptOutOnlyFromTemplate(t *testing.T) {
	mission, mk, rt := ephemeralFixtures()
	tpl := rt.Spec.KnightTemplates["base"]
	tpl.Isolation = &aiv1alpha1.KnightIsolation{Egress: aiv1alpha1.IsolationUnrestricted, Reason: "crawls arbitrary sites"}
	rt.Spec.KnightTemplates["base"] = tpl
	k, err := (&KnightAssembler{}).buildEphemeralKnight(mission, mk, rt)
	if err != nil {
		t.Fatalf("buildEphemeralKnight: %v", err)
	}
	knightpkg.ApplyIsolationDefaults(k, knightpkg.DefaultIsolationSettings())
	if k.Spec.Egress != nil {
		t.Fatalf("egress = %+v, want nil for a template that opts out", k.Spec.Egress)
	}
}

func TestMissionNetworkPolicySkippedUnderDefaultDeny(t *testing.T) {
	s := k8sruntime.NewScheme()
	_ = aiv1alpha1.AddToScheme(s)
	_ = networkingv1.AddToScheme(s)
	for _, deny := range []bool{true, false} {
		c := fake.NewClientBuilder().WithScheme(s).Build()
		a := &KnightAssembler{Client: c, Scheme: s, EgressDeny: deny}
		if err := a.EnsureMissionNetworkPolicy(context.Background(), netpolMission()); err != nil {
			t.Fatalf("EnsureMissionNetworkPolicy(deny=%v): %v", deny, err)
		}
		list := &networkingv1.NetworkPolicyList{}
		if err := c.List(context.Background(), list); err != nil {
			t.Fatal(err)
		}
		if want := map[bool]int{true: 0, false: 1}[deny]; len(list.Items) != want {
			t.Fatalf("deny=%v: %d mission NetworkPolicies, want %d", deny, len(list.Items), want)
		}
	}
}
