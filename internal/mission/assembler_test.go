package mission

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
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
		Spec:       aiv1alpha1.MissionSpec{RoundTableRef: "personal"},
	}
	mk := aiv1alpha1.MissionKnight{
		Name:        "haiku-writer",
		Ephemeral:   true,
		TemplateRef: "base",
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
		TokenSecretRef: &aiv1alpha1.GitHubTokenSecretRef{Name: testGitHubTokenSecret, Key: "token"},
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
		TokenSecretRef: &aiv1alpha1.GitHubTokenSecretRef{Name: "table-token"},
	}
	tmpl := rt.Spec.KnightTemplates["base"]
	tmpl.GitHub = &aiv1alpha1.GitHubConfig{
		TokenSecretRef: &aiv1alpha1.GitHubTokenSecretRef{Name: "template-token"},
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
		TokenSecretRef: &aiv1alpha1.GitHubTokenSecretRef{Name: testGitHubTokenSecret, Key: "token"},
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
