package github

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func ref(name, key string) *corev1.SecretKeySelector {
	return &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: name}, Key: key}
}

func TestResolveTokenRef(t *testing.T) {
	tests := []struct {
		name     string
		sources  []*corev1.SecretKeySelector
		wantName string
		wantKey  string
	}{
		{"none", nil, "", ""},
		{"all nil", []*corev1.SecretKeySelector{nil, nil}, "", ""},
		{"empty name skipped", []*corev1.SecretKeySelector{ref("", "k"), ref("table", "")}, "table", "token"},
		{"first wins (prFeedback over table)", []*corev1.SecretKeySelector{ref("pr", "pk"), ref("table", "tk")}, "pr", "pk"},
		{"knight over table", []*corev1.SecretKeySelector{ref("knight", ""), ref("table", "tk")}, "knight", "token"},
		{"falls back to table", []*corev1.SecretKeySelector{nil, ref("table", "tk")}, "table", "tk"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ResolveTokenRef(tc.sources...)
			if tc.wantName == "" {
				if got != nil {
					t.Fatalf("got %+v, want nil", got)
				}
				return
			}
			if got == nil || got.Name != tc.wantName || got.Key != tc.wantKey {
				t.Fatalf("got %+v, want %s/%s", got, tc.wantName, tc.wantKey)
			}
		})
	}
	in := ref("x", "")
	ResolveTokenRef(in)
	if in.Key != "" {
		t.Fatal("input mutated")
	}
}

func TestReadToken(t *testing.T) {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "ns"},
		Data:       map[string][]byte{"token": []byte("  abc\n"), "blank": []byte(" \n")},
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(sec).Build()
	ctx := context.Background()
	tests := []struct {
		name    string
		ns      string
		ref     *corev1.SecretKeySelector
		want    string
		wantErr bool
	}{
		{"trimmed, default key", "ns", ref("s", ""), "abc", false},
		{"explicit key", "ns", ref("s", "token"), "abc", false},
		{"whitespace-only token", "ns", ref("s", "blank"), "", true},
		{"missing key", "ns", ref("s", "nope"), "", true},
		{"missing secret", "ns", ref("gone", ""), "", true},
		{"wrong namespace", "other", ref("s", ""), "", true},
		{"nil ref", "ns", nil, "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ReadToken(ctx, c, tc.ns, tc.ref)
			if (err != nil) != tc.wantErr || got != tc.want {
				t.Fatalf("got %q, %v", got, err)
			}
		})
	}
	// rotation: a later read sees the new value.
	sec.Data["token"] = []byte("rotated")
	if err := c.Update(ctx, sec); err != nil {
		t.Fatal(err)
	}
	if got, _ := ReadToken(ctx, c, "ns", ref("s", "")); got != "rotated" {
		t.Fatalf("got %q after rotation", got)
	}
}

func TestInheritGitHub(t *testing.T) {
	rt := &aiv1alpha1.RoundTable{}
	rt.Spec.GitHub = &aiv1alpha1.RoundTableGitHubConfig{TokenSecretRef: ref("table", "")}

	spec := &aiv1alpha1.KnightSpec{}
	InheritGitHub(spec, rt)
	if spec.GitHub == nil || spec.GitHub.TokenSecretRef.Name != "table" {
		t.Fatalf("not inherited: %+v", spec.GitHub)
	}
	spec.GitHub.TokenSecretRef.Name = "mutated"
	if rt.Spec.GitHub.TokenSecretRef.Name != "table" {
		t.Fatal("table mutated")
	}

	own := &aiv1alpha1.KnightSpec{GitHub: &aiv1alpha1.GitHubConfig{TokenSecretRef: ref("own", "")}}
	InheritGitHub(own, rt)
	if own.GitHub.TokenSecretRef.Name != "own" {
		t.Fatal("own overridden")
	}
	none := &aiv1alpha1.KnightSpec{}
	InheritGitHub(none, &aiv1alpha1.RoundTable{})
	if none.GitHub != nil {
		t.Fatal("unexpected inherit")
	}
}
