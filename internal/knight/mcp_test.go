package knight

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

const (
	claudeImg = "ghcr.io/roundtable-k8s/claude-knight:latest"
	piImg     = "ghcr.io/roundtable-k8s/pi-knight:latest"
)

func mcpKnight(env ...corev1.EnvVar) *aiv1alpha1.Knight {
	k := &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{Name: "k", Namespace: "default"},
		Spec: aiv1alpha1.KnightSpec{
			Domain: "d", Env: env,
			MCPServers: []aiv1alpha1.KnightMCPServer{
				{Name: "vault", URL: "http://vault/mcp"},
				{Name: "feeds", URL: "http://feeds/sse", Transport: "sse"},
			},
		},
	}
	return k
}

func envOf(t *testing.T, k *aiv1alpha1.Knight, image string) map[string]string {
	t.Helper()
	spec := NewPodBuilder(k, image).Build(context.Background())
	out := map[string]string{}
	for _, c := range spec.Containers {
		if c.Name != "app" {
			continue
		}
		for _, e := range c.Env {
			out[e.Name] = e.Value
		}
	}
	return out
}

func TestMCPServersRenderedForClaude(t *testing.T) {
	env := envOf(t, mcpKnight(), claudeImg)
	var got, want map[string]any
	if err := json.Unmarshal([]byte(env["MCP_SERVERS_JSON"]), &got); err != nil {
		t.Fatalf("MCP_SERVERS_JSON: %v (%q)", err, env["MCP_SERVERS_JSON"])
	}
	_ = json.Unmarshal([]byte(`{"mcpServers":{"vault":{"type":"http","url":"http://vault/mcp"},"feeds":{"type":"sse","url":"http://feeds/sse"}}}`), &want)
	gb, _ := json.Marshal(got)
	wb, _ := json.Marshal(want)
	if string(gb) != string(wb) {
		t.Errorf("MCP_SERVERS_JSON = %s, want %s", gb, wb)
	}
	if _, ok := env["CLAUDE_DISALLOWED_TOOLS"]; ok {
		t.Errorf("CLAUDE_DISALLOWED_TOOLS rendered without a user value")
	}
}

func TestMCPServersMergeWithUserEnv(t *testing.T) {
	k := mcpKnight(
		corev1.EnvVar{Name: "MCP_SERVERS_JSON", Value: `{"mcpServers":{"mine":{"type":"http","url":"http://mine"},"vault":{"type":"http","url":"http://override"}}}`},
		corev1.EnvVar{Name: "CLAUDE_DISALLOWED_TOOLS", Value: "Bash,mcp__vault__delete"},
	)
	env := envOf(t, k, claudeImg)
	var got map[string]map[string]map[string]string
	if err := json.Unmarshal([]byte(env["MCP_SERVERS_JSON"]), &got); err != nil {
		t.Fatal(err)
	}
	s := got["mcpServers"]
	if len(s) != 3 || s["mine"]["url"] != "http://mine" || s["vault"]["url"] != "http://override" || s["feeds"]["type"] != "sse" {
		t.Errorf("merged servers = %v", s)
	}
	if v := env["CLAUDE_DISALLOWED_TOOLS"]; v != "Bash,mcp__vault__delete" {
		t.Errorf("CLAUDE_DISALLOWED_TOOLS = %q", v)
	}
}

func TestMCPServersMergeBareMap(t *testing.T) {
	k := mcpKnight(corev1.EnvVar{Name: "MCP_SERVERS_JSON", Value: `{"mine":{"type":"http","url":"http://mine"}}`})
	var got map[string]map[string]map[string]string
	if err := json.Unmarshal([]byte(envOf(t, k, claudeImg)["MCP_SERVERS_JSON"]), &got); err != nil {
		t.Fatal(err)
	}
	if len(got["mcpServers"]) != 3 {
		t.Errorf("got %v", got)
	}
}

func TestMCPServersNotRenderedForPi(t *testing.T) {
	env := envOf(t, mcpKnight(), piImg)
	for _, n := range []string{"MCP_SERVERS_JSON", "CLAUDE_DISALLOWED_TOOLS"} {
		if _, ok := env[n]; ok {
			t.Errorf("%s rendered for pi-knight", n)
		}
	}
}

func TestMCPServersNullUserValueDoesNotPanic(t *testing.T) {
	for _, v := range []string{`null`, `{"mcpServers":null}`, ``} {
		k := mcpKnight(corev1.EnvVar{Name: "MCP_SERVERS_JSON", Value: v})
		var got map[string]map[string]any
		if err := json.Unmarshal([]byte(envOf(t, k, claudeImg)["MCP_SERVERS_JSON"]), &got); err != nil {
			t.Fatalf("value %q: %v", v, err)
		}
		if got["mcpServers"]["vault"] == nil {
			t.Errorf("value %q: operator servers missing: %v", v, got)
		}
	}
}

func TestMCPServersValueFromNotDuplicated(t *testing.T) {
	ref := &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{Key: "k"}}
	k := mcpKnight(
		corev1.EnvVar{Name: "MCP_SERVERS_JSON", ValueFrom: ref},
		corev1.EnvVar{Name: "CLAUDE_DISALLOWED_TOOLS", ValueFrom: ref},
	)
	env := applyMCPServersEnv(append([]corev1.EnvVar{}, k.Spec.Env...), k, claudeImg)
	var js, dt []corev1.EnvVar
	for _, e := range env {
		switch e.Name {
		case "MCP_SERVERS_JSON":
			js = append(js, e)
		case "CLAUDE_DISALLOWED_TOOLS":
			dt = append(dt, e)
		}
	}
	if len(js) != 1 || js[0].ValueFrom == nil {
		t.Errorf("MCP_SERVERS_JSON must stay the user's single valueFrom entry: %+v", js)
	}
	if len(dt) != 1 || dt[0].ValueFrom == nil {
		t.Errorf("deny list must stay the user's single valueFrom entry: %+v", dt)
	}
}

func headerKnight(env ...corev1.EnvVar) *aiv1alpha1.Knight {
	k := mcpKnight(env...)
	k.Spec.MCPServers[0].HeadersFrom = []aiv1alpha1.KnightMCPHeader{{
		Name:      "Authorization",
		ValueFrom: aiv1alpha1.KnightMCPHeaderSource{SecretKeyRef: aiv1alpha1.KnightSecretKeyRef{Name: "vault-mcp", Key: "token"}},
	}}
	k.Spec.MCPServers[1].HeadersFrom = []aiv1alpha1.KnightMCPHeader{{
		Name:      "X-Api-Key",
		ValueFrom: aiv1alpha1.KnightMCPHeaderSource{SecretKeyRef: aiv1alpha1.KnightSecretKeyRef{Name: "feeds-mcp", Key: "api-key"}},
	}}
	return k
}

func appEnv(t *testing.T, k *aiv1alpha1.Knight, image string) []corev1.EnvVar {
	t.Helper()
	for _, c := range NewPodBuilder(k, image).Build(context.Background()).Containers {
		if c.Name == "app" {
			return c.Env
		}
	}
	t.Fatal("no app container")
	return nil
}

func findEnv(env []corev1.EnvVar, name string) []corev1.EnvVar {
	var out []corev1.EnvVar
	for _, e := range env {
		if e.Name == name {
			out = append(out, e)
		}
	}
	return out
}

func TestMCPHeadersRenderedAsSecretEnvAndPlaceholders(t *testing.T) {
	env := appEnv(t, headerKnight(), claudeImg)
	for name, want := range map[string][2]string{
		"MCP_VAULT_AUTHORIZATION": {"vault-mcp", "token"},
		"MCP_FEEDS_X_API_KEY":     {"feeds-mcp", "api-key"},
	} {
		got := findEnv(env, name)
		if len(got) != 1 {
			t.Fatalf("%s: got %d entries: %+v", name, len(got), got)
		}
		e := got[0]
		if e.Value != "" || e.ValueFrom == nil || e.ValueFrom.SecretKeyRef == nil ||
			e.ValueFrom.SecretKeyRef.Name != want[0] || e.ValueFrom.SecretKeyRef.Key != want[1] {
			t.Errorf("%s = %+v, want secretKeyRef %s/%s", name, e, want[0], want[1])
		}
	}
	js := findEnv(env, EnvMCPServersJSON)
	if len(js) != 1 {
		t.Fatalf("MCP_SERVERS_JSON entries: %+v", js)
	}
	var got map[string]map[string]struct {
		Headers map[string]string `json:"headers"`
	}
	if err := json.Unmarshal([]byte(js[0].Value), &got); err != nil {
		t.Fatal(err)
	}
	if h := got["mcpServers"]["vault"].Headers; len(h) != 1 || h["Authorization"] != "${MCP_VAULT_AUTHORIZATION}" {
		t.Errorf("vault headers = %v", h)
	}
	if h := got["mcpServers"]["feeds"].Headers; len(h) != 1 || h["X-Api-Key"] != "${MCP_FEEDS_X_API_KEY}" {
		t.Errorf("feeds headers = %v", h)
	}
}

func TestMCPHeaderEnvNameCollisions(t *testing.T) {
	ref := aiv1alpha1.KnightMCPHeaderSource{SecretKeyRef: aiv1alpha1.KnightSecretKeyRef{Name: "s", Key: "k"}}
	k := mcpKnight(corev1.EnvVar{Name: "MCP_A_B_C", Value: "user"})
	k.Spec.MCPServers = []aiv1alpha1.KnightMCPServer{
		{Name: "a-b", URL: "http://ab", HeadersFrom: []aiv1alpha1.KnightMCPHeader{{Name: "c", ValueFrom: ref}}},
		{Name: "a", URL: "http://a", HeadersFrom: []aiv1alpha1.KnightMCPHeader{{Name: "b-c", ValueFrom: ref}, {Name: "b.c", ValueFrom: ref}}},
	}
	env := appEnv(t, k, claudeImg)
	if u := findEnv(env, "MCP_A_B_C"); len(u) != 1 || u[0].Value != "user" {
		t.Errorf("user's MCP_A_B_C must be untouched: %+v", u)
	}
	for _, n := range []string{"MCP_A_B_C_2", "MCP_A_B_C_3", "MCP_A_B_C_4"} {
		if e := findEnv(env, n); len(e) != 1 || e[0].ValueFrom == nil {
			t.Errorf("%s: %+v", n, e)
		}
	}
	var got map[string]map[string]struct {
		Headers map[string]string `json:"headers"`
	}
	if err := json.Unmarshal([]byte(findEnv(env, EnvMCPServersJSON)[0].Value), &got); err != nil {
		t.Fatal(err)
	}
	s := got["mcpServers"]
	if s["a-b"].Headers["c"] != "${MCP_A_B_C_2}" || s["a"].Headers["b-c"] != "${MCP_A_B_C_3}" || s["a"].Headers["b.c"] != "${MCP_A_B_C_4}" {
		t.Errorf("headers = %+v", s)
	}
}

func TestMCPHeadersNotRenderedForPi(t *testing.T) {
	for _, e := range appEnv(t, headerKnight(), piImg) {
		if strings.HasPrefix(e.Name, "MCP_") {
			t.Errorf("%s rendered for pi-knight", e.Name)
		}
	}
}

func TestMCPHeaderEnvName(t *testing.T) {
	for in, want := range map[[2]string]string{
		{"vault", "Authorization"}: "MCP_VAULT_AUTHORIZATION",
		{"web-fetch", "X-Api-Key"}: "MCP_WEB_FETCH_X_API_KEY",
		{"m1", "x.trace~id"}:       "MCP_M1_X_TRACE_ID",
	} {
		if got := MCPHeaderEnvName(in[0], in[1]); got != want {
			t.Errorf("MCPHeaderEnvName(%q, %q) = %q, want %q", in[0], in[1], got, want)
		}
	}
}

func TestMCPHeadersCaseDuplicateDropped(t *testing.T) {
	k := headerKnight()
	k.Spec.MCPServers[0].HeadersFrom = append(k.Spec.MCPServers[0].HeadersFrom, aiv1alpha1.KnightMCPHeader{
		Name:      "authorization",
		ValueFrom: aiv1alpha1.KnightMCPHeaderSource{SecretKeyRef: aiv1alpha1.KnightSecretKeyRef{Name: "other", Key: "k"}},
	})
	env := appEnv(t, k, claudeImg)
	for _, e := range env {
		if strings.HasPrefix(e.Name, "MCP_VAULT_AUTHORIZATION_") {
			t.Errorf("case duplicate rendered: %+v", e)
		}
	}
	if e := findEnv(env, "MCP_VAULT_AUTHORIZATION"); len(e) != 1 || e[0].ValueFrom.SecretKeyRef.Name != "vault-mcp" {
		t.Errorf("first header must win: %+v", e)
	}
}
