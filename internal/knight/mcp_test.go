package knight

import (
	"context"
	"encoding/json"
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
				{Name: "vault", URL: "http://vault/mcp", ToolFilter: &aiv1alpha1.KnightMCPToolFilter{Exclude: []string{"delete", "write"}}},
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
	if v := env["CLAUDE_DISALLOWED_TOOLS"]; v != "mcp__vault__delete,mcp__vault__write" {
		t.Errorf("CLAUDE_DISALLOWED_TOOLS = %q", v)
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
	if v := env["CLAUDE_DISALLOWED_TOOLS"]; v != "Bash,mcp__vault__delete,mcp__vault__write" {
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
