package knight

import (
	"encoding/json"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

const mcpTransportSSE = "sse"

const (
	// EnvMCPServersJSON is the claude-knight env var holding MCP servers.
	EnvMCPServersJSON = "MCP_SERVERS_JSON"
	// EnvClaudeDisallowedTools is the claude-knight comma-separated deny list.
	EnvClaudeDisallowedTools = "CLAUDE_DISALLOWED_TOOLS"
)

// MCPServersEnv renders spec.mcpServers into the MCP_SERVERS_JSON value and
// the CLAUDE_DISALLOWED_TOOLS entries. It returns empty values when no
// servers are declared.
func MCPServersEnv(servers []aiv1alpha1.KnightMCPServer) (string, []string) {
	if len(servers) == 0 {
		return "", nil
	}
	m := make(map[string]map[string]string, len(servers))
	var denied []string
	for _, s := range servers {
		typ := "http"
		if s.Transport == mcpTransportSSE {
			typ = mcpTransportSSE
		}
		m[s.Name] = map[string]string{"type": typ, "url": s.URL}
		if s.ToolFilter != nil {
			for _, t := range s.ToolFilter.Exclude {
				denied = append(denied, fmt.Sprintf("mcp__%s__%s", s.Name, t))
			}
		}
	}
	b, _ := json.Marshal(map[string]any{"mcpServers": m}) // map of strings: cannot fail
	return string(b), denied
}

// applyMCPServersEnv adds the rendered MCP env to env for claude-knight
// images; other runtimes get nothing. If the user already set a variable via
// env, the user's entries are kept and the operator's are merged in (for
// MCP_SERVERS_JSON the user's servers win on a name clash; for the deny list
// the operator's entries are appended). A user value set via valueFrom cannot
// be merged: MCP_SERVERS_JSON is left as the user set it, and the deny list
// gets the operator's entries appended through $(NAME) expansion.
func applyMCPServersEnv(env []corev1.EnvVar, k *aiv1alpha1.Knight, image string) []corev1.EnvVar {
	if AgentRuntimeForImage(image) != AgentRuntimeClaude {
		return env
	}
	servers, denied := MCPServersEnv(k.Spec.MCPServers)
	if servers == "" {
		return env
	}
	env = mergeEnvValue(env, EnvMCPServersJSON, servers, false, mergeMCPJSON)
	if len(denied) > 0 {
		env = mergeEnvValue(env, EnvClaudeDisallowedTools, strings.Join(denied, ","), true, mergeCSV)
	}
	return env
}

func mergeEnvValue(env []corev1.EnvVar, name, value string, refMerge bool, merge func(user, op string) string) []corev1.EnvVar {
	for i := range env {
		if env[i].Name != name {
			continue
		}
		if env[i].ValueFrom != nil {
			// The user's value comes from a Secret/ConfigMap/field and cannot
			// be merged at build time. Never append a duplicate (the last one
			// wins and would drop the user's value).
			if refMerge {
				// Kubernetes expands $(NAME) against earlier entries, so the
				// operator's entries are appended to the user's value.
				env = append(env, corev1.EnvVar{Name: name, Value: "$(" + name + ")," + value})
			}
			return env
		}
		if strings.TrimSpace(env[i].Value) == "" {
			env[i].Value = value
		} else {
			env[i].Value = merge(env[i].Value, value)
		}
		return env
	}
	return append(env, corev1.EnvVar{Name: name, Value: value})
}

func mergeCSV(user, op string) string {
	if strings.TrimSpace(user) == "" {
		return op
	}
	seen := map[string]bool{}
	var out []string
	for t := range strings.SplitSeq(user, ",") {
		if t = strings.TrimSpace(t); t != "" && !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	for t := range strings.SplitSeq(op, ",") {
		if !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	return strings.Join(out, ",")
}

// mergeMCPJSON merges the operator's servers into the user's MCP_SERVERS_JSON
// (a {"mcpServers":{...}} object or a bare map). User entries win on a name
// clash. An unparsable user value is kept unchanged.
func mergeMCPJSON(user, op string) string {
	var opObj map[string]map[string]json.RawMessage
	if json.Unmarshal([]byte(op), &opObj) != nil {
		return user
	}
	var userMap map[string]json.RawMessage
	if json.Unmarshal([]byte(user), &userMap) != nil {
		return user
	}
	servers := userMap
	if raw, ok := userMap["mcpServers"]; ok {
		servers = nil
		if json.Unmarshal(raw, &servers) != nil {
			return user
		}
	}
	if servers == nil { // JSON null
		servers = map[string]json.RawMessage{}
	}
	for name, v := range opObj["mcpServers"] {
		if _, ok := servers[name]; !ok {
			servers[name] = v
		}
	}
	b, err := json.Marshal(map[string]any{"mcpServers": servers})
	if err != nil {
		return user
	}
	return string(b)
}
