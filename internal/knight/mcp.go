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
)

// MCPHeaderVar is one spec.mcpServers[].headersFrom entry rendered as an env
// var on the knight container.
type MCPHeaderVar struct {
	Server  string
	Header  string
	EnvName string
	Ref     aiv1alpha1.KnightSecretKeyRef
}

// MCPHeaderEnvName is the base env var name for a header:
// MCP_<SERVER>_<HEADER>, upper-cased, every character other than A-Z and 0-9
// replaced by "_". Collisions are resolved by renderMCP.
func MCPHeaderEnvName(server, header string) string {
	return "MCP_" + sanitizeEnvPart(server) + "_" + sanitizeEnvPart(header)
}

func sanitizeEnvPart(s string) string {
	b := []byte(strings.ToUpper(s))
	for i, c := range b {
		if (c < 'A' || c > 'Z') && (c < '0' || c > '9') {
			b[i] = '_'
		}
	}
	return string(b)
}

// UniqueMCPHeaders returns headers without entries whose name repeats an
// earlier one case-insensitively (HTTP header names are case-insensitive;
// the CRD can only reject exact duplicates).
func UniqueMCPHeaders(headers []aiv1alpha1.KnightMCPHeader) []aiv1alpha1.KnightMCPHeader {
	seen := make(map[string]bool, len(headers))
	out := make([]aiv1alpha1.KnightMCPHeader, 0, len(headers))
	for _, h := range headers {
		if k := strings.ToLower(h.Name); !seen[k] {
			seen[k] = true
			out = append(out, h)
		}
	}
	return out
}

// renderMCP renders the MCP config JSON and the header env vars. taken holds
// env var names already in use on the container; a header whose base name is taken (or was assigned to an earlier header, e.g.
// server "a-b"/header "c" and server "a"/header "b-c") gets a numeric
// suffix (_2, _3, ...), assigned in spec order. The JSON only ever contains
// "${NAME}" placeholders, never a header value.
func renderMCP(servers []aiv1alpha1.KnightMCPServer, taken map[string]bool) (string, []MCPHeaderVar) {
	if len(servers) == 0 {
		return "", nil
	}
	used := make(map[string]bool, len(taken))
	for n := range taken {
		used[n] = true
	}
	m := make(map[string]map[string]any, len(servers))
	var vars []MCPHeaderVar
	for _, s := range servers {
		typ := "http"
		if s.Transport == mcpTransportSSE {
			typ = mcpTransportSSE
		}
		entry := map[string]any{"type": typ, "url": s.URL}
		if len(s.HeadersFrom) > 0 {
			headers := make(map[string]string, len(s.HeadersFrom))
			for _, h := range UniqueMCPHeaders(s.HeadersFrom) {
				name := MCPHeaderEnvName(s.Name, h.Name)
				for i := 2; used[name]; i++ {
					name = fmt.Sprintf("%s_%d", MCPHeaderEnvName(s.Name, h.Name), i)
				}
				used[name] = true
				headers[h.Name] = "${" + name + "}"
				vars = append(vars, MCPHeaderVar{Server: s.Name, Header: h.Name, EnvName: name, Ref: h.ValueFrom.SecretKeyRef})
			}
			entry["headers"] = headers
		}
		m[s.Name] = entry
	}
	b, _ := json.Marshal(map[string]any{"mcpServers": m}) // maps of strings: cannot fail
	return string(b), vars
}

// applyMCPServersEnv adds the rendered MCP env to env for claude-knight
// images; other runtimes get nothing. Each headersFrom entry becomes an env
// var whose valueFrom is a secretKeyRef copied from the spec. If the user
// already set a variable via env, the user's entries are kept and the
// operator's are merged in (the user's servers win on a name clash). A user
// MCP_SERVERS_JSON set via valueFrom cannot be merged and is left as the user
// set it. CLAUDE_DISALLOWED_TOOLS is the user's own variable and is never
// touched.
func applyMCPServersEnv(env []corev1.EnvVar, k *aiv1alpha1.Knight, image string) []corev1.EnvVar {
	if AgentRuntimeForImage(image) != AgentRuntimeClaude {
		return env
	}
	taken := make(map[string]bool, len(env))
	for _, e := range env {
		taken[e.Name] = true
	}
	servers, vars := renderMCP(k.Spec.MCPServers, taken)
	if servers == "" {
		return env
	}
	for _, v := range vars {
		env = append(env, corev1.EnvVar{Name: v.EnvName, ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: v.Ref.Name},
				Key:                  v.Ref.Key,
			},
		}})
	}
	return mergeEnvValue(env, EnvMCPServersJSON, servers, mergeMCPJSON)
}

func mergeEnvValue(env []corev1.EnvVar, name, value string, merge func(user, op string) string) []corev1.EnvVar {
	for i := range env {
		if env[i].Name != name {
			continue
		}
		if env[i].ValueFrom != nil {
			// The user's value comes from a Secret/ConfigMap/field and cannot
			// be merged at build time. Never append a duplicate (the last one
			// wins and would drop the user's value).
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
