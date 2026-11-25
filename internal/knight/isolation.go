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

package knight

import (
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// Operator env settings for the isolation defaults (set by the chart's
// isolation values).
const (
	// EnvDefaultRuntimeClass is the RuntimeClass every knight runs under
	// unless the knight or its table names one. Unset means "gvisor"; set to
	// "" to turn the default off for the whole install.
	EnvDefaultRuntimeClass = "KNIGHT_DEFAULT_RUNTIME_CLASS"
	// EnvDefaultEgress is the default egress mode: "deny" (a knight without
	// spec.egress, and without a table default, gets an empty allowlist on top
	// of the baseline) or "off" (no policy unless spec.egress is set).
	EnvDefaultEgress = "KNIGHT_DEFAULT_EGRESS"
	// EnvEgressBaseHosts is a comma-separated list of hosts every knight's
	// egress policy allows on port 443 (the model providers).
	EnvEgressBaseHosts = "KNIGHT_EGRESS_BASE_HOSTS"

	// DefaultRuntimeClass is the RuntimeClass used when EnvDefaultRuntimeClass is unset.
	DefaultRuntimeClass = "gvisor"
	// EgressModeDeny renders a default-deny egress policy for every knight.
	EgressModeDeny = "deny"
	// EgressModeOff renders an egress policy only for knights with spec.egress.
	EgressModeOff = "off"
)

// DefaultEgressBaseHosts are the base hosts used when EnvEgressBaseHosts is unset.
var DefaultEgressBaseHosts = []string{"openrouter.ai", "api.anthropic.com"}

// IsolationSettings are the operator's isolation defaults. The zero value
// turns them off (no default RuntimeClass, no default egress policy, no base
// hosts); the operator reads them with IsolationSettingsFromEnv.
type IsolationSettings struct {
	// RuntimeClassName is the default RuntimeClass; "" applies none.
	RuntimeClassName string
	// EgressDeny gives a knight without spec.egress (or a table default) an
	// empty allowlist, so it gets the default-deny baseline policy.
	EgressDeny bool
	// BaseHosts are allowed on port 443 in every knight's egress policy.
	BaseHosts []string
}

// DefaultIsolationSettings returns the settings used when no env is set:
// gvisor, default-deny egress and the default base hosts.
func DefaultIsolationSettings() IsolationSettings {
	return IsolationSettings{
		RuntimeClassName: DefaultRuntimeClass,
		EgressDeny:       true,
		BaseHosts:        slices.Clone(DefaultEgressBaseHosts),
	}
}

// IsolationSettingsFromEnv reads the isolation env vars over the defaults. A
// variable that is set but empty counts: KNIGHT_DEFAULT_RUNTIME_CLASS="" turns
// the default RuntimeClass off, KNIGHT_EGRESS_BASE_HOSTS="" leaves no base hosts.
func IsolationSettingsFromEnv() IsolationSettings {
	s := DefaultIsolationSettings()
	if v, ok := os.LookupEnv(EnvDefaultRuntimeClass); ok {
		s.RuntimeClassName = strings.TrimSpace(v)
	}
	if v, ok := os.LookupEnv(EnvDefaultEgress); ok {
		s.EgressDeny = !strings.EqualFold(strings.TrimSpace(v), EgressModeOff)
	}
	if v, ok := os.LookupEnv(EnvEgressBaseHosts); ok {
		s.BaseHosts = nil
		for h := range strings.SplitSeq(v, ",") {
			if h = strings.ToLower(strings.TrimSpace(h)); h != "" {
				s.BaseHosts = append(s.BaseHosts, h)
			}
		}
	}
	return s
}

// ApplyIsolationDefaults fills the knight's RuntimeClass and egress allowlist
// in memory, after ApplyTableDefaults: a knight or table value wins, then the
// operator default. isolation.runtime: none clears the RuntimeClass and
// isolation.egress: unrestricted clears spec.egress (no policy). Never persisted.
func ApplyIsolationDefaults(k *aiv1alpha1.Knight, s IsolationSettings) {
	switch {
	case k.Spec.Isolation.RuntimeOptedOut():
		k.Spec.RuntimeClassName = nil
	case k.Spec.RuntimeClassName == nil && s.RuntimeClassName != "":
		k.Spec.RuntimeClassName = new(string)
		*k.Spec.RuntimeClassName = s.RuntimeClassName
	}
	switch {
	case k.Spec.Isolation.EgressOptedOut():
		k.Spec.Egress = nil
	case k.Spec.Egress == nil && s.EgressDeny:
		k.Spec.Egress = &aiv1alpha1.KnightEgress{}
	}
}

// GitHost is a host a knight's git-sync sidecar reaches, with its port.
type GitHost struct {
	Host string
	Port int32
}

// ArsenalGitHosts returns the hosts (and ports) of the knight's arsenal repos
// (spec.arsenals, or the default platform arsenal when empty; an arsenal without repo uses the default
// arsenal repo), sorted and deduplicated. The git-sync sidecars share the
// pod's network, so the egress policy must allow them.
func ArsenalGitHosts(k *aiv1alpha1.Knight) []GitHost {
	var repos []string
	// OCI arsenals are image volumes pulled by the kubelet: no git egress.
	for _, a := range effectiveArsenals(k.Spec.Arsenals) {
		if a.OCI == nil {
			repos = append(repos, a.Repo)
		}
	}
	var out []GitHost
	for _, r := range repos {
		if strings.TrimSpace(r) == "" {
			r = defaultArsenalRepo
		}
		if h, ok := gitRepoHost(r); ok {
			out = append(out, h)
		}
	}
	slices.SortFunc(out, func(a, b GitHost) int {
		if c := strings.Compare(a.Host, b.Host); c != 0 {
			return c
		}
		return int(a.Port - b.Port)
	})
	return slices.Compact(out)
}

// gitSchemePorts are the default ports of the git URL schemes git-sync uses.
var gitSchemePorts = map[string]int32{"https": 443, "http": 80, "ssh": 22, "git+ssh": 22, "git": 9418}

// gitRepoHost parses a git repo URL (https://, http://, ssh://, git:// or the
// scp-like user@host:path) into its host and port.
func gitRepoHost(repo string) (GitHost, bool) {
	repo = strings.TrimSpace(repo)
	if !strings.Contains(repo, "://") {
		// scp-like: [user@]host:path
		hostPart, _, ok := strings.Cut(repo, ":")
		if !ok {
			return GitHost{}, false
		}
		if i := strings.LastIndex(hostPart, "@"); i >= 0 {
			hostPart = hostPart[i+1:]
		}
		if hostPart == "" || strings.Contains(hostPart, "/") {
			return GitHost{}, false
		}
		return GitHost{Host: strings.ToLower(hostPart), Port: 22}, true
	}
	u, err := url.Parse(repo)
	if err != nil || u.Hostname() == "" {
		return GitHost{}, false
	}
	port, ok := gitSchemePorts[strings.ToLower(u.Scheme)]
	if !ok {
		return GitHost{}, false
	}
	if p := u.Port(); p != "" {
		n, err := strconv.Atoi(p)
		if err != nil || n <= 0 || n > 65535 {
			return GitHost{}, false
		}
		port = int32(n) //nolint:gosec // range checked above
	}
	return GitHost{Host: strings.ToLower(u.Hostname()), Port: port}, true
}
