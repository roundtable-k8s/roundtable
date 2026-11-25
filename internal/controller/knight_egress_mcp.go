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

package controller

import (
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// mcpDestination is where one spec.mcpServers[].url points.
type mcpDestination struct {
	// Service is set for http(s)://<svc>.<ns>.svc[.cluster.local] URLs.
	Service   string
	Namespace string
	// Host is set for every other URL.
	Host string
	Port int32
}

// parseMCPURL resolves an MCP server URL to its destination. The port defaults
// by scheme (80 for http, 443 for https).
func parseMCPURL(raw string) (mcpDestination, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return mcpDestination{}, fmt.Errorf("not a valid URL: %w", err)
	}
	var port int32
	switch strings.ToLower(u.Scheme) {
	case "http":
		port = 80
	case "https":
		port = 443
	default:
		return mcpDestination{}, fmt.Errorf("scheme must be http or https, got %q", u.Scheme)
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return mcpDestination{}, fmt.Errorf("has no host")
	}
	if p := u.Port(); p != "" {
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 || n > 65535 {
			return mcpDestination{}, fmt.Errorf("port %q is outside 1-65535", p)
		}
		port = int32(n)
	}
	trimmed := strings.TrimSuffix(strings.TrimSuffix(host, ".cluster.local"), ".")
	if name, ok := strings.CutSuffix(trimmed, ".svc"); ok {
		if parts := strings.Split(name, "."); len(parts) == 2 && parts[0] != "" && parts[1] != "" {
			return mcpDestination{Service: parts[0], Namespace: parts[1], Port: port}, nil
		}
	}
	if err := aiv1alpha1.ValidateEgressHost(aiv1alpha1.EgressHost{Host: host, Ports: []int32{port}}); err != nil {
		return mcpDestination{}, err
	}
	return mcpDestination{Host: host, Port: port}, nil
}

// hostCovers reports whether an allowedHosts name (exact or "*." wildcard)
// matches host.
func hostCovers(pattern, host string) bool {
	if rest, ok := strings.CutPrefix(pattern, "*."); ok {
		return strings.HasSuffix(host, "."+rest)
	}
	return pattern == host
}

// deriveMCPEgress returns the egress destinations implied by the knight's
// spec.mcpServers URLs that spec.egress does not already list, and one error
// per URL that can't be parsed. It returns nothing when the knight has no
// spec.egress (opted out, or egress control is off). Call it after
// ApplyIsolationDefaults.
func deriveMCPEgress(k *aiv1alpha1.Knight) (*aiv1alpha1.KnightDerivedEgress, []error) {
	if k.Spec.Egress == nil || len(k.Spec.MCPServers) == 0 {
		return nil, nil
	}
	listedHosts, _ := aiv1alpha1.ParseEgressHosts(k.Spec.Egress.AllowedHosts)
	listed := EgressAllowlist{AllowedHosts: listedHosts, AllowedPorts: k.Spec.Egress.AllowedPorts}
	listedServices := egressServiceRefs(&aiv1alpha1.Knight{
		ObjectMeta: k.ObjectMeta,
		Spec:       aiv1alpha1.KnightSpec{Egress: &aiv1alpha1.KnightEgress{AllowedServices: k.Spec.Egress.AllowedServices}},
	})

	out := &aiv1alpha1.KnightDerivedEgress{}
	var errs []error
	for _, s := range k.Spec.MCPServers {
		d, err := parseMCPURL(s.URL)
		if err != nil {
			errs = append(errs, fmt.Errorf("mcpServers entry %q url %q: %w", s.Name, s.URL, err))
			continue
		}
		if d.Service != "" {
			covered := slices.ContainsFunc(listedServices, func(r egressServiceRef) bool {
				return r.Namespace == d.Namespace && r.Name == d.Service && slices.Contains(r.Ports, d.Port)
			}) || slices.ContainsFunc(out.AllowedServices, func(e aiv1alpha1.EgressService) bool {
				return e.Namespace == d.Namespace && e.Name == d.Service && slices.Contains(e.Ports, d.Port)
			})
			if !covered {
				out.AllowedServices = append(out.AllowedServices,
					aiv1alpha1.EgressService{Name: d.Service, Namespace: d.Namespace, Ports: []int32{d.Port}})
			}
			continue
		}
		covered := false
		for name, ports := range listed.hostPorts() {
			if hostCovers(name, d.Host) && slices.Contains(ports, d.Port) {
				covered = true
			}
		}
		covered = covered || slices.ContainsFunc(out.AllowedHosts, func(h aiv1alpha1.EgressHost) bool {
			return h.Host == d.Host && slices.Contains(h.Ports, d.Port)
		})
		if !covered {
			out.AllowedHosts = append(out.AllowedHosts, aiv1alpha1.EgressHost{Host: d.Host, Ports: []int32{d.Port}})
		}
	}
	if len(out.AllowedServices) == 0 && len(out.AllowedHosts) == 0 {
		out = nil
	}
	return out, errs
}
