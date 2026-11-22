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

package runtime

import (
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/discovery"
	sandboxv1alpha1 "sigs.k8s.io/agent-sandbox/api/v1alpha1"
)

// SandboxMode controls whether the sandbox runtime backend is enabled.
type SandboxMode string

const (
	// SandboxModeAuto enables the sandbox backend only when the API server
	// serves agents.x-k8s.io/v1alpha1 Sandbox.
	SandboxModeAuto SandboxMode = "auto"
	// SandboxModeEnabled forces the sandbox backend on without probing.
	SandboxModeEnabled SandboxMode = "true"
	// SandboxModeDisabled forces the sandbox backend off.
	SandboxModeDisabled SandboxMode = "false"
)

// ParseSandboxMode parses the --enable-sandbox-backend flag value.
func ParseSandboxMode(s string) (SandboxMode, error) {
	switch m := SandboxMode(strings.ToLower(strings.TrimSpace(s))); m {
	case SandboxModeAuto, SandboxModeEnabled, SandboxModeDisabled:
		return m, nil
	default:
		return "", fmt.Errorf("invalid sandbox backend mode %q (want auto, true or false)", s)
	}
}

// SandboxAPIServed reports whether the API server serves the Sandbox kind at
// the agent-sandbox version this operator is built against (v1alpha1).
// A group/version that isn't served at all (agent-sandbox not installed, or
// agent-sandbox >= v1.0 which only serves v1beta1) returns false, nil.
func SandboxAPIServed(dc discovery.DiscoveryInterface) (bool, error) {
	list, err := dc.ServerResourcesForGroupVersion(sandboxv1alpha1.GroupVersion.String())
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("discovering %s: %w", sandboxv1alpha1.GroupVersion, err)
	}
	if list == nil {
		return false, nil
	}
	for _, r := range list.APIResources {
		if r.Kind == "Sandbox" && !strings.Contains(r.Name, "/") {
			return true, nil
		}
	}
	return false, nil
}

// ResolveSandboxBackend decides whether to enable the sandbox runtime backend.
// It returns the decision and a short human-readable reason for the startup log.
func ResolveSandboxBackend(mode SandboxMode, dc discovery.DiscoveryInterface) (bool, string, error) {
	switch mode {
	case SandboxModeEnabled:
		return true, "forced on by --enable-sandbox-backend=true", nil
	case SandboxModeDisabled:
		return false, "disabled by --enable-sandbox-backend=false", nil
	}
	served, err := SandboxAPIServed(dc)
	if err != nil {
		return false, "", err
	}
	if served {
		return true, fmt.Sprintf("%s Sandbox is served", sandboxv1alpha1.GroupVersion), nil
	}
	return false, fmt.Sprintf("%s Sandbox is not served by the API server", sandboxv1alpha1.GroupVersion), nil
}
