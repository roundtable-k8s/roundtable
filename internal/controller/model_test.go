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

import "testing"

func TestAgentRuntimeForImage(t *testing.T) {
	tests := []struct {
		image string
		want  agentRuntime
	}{
		{"ghcr.io/roundtable-k8s/claude-knight:latest", agentRuntimeClaude},
		{"ghcr.io/roundtable-k8s/claude-knight", agentRuntimeClaude},
		{"registry.local:5000/mirror/claude-knight-dev:v1@sha256:abc", agentRuntimeClaude},
		{"ghcr.io/roundtable-k8s/pi-knight:latest", agentRuntimePi},
		{"pi-knight@sha256:abc", agentRuntimePi},
		{"registry.local:5000/pi-knight", agentRuntimePi},
		{"ghcr.io/acme/my-agent:1.0", agentRuntimeUnknown},
		{"ghcr.io/claude-knight/agent:1.0", agentRuntimeUnknown}, // only the repo name counts
		{"", agentRuntimeUnknown},
	}
	for _, tt := range tests {
		if got := agentRuntimeForImage(tt.image); got != tt.want {
			t.Errorf("agentRuntimeForImage(%q) = %q, want %q", tt.image, got, tt.want)
		}
	}
}

func TestResolveStepModel(t *testing.T) {
	const (
		claude  = "ghcr.io/roundtable-k8s/claude-knight:v1"
		pi      = "ghcr.io/roundtable-k8s/pi-knight:v1"
		unknown = "ghcr.io/acme/agent:v1"
	)
	aliases := map[string]string{
		"deep":   "opus",
		"fast":   "haiku",
		"review": "openrouter/z-ai/glm-5.3",
		"opus":   "claude-opus-4-1", // alias shadows a literal model name
		"empty":  "",
	}
	tests := []struct {
		name, requested, image, want string
		wantErr                      bool
	}{
		{"no override", "", claude, "", false},
		{"no override ignores aliases", "", pi, "", false},
		{"claude alias name", "sonnet", claude, "sonnet", false},
		{"claude model id", "claude-sonnet-4-5", claude, "claude-sonnet-4-5", false},
		{"claude context suffix", "sonnet[1m]", claude, "sonnet[1m]", false},
		{"claude id with suffix", "claude-sonnet-4-5[1m]", claude, "claude-sonnet-4-5[1m]", false},
		{"roundtable alias to claude", "deep", claude, "opus", false},
		{"alias shadows literal", "opus", claude, "claude-opus-4-1", false},
		{"provider model on claude", "openrouter/z-ai/glm-5.3", claude, "", true},
		{"alias to provider model on claude", "review", claude, "", true},
		{"anthropic provider prefix on claude", "anthropic/claude-opus-4", claude, "", true},
		{"unknown name on claude", "gpt-5", claude, "", true},
		{"bare claude- on claude", "claude-", claude, "", true},
		{"unterminated suffix on claude", "sonnet[1m", claude, "", true},
		{"provider model on pi", "openrouter/z-ai/glm-5.3", pi, "openrouter/z-ai/glm-5.3", false},
		{"alias to provider model on pi", "review", pi, "openrouter/z-ai/glm-5.3", false},
		{"bare model on pi", "haiku", pi, "", true},
		{"alias to claude alias on pi", "fast", pi, "", true},
		{"empty provider on pi", "/glm", pi, "", true},
		{"empty model on pi", "openrouter/", pi, "", true},
		{"alias to empty", "empty", claude, "", true},
		{"unknown runtime passes through", "anything-goes", unknown, "anything-goes", false},
		{"unknown runtime still resolves aliases", "deep", unknown, "opus", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveStepModel(tt.requested, aliases, tt.image)
			if (err != nil) != tt.wantErr {
				t.Fatalf("resolveStepModel(%q, %q) error = %v, wantErr %v", tt.requested, tt.image, err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("resolveStepModel(%q, %q) = %q, want %q", tt.requested, tt.image, got, tt.want)
			}
		})
	}
}
