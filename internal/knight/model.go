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
	"fmt"
	"strings"
)

// AgentRuntime is the agent harness a knight image runs. It is unrelated to
// Knight.spec.runtime, which selects the pod backend (deployment/sandbox).
type AgentRuntime string

const (
	AgentRuntimeClaude  AgentRuntime = "claude-knight"
	AgentRuntimePi      AgentRuntime = "pi-knight"
	AgentRuntimeUnknown AgentRuntime = ""
)

// AgentRuntimeForImage infers the agent runtime from a container image
// reference by its repository name: ".../claude-knight[-*][:tag][@digest]"
// is claude-knight, ".../pi-knight..." is pi-knight, anything else is unknown.
func AgentRuntimeForImage(image string) AgentRuntime {
	ref := image
	if i := strings.Index(ref, "@"); i >= 0 {
		ref = ref[:i]
	}
	if i := strings.LastIndex(ref, "/"); i >= 0 {
		ref = ref[i+1:]
	}
	if i := strings.Index(ref, ":"); i >= 0 {
		ref = ref[:i]
	}
	switch {
	case strings.HasPrefix(ref, string(AgentRuntimeClaude)):
		return AgentRuntimeClaude
	case strings.HasPrefix(ref, string(AgentRuntimePi)):
		return AgentRuntimePi
	default:
		return AgentRuntimeUnknown
	}
}

// claudeModelAliases are the short model names the Claude Code CLI accepts
// for --model.
var claudeModelAliases = map[string]bool{
	"opus":     true,
	"sonnet":   true,
	"haiku":    true,
	"fable":    true,
	"opusplan": true,
}

// isClaudeModel reports whether model is something claude-knight can pass to
// `claude --model`: a CLI alias or a "claude-..." model ID, optionally with a
// context-window suffix such as "[1m]".
func isClaudeModel(model string) bool {
	base := model
	if i := strings.Index(base, "["); i >= 0 {
		if !strings.HasSuffix(base, "]") {
			return false
		}
		base = base[:i]
	}
	if strings.Contains(base, "/") {
		return false
	}
	return claudeModelAliases[base] || (strings.HasPrefix(base, "claude-") && len(base) > len("claude-"))
}

// isProviderModel reports whether model has the "provider/model" form
// pi-knight expects (the model part may itself contain slashes, e.g.
// "openrouter/z-ai/glm-5.3").
func isProviderModel(model string) bool {
	provider, rest, ok := strings.Cut(model, "/")
	return ok && provider != "" && rest != "" && !strings.HasSuffix(rest, "/")
}

// ResolveModel resolves a step's requested model through the RoundTable's
// aliases and checks it against the knight's agent runtime. An empty request
// resolves to "" (use the knight's default). Knights whose runtime can't be
// inferred from the image get the resolved model unvalidated.
func ResolveModel(requested string, aliases map[string]string, image string) (string, error) {
	if requested == "" {
		return "", nil
	}
	model := requested
	if target, ok := aliases[requested]; ok {
		if target == "" {
			return "", fmt.Errorf("model alias %q resolves to an empty model", requested)
		}
		model = target
	}
	describe := fmt.Sprintf("model %q", model)
	if model != requested {
		describe = fmt.Sprintf("model %q (alias %q)", model, requested)
	}

	switch rt := AgentRuntimeForImage(image); rt {
	case AgentRuntimeClaude:
		if !isClaudeModel(model) {
			return "", fmt.Errorf("%s is not a Claude model; knight image %q runs %s, which accepts opus, sonnet, haiku or claude-* model IDs",
				describe, image, rt)
		}
	case AgentRuntimePi:
		if !isProviderModel(model) {
			return "", fmt.Errorf("%s is not in provider/model form; knight image %q runs %s, which needs e.g. anthropic/claude-sonnet-4-5 or openrouter/z-ai/glm-5.3",
				describe, image, rt)
		}
	}
	return model, nil
}
