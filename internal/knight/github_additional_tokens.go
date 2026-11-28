/*
Copyright 2026.

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
	"context"
	"encoding/json"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	ghpkg "github.com/roundtable-k8s/roundtable/internal/github"
)

const (
	// GitHubAdditionalTokensRoot is the parent of the additional GitHub token
	// mounts; item i of spec.github.additionalTokens is mounted at
	// GitHubAdditionalTokensRoot/<i>/token.
	GitHubAdditionalTokensRoot = "/var/run/secrets/roundtable/github-additional"
	// GitHubAdditionalTokensEnv names the env var holding the JSON array of
	// {"match","file"} objects that describes the mounted additional tokens.
	GitHubAdditionalTokensEnv = "GITHUB_ADDITIONAL_TOKENS"
)

// GitHubAdditionalTokenFile is one entry of GITHUB_ADDITIONAL_TOKENS.
type GitHubAdditionalTokenFile struct {
	Match string `json:"match"`
	File  string `json:"file"`
}

// AdditionalGitHubToken is a table additional token that applies to a knight,
// with its index in spec.github.additionalTokens.
type AdditionalGitHubToken struct {
	Index int
	aiv1alpha1.GitHubAdditionalToken
}

// ResolveAdditionalGitHubTokens returns the items of the knight's RoundTable
// spec.github.additionalTokens that apply to it, in list order: items whose
// knights list is empty or names the knight. Mission knights (labelled
// ai.roundtable.io/mission) only get items with an empty knights list. Returns
// nil when the knight opts out with spec.github.disableAdditionalTokens or has
// no table.
func ResolveAdditionalGitHubTokens(ctx context.Context, r client.Reader, k *aiv1alpha1.Knight) []AdditionalGitHubToken {
	if k.Spec.GitHub != nil && k.Spec.GitHub.DisableAdditionalTokens {
		return nil
	}
	rt := findTable(ctx, r, k, func(rt *aiv1alpha1.RoundTable) bool {
		return rt.Spec.GitHub != nil && len(rt.Spec.GitHub.AdditionalTokens) > 0
	})
	if rt == nil {
		return nil
	}
	_, isMission := k.Labels[aiv1alpha1.LabelMission]
	var out []AdditionalGitHubToken
	for i, t := range rt.Spec.GitHub.AdditionalTokens {
		if t.Match == "" || t.TokenSecretRef.Name == "" {
			continue
		}
		if len(t.Knights) > 0 && (isMission || !slices.Contains(t.Knights, k.Name)) {
			continue
		}
		out = append(out, AdditionalGitHubToken{Index: i, GitHubAdditionalToken: *t.DeepCopy()})
	}
	return out
}

// GitHubAdditionalTokenDir is the mount directory of additional token i.
func GitHubAdditionalTokenDir(i int) string {
	return fmt.Sprintf("%s/%d", GitHubAdditionalTokensRoot, i)
}

// WithGitHubAdditionalTokens mounts each of the table's additional GitHub
// tokens that applies to the knight (see ResolveAdditionalGitHubTokens)
// read-only at GitHubAdditionalTokensRoot/<index>/token and sets
// GITHUB_ADDITIONAL_TOKENS to the JSON list of {match, file}. Whole Secret
// volumes are mounted (no subPath) so rotated Secrets reach the files without
// a restart; no token value is put in the environment.
func (b *PodBuilder) WithGitHubAdditionalTokens(ctx context.Context) *PodBuilder {
	tokens := ResolveAdditionalGitHubTokens(ctx, b.reader, b.knight)
	if len(tokens) == 0 {
		return b
	}
	files := make([]GitHubAdditionalTokenFile, 0, len(tokens))
	for _, t := range tokens {
		ref := ghpkg.ResolveTokenRef(&t.TokenSecretRef)
		name := fmt.Sprintf("github-additional-%d", t.Index)
		dir := GitHubAdditionalTokenDir(t.Index)
		b.volumes = append(b.volumes, corev1.Volume{
			Name: name,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: ref.Name,
					Items:      []corev1.KeyToPath{{Key: ref.Key, Path: "token"}},
					Optional:   ref.Optional,
				},
			},
		})
		b.mounts = append(b.mounts, corev1.VolumeMount{Name: name, MountPath: dir, ReadOnly: true})
		files = append(files, GitHubAdditionalTokenFile{Match: t.Match, File: dir + "/token"})
	}
	// Marshalling a slice of plain string structs cannot fail.
	data, _ := json.Marshal(files)
	b.env = append(b.env, corev1.EnvVar{Name: GitHubAdditionalTokensEnv, Value: string(data)})
	return b
}
