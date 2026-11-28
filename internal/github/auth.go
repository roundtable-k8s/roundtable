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

package github

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// DefaultTokenKey is the Secret key read when a token ref names none.
const DefaultTokenKey = "token"

// ResolveTokenRef returns a copy of the first source that names a Secret, with
// the key defaulted to DefaultTokenKey, or nil when none does.
func ResolveTokenRef(sources ...*corev1.SecretKeySelector) *corev1.SecretKeySelector {
	for _, s := range sources {
		if s == nil || s.Name == "" {
			continue
		}
		out := s.DeepCopy()
		if out.Key == "" {
			out.Key = DefaultTokenKey
		}
		return out
	}
	return nil
}

// ReadToken reads the token from the referenced Secret in namespace. It reads
// on every call (no caching) so rotated tokens are picked up without a restart.
func ReadToken(ctx context.Context, r client.Reader, namespace string, ref *corev1.SecretKeySelector) (string, error) {
	if ref == nil || ref.Name == "" {
		return "", fmt.Errorf("no GitHub token Secret configured")
	}
	key := ref.Key
	if key == "" {
		key = DefaultTokenKey
	}
	sec := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name}, sec); err != nil {
		return "", fmt.Errorf("read GitHub token Secret %q: %w", ref.Name, err)
	}
	tok := strings.TrimSpace(string(sec.Data[key]))
	if tok == "" {
		return "", fmt.Errorf("GitHub token Secret %q has no key %q", ref.Name, key)
	}
	return tok, nil
}

// TableTokenRef returns the RoundTable's spec.github.tokenSecretRef, or nil.
func TableTokenRef(rt *aiv1alpha1.RoundTable) *corev1.SecretKeySelector {
	if rt == nil || rt.Spec.GitHub == nil {
		return nil
	}
	return rt.Spec.GitHub.TokenSecretRef
}

// InheritGitHub copies the table's spec.github.tokenSecretRef into a
// generated knight spec unless the spec already names its own token Secret.
// additionalTokens are not copied: the pod builder reads them from the table.
func InheritGitHub(spec *aiv1alpha1.KnightSpec, rt *aiv1alpha1.RoundTable) {
	if rt.Spec.GitHub == nil || rt.Spec.GitHub.TokenSecretRef == nil {
		return
	}
	if spec.GitHub == nil {
		spec.GitHub = &aiv1alpha1.GitHubConfig{}
	}
	if spec.GitHub.TokenSecretRef == nil {
		spec.GitHub.TokenSecretRef = rt.Spec.GitHub.TokenSecretRef.DeepCopy()
	}
}

// ClientFactory builds GitHub clients for one API URL.
type ClientFactory struct {
	baseURL string
}

// NewClientFactory returns a factory for baseURL ("" means api.github.com).
func NewClientFactory(baseURL string) *ClientFactory {
	return &ClientFactory{baseURL: baseURL}
}

// New returns a client authenticated with token. A nil factory targets api.github.com.
func (f *ClientFactory) New(token string) *Client {
	if f == nil {
		return NewClient("", token)
	}
	return NewClient(f.baseURL, token)
}
