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
	"testing"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func knightWithNix(tools ...string) *aiv1alpha1.Knight {
	return &aiv1alpha1.Knight{
		Spec: aiv1alpha1.KnightSpec{Tools: &aiv1alpha1.KnightTools{Nix: tools}},
	}
}

func TestNixpkgsRefDefaultAndOverride(t *testing.T) {
	if NixpkgsRef() != defaultNixpkgsRef {
		t.Errorf("default = %s, want %s", NixpkgsRef(), defaultNixpkgsRef)
	}
	t.Setenv("KNIGHT_NIXPKGS_REF", "abc123")
	if NixpkgsRef() != "abc123" {
		t.Errorf("override = %s, want abc123", NixpkgsRef())
	}
}
