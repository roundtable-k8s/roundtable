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
	"strings"
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

const (
	flakeRevA = "0123456789abcdef0123456789abcdef01234567"
	flakeRevB = "89abcdef0123456789abcdef0123456789abcdef"
)

func knightWithFlakes(nix []string, flakes ...aiv1alpha1.KnightToolsFlake) *aiv1alpha1.Knight {
	return &aiv1alpha1.Knight{
		Spec: aiv1alpha1.KnightSpec{Tools: &aiv1alpha1.KnightTools{Nix: nix, Flakes: flakes}},
	}
}

func TestFlakesOnlyKnightHasToolsImage(t *testing.T) {
	k := knightWithFlakes(nil, aiv1alpha1.KnightToolsFlake{Ref: "github:o/r/" + flakeRevA, Packages: []string{"tool"}})
	if !HasToolsImage(k) {
		t.Error("flakes alone should need a tools image")
	}
	if ToolsImageTag(k) == "" {
		t.Error("flakes alone should yield a tag")
	}
}

func TestToolsImageTagUnchangedWithoutFlakes(t *testing.T) {
	plain := knightWithNix("nmap")
	empty := knightWithFlakes([]string{"nmap"})
	if ToolsImageTag(plain) != ToolsImageTag(empty) {
		t.Error("an empty flakes list must not change the tag")
	}
}

func TestToolsImageTagIncludesFlakes(t *testing.T) {
	flake := func(rev string, pkgs ...string) aiv1alpha1.KnightToolsFlake {
		return aiv1alpha1.KnightToolsFlake{Ref: "github:o/r/" + rev, Packages: pkgs}
	}
	base := ToolsImageTag(knightWithFlakes([]string{"nmap"}, flake(flakeRevA, "a")))
	if base == ToolsImageTag(knightWithNix("nmap")) {
		t.Error("adding a flake must change the tag")
	}
	if base == ToolsImageTag(knightWithFlakes([]string{"nmap"}, flake(flakeRevB, "a"))) {
		t.Error("changing the pinned rev must change the tag")
	}
	if base == ToolsImageTag(knightWithFlakes([]string{"nmap"}, flake(flakeRevA, "b"))) {
		t.Error("changing the packages must change the tag")
	}
	reordered := ToolsImageTag(knightWithFlakes([]string{"nmap"}, flake(flakeRevA, "b", "a", "b")))
	if reordered != ToolsImageTag(knightWithFlakes([]string{"nmap"}, flake(flakeRevA, "a"), flake(flakeRevA, "b"))) {
		t.Error("order and duplicates must not change the tag")
	}
}

func TestGenerateToolsImageFlakeWithFlakes(t *testing.T) {
	k := knightWithFlakes([]string{"nmap"},
		aiv1alpha1.KnightToolsFlake{Ref: "github:o/z/" + flakeRevB, Packages: []string{"zed"}},
		aiv1alpha1.KnightToolsFlake{Ref: "github:o/a/" + flakeRevA, Packages: []string{"beta", "alpha"}},
	)
	got := GenerateToolsImageFlake(k)
	for _, want := range []string{
		`inputs.tools-0.url = "github:o/a/` + flakeRevA + `";`,
		`inputs.tools-1.url = "github:o/z/` + flakeRevB + `";`,
		"outputs = { self, nixpkgs, nix2container, ... }@inputs:",
		"          nmap\n",
		`          inputs.tools-0.packages.${system}."alpha"` + "\n",
		`          inputs.tools-0.packages.${system}."beta"` + "\n",
		`          inputs.tools-1.packages.${system}."zed"` + "\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("flake missing %q:\n%s", want, got)
		}
	}
	if strings.Index(got, "inputs.tools-0.packages") < strings.Index(got, "paths = with pkgs;") {
		t.Error("flake packages must be inside the buildEnv paths")
	}
}

func TestGenerateToolsImageFlakeWithoutFlakesUnchanged(t *testing.T) {
	got := GenerateToolsImageFlake(knightWithNix("nmap"))
	if !strings.Contains(got, "outputs = { self, nixpkgs, nix2container }:") || strings.Contains(got, "tools-0") {
		t.Errorf("flake without flakes changed:\n%s", got)
	}
}
