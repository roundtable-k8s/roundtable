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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func TestToolsImageDisabledByDefault(t *testing.T) {
	k := knightWithNix("nmap")
	if ToolsImageEnabled() {
		t.Fatal("tools images must be off unless KNIGHT_TOOLS_IMAGE_REPO is set")
	}
	if got := ToolsImageRef(k); got != "" {
		t.Errorf("ToolsImageRef = %q, want empty when disabled", got)
	}
	if got := ToolsImagePushRef(k); got != "" {
		t.Errorf("ToolsImagePushRef = %q, want empty when disabled", got)
	}
}

func TestToolsImageTagIgnoresOrderDuplicatesAndName(t *testing.T) {
	a := knightWithNix("nmap", "jq", "curl")
	a.ObjectMeta = metav1.ObjectMeta{Name: "galahad"}
	b := knightWithNix("curl", "nmap", "jq", "nmap")
	b.ObjectMeta = metav1.ObjectMeta{Name: "percival"}

	if ToolsImageTag(a) == "" {
		t.Fatal("expected a tag for a knight with nix tools")
	}
	if ToolsImageTag(a) != ToolsImageTag(b) {
		t.Error("the same tool set must yield the same tag regardless of order, duplicates or knight name")
	}
	if ToolsImageTag(a) == ToolsImageTag(knightWithNix("nmap", "jq")) {
		t.Error("a different tool set must yield a different tag")
	}
	if ToolsImageTag(&aiv1alpha1.Knight{}) != "" {
		t.Error("a knight with no nix tools must have no tag")
	}
}

func TestToolsImageTagChangesWithPins(t *testing.T) {
	k := knightWithNix("nmap")
	base := ToolsImageTag(k)

	t.Setenv("KNIGHT_NIXPKGS_REF", "different-ref")
	withNixpkgs := ToolsImageTag(k)
	if withNixpkgs == base {
		t.Error("tag must change when the nixpkgs pin changes")
	}

	t.Setenv("KNIGHT_NIX2CONTAINER_REV", "different-rev")
	if ToolsImageTag(k) == withNixpkgs {
		t.Error("tag must change when the nix2container pin changes")
	}
}

func TestToolsImageRefs(t *testing.T) {
	k := knightWithNix("nmap")
	t.Setenv("KNIGHT_TOOLS_IMAGE_REPO", "registry.example.com/knight-tools/")
	tag := ToolsImageTag(k)

	// Push repo defaults to the pull repo, over HTTPS.
	if got, want := ToolsImageRef(k), "registry.example.com/knight-tools:"+tag; got != want {
		t.Errorf("ToolsImageRef = %q, want %q", got, want)
	}
	if got, want := ToolsImagePushRef(k), "registry.example.com/knight-tools:"+tag; got != want {
		t.Errorf("ToolsImagePushRef = %q, want %q", got, want)
	}
	if got, want := ToolsImageManifestURL(k), "https://registry.example.com/v2/knight-tools/manifests/"+tag; got != want {
		t.Errorf("ToolsImageManifestURL = %q, want %q", got, want)
	}

	// A separate in-cluster push address over plain HTTP.
	t.Setenv("KNIGHT_TOOLS_IMAGE_PUSH_REPO", "registry.rt.svc:5000/knight-tools")
	t.Setenv("KNIGHT_TOOLS_IMAGE_PUSH_INSECURE", "true")
	if got, want := ToolsImageRef(k), "registry.example.com/knight-tools:"+tag; got != want {
		t.Errorf("pull ref must not follow the push repo: got %q, want %q", got, want)
	}
	if got, want := ToolsImagePushRef(k), "registry.rt.svc:5000/knight-tools:"+tag; got != want {
		t.Errorf("ToolsImagePushRef = %q, want %q", got, want)
	}
	if got, want := ToolsImageManifestURL(k), "http://registry.rt.svc:5000/v2/knight-tools/manifests/"+tag; got != want {
		t.Errorf("ToolsImageManifestURL = %q, want %q", got, want)
	}

	// No nix tools → no refs even when enabled.
	if got := ToolsImageRef(&aiv1alpha1.Knight{}); got != "" {
		t.Errorf("ToolsImageRef without tools = %q, want empty", got)
	}
}

func TestGenerateToolsImageFlake(t *testing.T) {
	flake := GenerateToolsImageFlake(knightWithNix("nmap", "jq", "nmap"))

	for _, want := range []string{
		"github:NixOS/nixpkgs/" + defaultNixpkgsRef,
		"github:nlewo/nix2container/" + defaultNix2ContainerRev,
		"allowUnfree = true",
		"packages.${system}.tools = n2c.buildImage",
		"ln -s ${env} $out/nix/profile",
	} {
		if !strings.Contains(flake, want) {
			t.Errorf("flake missing %q; got:\n%s", want, flake)
		}
	}

	// Packages are sorted and de-duplicated, so equal sets give equal flakes.
	if !strings.Contains(flake, "          jq\n          nmap\n        ];") {
		t.Errorf("flake packages should be sorted and de-duplicated; got:\n%s", flake)
	}
	if flake != GenerateToolsImageFlake(knightWithNix("jq", "nmap")) {
		t.Error("equal tool sets must generate identical flakes")
	}
}
