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
	"os"
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

func TestMountedToolsImageRefTrailsUntilPublished(t *testing.T) {
	t.Setenv("KNIGHT_TOOLS_IMAGE_REPO", "registry.example.com/knight-tools")
	k := knightWithNix("nmap")
	desired := ToolsImageRef(k)

	// Nothing published yet: mount the desired image and wait for it.
	if got := MountedToolsImageRef(k); got != desired {
		t.Errorf("unpublished knight mounts %q, want desired %q", got, desired)
	}

	// Published and current.
	k.Status.ToolsImage = desired
	if got := MountedToolsImageRef(k); got != desired {
		t.Errorf("published knight mounts %q, want %q", got, desired)
	}

	// Tool list changed, new image still building: keep the published one.
	k.Spec.Tools.Nix = []string{"nmap", "jq"}
	if ToolsImageRef(k) == desired {
		t.Fatal("test setup: changing the tool list must change the desired image")
	}
	if got := MountedToolsImageRef(k); got != desired {
		t.Errorf("mid-build knight mounts %q, want the previously published %q", got, desired)
	}

	// Tools removed, or tools images disabled: mount nothing, whatever the status says.
	k.Spec.Tools = nil
	if got := MountedToolsImageRef(k); got != "" {
		t.Errorf("knight without nix tools mounts %q, want nothing", got)
	}
	k = knightWithNix("nmap")
	k.Status.ToolsImage = desired
	t.Setenv("KNIGHT_TOOLS_IMAGE_REPO", "")
	if got := MountedToolsImageRef(k); got != "" {
		t.Errorf("with tools images disabled the knight mounts %q, want nothing", got)
	}
}

func TestToolsBuilderImageSelection(t *testing.T) {
	// Built-in pins: the prebuilt builder published for exactly those pins.
	prebuilt := prebuiltToolsBuilderRepo + ":" + ToolsBuilderTag()
	if got := ToolsBuilderImage(); got != prebuilt {
		t.Errorf("default builder = %q, want prebuilt %q", got, prebuilt)
	}

	// An overridden pin has no prebuilt image: fall back to plain Nix.
	t.Setenv("KNIGHT_NIXPKGS_REF", "some-other-ref")
	if got := ToolsBuilderImage(); got != stockNixImage {
		t.Errorf("builder with a custom nixpkgs pin = %q, want stock %q", got, stockNixImage)
	}
	t.Setenv("KNIGHT_NIXPKGS_REF", "")
	t.Setenv("KNIGHT_NIX2CONTAINER_REV", "some-other-rev")
	if got := ToolsBuilderImage(); got != stockNixImage {
		t.Errorf("builder with a custom nix2container pin = %q, want stock %q", got, stockNixImage)
	}

	// An explicit image always wins.
	t.Setenv("KNIGHT_TOOLS_BUILDER_IMAGE", "example.com/builder:1")
	if got := ToolsBuilderImage(); got != "example.com/builder:1" {
		t.Errorf("explicit builder = %q, want example.com/builder:1", got)
	}
}

func TestToolsBuilderTagFollowsThePins(t *testing.T) {
	base := ToolsBuilderTag()
	if len(base) != 16 {
		t.Fatalf("tag %q should be a 16-char hash", base)
	}
	t.Setenv("KNIGHT_NIXPKGS_REF", "different-ref")
	if ToolsBuilderTag() == base {
		t.Error("builder tag must change with the nixpkgs pin")
	}
}

// The builder image is built FROM the same stock Nix image the operator falls
// back to; the digest lives in both places, so keep them identical.
func TestToolsBuilderDockerfileUsesStockNixImage(t *testing.T) {
	dockerfile, err := os.ReadFile("../../Dockerfile.tools-builder")
	if err != nil {
		t.Fatalf("read Dockerfile.tools-builder: %v", err)
	}
	if !strings.Contains(string(dockerfile), "FROM "+stockNixImage+"\n") {
		t.Errorf("Dockerfile.tools-builder must build FROM %s", stockNixImage)
	}
}
