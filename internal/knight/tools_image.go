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
	"crypto/sha256"
	"encoding/hex"
	"os"
	"sort"
	"strings"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// Tools images: the operator builds a knight's Nix tools once per unique tool
// list into an OCI image (nix2container), pushes it to a registry, and the
// knight mounts that image read-only at /nix as an image volume. The image tag is a hash of the tool
// list, so identical lists resolve to the same image and are built only once.

const (
	// ToolsImageProfilePath is the stable profile path baked into every tools
	// image (a symlink to the buildEnv), as seen from inside the knight.
	ToolsImageProfilePath = "/nix/profile"

	// defaultNix2ContainerRev pins nix2container to an immutable commit so the
	// generated flake does not float. Overridable via KNIGHT_NIX2CONTAINER_REV.
	defaultNix2ContainerRev = "42d1df94f001a9416ef08be7b305144074aca9d2"

	// stockNixImage is plain Nix pinned by digest (nix 2.36.0). It is the base
	// of the prebuilt tools builder (Dockerfile.tools-builder) and the fallback
	// builder when no prebuilt image matches the configured pins. On it a build
	// Job compiles the nix2container tooling from source every time.
	stockNixImage = "nixos/nix@sha256:746280551ddc1c72177d1a597c05eacac86dfaa18d5d649bf86411278bcd5470"

	// prebuiltToolsBuilderRepo is where CI publishes the prebuilt tools builder.
	prebuiltToolsBuilderRepo = "ghcr.io/roundtable-k8s/tools-builder"
)

// ToolsImageRepo returns the repository knights pull tools images from, e.g.
// "registry.example.com/knight-tools" (KNIGHT_TOOLS_IMAGE_REPO, chart value
// toolsImage.repository). Empty disables tools images: knights then run
// without their Nix tools.
func ToolsImageRepo() string {
	return strings.TrimSuffix(os.Getenv("KNIGHT_TOOLS_IMAGE_REPO"), "/")
}

// ToolsImageEnabled reports whether a tools image repository is configured.
func ToolsImageEnabled() bool {
	return ToolsImageRepo() != ""
}

// ToolsImagePushRepo returns the repository the build Job pushes to. It
// defaults to the pull repository; set KNIGHT_TOOLS_IMAGE_PUSH_REPO when the
// Job reaches the registry by a different address than the nodes do (e.g. an
// in-cluster Service versus a gateway hostname).
func ToolsImagePushRepo() string {
	if v := strings.TrimSuffix(os.Getenv("KNIGHT_TOOLS_IMAGE_PUSH_REPO"), "/"); v != "" {
		return v
	}
	return ToolsImageRepo()
}

// ToolsImagePushInsecure reports whether the build Job talks plain HTTP to the
// push repository (KNIGHT_TOOLS_IMAGE_PUSH_INSECURE=true).
func ToolsImagePushInsecure() bool {
	return os.Getenv("KNIGHT_TOOLS_IMAGE_PUSH_INSECURE") == "true"
}

// ToolsBuilderImage returns the image the tools build Job runs:
//
//   - KNIGHT_TOOLS_BUILDER_IMAGE (chart value toolsImage.builderImage) if set;
//   - otherwise the prebuilt builder CI publishes for the operator's built-in
//     pins, which has the nix2container tooling already in its Nix store;
//   - or plain Nix when the nixpkgs or nix2container pin has been overridden,
//     because no prebuilt image exists for other pins. That still works, it
//     is just slow.
func ToolsBuilderImage() string {
	if v := os.Getenv("KNIGHT_TOOLS_BUILDER_IMAGE"); v != "" {
		return v
	}
	if NixpkgsRef() == defaultNixpkgsRef && Nix2ContainerRev() == defaultNix2ContainerRev {
		return prebuiltToolsBuilderRepo + ":" + ToolsBuilderTag()
	}
	return stockNixImage
}

// ToolsBuilderTag returns the tag of the prebuilt tools builder for the
// current pins: a hash of everything that determines the image content (the
// base Nix image and the warm-up flake, which carries the nixpkgs and
// nix2container pins). CI computes the same tag when it builds the image
// (cmd/toolsflake -tag), so an operator and its builder always line up.
func ToolsBuilderTag() string {
	h := sha256.Sum256([]byte("tools-builder/v1\n" + stockNixImage + "\n" + ToolsBuilderWarmupFlake()))
	return hex.EncodeToString(h[:8])
}

// Nix2ContainerRev returns the pinned nix2container commit.
func Nix2ContainerRev() string {
	if v := os.Getenv("KNIGHT_NIX2CONTAINER_REV"); v != "" {
		return v
	}
	return defaultNix2ContainerRev
}

// HasToolsImage reports whether the knight declares Nix tools that a tools
// image can carry. Only spec.tools.nix is supported; the legacy
// spec.nixPackages field is not.
func HasToolsImage(knight *aiv1alpha1.Knight) bool {
	return knight.Spec.Tools != nil && len(knight.Spec.Tools.Nix) > 0
}

// sortedNixTools returns the knight's Nix tools de-duplicated and sorted, so
// the same set always yields the same tag and the same flake.
func sortedNixTools(knight *aiv1alpha1.Knight) []string {
	if !HasToolsImage(knight) {
		return nil
	}
	seen := make(map[string]struct{}, len(knight.Spec.Tools.Nix))
	tools := make([]string, 0, len(knight.Spec.Tools.Nix))
	for _, t := range knight.Spec.Tools.Nix {
		if _, dup := seen[t]; dup {
			continue
		}
		seen[t] = struct{}{}
		tools = append(tools, t)
	}
	sort.Strings(tools)
	return tools
}

// ToolsImageTag returns the image tag for the knight's tool list: a hash of
// the sorted tools plus everything else that determines the image content (the
// nixpkgs and nix2container pins). It is independent of the knight's name, so
// knights with identical lists share one image. Empty when there are no tools.
func ToolsImageTag(knight *aiv1alpha1.Knight) string {
	tools := sortedNixTools(knight)
	if len(tools) == 0 {
		return ""
	}
	h := sha256.Sum256([]byte(
		"tools-image/v1\n" + strings.Join(tools, ",") +
			"\nnixpkgs@" + NixpkgsRef() +
			"\nnix2container@" + Nix2ContainerRev()))
	return hex.EncodeToString(h[:8]) // 16-char hex prefix
}

// ToolsImageRef returns the reference knights pull the tools image by, or ""
// when tools images are disabled or the knight has no Nix tools.
func ToolsImageRef(knight *aiv1alpha1.Knight) string {
	tag := ToolsImageTag(knight)
	if tag == "" || !ToolsImageEnabled() {
		return ""
	}
	return ToolsImageRepo() + ":" + tag
}

// MountedToolsImageRef returns the tools image the knight's pod should mount.
// That is the image last published for the knight (status.toolsImage), which
// trails ToolsImageRef while a changed tool list is still building: the pod
// keeps its current tools until the new image exists and then rolls once,
// instead of being recreated against a tag that is not in the registry yet.
// A knight with nothing published mounts the desired image and waits for it.
func MountedToolsImageRef(knight *aiv1alpha1.Knight) string {
	desired := ToolsImageRef(knight)
	if desired == "" {
		return ""
	}
	if published := knight.Status.ToolsImage; published != "" {
		return published
	}
	return desired
}

// ToolsImagePushRef returns the reference the build Job pushes to.
func ToolsImagePushRef(knight *aiv1alpha1.Knight) string {
	tag := ToolsImageTag(knight)
	if tag == "" || !ToolsImageEnabled() {
		return ""
	}
	return ToolsImagePushRepo() + ":" + tag
}

// ToolsImageManifestURL returns the registry API URL of the pushed image's
// manifest; the build Job probes it to skip work that is already published.
func ToolsImageManifestURL(knight *aiv1alpha1.Knight) string {
	tag := ToolsImageTag(knight)
	if tag == "" || !ToolsImageEnabled() {
		return ""
	}
	host, path, _ := strings.Cut(ToolsImagePushRepo(), "/")
	scheme := "https"
	if ToolsImagePushInsecure() {
		scheme = "http"
	}
	return scheme + "://" + host + "/v2/" + path + "/manifests/" + tag
}

// GenerateToolsImageFlake produces the flake the build Job turns into the
// knight's tools image. The image holds the tools' closure under /nix/store
// plus a /nix/profile symlink to their buildEnv; mounted at /nix in the knight
// (subPath "nix") that gives ToolsImageProfilePath a bin directory.
func GenerateToolsImageFlake(knight *aiv1alpha1.Knight) string {
	var sb strings.Builder
	sb.WriteString("# Auto-generated by roundtable-operator\n")
	sb.WriteString("# Tools image: " + ToolsImageTag(knight) + "\n")
	sb.WriteString("{\n")
	sb.WriteString("  inputs.nixpkgs.url = \"github:NixOS/nixpkgs/" + NixpkgsRef() + "\";\n")
	sb.WriteString("  inputs.nix2container.url = \"github:nlewo/nix2container/" + Nix2ContainerRev() + "\";\n")
	sb.WriteString("  inputs.nix2container.inputs.nixpkgs.follows = \"nixpkgs\";\n")
	sb.WriteString("  outputs = { self, nixpkgs, nix2container }:\n")
	sb.WriteString("    let\n")
	sb.WriteString("      system = \"x86_64-linux\";\n")
	// allowUnfree: many security/pentest tools (wpscan, burpsuite, …) are
	// unfree and otherwise fail the build with an allowUnfree error.
	sb.WriteString("      pkgs = import nixpkgs { inherit system; config.allowUnfree = true; };\n")
	sb.WriteString("      n2c = nix2container.packages.${system}.nix2container;\n")
	sb.WriteString("      env = pkgs.buildEnv {\n")
	sb.WriteString("        name = \"knight-tools\";\n")
	sb.WriteString("        paths = with pkgs; [\n")
	for _, pkg := range sortedNixTools(knight) {
		sb.WriteString("          " + pkg + "\n")
	}
	sb.WriteString("        ];\n")
	sb.WriteString("      };\n")
	sb.WriteString("      profile = pkgs.runCommand \"knight-profile\" { } ''\n")
	sb.WriteString("        mkdir -p $out/nix\n")
	sb.WriteString("        ln -s ${env} $out/nix/profile\n")
	sb.WriteString("      '';\n")
	sb.WriteString("    in {\n")
	sb.WriteString("      packages.${system}.tools = n2c.buildImage {\n")
	sb.WriteString("        name = \"knight-tools\";\n")
	sb.WriteString("        copyToRoot = [ profile ];\n")
	sb.WriteString("        maxLayers = 100;\n")
	sb.WriteString("      };\n")
	sb.WriteString("    };\n")
	sb.WriteString("}\n")
	return sb.String()
}

// ToolsBuilderWarmupFlake returns the flake the prebuilt tools builder image
// is warmed up with: the same flake a build Job gets, for a single small
// package. Building it bakes the nix2container tooling (its patched skopeo is
// compiled from source and otherwise dominates every build) and the pinned
// flake inputs into the image's Nix store.
func ToolsBuilderWarmupFlake() string {
	return GenerateToolsImageFlake(&aiv1alpha1.Knight{
		Spec: aiv1alpha1.KnightSpec{Tools: &aiv1alpha1.KnightTools{Nix: []string{"hello"}}},
	})
}
