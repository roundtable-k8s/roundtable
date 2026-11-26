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
	"encoding/json"
	"fmt"
	"os"

	appsv1 "k8s.io/api/apps/v1"
)

// defaultNixpkgsRef pins nixpkgs to a specific immutable commit (nixos-unstable
// HEAD as of 2026-06-10) for reproducible tools images. Overridable
// per-deployment via the KNIGHT_NIXPKGS_REF env var (chart value nixpkgs.ref);
// bump to roll the whole fleet's tooling forward.
const defaultNixpkgsRef = "9ae611a455b90cf061d8f332b977e387bda8e1ca"

// NixpkgsRef returns the nixpkgs ref (commit/branch) to pin flakes to.
func NixpkgsRef() string {
	if r := os.Getenv("KNIGHT_NIXPKGS_REF"); r != "" {
		return r
	}
	return defaultNixpkgsRef
}

// DeploymentSpecHash computes a hash of the deployment spec for drift detection.
func DeploymentSpecHash(deploy *appsv1.Deployment) string {
	// Hash the pod template spec to detect meaningful changes
	// (ignore replicas, status, metadata timestamps)
	data, _ := json.Marshal(deploy.Spec.Template.Spec)
	hash := sha256.Sum256(data)
	return fmt.Sprintf("%x", hash[:8])
}
