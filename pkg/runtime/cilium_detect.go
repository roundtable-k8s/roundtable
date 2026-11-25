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

package runtime

import (
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
)

// CiliumNetworkPolicyGVK is the Cilium policy kind the operator renders for
// knights with spec.egress. The operator handles it as unstructured, so
// Cilium is not a build dependency.
var CiliumNetworkPolicyGVK = schema.GroupVersionKind{Group: "cilium.io", Version: "v2", Kind: "CiliumNetworkPolicy"}

// CiliumNetworkPolicyAPIServed reports whether the API server serves the
// CiliumNetworkPolicy kind. A group/version that isn't served at all (Cilium
// not installed) returns false, nil.
func CiliumNetworkPolicyAPIServed(dc discovery.DiscoveryInterface) (bool, error) {
	gv := CiliumNetworkPolicyGVK.GroupVersion().String()
	list, err := dc.ServerResourcesForGroupVersion(gv)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("discovering %s: %w", gv, err)
	}
	if list == nil {
		return false, nil
	}
	for _, r := range list.APIResources {
		if r.Kind == CiliumNetworkPolicyGVK.Kind && !strings.Contains(r.Name, "/") {
			return true, nil
		}
	}
	return false, nil
}
