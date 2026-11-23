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

// ScaledObjectGVK is the KEDA ScaledObject kind the operator renders for
// autoscaled knights. The operator handles it as unstructured, so KEDA is not
// a build dependency.
var ScaledObjectGVK = schema.GroupVersionKind{Group: "keda.sh", Version: "v1alpha1", Kind: "ScaledObject"}

// KEDAMode controls whether the operator renders KEDA ScaledObjects.
type KEDAMode string

const (
	// KEDAModeAuto enables autoscaling only when the API server serves
	// keda.sh/v1alpha1 ScaledObject.
	KEDAModeAuto KEDAMode = "auto"
	// KEDAModeEnabled forces autoscaling on without probing.
	KEDAModeEnabled KEDAMode = "true"
	// KEDAModeDisabled forces autoscaling off.
	KEDAModeDisabled KEDAMode = "false"
)

// ParseKEDAMode parses the --enable-keda flag value.
func ParseKEDAMode(s string) (KEDAMode, error) {
	switch m := KEDAMode(strings.ToLower(strings.TrimSpace(s))); m {
	case KEDAModeAuto, KEDAModeEnabled, KEDAModeDisabled:
		return m, nil
	default:
		return "", fmt.Errorf("invalid KEDA mode %q (want auto, true or false)", s)
	}
}

// ScaledObjectAPIServed reports whether the API server serves the KEDA
// ScaledObject kind. A group/version that isn't served at all (KEDA not
// installed) returns false, nil.
func ScaledObjectAPIServed(dc discovery.DiscoveryInterface) (bool, error) {
	gv := ScaledObjectGVK.GroupVersion().String()
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
		if r.Kind == ScaledObjectGVK.Kind && !strings.Contains(r.Name, "/") {
			return true, nil
		}
	}
	return false, nil
}

// ResolveKEDA decides whether to render KEDA ScaledObjects. It returns the
// decision and a short human-readable reason for the startup log.
func ResolveKEDA(mode KEDAMode, dc discovery.DiscoveryInterface) (bool, string, error) {
	switch mode {
	case KEDAModeEnabled:
		return true, "forced on by --enable-keda=true", nil
	case KEDAModeDisabled:
		return false, "disabled by --enable-keda=false", nil
	}
	served, err := ScaledObjectAPIServed(dc)
	if err != nil {
		return false, "", err
	}
	gv := ScaledObjectGVK.GroupVersion().String()
	if served {
		return true, fmt.Sprintf("%s ScaledObject is served", gv), nil
	}
	return false, fmt.Sprintf("%s ScaledObject is not served by the API server", gv), nil
}
