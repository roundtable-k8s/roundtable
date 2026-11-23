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

package v1alpha1

import corev1 "k8s.io/api/core/v1"

// RefName returns the name an optional object reference points at, or ""
// when the reference is unset.
func RefName(ref *corev1.LocalObjectReference) string {
	if ref == nil {
		return ""
	}
	return ref.Name
}

// LocalRef returns a reference to the named object in the same namespace, or
// nil when name is empty.
func LocalRef(name string) *corev1.LocalObjectReference {
	if name == "" {
		return nil
	}
	return &corev1.LocalObjectReference{Name: name}
}
