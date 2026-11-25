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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	clienttesting "k8s.io/client-go/testing"
)

func TestCiliumNetworkPolicyAPIServed(t *testing.T) {
	t.Run("not installed", func(t *testing.T) {
		served, err := CiliumNetworkPolicyAPIServed(newFakeDiscovery())
		require.NoError(t, err)
		assert.False(t, served)
	})
	t.Run("served", func(t *testing.T) {
		served, err := CiliumNetworkPolicyAPIServed(newFakeDiscovery(&metav1.APIResourceList{
			GroupVersion: "cilium.io/v2",
			APIResources: []metav1.APIResource{
				{Name: "ciliumnetworkpolicies", Kind: "CiliumNetworkPolicy", Namespaced: true},
				{Name: "ciliumnetworkpolicies/status", Kind: "CiliumNetworkPolicy", Namespaced: true},
			},
		}))
		require.NoError(t, err)
		assert.True(t, served)
	})
	t.Run("group served without CiliumNetworkPolicy", func(t *testing.T) {
		served, err := CiliumNetworkPolicyAPIServed(newFakeDiscovery(&metav1.APIResourceList{
			GroupVersion: "cilium.io/v2",
			APIResources: []metav1.APIResource{{Name: "ciliumendpoints", Kind: "CiliumEndpoint"}},
		}))
		require.NoError(t, err)
		assert.False(t, served)
	})
	t.Run("discovery error is returned", func(t *testing.T) {
		dc := newFakeDiscovery()
		dc.PrependReactor("get", "resource", func(clienttesting.Action) (bool, k8sruntime.Object, error) {
			return true, nil, errors.New("connection refused")
		})
		_, err := CiliumNetworkPolicyAPIServed(dc)
		assert.Error(t, err)
	})
}
