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
	fakediscovery "k8s.io/client-go/discovery/fake"
	clienttesting "k8s.io/client-go/testing"
)

func newFakeDiscovery(resources ...*metav1.APIResourceList) *fakediscovery.FakeDiscovery {
	return &fakediscovery.FakeDiscovery{Fake: &clienttesting.Fake{Resources: resources}}
}

func sandboxResourceList(gv string) *metav1.APIResourceList {
	return &metav1.APIResourceList{
		GroupVersion: gv,
		APIResources: []metav1.APIResource{
			{Name: "sandboxes", Kind: "Sandbox", Namespaced: true},
			{Name: "sandboxes/status", Kind: "Sandbox", Namespaced: true},
		},
	}
}

func TestParseSandboxMode(t *testing.T) {
	for in, want := range map[string]SandboxMode{
		"auto": SandboxModeAuto, "AUTO": SandboxModeAuto, " true ": SandboxModeEnabled, "false": SandboxModeDisabled,
	} {
		got, err := ParseSandboxMode(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}
	_, err := ParseSandboxMode("maybe")
	assert.Error(t, err)
}

func TestSandboxAPIServed(t *testing.T) {
	t.Run("not installed", func(t *testing.T) {
		served, err := SandboxAPIServed(newFakeDiscovery())
		require.NoError(t, err)
		assert.False(t, served)
	})
	t.Run("only v1beta1 served (agent-sandbox >= v1.0)", func(t *testing.T) {
		served, err := SandboxAPIServed(newFakeDiscovery(sandboxResourceList("agents.x-k8s.io/v1beta1")))
		require.NoError(t, err)
		assert.False(t, served)
	})
	t.Run("v1alpha1 served", func(t *testing.T) {
		served, err := SandboxAPIServed(newFakeDiscovery(sandboxResourceList("agents.x-k8s.io/v1alpha1")))
		require.NoError(t, err)
		assert.True(t, served)
	})
	t.Run("group served without Sandbox kind", func(t *testing.T) {
		served, err := SandboxAPIServed(newFakeDiscovery(&metav1.APIResourceList{
			GroupVersion: "agents.x-k8s.io/v1alpha1",
			APIResources: []metav1.APIResource{{Name: "sandboxtemplates", Kind: "SandboxTemplate"}},
		}))
		require.NoError(t, err)
		assert.False(t, served)
	})
	t.Run("discovery error is returned", func(t *testing.T) {
		dc := newFakeDiscovery()
		dc.PrependReactor("get", "resource", func(clienttesting.Action) (bool, k8sruntime.Object, error) {
			return true, nil, errors.New("connection refused")
		})
		_, err := SandboxAPIServed(dc)
		assert.Error(t, err)
	})
}

func TestResolveSandboxBackend(t *testing.T) {
	absent := newFakeDiscovery()
	present := newFakeDiscovery(sandboxResourceList("agents.x-k8s.io/v1alpha1"))

	cases := []struct {
		name string
		mode SandboxMode
		dc   *fakediscovery.FakeDiscovery
		want bool
	}{
		{"auto, absent", SandboxModeAuto, absent, false},
		{"auto, present", SandboxModeAuto, present, true},
		{"forced on, absent", SandboxModeEnabled, absent, true},
		{"forced off, present", SandboxModeDisabled, present, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, reason, err := ResolveSandboxBackend(tc.mode, tc.dc)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
			assert.NotEmpty(t, reason)
		})
	}
}
