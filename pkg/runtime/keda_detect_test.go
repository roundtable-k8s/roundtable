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

func kedaResourceList() *metav1.APIResourceList {
	return &metav1.APIResourceList{
		GroupVersion: "keda.sh/v1alpha1",
		APIResources: []metav1.APIResource{
			{Name: "scaledobjects", Kind: "ScaledObject", Namespaced: true},
			{Name: "scaledobjects/status", Kind: "ScaledObject", Namespaced: true},
			{Name: "scaledjobs", Kind: "ScaledJob", Namespaced: true},
		},
	}
}

func TestParseKEDAMode(t *testing.T) {
	for in, want := range map[string]KEDAMode{
		"auto": KEDAModeAuto, "Auto": KEDAModeAuto, " true": KEDAModeEnabled, "false": KEDAModeDisabled,
	} {
		got, err := ParseKEDAMode(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}
	_, err := ParseKEDAMode("yes")
	assert.Error(t, err)
}

func TestScaledObjectAPIServed(t *testing.T) {
	t.Run("not installed", func(t *testing.T) {
		served, err := ScaledObjectAPIServed(newFakeDiscovery())
		require.NoError(t, err)
		assert.False(t, served)
	})
	t.Run("served", func(t *testing.T) {
		served, err := ScaledObjectAPIServed(newFakeDiscovery(kedaResourceList()))
		require.NoError(t, err)
		assert.True(t, served)
	})
	t.Run("group served without ScaledObject", func(t *testing.T) {
		served, err := ScaledObjectAPIServed(newFakeDiscovery(&metav1.APIResourceList{
			GroupVersion: "keda.sh/v1alpha1",
			APIResources: []metav1.APIResource{{Name: "scaledjobs", Kind: "ScaledJob"}},
		}))
		require.NoError(t, err)
		assert.False(t, served)
	})
	t.Run("discovery error is returned", func(t *testing.T) {
		dc := newFakeDiscovery()
		dc.PrependReactor("get", "resource", func(clienttesting.Action) (bool, k8sruntime.Object, error) {
			return true, nil, errors.New("connection refused")
		})
		_, err := ScaledObjectAPIServed(dc)
		assert.Error(t, err)
	})
}

func TestResolveKEDA(t *testing.T) {
	absent := newFakeDiscovery()
	present := newFakeDiscovery(kedaResourceList())
	cases := []struct {
		name string
		mode KEDAMode
		dc   *fakediscovery.FakeDiscovery
		want bool
	}{
		{"auto, absent", KEDAModeAuto, absent, false},
		{"auto, present", KEDAModeAuto, present, true},
		{"forced on, absent", KEDAModeEnabled, absent, true},
		{"forced off, present", KEDAModeDisabled, present, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, reason, err := ResolveKEDA(tc.mode, tc.dc)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
			assert.NotEmpty(t, reason)
		})
	}
}
