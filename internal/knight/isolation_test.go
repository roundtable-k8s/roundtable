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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func TestIsolationSettingsFromEnv(t *testing.T) {
	t.Run("unset means gvisor, deny and the default base hosts", func(t *testing.T) {
		assert.Equal(t, DefaultIsolationSettings(), IsolationSettingsFromEnv())
		assert.Equal(t, "gvisor", DefaultIsolationSettings().RuntimeClassName)
		assert.True(t, DefaultIsolationSettings().EgressDeny)
		assert.Equal(t, []string{"openrouter.ai", "api.anthropic.com"}, DefaultIsolationSettings().BaseHosts)
	})
	t.Run("the dapper-cluster off switch", func(t *testing.T) {
		t.Setenv(EnvDefaultRuntimeClass, "")
		t.Setenv(EnvDefaultEgress, "off")
		s := IsolationSettingsFromEnv()
		assert.Empty(t, s.RuntimeClassName)
		assert.False(t, s.EgressDeny)
	})
	t.Run("explicit values", func(t *testing.T) {
		t.Setenv(EnvDefaultRuntimeClass, " kata ")
		t.Setenv(EnvDefaultEgress, "deny")
		t.Setenv(EnvEgressBaseHosts, " API.example.com, ,other.example.com")
		s := IsolationSettingsFromEnv()
		assert.Equal(t, "kata", s.RuntimeClassName)
		assert.True(t, s.EgressDeny)
		assert.Equal(t, []string{"api.example.com", "other.example.com"}, s.BaseHosts)
	})
	t.Run("an empty base host list", func(t *testing.T) {
		t.Setenv(EnvEgressBaseHosts, "")
		assert.Empty(t, IsolationSettingsFromEnv().BaseHosts)
	})
}

func isolationTestKnight() *aiv1alpha1.Knight {
	return &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{
			Name: "iso-knight", Namespace: "default",
			Labels: map[string]string{aiv1alpha1.LabelRoundTable: "iso-table"},
		},
		Spec: aiv1alpha1.KnightSpec{
			Domain: "d",
			NATS:   aiv1alpha1.KnightNATS{URL: "nats://x:4222", Stream: "s", ResultsStream: "r", Subjects: []string{"a.>"}},
		},
	}
}

// effective applies the table and isolation defaults like the reconciler
// and returns the knight and its pod's RuntimeClass.
func effective(t *testing.T, k *aiv1alpha1.Knight, d *aiv1alpha1.RoundTableDefaults, s IsolationSettings) (*aiv1alpha1.Knight, *string) {
	t.Helper()
	sch := runtime.NewScheme()
	require.NoError(t, aiv1alpha1.AddToScheme(sch))
	rt := &aiv1alpha1.RoundTable{
		ObjectMeta: metav1.ObjectMeta{Name: "iso-table", Namespace: "default"},
		Spec:       aiv1alpha1.RoundTableSpec{Defaults: d},
	}
	r := fake.NewClientBuilder().WithScheme(sch).WithObjects(rt).Build()
	ctx := context.Background()
	ApplyTableDefaults(ctx, r, k)
	ApplyIsolationDefaults(k, s)
	return k, NewPodBuilder(k, "img").Build(ctx).RuntimeClassName
}

func TestApplyIsolationDefaults(t *testing.T) {
	def := DefaultIsolationSettings()
	str := func(s string) *string { return &s }

	t.Run("no fields set: gvisor and an empty allowlist", func(t *testing.T) {
		k, rc := effective(t, isolationTestKnight(), nil, def)
		assert.Equal(t, str("gvisor"), rc)
		require.NotNil(t, k.Spec.Egress)
		assert.Empty(t, k.Spec.Egress.AllowedHosts)
	})
	t.Run("isolation.runtime none: no RuntimeClass, even if one is set", func(t *testing.T) {
		k := isolationTestKnight()
		k.Spec.RuntimeClassName = str("kata")
		k.Spec.Isolation = &aiv1alpha1.KnightIsolation{Runtime: "none", Reason: "needs ptrace for the debugger"}
		k, rc := effective(t, k, &aiv1alpha1.RoundTableDefaults{RuntimeClassName: str("gvisor")}, def)
		assert.Nil(t, rc)
		assert.NotNil(t, k.Spec.Egress, "egress stays default-deny")
	})
	t.Run("isolation.egress unrestricted: no policy, even with a table default", func(t *testing.T) {
		k := isolationTestKnight()
		k.Spec.Isolation = &aiv1alpha1.KnightIsolation{Egress: "unrestricted", Reason: "crawls arbitrary sites"}
		k, rc := effective(t, k, &aiv1alpha1.RoundTableDefaults{
			Egress: &aiv1alpha1.KnightEgress{AllowedHosts: aiv1alpha1.EgressHostNames("a.example.com")},
		}, def)
		assert.Nil(t, k.Spec.Egress)
		assert.Equal(t, str("gvisor"), rc, "runtime stays isolated")
	})
	t.Run("the knight's values beat the table and the operator default", func(t *testing.T) {
		k := isolationTestKnight()
		k.Spec.RuntimeClassName = str("kata")
		k.Spec.Egress = &aiv1alpha1.KnightEgress{AllowedHosts: aiv1alpha1.EgressHostNames("k.example.com")}
		k, rc := effective(t, k, &aiv1alpha1.RoundTableDefaults{
			RuntimeClassName: str("other"),
			Egress:           &aiv1alpha1.KnightEgress{AllowedHosts: aiv1alpha1.EgressHostNames("t.example.com")},
		}, def)
		assert.Equal(t, str("kata"), rc)
		assert.Equal(t, aiv1alpha1.EgressHostNames("k.example.com"), k.Spec.Egress.AllowedHosts)
	})
	t.Run("the table's values beat the operator default", func(t *testing.T) {
		k, rc := effective(t, isolationTestKnight(), &aiv1alpha1.RoundTableDefaults{
			RuntimeClassName: str("kata"),
			Egress:           &aiv1alpha1.KnightEgress{AllowedHosts: aiv1alpha1.EgressHostNames("t.example.com")},
		}, def)
		assert.Equal(t, str("kata"), rc)
		assert.Equal(t, aiv1alpha1.EgressHostNames("t.example.com"), k.Spec.Egress.AllowedHosts)
	})
	t.Run("a table runtimeClassName of empty string keeps the cluster default", func(t *testing.T) {
		_, rc := effective(t, isolationTestKnight(), &aiv1alpha1.RoundTableDefaults{RuntimeClassName: str("")}, def)
		assert.Nil(t, rc)
	})
	t.Run("operator defaults off: nothing applied, explicit values honoured", func(t *testing.T) {
		k, rc := effective(t, isolationTestKnight(), nil, IsolationSettings{})
		assert.Nil(t, rc)
		assert.Nil(t, k.Spec.Egress)

		k = isolationTestKnight()
		k.Spec.RuntimeClassName = str("gvisor")
		k.Spec.Egress = &aiv1alpha1.KnightEgress{}
		k, rc = effective(t, k, nil, IsolationSettings{})
		assert.Equal(t, str("gvisor"), rc)
		assert.NotNil(t, k.Spec.Egress)
	})
	t.Run("isolation default values change nothing", func(t *testing.T) {
		k := isolationTestKnight()
		k.Spec.Isolation = &aiv1alpha1.KnightIsolation{Runtime: "default", Egress: "default"}
		k, rc := effective(t, k, nil, def)
		assert.Equal(t, str("gvisor"), rc)
		assert.NotNil(t, k.Spec.Egress)
	})
}

func TestArsenalGitHosts(t *testing.T) {
	k := isolationTestKnight()
	assert.Equal(t, []GitHost{{Host: "github.com", Port: 443}}, ArsenalGitHosts(k), "empty list = default platform arsenal")

	k.Spec.Arsenals = []aiv1alpha1.KnightArsenal{
		{}, // default repo
		{Repo: "https://GitHub.com/org/other"},
		{Repo: "git@git.example.org:team/skills.git"},
		{Repo: "ssh://git@gitea.example.net:2222/x/y"},
		{Repo: "https://code.example.com:8443/x"},
		{Repo: "not a url"},
	}
	assert.Equal(t, []GitHost{
		{Host: "code.example.com", Port: 8443},
		{Host: "git.example.org", Port: 22},
		{Host: "gitea.example.net", Port: 2222},
		{Host: "github.com", Port: 443},
	}, ArsenalGitHosts(k))
}

func TestApplyTableDefaultsArsenals(t *testing.T) {
	tableArsenals := []aiv1alpha1.KnightArsenal{
		{Name: "team", Repo: "https://github.com/team/skills"},
		{Name: "pack", Repo: "https://github.com/team/pack"},
	}
	defaults := &aiv1alpha1.RoundTableDefaults{Arsenals: tableArsenals}

	t.Run("inherited when the knight sets none", func(t *testing.T) {
		k, _ := effective(t, isolationTestKnight(), defaults, DefaultIsolationSettings())
		assert.Equal(t, tableArsenals, k.Spec.Arsenals)
		sidecars := NewPodBuilder(k, "img").WithArsenal().WithGitSync().sidecars
		names := make([]string, 0, len(sidecars))
		for _, c := range sidecars {
			names = append(names, c.Name)
		}
		assert.Equal(t, []string{"git-sync-team", "git-sync-pack"}, names)
	})
	t.Run("not inherited when the knight sets its own", func(t *testing.T) {
		own := isolationTestKnight()
		own.Spec.Arsenals = []aiv1alpha1.KnightArsenal{{Name: "mine", Repo: "https://github.com/me/skills"}}
		k, _ := effective(t, own, defaults, DefaultIsolationSettings())
		assert.Equal(t, []aiv1alpha1.KnightArsenal{{Name: "mine", Repo: "https://github.com/me/skills"}}, k.Spec.Arsenals)
	})
	t.Run("no table default and no knight list leaves it empty (pod builder defaults it)", func(t *testing.T) {
		k, _ := effective(t, isolationTestKnight(), nil, DefaultIsolationSettings())
		assert.Empty(t, k.Spec.Arsenals)
	})
}
