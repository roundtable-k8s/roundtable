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

package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func mcpKnight(egress *aiv1alpha1.KnightEgress, urls ...string) *aiv1alpha1.Knight {
	k := testEgressKnight("")
	k.Spec.Egress = egress
	for _, u := range urls {
		k.Spec.MCPServers = append(k.Spec.MCPServers, aiv1alpha1.KnightMCPServer{Name: "srv", URL: u})
	}
	return k
}

func TestDeriveMCPEgressInClusterService(t *testing.T) {
	k := mcpKnight(&aiv1alpha1.KnightEgress{},
		"http://miniflux-mcp.selfhosted.svc.cluster.local:8080/mcp",
		"https://web-fetch.tools.svc/mcp",
		"http://plain.roundtable.svc/sse")
	d, errs := deriveMCPEgress(k)
	require.Empty(t, errs)
	require.NotNil(t, d)
	assert.Empty(t, d.AllowedHosts)
	assert.Equal(t, []aiv1alpha1.EgressService{
		{Name: "miniflux-mcp", Namespace: "selfhosted", Ports: []int32{8080}},
		{Name: "web-fetch", Namespace: "tools", Ports: []int32{443}},
		{Name: "plain", Namespace: "roundtable", Ports: []int32{80}},
	}, d.AllowedServices)

	// The derived Service reaches the rendered refs, merged by Service.
	refs := egressServiceRefs(k)
	require.Len(t, refs, 3)
	assert.Equal(t, egressServiceRef{Namespace: "roundtable", Name: "plain", Ports: []int32{80}}, refs[0])
}

func TestDeriveMCPEgressExternalHost(t *testing.T) {
	k := mcpKnight(&aiv1alpha1.KnightEgress{}, "https://MCP.Example.com/mcp", "http://mcp.example.com:8080/sse")
	d, errs := deriveMCPEgress(k)
	require.Empty(t, errs)
	require.NotNil(t, d)
	assert.Empty(t, d.AllowedServices)
	assert.Equal(t, []aiv1alpha1.EgressHost{
		{Host: "mcp.example.com", Ports: []int32{443}},
		{Host: "mcp.example.com", Ports: []int32{8080}},
	}, d.AllowedHosts)

	allow := EgressAllowlist{AllowedHosts: d.AllowedHosts}
	assert.Equal(t, map[string][]int32{"mcp.example.com": {443, 8080}}, allow.hostPorts())
}

func TestDeriveMCPEgressNoDuplicates(t *testing.T) {
	k := mcpKnight(&aiv1alpha1.KnightEgress{
		AllowedHosts: []aiv1alpha1.EgressHostEntry{
			aiv1alpha1.NewEgressHostEntry("mcp.example.com"),
			aiv1alpha1.NewEgressHostEntry("api.example.org", 8443),
			aiv1alpha1.NewEgressHostEntry("*.wild.example"),
		},
		AllowedServices: []aiv1alpha1.EgressService{{Name: "searxng", Namespace: "selfhosted", Ports: []int32{8080}}},
	},
		"https://mcp.example.com/mcp",              // listed on the default port
		"https://api.example.org:8443/x",           // listed with its own port
		"https://a.wild.example/mcp",               // covered by the wildcard
		"http://searxng.selfhosted.svc:8080/mcp",   // listed Service and port
		"http://searxng.selfhosted.svc:8080/other", // repeated URL
	)
	d, errs := deriveMCPEgress(k)
	require.Empty(t, errs)
	assert.Nil(t, d)

	// The same host on another port is not covered.
	k.Spec.MCPServers = k.Spec.MCPServers[:1]
	k.Spec.MCPServers[0].URL = "http://mcp.example.com:8080/mcp"
	d, _ = deriveMCPEgress(k)
	require.NotNil(t, d)
	assert.Equal(t, []aiv1alpha1.EgressHost{{Host: "mcp.example.com", Ports: []int32{8080}}}, d.AllowedHosts)
}

func TestDeriveMCPEgressOptedOut(t *testing.T) {
	// ApplyIsolationDefaults clears spec.egress for isolation.egress: unrestricted.
	k := mcpKnight(nil, "https://mcp.example.com/mcp")
	d, errs := deriveMCPEgress(k)
	assert.Nil(t, d)
	assert.Empty(t, errs)
}

func TestDeriveMCPEgressInvalidURL(t *testing.T) {
	k := mcpKnight(&aiv1alpha1.KnightEgress{},
		"ftp://mcp.example.com/mcp", "https:///nohost", "http://[::1", "https://bad_host.example/mcp",
		"https://ok.example.com/mcp")
	d, errs := deriveMCPEgress(k)
	assert.Len(t, errs, 4)
	require.NotNil(t, d)
	assert.Equal(t, []aiv1alpha1.EgressHost{{Host: "ok.example.com", Ports: []int32{443}}}, d.AllowedHosts)
	assert.ErrorContains(t, errs[0], `mcpServers entry "srv" url "ftp://mcp.example.com/mcp"`)
}

func TestHostCoversOneLabel(t *testing.T) {
	for _, c := range []struct {
		pattern, host string
		want          bool
	}{
		{"*.x.example", "a.x.example", true},
		{"*.x.example", "a.b.x.example", false},
		{"*.x.example", "x.example", false},
		{"*.x.example", ".x.example", false},
		{"x.example", "x.example", true},
		{"x.example", "a.x.example", false},
	} {
		assert.Equal(t, c.want, hostCovers(c.pattern, c.host), "%s vs %s", c.pattern, c.host)
	}
}

func TestDeriveMCPEgressDeepHostUnderWildcard(t *testing.T) {
	k := mcpKnight(&aiv1alpha1.KnightEgress{
		AllowedHosts: []aiv1alpha1.EgressHostEntry{aiv1alpha1.NewEgressHostEntry("*.wild.example")},
	}, "https://a.b.wild.example/mcp")
	d, errs := deriveMCPEgress(k)
	require.Empty(t, errs)
	require.NotNil(t, d)
	assert.Equal(t, []aiv1alpha1.EgressHost{{Host: "a.b.wild.example", Ports: []int32{443}}}, d.AllowedHosts)
}

func TestReconcileEgressDerivedFromMCP(t *testing.T) {
	sch := k8sruntime.NewScheme()
	require.NoError(t, scheme.AddToScheme(sch))
	require.NoError(t, aiv1alpha1.AddToScheme(sch))
	k := mcpKnight(&aiv1alpha1.KnightEgress{}, "https://mcp.example.com/mcp", "http://web-fetch.tools.svc:8080/mcp")
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "tools", Name: "web-fetch"},
		Spec:       corev1.ServiceSpec{Selector: map[string]string{"app": "web-fetch"}},
	}
	c := fake.NewClientBuilder().WithScheme(sch).WithObjects(k, svc).Build()
	r := &KnightReconciler{Client: c, Scheme: sch, CiliumEnabled: true}

	require.NoError(t, r.reconcileEgress(context.Background(), k))
	require.NotNil(t, k.Status.DerivedEgress)
	assert.Equal(t, []aiv1alpha1.EgressHost{{Host: "mcp.example.com", Ports: []int32{443}}}, k.Status.DerivedEgress.AllowedHosts)
	assert.Equal(t, []aiv1alpha1.EgressService{{Name: "web-fetch", Namespace: "tools", Ports: []int32{8080}}}, k.Status.DerivedEgress.AllowedServices)

	cnp := newCiliumEgressPolicy(k)
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(cnp), cnp))
	egress, _, _ := unstructured.NestedSlice(cnp.Object, "spec", "egress")
	var fqdn, endpoint bool
	for _, e := range egress {
		rule := e.(map[string]any)
		if fq, ok := rule["toFQDNs"].([]any); ok {
			for _, f := range fq {
				fqdn = fqdn || f.(map[string]any)["matchName"] == "mcp.example.com"
			}
		}
		if eps, ok := rule["toEndpoints"].([]any); ok {
			lbl := eps[0].(map[string]any)["matchLabels"].(map[string]any)
			endpoint = endpoint || lbl["app"] == "web-fetch"
		}
	}
	assert.True(t, fqdn, "derived host rendered")
	assert.True(t, endpoint, "derived Service rendered")
	cond := meta.FindStatusCondition(k.Status.Conditions, aiv1alpha1.ConditionKnightEgressRestricted)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)

	// A bad MCP URL is reported, with the valid entries still rendered.
	k.Spec.MCPServers = append(k.Spec.MCPServers, aiv1alpha1.KnightMCPServer{Name: "bad", URL: "http://[::1"})
	require.Error(t, r.reconcileEgress(context.Background(), k))
	cond = meta.FindStatusCondition(k.Status.Conditions, aiv1alpha1.ConditionKnightEgressRestricted)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, aiv1alpha1.ReasonEgressPolicyFailed, cond.Reason)
	assert.Contains(t, cond.Message, `mcpServers entry "bad"`)
	require.NotNil(t, k.Status.DerivedEgress)
}

func TestReconcileEgressFailureNamesDroppedHosts(t *testing.T) {
	sch := k8sruntime.NewScheme()
	require.NoError(t, scheme.AddToScheme(sch))
	require.NoError(t, aiv1alpha1.AddToScheme(sch))
	k := mcpKnight(&aiv1alpha1.KnightEgress{
		AllowedHosts:    []aiv1alpha1.EgressHostEntry{aiv1alpha1.NewEgressHostEntry("bad host!")},
		AllowedServices: []aiv1alpha1.EgressService{{Name: "web-fetch", Namespace: "tools"}},
	})
	c := fake.NewClientBuilder().WithScheme(sch).WithObjects(k).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*corev1.Service); ok {
				return errors.New("boom")
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	}).Build()
	r := &KnightReconciler{Client: c, Scheme: sch, CiliumEnabled: true}

	require.Error(t, r.reconcileEgress(context.Background(), k))
	cond := meta.FindStatusCondition(k.Status.Conditions, aiv1alpha1.ConditionKnightEgressRestricted)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, aiv1alpha1.ReasonEgressPolicyFailed, cond.Reason)
	assert.Contains(t, cond.Message, "failed to get allowed Service tools/web-fetch")
	assert.Contains(t, cond.Message, "bad host!")
}
