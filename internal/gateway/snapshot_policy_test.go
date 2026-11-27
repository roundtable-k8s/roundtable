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

package gateway

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// gatewaySA is the gateway ServiceAccount of `helm template rt -n roundtable`.
const gatewaySA = "system:serviceaccount:" + testNS + ":rt-roundtable-operator-gateway"

// renderGatewayRBAC renders the chart's gateway template with snapshots on
// and returns its RBAC objects and the snapshot ValidatingAdmissionPolicy and
// binding.
func renderGatewayRBAC(t *testing.T, helm string) []*unstructured.Unstructured {
	t.Helper()
	chart := filepath.Join("..", "..", "charts", "roundtable-operator")
	out, err := exec.Command(helm, "template", "rt", chart, "--namespace", testNS,
		"--set", "gateway.knightSnapshots.enabled=true", "--show-only", "templates/gateway.yaml").Output()
	if err != nil {
		t.Fatalf("helm template: %v", err)
	}
	keep := map[string]bool{"Role": true, "RoleBinding": true, "ClusterRole": true, "ClusterRoleBinding": true,
		"ValidatingAdmissionPolicy": true, "ValidatingAdmissionPolicyBinding": true}
	dec := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(out), 4096)
	var objs []*unstructured.Unstructured
	for {
		u := &unstructured.Unstructured{}
		if err := dec.Decode(&u.Object); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("decode render: %v", err)
		}
		if keep[u.GetKind()] {
			objs = append(objs, u)
		}
	}
	return objs
}

// TestSnapshotPolicy loads the chart's snapshot ValidatingAdmissionPolicy into
// an API server and checks that the gateway ServiceAccount can still request
// a snapshot but cannot change anything else on a Knight.
func TestSnapshotPolicy(t *testing.T) {
	// In CI a missing prerequisite must fail loudly, not skip: the policy
	// would otherwise go untested without anyone noticing.
	unmet := t.Skipf
	if os.Getenv("CI") != "" {
		unmet = t.Fatalf
	}
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		unmet("KUBEBUILDER_ASSETS not set (run through make test)")
	}
	helm := os.Getenv("HELM")
	if helm == "" {
		helm = "helm"
	}
	if _, err := exec.LookPath(helm); err != nil {
		unmet("helm not found: %v", err)
	}
	objs := renderGatewayRBAC(t, helm)

	testEnv := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true}
	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testEnv.Stop() })

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = aiv1alpha1.AddToScheme(scheme)
	ctx := t.Context()
	admin, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNS}}); err != nil {
		t.Fatal(err)
	}
	kinds := make([]string, 0, len(objs))
	for _, o := range objs {
		if err := admin.Create(ctx, o); err != nil {
			t.Fatalf("create %s %s: %v", o.GetKind(), o.GetName(), err)
		}
		kinds = append(kinds, o.GetKind())
	}
	for _, want := range []string{"ValidatingAdmissionPolicy", "ValidatingAdmissionPolicyBinding", "Role"} {
		if !strings.Contains(strings.Join(kinds, ","), want) {
			t.Fatalf("render lacks a %s: %v", want, kinds)
		}
	}

	knight := &aiv1alpha1.Knight{
		ObjectMeta: metav1.ObjectMeta{Name: "galahad", Namespace: testNS,
			Labels: map[string]string{"team": "a"}, Annotations: map[string]string{"note": "x"}},
		Spec: aiv1alpha1.KnightSpec{Domain: "security", Model: "m", Skills: []string{"shared"},
			NATS: aiv1alpha1.KnightNATS{URL: "nats://nats.test:4222", Subjects: []string{"t.tasks.security.>"}}},
	}
	if err := admin.Create(ctx, knight); err != nil {
		t.Fatal(err)
	}

	gwCfg := rest.CopyConfig(cfg)
	gwCfg.Impersonate = rest.ImpersonationConfig{UserName: gatewaySA}
	gw, err := client.New(gwCfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	key := types.NamespacedName{Namespace: testNS, Name: "galahad"}
	// patchAs merge-patches the knight as c after mutate and returns the error.
	patchAs := func(c client.Client, mutate func(*aiv1alpha1.Knight)) error {
		k := &aiv1alpha1.Knight{}
		if err := c.Get(ctx, key, k); err != nil {
			t.Fatalf("get knight: %v", err)
		}
		base := k.DeepCopy()
		mutate(k)
		return c.Patch(ctx, k, client.MergeFrom(base))
	}
	denied := func(err error) bool {
		return err != nil && (apierrors.IsForbidden(err) || apierrors.IsInvalid(err)) &&
			strings.Contains(err.Error(), "rt-roundtable-operator-gateway-snapshots")
	}

	// The policy takes effect once the API server has loaded it.
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := patchAs(gw, func(k *aiv1alpha1.Knight) { k.Spec.Model = "other" })
		if denied(err) {
			break
		}
		if err == nil {
			// Allowed before the policy loaded: undo and retry.
			if err := patchAs(admin, func(k *aiv1alpha1.Knight) { k.Spec.Model = "m" }); err != nil {
				t.Fatal(err)
			}
		} else if !apierrors.IsForbidden(err) || strings.Contains(err.Error(), "cannot patch") {
			t.Fatalf("spec patch: unexpected error %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("spec patch by the gateway not denied by the policy: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	baseline := func(k *aiv1alpha1.Knight) {
		k.Spec.Model, k.Spec.Suspend, k.Finalizers = "m", false, nil
		k.Labels = map[string]string{"team": "a"}
		k.Annotations = map[string]string{"note": "x"}
	}
	for _, c := range []struct {
		name   string
		mutate func(*aiv1alpha1.Knight)
	}{
		{"spec", func(k *aiv1alpha1.Knight) { k.Spec.Suspend = true }},
		{"new label", func(k *aiv1alpha1.Knight) { k.Labels["roundtable.io/x"] = "y" }},
		{"removed label", func(k *aiv1alpha1.Knight) { delete(k.Labels, "team") }},
		{"other annotation", func(k *aiv1alpha1.Knight) { k.Annotations["note"] = "y" }},
		{"new annotation", func(k *aiv1alpha1.Knight) { k.Annotations["extra"] = "y" }},
		{"removed annotation", func(k *aiv1alpha1.Knight) { delete(k.Annotations, "note") }},
		{"finalizer", func(k *aiv1alpha1.Knight) { k.Finalizers = append(k.Finalizers, "example.com/hold") }},
		{"snapshot and spec", func(k *aiv1alpha1.Knight) {
			k.Annotations[aiv1alpha1.AnnotationKnightSnapshot] = "1"
			k.Spec.Model = "x"
		}},
	} {
		if err := patchAs(gw, c.mutate); !denied(err) {
			t.Errorf("%s: gateway patch not denied by the policy: %v", c.name, err)
			if err := patchAs(admin, baseline); err != nil {
				t.Fatal(err)
			}
		}
	}

	// The gateway's own snapshot request goes through.
	svc := NewService(&Resolver{Client: gw}, nil, Options{Namespace: testNS, MaxTimeout: time.Minute}, logr.Discard())
	svc.Knights = gw
	req, err := svc.SnapshotKnight(ctx, "", "galahad", nil)
	if err != nil {
		t.Fatalf("SnapshotKnight with the policy in place: %v", err)
	}
	got := &aiv1alpha1.Knight{}
	if err := admin.Get(ctx, key, got); err != nil {
		t.Fatal(err)
	}
	if got.Annotations[aiv1alpha1.AnnotationKnightSnapshot] != req.Request {
		t.Errorf("snapshot annotation = %q, want %q", got.Annotations[aiv1alpha1.AnnotationKnightSnapshot], req.Request)
	}
	if got.Spec.Model != "m" || got.Labels["team"] != "a" || got.Annotations["note"] != "x" {
		t.Errorf("knight changed beyond the snapshot annotation: %+v", got.ObjectMeta)
	}

	// Others (here the admin) are not matched by the policy.
	if err := patchAs(admin, func(k *aiv1alpha1.Knight) { k.Spec.Model = "admin" }); err != nil {
		t.Errorf("admin spec patch denied: %v", err)
	}
}
