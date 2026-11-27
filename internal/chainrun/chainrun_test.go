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

package chainrun

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func testScheme(t *testing.T) *runtime.Scheme {
	s := runtime.NewScheme()
	if err := aiv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func template() *aiv1alpha1.Chain {
	return &aiv1alpha1.Chain{
		ObjectMeta: metav1.ObjectMeta{Name: "tmpl", Namespace: "ns"},
		Spec: aiv1alpha1.ChainSpec{
			Schedule: "* * * * *", Suspend: true, Input: "old",
			StartingDeadlineSeconds: func() *int64 { v := int64(5); return &v }(),
		},
	}
}

func TestStartNewCopy(t *testing.T) {
	s := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).Build()
	owner := &aiv1alpha1.RoundTable{ObjectMeta: metav1.ObjectMeta{Name: "rt", Namespace: "ns", UID: "u1"}}
	got, err := Start(context.Background(), c, s, template(), StartOptions{
		Name: "run-1", Namespace: "ns", Input: "in", Trigger: "t1", Source: SourceTrigger,
		Labels: map[string]string{"a": "b"}, Annotations: map[string]string{"x": "y"}, Owner: owner,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Spec.Input != "in" || got.Spec.Schedule != "" || got.Spec.Suspend || got.Spec.StartingDeadlineSeconds != nil {
		t.Fatalf("spec not reset: %+v", got.Spec)
	}
	if got.Labels[LabelRunSource] != SourceTrigger || got.Labels["a"] != "b" {
		t.Fatalf("labels: %v", got.Labels)
	}
	if got.Annotations[aiv1alpha1.AnnotationChainRun] != "t1" || got.Annotations["x"] != "y" {
		t.Fatalf("annotations: %v", got.Annotations)
	}
	if len(got.OwnerReferences) != 1 || got.OwnerReferences[0].Name != "rt" {
		t.Fatalf("owner refs: %v", got.OwnerReferences)
	}
	var stored aiv1alpha1.Chain
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "run-1"}, &stored); err != nil {
		t.Fatal(err)
	}
}

func TestStartRerunExisting(t *testing.T) {
	s := testScheme(t)
	existing := &aiv1alpha1.Chain{ObjectMeta: metav1.ObjectMeta{Name: "run-1", Namespace: "ns",
		Annotations: map[string]string{aiv1alpha1.AnnotationChainRun: "t1", "keep": "me"}}}
	existing.Spec.Input = "in"
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(existing).Build()
	tmpl := template()
	tmpl.Spec.Input = "ignored"
	got, err := Start(context.Background(), c, s, tmpl, StartOptions{Input: "in2", Trigger: "t2", Source: SourceGateway, Existing: existing})
	if err != nil {
		t.Fatal(err)
	}
	if got.Spec.Input != "in2" || got.Spec.Schedule != "" || got.Annotations[aiv1alpha1.AnnotationChainRun] != "t2" ||
		got.Annotations["keep"] != "me" || got.Labels[LabelRunSource] != SourceGateway {
		t.Fatalf("unexpected rerun: %+v", got.ObjectMeta)
	}
	var stored aiv1alpha1.Chain
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "run-1"}, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Spec.Input != "in2" || stored.Annotations[aiv1alpha1.AnnotationChainRun] != "t2" {
		t.Fatalf("patch not persisted: %+v", stored.ObjectMeta)
	}
}

func TestName(t *testing.T) {
	if got := Name("pr-pipeline", "My_Repo.go", "1"); got != "pr-pipeline-my-repo-go-1" {
		t.Fatal(got)
	}
	long := Name(strings.Repeat("a", 250), "r", "12345")
	if len(long) > 253 || !strings.HasSuffix(long, "-12345") {
		t.Fatalf("%d %s", len(long), long)
	}
	if got := Name("base"); got != "base" {
		t.Fatal(got)
	}
}

var dnsName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`)

func TestNameEmptyLastPart(t *testing.T) {
	cases := []struct {
		name  string
		base  string
		parts []string
		want  string
	}{
		{"empty last part drops separator", "pr-pipeline", []string{"mid", ""}, "pr-pipeline-mid"},
		{"unsafe-only last part drops separator", "pr-pipeline", []string{"!!!"}, "pr-pipeline"},
		{"empty base falls back to hash of last part", "", []string{"!!!"}, KeyHash("!!!")},
		{"empty base and empty last part hash the empty part", "", []string{""}, KeyHash("")},
		{"no parts and empty base", "", nil, ""},
	}
	for _, tc := range cases {
		got := Name(tc.base, tc.parts...)
		if got != tc.want {
			t.Fatalf("%s: got %q, want %q", tc.name, got, tc.want)
		}
		if got != "" && (!dnsName.MatchString(got) || len(got) > 253) {
			t.Fatalf("%s: invalid name %q", tc.name, got)
		}
	}
	if Name("", "!!!") == Name("", "???") {
		t.Fatal("different unsafe last parts collided on an empty name")
	}
}

func TestNameOverlongLastPart(t *testing.T) {
	for _, n := range []int{252, 253, 300} {
		got := Name("base", "mid", strings.Repeat("a", n))
		if len(got) > 253 || got == "" || strings.HasPrefix(got, "-") || strings.HasSuffix(got, "-") {
			t.Fatalf("last part %d: invalid name %q (len %d)", n, got, len(got))
		}
		if want := strings.Repeat("a", min(n, 252)); !strings.HasSuffix(got, want) {
			t.Fatalf("last part %d: tail not kept", n)
		}
	}
}

func TestActiveExpired(t *testing.T) {
	c := &aiv1alpha1.Chain{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{aiv1alpha1.AnnotationChainRun: "2"}}}
	c.Status.LastRunTrigger = "1"
	c.Status.Phase = aiv1alpha1.ChainPhaseSucceeded
	if !Active(c) {
		t.Fatal("pending run should be active")
	}
	c.Status.LastRunTrigger = "2"
	if Active(c) {
		t.Fatal("should be idle")
	}
	c.Status.Phase = aiv1alpha1.ChainPhaseRunning
	if !Active(c) {
		t.Fatal("running should be active")
	}

	now := time.Now()
	ttl := int32(60)
	if Expired(c, &ttl, now) {
		t.Fatal("no completedAt")
	}
	done := metav1.NewTime(now.Add(-61 * time.Second))
	c.Status.CompletedAt = &done
	if !Expired(c, &ttl, now) || Expired(c, nil, now) {
		t.Fatal("ttl handling")
	}
	zero := int32(0)
	if Expired(c, &zero, now) {
		t.Fatal("zero ttl never expires")
	}
	recent := metav1.NewTime(now.Add(-10 * time.Second))
	c.Status.CompletedAt = &recent
	if Expired(c, &ttl, now) {
		t.Fatal("not yet expired")
	}
}
