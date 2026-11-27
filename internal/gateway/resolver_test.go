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
	"context"
	"errors"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func TestTriggerTable(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := aiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	meta := func(name string) metav1.ObjectMeta { return metav1.ObjectMeta{Name: name, Namespace: "ns"} }
	k8s := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		&aiv1alpha1.Chain{ObjectMeta: meta("with-table"), Spec: aiv1alpha1.ChainSpec{RoundTableRef: aiv1alpha1.LocalRef("chains")}},
		&aiv1alpha1.Chain{ObjectMeta: meta("no-table")},
		&aiv1alpha1.Knight{ObjectMeta: metav1.ObjectMeta{Name: "k", Namespace: "ns",
			Labels: map[string]string{aiv1alpha1.LabelRoundTable: "knights"}}},
		&aiv1alpha1.RoundTable{ObjectMeta: meta("knights"), Spec: aiv1alpha1.RoundTableSpec{
			NATS: aiv1alpha1.RoundTableNATS{SubjectPrefix: "kn"}}},
	).Build()
	r := &Resolver{Client: k8s}
	trigger := func(ref string, target aiv1alpha1.TriggerTarget) *aiv1alpha1.Trigger {
		return &aiv1alpha1.Trigger{ObjectMeta: meta("t"), Spec: aiv1alpha1.TriggerSpec{
			RoundTableRef: aiv1alpha1.LocalRef(ref), Target: target}}
	}
	chain := func(name string) aiv1alpha1.TriggerTarget {
		return aiv1alpha1.TriggerTarget{ChainRef: &corev1.LocalObjectReference{Name: name}}
	}
	for _, tc := range []struct {
		name string
		trg  *aiv1alpha1.Trigger
		want string
	}{
		{"roundTableRef wins", trigger("own", chain("with-table")), "own"},
		{"target chain's table", trigger("", chain("with-table")), "chains"},
		{"chain without a table", trigger("", chain("no-table")), "default"},
		{"target knight's table", trigger("", aiv1alpha1.TriggerTarget{Knight: &aiv1alpha1.TriggerKnightTarget{Name: "k"}}), "knights"},
	} {
		got, err := r.TriggerTable(context.Background(), tc.trg)
		if err != nil || got != tc.want {
			t.Errorf("%s: TriggerTable = %q, %v; want %q", tc.name, got, err, tc.want)
		}
	}
	if _, err := r.TriggerTable(context.Background(), trigger("", chain("missing"))); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing chain: err = %v, want ErrNotFound", err)
	}
}

func TestResolverKnightTable(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := aiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	knight := func(name string, labels map[string]string) *aiv1alpha1.Knight {
		return &aiv1alpha1.Knight{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", Labels: labels}}
	}
	table := func(name string) *aiv1alpha1.RoundTable {
		return &aiv1alpha1.RoundTable{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
			Spec: aiv1alpha1.RoundTableSpec{NATS: aiv1alpha1.RoundTableNATS{SubjectPrefix: name}}}
	}
	ownerless := knight("ownerless", nil)
	ownerless.Spec.NATS.Subjects = []string{"own.tasks.>"}
	k8s := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		table("labelled"), table("legacy"),
		knight("by-label", map[string]string{aiv1alpha1.LabelRoundTable: "labelled"}),
		knight("by-legacy", map[string]string{knightpkg.LegacyTableLabel: "legacy"}),
		ownerless,
	).Build()
	r := &Resolver{Client: k8s}
	for _, tc := range []struct{ name, knight, want string }{
		{"table label", "by-label", "labelled"},
		{"legacy label", "by-legacy", "legacy"},
		{"ownerless knight", "ownerless", ""},
	} {
		got, err := r.KnightTable(context.Background(), "ns", tc.knight)
		if err != nil || got != tc.want {
			t.Errorf("%s: KnightTable = %q, %v; want %q", tc.name, got, err, tc.want)
		}
	}
	if _, err := r.KnightTable(context.Background(), "ns", "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing knight: err = %v, want ErrNotFound", err)
	}
}
