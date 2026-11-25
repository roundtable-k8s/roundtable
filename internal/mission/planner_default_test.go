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

package mission

import (
	"context"
	"maps"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func plannerTestMission(planner *aiv1alpha1.MissionPlanner) *aiv1alpha1.Mission {
	now := metav1.Now()
	return &aiv1alpha1.Mission{
		ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "default"},
		Spec: aiv1alpha1.MissionSpec{
			Objective:   "plan something",
			MetaMission: true,
			Planner:     planner,
		},
		Status: aiv1alpha1.MissionStatus{
			Phase:     aiv1alpha1.MissionPhasePlanning,
			StartedAt: &now,
		},
	}
}

func roleKnight(name string, extra map[string]string) *aiv1alpha1.Knight {
	labels := map[string]string{aiv1alpha1.LabelRole: "planner"}
	maps.Copy(labels, extra)
	return &aiv1alpha1.Knight{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: labels}}
}

// reconcilePlanning runs ReconcilePlanning twice (initialise the planner and
// planning result, then resolve the planner knight), re-reading the mission
// between runs like the controller does, and returns the stored mission.
func reconcilePlanning(t *testing.T, c client.Client) *aiv1alpha1.Mission {
	t.Helper()
	p := &Planner{Client: c}
	key := types.NamespacedName{Name: "m", Namespace: "default"}
	m := &aiv1alpha1.Mission{}
	for range 2 {
		if err := c.Get(context.Background(), key, m); err != nil {
			t.Fatalf("get mission: %v", err)
		}
		if _, err := p.ReconcilePlanning(context.Background(), m); err != nil {
			t.Fatalf("ReconcilePlanning: %v", err)
		}
	}
	if err := c.Get(context.Background(), key, m); err != nil {
		t.Fatalf("get mission: %v", err)
	}
	return m
}

func plannerTestClient(objs ...client.Object) client.Client {
	scheme := runtime.NewScheme()
	_ = aiv1alpha1.AddToScheme(scheme)
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
		WithStatusSubresource(&aiv1alpha1.Mission{}, &aiv1alpha1.Knight{}).Build()
}

// TestOptionsOnlyPlannerUsesRoleLabelledKnight: spec.planner with only
// options and no knightRef, templateName or ephemeralSpec uses the knight
// labelled ai.roundtable.io/role=planner and keeps the options (#534).
func TestOptionsOnlyPlannerUsesRoleLabelledKnight(t *testing.T) {
	opts := &aiv1alpha1.MissionPlanner{
		TimeoutSeconds:       900,
		MaxChains:            3,
		MaxKnights:           4,
		AllowSkillGeneration: true,
		Context:              "extra context",
	}
	c := plannerTestClient(
		plannerTestMission(opts.DeepCopy()),
		// Another mission's ephemeral planner is not a table planner.
		roleKnight("a-other-planner", map[string]string{aiv1alpha1.LabelEphemeral: "true"}),
		roleKnight("planner", nil),
	)

	m := reconcilePlanning(t, c)

	got := m.Spec.Planner
	if aiv1alpha1.RefName(got.KnightRef) != "planner" {
		t.Fatalf("knightRef = %q, want the role-labelled knight %q", aiv1alpha1.RefName(got.KnightRef), "planner")
	}
	if got.TimeoutSeconds != 900 || got.MaxChains != 3 || got.MaxKnights != 4 ||
		!got.AllowSkillGeneration || got.Context != "extra context" {
		t.Errorf("planner options not kept: %+v", got)
	}
	if m.Status.PlanningResult == nil || m.Status.PlanningResult.Error != "" {
		t.Errorf("planning should not fail, got %+v", m.Status.PlanningResult)
	}
	// No ephemeral planner knight is created for the mission.
	k := &aiv1alpha1.Knight{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "m-planner", Namespace: "default"}, k); err == nil {
		t.Errorf("unexpected ephemeral planner knight m-planner")
	}
}

// TestOptionsOnlyPlannerWithoutLabelledKnightFails: with no role-labelled
// knight the mission's planning fails with an error naming both fixes.
func TestOptionsOnlyPlannerWithoutLabelledKnightFails(t *testing.T) {
	c := plannerTestClient(plannerTestMission(&aiv1alpha1.MissionPlanner{TimeoutSeconds: 900, MaxChains: 3}))

	m := reconcilePlanning(t, c)

	if aiv1alpha1.RefName(m.Spec.Planner.KnightRef) != "" {
		t.Errorf("knightRef = %q, want empty", aiv1alpha1.RefName(m.Spec.Planner.KnightRef))
	}
	if m.Spec.Planner.TimeoutSeconds != 900 || m.Spec.Planner.MaxChains != 3 {
		t.Errorf("planner options changed: %+v", m.Spec.Planner)
	}
	pr := m.Status.PlanningResult
	if pr == nil || pr.Error == "" {
		t.Fatalf("expected a planning error, got %+v", pr)
	}
	for _, want := range []string{"spec.planner.knightRef", "ai.roundtable.io/role=planner", `"default"`} {
		if !strings.Contains(pr.Error, want) {
			t.Errorf("error %q does not mention %q", pr.Error, want)
		}
	}
}

// TestAbsentPlannerUsesDefaults: with spec.planner absent the operator fills
// in the defaults and the role-labelled knight, as before.
func TestAbsentPlannerUsesDefaults(t *testing.T) {
	c := plannerTestClient(plannerTestMission(nil), roleKnight("planner", nil))

	m := reconcilePlanning(t, c)

	got := m.Spec.Planner
	if got == nil {
		t.Fatal("spec.planner not initialised")
	}
	if aiv1alpha1.RefName(got.KnightRef) != "planner" {
		t.Errorf("knightRef = %q, want %q", aiv1alpha1.RefName(got.KnightRef), "planner")
	}
	if got.TimeoutSeconds != 600 || got.MaxChains != 10 || got.MaxKnights != 10 {
		t.Errorf("defaults not applied: %+v", got)
	}
	if m.Status.PlanningResult == nil || m.Status.PlanningResult.Error != "" {
		t.Errorf("planning should not fail, got %+v", m.Status.PlanningResult)
	}
}

// TestExplicitPlannerKnightRefIsKept: a named knightRef is not replaced by
// the role-labelled knight.
func TestExplicitPlannerKnightRefIsKept(t *testing.T) {
	c := plannerTestClient(
		plannerTestMission(&aiv1alpha1.MissionPlanner{KnightRef: aiv1alpha1.LocalRef("mine")}),
		roleKnight("planner", nil),
		&aiv1alpha1.Knight{ObjectMeta: metav1.ObjectMeta{Name: "mine", Namespace: "default"}},
	)

	m := reconcilePlanning(t, c)

	if aiv1alpha1.RefName(m.Spec.Planner.KnightRef) != "mine" {
		t.Errorf("knightRef = %q, want %q", aiv1alpha1.RefName(m.Spec.Planner.KnightRef), "mine")
	}
}
