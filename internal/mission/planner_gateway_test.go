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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// TestValidatePlanTemplatesOnlyForGatewayMissions: a gateway-created mission
// rejects planned knights with their own spec, env or tools, so a caller
// can't reach an image or a Secret through the planner.
func TestValidatePlanTemplatesOnlyForGatewayMissions(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = aiv1alpha1.AddToScheme(scheme)
	rt := &aiv1alpha1.RoundTable{
		ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "default"},
		Spec: aiv1alpha1.RoundTableSpec{
			KnightTemplates: map[string]aiv1alpha1.KnightSpec{"researcher": {Domain: "research"}},
		},
	}
	p := &Planner{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(rt).Build()}

	newMission := func(gateway bool) *aiv1alpha1.Mission {
		m := &aiv1alpha1.Mission{
			ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "default"},
			Spec: aiv1alpha1.MissionSpec{
				Objective:     "research",
				MetaMission:   true,
				RoundTableRef: aiv1alpha1.LocalRef("dev"),
				Planner:       &aiv1alpha1.MissionPlanner{},
			},
		}
		if gateway {
			m.Labels = map[string]string{aiv1alpha1.LabelCreatedBy: aiv1alpha1.CreatedByGateway}
		}
		return m
	}
	plan := func(k PlannerKnight) *PlannerOutput {
		return &PlannerOutput{
			PlanVersion: "v1alpha1",
			Metadata:    PlannerMetadata{Objective: "research"},
			Knights:     []PlannerKnight{k},
			Chains: []PlannerChain{{Name: "c", Steps: []aiv1alpha1.ChainStep{
				{Name: "s", KnightRef: corev1.LocalObjectReference{Name: k.Name}, Task: "do it"},
			}}},
		}
	}

	inline := PlannerKnight{Name: "k", Ephemeral: true, EphemeralSpec: &aiv1alpha1.KnightSpec{Domain: "x", Image: "evil:latest"}}
	env := PlannerKnight{Name: "k", Ephemeral: true, TemplateName: "researcher", SpecOverrides: &aiv1alpha1.KnightSpecOverrides{
		Env: []corev1.EnvVar{{Name: "T", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "other"}, Key: "token"}}}},
	}}
	tools := PlannerKnight{Name: "k", Ephemeral: true, TemplateName: "researcher", SpecOverrides: &aiv1alpha1.KnightSpecOverrides{
		Tools: &aiv1alpha1.KnightTools{Nix: []string{"curl"}},
	}}
	tmpl := PlannerKnight{Name: "k", Ephemeral: true, TemplateName: "researcher",
		SpecOverrides: &aiv1alpha1.KnightSpecOverrides{Model: "sonnet"}}

	ctx := context.Background()
	for name, k := range map[string]PlannerKnight{"ephemeralSpec": inline, "env": env, "tools": tools} {
		err := p.validatePlan(ctx, newMission(true), plan(k))
		if err == nil || !strings.Contains(err.Error(), "gateway-created") {
			t.Errorf("%s: gateway mission accepted the plan: %v", name, err)
		}
		if err := p.validatePlan(ctx, newMission(false), plan(k)); err != nil {
			t.Errorf("%s: ordinary mission rejected the plan: %v", name, err)
		}
	}
	if err := p.validatePlan(ctx, newMission(true), plan(tmpl)); err != nil {
		t.Errorf("template knight rejected on a gateway mission: %v", err)
	}
	if !strings.Contains(p.buildPlanningPrompt(ctx, newMission(true)), "templateName only") {
		t.Error("planning prompt does not tell the planner about the template-only rule")
	}
}
