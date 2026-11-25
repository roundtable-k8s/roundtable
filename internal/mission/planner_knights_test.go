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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/knight"
)

func TestPlanningPromptListsKnightsByEitherTableLabel(t *testing.T) {
	s := runtime.NewScheme()
	_ = aiv1alpha1.AddToScheme(s)
	mk := func(name string, labels map[string]string) *aiv1alpha1.Knight {
		return &aiv1alpha1.Knight{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: labels},
			Spec:       aiv1alpha1.KnightSpec{Domain: name + "-domain"},
		}
	}
	rt := &aiv1alpha1.RoundTable{ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: "default"}}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(rt,
		mk("current-only", map[string]string{aiv1alpha1.LabelRoundTable: "dev"}),
		mk("legacy-only", map[string]string{knight.LegacyTableLabel: "dev"}),
		mk("both", map[string]string{aiv1alpha1.LabelRoundTable: "dev", knight.LegacyTableLabel: "dev"}),
		mk("other-table", map[string]string{aiv1alpha1.LabelRoundTable: "other"}),
		mk("unlabelled", nil),
	).Build()
	p := &Planner{Client: c}
	m := &aiv1alpha1.Mission{
		ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "default"},
		Spec: aiv1alpha1.MissionSpec{
			Objective:       "x",
			RecruitExisting: true,
			RoundTableRef:   aiv1alpha1.LocalRef("dev"),
			Planner:         &aiv1alpha1.MissionPlanner{},
		},
	}
	out := p.buildPlanningPrompt(context.Background(), m)
	for _, name := range []string{"current-only", "legacy-only", "both"} {
		if got := strings.Count(out, "- "+name+" (domain:"); got != 1 {
			t.Errorf("knight %s listed %d times, want 1\n%s", name, got, out)
		}
	}
	for _, name := range []string{"other-table", "unlabelled"} {
		if strings.Contains(out, "- "+name+" (") {
			t.Errorf("knight %s should not be listed", name)
		}
	}
}
