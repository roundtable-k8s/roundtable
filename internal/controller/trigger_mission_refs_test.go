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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func TestCheckMissionRefsDefaultPlanner(t *testing.T) {
	const ns = "ns"
	scheme := runtime.NewScheme()
	if err := aiv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	table := func(planner *aiv1alpha1.RoundTablePlanner) *aiv1alpha1.RoundTable {
		return &aiv1alpha1.RoundTable{ObjectMeta: metav1.ObjectMeta{Name: "rt", Namespace: ns}, Spec: aiv1alpha1.RoundTableSpec{Planner: planner}}
	}
	plannerKnight := &aiv1alpha1.Knight{ObjectMeta: metav1.ObjectMeta{Name: "rt-planner", Namespace: ns}}
	cases := []struct {
		name     string
		objs     []client.Object
		mission  aiv1alpha1.TriggerMissionTarget
		wantMsg  string // substring of the not-found message; empty means ok
		wantFind bool
	}{
		{"default planner exists", []client.Object{table(nil), plannerKnight}, aiv1alpha1.TriggerMissionTarget{}, "", false},
		{"default planner missing", []client.Object{table(nil)}, aiv1alpha1.TriggerMissionTarget{}, "no default planner", true},
		{"planner disabled", []client.Object{table(&aiv1alpha1.RoundTablePlanner{Enabled: new(false)})}, aiv1alpha1.TriggerMissionTarget{}, "no default planner", true},
		{"not a meta-mission", []client.Object{table(nil)}, aiv1alpha1.TriggerMissionTarget{MetaMission: new(false)}, "", false},
		{"planner without a knight", []client.Object{table(nil)}, aiv1alpha1.TriggerMissionTarget{Planner: &aiv1alpha1.TriggerMissionPlanner{MaxChains: 2}}, "no default planner", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &TriggerReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.objs...).Build()}
			trg := &aiv1alpha1.Trigger{
				ObjectMeta: metav1.ObjectMeta{Name: "t", Namespace: ns},
				Spec: aiv1alpha1.TriggerSpec{
					RoundTableRef: &corev1.LocalObjectReference{Name: "rt"},
					Target:        aiv1alpha1.TriggerTarget{Mission: &tc.mission},
				},
			}
			reason, msg, err := r.checkMissionRefs(context.Background(), trg)
			if err != nil {
				t.Fatal(err)
			}
			if !tc.wantFind {
				if reason != "" {
					t.Fatalf("unexpected %s: %s", reason, msg)
				}
				return
			}
			if reason != aiv1alpha1.ReasonTriggerTargetNotFound || !strings.Contains(msg, tc.wantMsg) {
				t.Fatalf("got %q %q, want TargetNotFound containing %q", reason, msg, tc.wantMsg)
			}
		})
	}
}
