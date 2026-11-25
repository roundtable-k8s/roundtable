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
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func admissionMission(name string, age time.Duration, table string, phase aiv1alpha1.MissionPhase) *aiv1alpha1.Mission {
	m := &aiv1alpha1.Mission{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "default", UID: types.UID(name),
			CreationTimestamp: metav1.NewTime(time.Now().Add(-age).Truncate(time.Second)),
		},
		Status: aiv1alpha1.MissionStatus{Phase: phase},
	}
	if table != "" {
		m.Spec.RoundTableRef = aiv1alpha1.LocalRef(table)
	}
	return m
}

func admissionRT(max int32) *aiv1alpha1.RoundTable {
	return &aiv1alpha1.RoundTable{
		ObjectMeta: metav1.ObjectMeta{Name: "t", Namespace: "default"},
		Spec:       aiv1alpha1.RoundTableSpec{Policies: &aiv1alpha1.RoundTablePolicies{MaxMissions: max}},
	}
}

func TestAdmitMission(t *testing.T) {
	const pending, active = aiv1alpha1.MissionPhasePending, aiv1alpha1.MissionPhaseActive
	tests := []struct {
		name     string
		rt       *aiv1alpha1.RoundTable
		missions []*aiv1alpha1.Mission
		subject  string
		wantHeld bool
	}{
		{"under limit", admissionRT(2), []*aiv1alpha1.Mission{
			admissionMission("a", time.Hour, "t", active), admissionMission("b", time.Minute, "t", pending)}, "b", false},
		{"at limit is held", admissionRT(1), []*aiv1alpha1.Mission{
			admissionMission("a", time.Hour, "t", active), admissionMission("b", time.Minute, "t", pending)}, "b", true},
		{"provisioning counts as active", admissionRT(1), []*aiv1alpha1.Mission{
			admissionMission("a", time.Hour, "t", aiv1alpha1.MissionPhaseProvisioning), admissionMission("b", time.Minute, "t", pending)}, "b", true},
		{"finished first mission frees slot", admissionRT(1), []*aiv1alpha1.Mission{
			admissionMission("a", time.Hour, "t", aiv1alpha1.MissionPhaseSucceeded), admissionMission("b", time.Minute, "t", pending)}, "b", false},
		{"maxMissions 0 is unlimited", admissionRT(0), []*aiv1alpha1.Mission{
			admissionMission("a", time.Hour, "t", active), admissionMission("b", time.Minute, "t", pending)}, "b", false},
		{"no roundTableRef is not limited", admissionRT(1), []*aiv1alpha1.Mission{
			admissionMission("a", time.Hour, "t", active), admissionMission("b", time.Minute, "", pending)}, "b", false},
		{"missing table is not limited", nil, []*aiv1alpha1.Mission{
			admissionMission("a", time.Hour, "t", active), admissionMission("b", time.Minute, "t", pending)}, "b", false},
		{"other table's missions do not count", admissionRT(1), []*aiv1alpha1.Mission{
			admissionMission("a", time.Hour, "other", active), admissionMission("b", time.Minute, "t", pending)}, "b", false},
		{"oldest waiting goes first", admissionRT(1), []*aiv1alpha1.Mission{
			admissionMission("old", time.Hour, "t", pending), admissionMission("new", time.Minute, "t", pending)}, "old", false},
		{"newer waiting is held while older waits", admissionRT(1), []*aiv1alpha1.Mission{
			admissionMission("old", time.Hour, "t", pending), admissionMission("new", time.Minute, "t", pending)}, "new", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := runtime.NewScheme()
			if err := aiv1alpha1.AddToScheme(s); err != nil {
				t.Fatal(err)
			}
			b := fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(&aiv1alpha1.Mission{})
			if tc.rt != nil {
				b = b.WithObjects(tc.rt)
			}
			for _, m := range tc.missions {
				b = b.WithObjects(m)
			}
			c := b.Build()
			rec := record.NewFakeRecorder(10)
			r := &MissionReconciler{Client: c, Scheme: s, Recorder: rec}

			var subject aiv1alpha1.Mission
			if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: tc.subject}, &subject); err != nil {
				t.Fatal(err)
			}
			res, held, err := r.admitMission(context.Background(), &subject)
			if err != nil {
				t.Fatal(err)
			}
			if held != tc.wantHeld {
				t.Fatalf("held = %v, want %v", held, tc.wantHeld)
			}
			if !tc.wantHeld {
				return
			}
			if res.RequeueAfter == 0 {
				t.Error("expected a requeue while held")
			}
			var got aiv1alpha1.Mission
			if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: tc.subject}, &got); err != nil {
				t.Fatal(err)
			}
			cond := meta.FindStatusCondition(got.Status.Conditions, aiv1alpha1.ConditionMissionAdmitted)
			if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != aiv1alpha1.ReasonTableAtMissionLimit {
				t.Fatalf("unexpected Admitted condition: %+v", cond)
			}
			// A second pass must not emit the event again.
			if _, _, err := r.admitMission(context.Background(), &got); err != nil {
				t.Fatal(err)
			}
			if n := len(rec.Events); n != 1 {
				t.Errorf("MissionQueued events = %d, want 1", n)
			}
		})
	}
}
