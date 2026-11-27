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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

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

// A mission held in Pending longer than timeoutSeconds must not time out because
// of its queue time: admission resets StartedAt.
func TestReconcilePendingResetsStartedAtOnAdmission(t *testing.T) {
	s := runtime.NewScheme()
	if err := aiv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	m := admissionMission("b", time.Hour, "t", aiv1alpha1.MissionPhasePending)
	m.Spec.TimeoutSeconds = 1800
	old := metav1.NewTime(time.Now().Add(-2 * time.Hour))
	m.Status.StartedAt = &old
	meta.SetStatusCondition(&m.Status.Conditions, metav1.Condition{
		Type: aiv1alpha1.ConditionMissionAdmitted, Status: metav1.ConditionFalse,
		Reason: aiv1alpha1.ReasonTableAtMissionLimit, Message: "held",
	})
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(admissionRT(1), m).
		WithStatusSubresource(&aiv1alpha1.Mission{}).Build()
	r := &MissionReconciler{Client: c, Scheme: s, Recorder: record.NewFakeRecorder(10)}

	var subject aiv1alpha1.Mission
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "b"}, &subject); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcilePending(context.Background(), &subject); err != nil {
		t.Fatal(err)
	}
	var got aiv1alpha1.Mission
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "b"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != aiv1alpha1.MissionPhaseProvisioning {
		t.Fatalf("phase = %s, want Provisioning", got.Status.Phase)
	}
	if got.Status.StartedAt == nil || time.Since(got.Status.StartedAt.Time) > time.Minute {
		t.Errorf("StartedAt not reset on admission: %v", got.Status.StartedAt)
	}
}

type getCountingClient struct {
	client.Client
	chainGets int
}

func (c *getCountingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*aiv1alpha1.Chain); ok {
		c.chainGets++
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

// A held mission must not repeat the chain-existence Gets on each requeue.
func TestReconcilePendingHeldSkipsChainValidation(t *testing.T) {
	s := runtime.NewScheme()
	if err := aiv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	active := admissionMission("a", 2*time.Hour, "t", aiv1alpha1.MissionPhaseActive)
	held := admissionMission("b", time.Hour, "t", aiv1alpha1.MissionPhasePending)
	held.Spec.Chains = []aiv1alpha1.MissionChainRef{{Name: "c"}}
	base := fake.NewClientBuilder().WithScheme(s).WithObjects(admissionRT(1), active, held).
		WithStatusSubresource(&aiv1alpha1.Mission{}).Build()
	c := &getCountingClient{Client: base}
	r := &MissionReconciler{Client: c, Scheme: s, Recorder: record.NewFakeRecorder(10)}
	for i := 0; i < 3; i++ {
		var subject aiv1alpha1.Mission
		if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "b"}, &subject); err != nil {
			t.Fatal(err)
		}
		if _, err := r.reconcilePending(context.Background(), &subject); err != nil {
			t.Fatal(err)
		}
	}
	if c.chainGets != 0 {
		t.Errorf("chain Gets while held = %d, want 0", c.chainGets)
	}
}

func TestMissionLeftActivePhasePredicate(t *testing.T) {
	p := missionLeftActivePhase()
	mk := func(ph aiv1alpha1.MissionPhase) *aiv1alpha1.Mission {
		return &aiv1alpha1.Mission{Status: aiv1alpha1.MissionStatus{Phase: ph}}
	}
	up := func(o, n aiv1alpha1.MissionPhase) bool {
		return p.Update(event.UpdateEvent{ObjectOld: mk(o), ObjectNew: mk(n)})
	}
	if !up(aiv1alpha1.MissionPhaseActive, aiv1alpha1.MissionPhaseSucceeded) {
		t.Error("Running->Succeeded should pass")
	}
	if up(aiv1alpha1.MissionPhasePending, aiv1alpha1.MissionPhaseProvisioning) {
		t.Error("Pending->Provisioning should not pass")
	}
	if up(aiv1alpha1.MissionPhaseSucceeded, aiv1alpha1.MissionPhaseSucceeded) {
		t.Error("terminal no-op should not pass")
	}
	if !p.Delete(event.DeleteEvent{Object: mk(aiv1alpha1.MissionPhaseActive)}) {
		t.Error("deleting an active mission should pass")
	}
}

func TestPendingMissionsOfFreedTable(t *testing.T) {
	s := runtime.NewScheme()
	if err := aiv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	done := admissionMission("a", 2*time.Hour, "t", aiv1alpha1.MissionPhaseSucceeded)
	held := admissionMission("b", time.Hour, "t", aiv1alpha1.MissionPhasePending)
	other := admissionMission("c", time.Hour, "u", aiv1alpha1.MissionPhasePending)
	running := admissionMission("d", time.Hour, "t", aiv1alpha1.MissionPhaseActive)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(done, held, other, running).Build()
	r := &MissionReconciler{Client: c, Scheme: s}
	reqs := r.pendingMissionsOfFreedTable(context.Background(), done)
	if len(reqs) != 1 || reqs[0].Name != "b" {
		t.Fatalf("requests = %v, want only b", reqs)
	}
}
