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

// validateSpec picks the ChainValid reason and rejectSpec records it, with two
// reasons carrying a status side effect: a missing roundTableRef/missionRef
// fails the chain, and an invalid schedule clears NextScheduledAt so a stale
// fire time is not left behind (#470).
func TestValidateAndRejectSpec(t *testing.T) {
	staleNext := metav1.NewTime(time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC))

	cases := []struct {
		name          string
		mutate        func(*aiv1alpha1.Chain)
		wantReason    string
		wantPhase     aiv1alpha1.ChainPhase // phase after rejectSpec
		wantNextSched bool                  // NextScheduledAt still set after rejectSpec
	}{
		{
			name:       "missing roundTableRef and missionRef fails the chain",
			mutate:     func(c *aiv1alpha1.Chain) { c.Spec.RoundTableRef = nil },
			wantReason: aiv1alpha1.ReasonMissingRoundTableRef,
			wantPhase:  aiv1alpha1.ChainPhaseFailed,
			// Not a schedule problem: the fire time is left alone.
			wantNextSched: true,
		},
		{
			name: "invalid schedule clears NextScheduledAt and leaves the phase",
			mutate: func(c *aiv1alpha1.Chain) {
				c.Spec.Schedule = "not a cron expression"
			},
			wantReason: aiv1alpha1.ReasonInvalidSchedule,
			wantPhase:  aiv1alpha1.ChainPhaseIdle,
		},
		{
			name: "invalid time zone is an invalid schedule",
			mutate: func(c *aiv1alpha1.Chain) {
				c.Spec.Schedule = "0 * * * *"
				c.Spec.TimeZone = "Local"
			},
			wantReason: aiv1alpha1.ReasonInvalidSchedule,
			wantPhase:  aiv1alpha1.ChainPhaseIdle,
		},
		{
			name:       "a valid spec returns no reason",
			mutate:     func(c *aiv1alpha1.Chain) { c.Spec.Schedule = "0 * * * *" },
			wantReason: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := runtime.NewScheme()
			if err := aiv1alpha1.AddToScheme(s); err != nil {
				t.Fatal(err)
			}
			chain := &aiv1alpha1.Chain{
				ObjectMeta: metav1.ObjectMeta{Name: "validate-chain", Namespace: "default", Generation: 3},
				Spec:       aiv1alpha1.ChainSpec{RoundTableRef: aiv1alpha1.LocalRef("tbl")},
				Status: aiv1alpha1.ChainStatus{
					Phase:           aiv1alpha1.ChainPhaseIdle,
					NextScheduledAt: &staleNext,
				},
			}
			tc.mutate(chain)
			c := fake.NewClientBuilder().WithScheme(s).WithObjects(chain.DeepCopy()).
				WithStatusSubresource(&aiv1alpha1.Chain{}).Build()
			r := &ChainReconciler{Client: c, Scheme: s, Recorder: record.NewFakeRecorder(10)}
			ctx := context.Background()
			// Reconcile works on the object it read, with its resourceVersion.
			if err := c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "validate-chain"}, chain); err != nil {
				t.Fatal(err)
			}

			reason, message, verr := r.validateSpec(ctx, chain)
			if reason != tc.wantReason {
				t.Fatalf("validateSpec reason = %q (%v), want %q", reason, verr, tc.wantReason)
			}
			if reason == "" {
				if verr != nil || message != "" {
					t.Fatalf("a valid spec returned message %q, err %v", message, verr)
				}
				return
			}
			if verr == nil || message == "" {
				t.Fatalf("reason %q returned message %q, err %v; want both set", reason, message, verr)
			}

			if got := r.rejectSpec(ctx, chain, reason, message, verr); got == nil || got.Error() != verr.Error() {
				t.Fatalf("rejectSpec returned %v, want the validation error %v", got, verr)
			}

			stored := &aiv1alpha1.Chain{}
			if err := c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "validate-chain"}, stored); err != nil {
				t.Fatal(err)
			}
			cond := meta.FindStatusCondition(stored.Status.Conditions, aiv1alpha1.ConditionChainValid)
			if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != tc.wantReason || cond.Message != message || cond.ObservedGeneration != 3 {
				t.Errorf("ChainValid condition = %+v, want False/%s/%q/gen 3", cond, tc.wantReason, message)
			}
			if stored.Status.ObservedGeneration != 3 {
				t.Errorf("observedGeneration = %d, want 3", stored.Status.ObservedGeneration)
			}
			if stored.Status.Phase != tc.wantPhase {
				t.Errorf("phase = %q, want %q", stored.Status.Phase, tc.wantPhase)
			}
			if gotNext := stored.Status.NextScheduledAt != nil; gotNext != tc.wantNextSched {
				t.Errorf("NextScheduledAt set = %v, want %v", gotNext, tc.wantNextSched)
			}
		})
	}
}
