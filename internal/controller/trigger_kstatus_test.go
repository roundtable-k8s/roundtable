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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/cli-utils/pkg/kstatus/status"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func triggerKstatusOf(t *testing.T, trg *aiv1alpha1.Trigger) status.Status {
	t.Helper()
	m, err := k8sruntime.DefaultUnstructuredConverter.ToUnstructured(trg)
	if err != nil {
		t.Fatal(err)
	}
	u := &unstructured.Unstructured{Object: m}
	u.SetAPIVersion(aiv1alpha1.GroupVersion.String())
	u.SetKind("Trigger")
	res, err := status.Compute(u)
	if err != nil {
		t.Fatal(err)
	}
	return res.Status
}

// TestTriggerKstatus checks what Helm's --wait and Flux health checks see: a
// Trigger that is suspended on purpose is Current once reconciled, a Trigger
// that cannot work until someone fixes it (invalid spec, missing target,
// operator without a trigger image) stays InProgress (Ready=False), and a
// Trigger whose spec is not yet observed is InProgress until the reconcile.
func TestTriggerKstatus(t *testing.T) {
	chainTarget := aiv1alpha1.TriggerTarget{ChainRef: &corev1.LocalObjectReference{Name: "missing"}}
	cases := []struct {
		name    string
		mutate  func(*aiv1alpha1.Trigger)
		reason  string
		healthy bool
	}{
		{"suspended", func(trg *aiv1alpha1.Trigger) {
			trg.Spec.Suspend = true
			trg.Spec.Source.Schedule = &aiv1alpha1.TriggerScheduleSource{Cron: "* * * * *"}
		}, aiv1alpha1.ReasonTriggerSuspended, true},
		{"invalid spec", func(trg *aiv1alpha1.Trigger) {
			trg.Spec.Target = aiv1alpha1.TriggerTarget{}
			trg.Spec.Source.Schedule = &aiv1alpha1.TriggerScheduleSource{Cron: "* * * * *"}
		}, aiv1alpha1.ReasonTriggerInvalidSpec, false},
		{"target not found", func(trg *aiv1alpha1.Trigger) {
			trg.Spec.Target = chainTarget
			trg.Spec.Source.Schedule = &aiv1alpha1.TriggerScheduleSource{Cron: "* * * * *"}
		}, aiv1alpha1.ReasonTriggerTargetNotFound, false},
		{"schedule without trigger image", func(trg *aiv1alpha1.Trigger) {
			trg.Spec.Target = chainTarget
			trg.Spec.Source.Schedule = &aiv1alpha1.TriggerScheduleSource{Cron: "* * * * *"}
		}, aiv1alpha1.ReasonTriggerCronJobError, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trg := &aiv1alpha1.Trigger{
				ObjectMeta: metav1.ObjectMeta{Name: "trg", Namespace: "default", Generation: 2},
				Spec:       aiv1alpha1.TriggerSpec{Target: chainTarget},
			}
			trg.Status.ObservedGeneration = 1
			tc.mutate(trg)
			if tc.name == "schedule without trigger image" {
				trg.Spec.Target.ChainRef.Name = "tmpl"
			}
			if got := triggerKstatusOf(t, trg); got != status.InProgressStatus {
				t.Fatalf("unobserved spec: kstatus = %s, want InProgress", got)
			}

			s := k8sruntime.NewScheme()
			if err := aiv1alpha1.AddToScheme(s); err != nil {
				t.Fatal(err)
			}
			objs := []k8sruntime.Object{trg.DeepCopy()}
			if tc.name == "schedule without trigger image" {
				objs = append(objs, &aiv1alpha1.Chain{ObjectMeta: metav1.ObjectMeta{Name: "tmpl", Namespace: "default"}})
			}
			r := &TriggerReconciler{
				Client: fake.NewClientBuilder().WithScheme(s).WithRuntimeObjects(objs...).
					WithStatusSubresource(&aiv1alpha1.Trigger{}).Build(),
				Scheme:   s,
				Recorder: record.NewFakeRecorder(100),
			}
			nn := types.NamespacedName{Namespace: "default", Name: "trg"}
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn}); err != nil {
				t.Fatal(err)
			}
			got := &aiv1alpha1.Trigger{}
			if err := r.Get(context.Background(), nn, got); err != nil {
				t.Fatal(err)
			}
			ready := metaCondition(got, aiv1alpha1.ConditionTriggerReady)
			if ready == nil || ready.Reason != tc.reason {
				t.Fatalf("Ready condition = %+v, want reason %s", ready, tc.reason)
			}
			want := status.InProgressStatus // needs a fix: Ready is False
			if tc.healthy {
				want = status.CurrentStatus
			}
			if s := triggerKstatusOf(t, got); s != want {
				t.Errorf("after reconcile: kstatus = %s, want %s (conditions %+v)", s, want, got.Status.Conditions)
			}
		})
	}
}

// TestTriggerKstatusListening: a Trigger that listens, with a recent firing
// and active runs, is Current.
func TestTriggerKstatusListening(t *testing.T) {
	trg := &aiv1alpha1.Trigger{
		ObjectMeta: metav1.ObjectMeta{Name: "trg", Namespace: "default", Generation: 1},
		Status: aiv1alpha1.TriggerStatus{
			ObservedGeneration: 1,
			ActiveRuns:         []string{"trg-abc"},
			Conditions: []metav1.Condition{
				{Type: aiv1alpha1.ConditionTriggerReady, Status: metav1.ConditionTrue, Reason: aiv1alpha1.ReasonTriggerListening, ObservedGeneration: 1},
				{Type: aiv1alpha1.ConditionTriggerScheduleHealthy, Status: metav1.ConditionFalse, Reason: "JobFailed", ObservedGeneration: 1},
				{Type: aiv1alpha1.ConditionTriggerGitHubReady, Status: metav1.ConditionFalse, Reason: "PollFailed", ObservedGeneration: 1},
			},
		},
	}
	if got := triggerKstatusOf(t, trg); got != status.CurrentStatus {
		t.Errorf("kstatus = %s, want Current", got)
	}
}

func metaCondition(trg *aiv1alpha1.Trigger, typ string) *metav1.Condition {
	for i := range trg.Status.Conditions {
		if trg.Status.Conditions[i].Type == typ {
			return &trg.Status.Conditions[i]
		}
	}
	return nil
}
