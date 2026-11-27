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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// Switching a table to the trigger engine drops the builtin engine's stale
// per-PR records from status.prFeedback.pullRequests.
func TestPatchEngineStatusClearsBuiltinRecords(t *testing.T) {
	s := k8sruntime.NewScheme()
	if err := aiv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	rt := &aiv1alpha1.RoundTable{
		ObjectMeta: metav1.ObjectMeta{Name: "t", Namespace: "ns"},
		Spec:       aiv1alpha1.RoundTableSpec{PRFeedback: &aiv1alpha1.PRFeedbackSpec{Enabled: true, Engine: aiv1alpha1.PRFeedbackEngineTrigger}},
		Status: aiv1alpha1.RoundTableStatus{PRFeedback: &aiv1alpha1.PRFeedbackStatus{
			PullRequests: []aiv1alpha1.PRFeedbackPullRequest{{Repo: "o/r", Number: 7}},
		}},
	}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(rt).WithStatusSubresource(rt).Build()
	r := &PRFeedbackReconciler{Client: c}
	if err := r.patchEngineStatus(context.Background(), rt, []string{"trg"}, nil); err != nil {
		t.Fatal(err)
	}
	got := &aiv1alpha1.RoundTable{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(rt), got); err != nil {
		t.Fatal(err)
	}
	if n := len(got.Status.PRFeedback.PullRequests); n != 0 {
		t.Fatalf("pullRequests = %d records, want none", n)
	}
}
