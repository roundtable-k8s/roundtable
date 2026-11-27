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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/cli-utils/pkg/kstatus/status"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func missionKstatusOf(t *testing.T, m *aiv1alpha1.Mission) status.Status {
	t.Helper()
	obj, err := k8sruntime.DefaultUnstructuredConverter.ToUnstructured(m)
	if err != nil {
		t.Fatal(err)
	}
	u := &unstructured.Unstructured{Object: obj}
	u.SetAPIVersion(aiv1alpha1.GroupVersion.String())
	u.SetKind("Mission")
	res, err := status.Compute(u)
	if err != nil {
		t.Fatal(err)
	}
	return res.Status
}

// TestMissionKstatus checks what Helm's --wait and Flux health checks see: a
// Mission in any phase is Current once the operator has accepted its spec,
// including a Mission that is waiting (RoundTable not ready) or finished.
func TestMissionKstatus(t *testing.T) {
	phases := []aiv1alpha1.MissionPhase{
		aiv1alpha1.MissionPhasePending, aiv1alpha1.MissionPhaseProvisioning,
		aiv1alpha1.MissionPhaseActive, aiv1alpha1.MissionPhaseSucceeded,
		aiv1alpha1.MissionPhaseFailed, aiv1alpha1.MissionPhaseCleaningUp,
	}
	for _, phase := range phases {
		t.Run(string(phase), func(t *testing.T) {
			m := admissionMission("m", time.Minute, "", phase)
			m.Generation = 1
			m.Status.ObservedGeneration = 1
			if got := missionKstatusOf(t, m); got != status.CurrentStatus {
				t.Errorf("steady state: kstatus = %s, want Current", got)
			}

			// Spec just changed (chart upgrade bumps generation).
			m = admissionMission("m", time.Minute, "", phase)
			m.Generation = 2
			m.Status.ObservedGeneration = 1
			m.Finalizers = []string{missionFinalizer}
			m.Status.Conditions = []metav1.Condition{}
			if phase == aiv1alpha1.MissionPhaseSucceeded || phase == aiv1alpha1.MissionPhaseFailed {
				meta.SetStatusCondition(&m.Status.Conditions, metav1.Condition{
					Type: aiv1alpha1.ConditionCleanupComplete, Status: metav1.ConditionTrue, Reason: "Done",
				})
			}
			if got := missionKstatusOf(t, m); got != status.InProgressStatus {
				t.Fatalf("unobserved spec: kstatus = %s, want InProgress", got)
			}

			s := k8sruntime.NewScheme()
			if err := aiv1alpha1.AddToScheme(s); err != nil {
				t.Fatal(err)
			}
			c := fake.NewClientBuilder().WithScheme(s).WithObjects(m.DeepCopy()).
				WithStatusSubresource(&aiv1alpha1.Mission{}).Build()
			r := &MissionReconciler{Client: c, Scheme: s, Recorder: record.NewFakeRecorder(20)}
			// The reconcile may go on to fail on the unconfigured services of
			// this bare reconciler; only the acceptance of the spec matters.
			func() {
				defer func() { _ = recover() }()
				_, _ = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: m.Namespace, Name: m.Name}})
			}()
			got := &aiv1alpha1.Mission{}
			if err := c.Get(context.Background(), types.NamespacedName{Namespace: m.Namespace, Name: m.Name}, got); err != nil {
				t.Fatal(err)
			}
			if got.Status.ObservedGeneration != got.Generation {
				t.Errorf("observedGeneration = %d, want %d", got.Status.ObservedGeneration, got.Generation)
			}
			if s := missionKstatusOf(t, got); s != status.CurrentStatus {
				t.Errorf("after reconcile (phase %s): kstatus = %s, want Current", got.Status.Phase, s)
			}
		})
	}
}
