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
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	rtruntime "github.com/roundtable-k8s/roundtable/pkg/runtime"
)

func (r *KnightReconciler) setWorkspaceReady(knight *aiv1alpha1.Knight, status metav1.ConditionStatus, reason, msg string) {
	meta.SetStatusCondition(&knight.Status.Conditions, metav1.Condition{
		Type: aiv1alpha1.ConditionKnightWorkspaceReady, Status: status, Reason: reason, Message: msg,
		ObservedGeneration: knight.Generation,
	})
}

// checkWorkspaceSnapshot looks up the VolumeSnapshot a new workspace PVC is
// cloned from. wait is true (with WorkspaceReady=False set) while the PVC
// can't be created: the snapshot API isn't served, or the snapshot is missing
// or not readyToUse. restoreSize is the snapshot's status.restoreSize, if set.
func (r *KnightReconciler) checkWorkspaceSnapshot(ctx context.Context, knight *aiv1alpha1.Knight, name string) (restoreSize *resource.Quantity, wait bool, err error) {
	if !r.snapshotsAvailable(ctx) {
		r.setWorkspaceReady(knight, metav1.ConditionFalse, aiv1alpha1.ReasonSnapshotAPIUnavailable,
			snapshotAPIMissingMessage)
		return nil, true, nil
	}
	vs := &unstructured.Unstructured{}
	vs.SetGroupVersionKind(rtruntime.VolumeSnapshotGVK)
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: knight.Namespace}, vs); err != nil {
		if apierrors.IsNotFound(err) {
			r.setWorkspaceReady(knight, metav1.ConditionFalse, aiv1alpha1.ReasonSnapshotNotReady,
				fmt.Sprintf("VolumeSnapshot %q not found", name))
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("get VolumeSnapshot %q: %w", name, err)
	}
	if ready, _, _ := unstructured.NestedBool(vs.Object, "status", "readyToUse"); !ready {
		r.setWorkspaceReady(knight, metav1.ConditionFalse, aiv1alpha1.ReasonSnapshotNotReady,
			fmt.Sprintf("VolumeSnapshot %q is not readyToUse", name))
		return nil, true, nil
	}
	// status.restoreSize is an int64 byte count; tolerate a quantity string.
	if v, ok, _ := unstructured.NestedFieldNoCopy(vs.Object, "status", "restoreSize"); ok {
		var q resource.Quantity
		switch t := v.(type) {
		case int64:
			q = *resource.NewQuantity(t, resource.BinarySI)
		case float64:
			q = *resource.NewQuantity(int64(t), resource.BinarySI)
		case string:
			q, err = resource.ParseQuantity(t)
			if err != nil {
				return nil, false, fmt.Errorf("VolumeSnapshot %q restoreSize %q: %w", name, t, err)
			}
		}
		restoreSize = &q
	}
	return restoreSize, false, nil
}
