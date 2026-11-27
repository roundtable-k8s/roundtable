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
	"sort"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/status"
)

// admitMission enforces the RoundTable's policies.maxMissions for a Pending
// mission. It returns held=true (with the result to return) when the mission
// must stay Pending. Missions without a roundTableRef, whose table is missing,
// or whose limit is 0 (unlimited) are never held.
func (r *MissionReconciler) admitMission(ctx context.Context, mission *aiv1alpha1.Mission) (ctrl.Result, bool, error) {
	tableName := aiv1alpha1.RefName(mission.Spec.RoundTableRef)
	if tableName == "" {
		return ctrl.Result{}, false, nil
	}

	rt := &aiv1alpha1.RoundTable{}
	if err := r.Get(ctx, types.NamespacedName{Name: tableName, Namespace: mission.Namespace}, rt); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, false, nil
		}
		return ctrl.Result{}, false, err
	}
	if rt.Spec.Policies == nil || rt.Spec.Policies.MaxMissions <= 0 {
		return ctrl.Result{}, false, nil
	}
	limit := int(rt.Spec.Policies.MaxMissions)

	missions := &aiv1alpha1.MissionList{}
	if err := r.List(ctx, missions, client.InNamespace(mission.Namespace)); err != nil {
		return ctrl.Result{}, false, fmt.Errorf("failed to list missions: %w", err)
	}

	active := 0
	var waiting []*aiv1alpha1.Mission
	for i := range missions.Items {
		m := &missions.Items[i]
		if aiv1alpha1.RefName(m.Spec.RoundTableRef) != tableName || !m.DeletionTimestamp.IsZero() {
			continue
		}
		switch {
		case m.Status.Phase.IsActive():
			active++
		case m.Status.Phase == aiv1alpha1.MissionPhasePending:
			waiting = append(waiting, m)
		}
	}

	// Oldest first; name breaks ties so every reconcile agrees on the order.
	sort.Slice(waiting, func(i, j int) bool {
		a, b := waiting[i], waiting[j]
		if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
			return a.CreationTimestamp.Before(&b.CreationTimestamp)
		}
		return a.Name < b.Name
	})

	free := limit - active
	for i, m := range waiting {
		if m.UID == mission.UID && i < free {
			return ctrl.Result{}, false, nil
		}
	}

	msg := fmt.Sprintf("RoundTable %s is at its mission limit (%d active, limit %d); mission is queued", tableName, active, limit)
	cond := meta.FindStatusCondition(mission.Status.Conditions, aiv1alpha1.ConditionMissionAdmitted)
	firstTime := cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != aiv1alpha1.ReasonTableAtMissionLimit
	if firstTime || cond.Message != msg {
		if err := status.ForMission(mission).
			Condition(aiv1alpha1.ConditionMissionAdmitted, aiv1alpha1.ReasonTableAtMissionLimit, msg, metav1.ConditionFalse).
			Apply(ctx, r.Client); err != nil {
			if apierrors.IsConflict(err) {
				return ctrl.Result{RequeueAfter: RequeueFast}, true, nil
			}
			return ctrl.Result{}, true, err
		}
	}
	if firstTime {
		r.Recorder.Event(mission, corev1.EventTypeNormal, "MissionQueued", msg)
	}
	return ctrl.Result{RequeueAfter: RequeueSlow}, true, nil
}
