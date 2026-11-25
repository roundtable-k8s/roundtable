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
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/gateway"
	"github.com/roundtable-k8s/roundtable/pkg/metrics"
)

const (
	// triggerMissionPoll is how often a Trigger with unfinished Missions
	// looks at them when no Mission event arrives.
	triggerMissionPoll = 5 * time.Minute
	// triggerMissionPrefixMax caps the Trigger name part of a Mission name:
	// the operator derives knight, stream and RoundTable names from it.
	triggerMissionPrefixMax = 40
)

// triggerMissionName is "<trigger>-<short firing key>".
func triggerMissionName(trg *aiv1alpha1.Trigger, key string) string {
	prefix := strings.TrimRight(truncate(trg.Name, triggerMissionPrefixMax), "-.")
	return prefix + "-" + dedupeHash(key)[:8]
}

// missionRequest renders a mission target into the gateway's mission request,
// so the gateway and the Trigger validate the same fields.
func missionRequest(trg *aiv1alpha1.Trigger, objective, criteria string) gateway.StartMissionRequest {
	mt := trg.Spec.Target.Mission
	req := gateway.StartMissionRequest{
		Objective:       objective,
		SuccessCriteria: criteria,
		RoundTable:      aiv1alpha1.RefName(trg.Spec.RoundTableRef),
		TTLSeconds:      mt.TTLSeconds,
		TimeoutSeconds:  mt.TimeoutSeconds,
		RecruitExisting: mt.RecruitExisting,
		Knights:         mt.Knights,
	}
	// An unparsable budget stays 0, which the gateway rejects.
	req.CostBudgetUSD, _ = strconv.ParseFloat(strings.TrimSpace(mt.CostBudgetUSD), 64)
	for _, c := range mt.Chains {
		req.Chains = append(req.Chains, gateway.MissionChainRequest{Name: c.Name, Input: c.Input})
	}
	if mt.MetaMission == nil || *mt.MetaMission {
		req.Planner = &gateway.MissionPlannerRequest{}
		if p := mt.Planner; p != nil {
			req.Planner = &gateway.MissionPlannerRequest{
				Knight: p.Knight, Context: p.Context, MaxChains: p.MaxChains, MaxKnights: p.MaxKnights,
			}
		}
	}
	return req
}

// missionFiring is what fire hands fireMission.
type missionFiring struct {
	nn     types.NamespacedName
	trg    *aiv1alpha1.Trigger
	data   triggerTemplateData
	key    string
	firing *aiv1alpha1.TriggerFiring
	now    time.Time
	table  string
	mutate func(*aiv1alpha1.TriggerStatus)
	// skip records a firing that created nothing; deduped does for a
	// duplicate dedupe key.
	skip    func(outcome triggerOutcome, format string, args ...any) (triggerOutcome, error)
	deduped func(format string, args ...any) (triggerOutcome, error)
}

// listMissions returns the Trigger's Missions, oldest first.
func (r *TriggerReconciler) listMissions(ctx context.Context, trg *aiv1alpha1.Trigger) ([]aiv1alpha1.Mission, error) {
	list := &aiv1alpha1.MissionList{}
	if err := r.reader().List(ctx, list, client.InNamespace(trg.Namespace),
		client.MatchingLabels{aiv1alpha1.LabelTrigger: trg.Name}); err != nil {
		return nil, fmt.Errorf("list trigger missions: %w", err)
	}
	items := slices.DeleteFunc(list.Items, func(m aiv1alpha1.Mission) bool {
		return !metav1.IsControlledBy(&m, trg) || !m.DeletionTimestamp.IsZero()
	})
	slices.SortFunc(items, func(a, b aiv1alpha1.Mission) int {
		if c := a.CreationTimestamp.Compare(b.CreationTimestamp.Time); c != 0 {
			return c
		}
		return strings.Compare(a.Name, b.Name)
	})
	return items, nil
}

// missionUnfinished reports whether a Mission still counts as an active run.
func missionUnfinished(m *aiv1alpha1.Mission) bool {
	return !missionFinished(m.Status.Phase)
}

func missionFinished(p aiv1alpha1.MissionPhase) bool {
	switch p {
	case aiv1alpha1.MissionPhaseSucceeded, aiv1alpha1.MissionPhaseFailed,
		aiv1alpha1.MissionPhaseExpired, aiv1alpha1.MissionPhaseCleaningUp:
		return true
	}
	return false
}

// missionOutcome maps a finished Mission's phase to a firing outcome.
func missionOutcome(p aiv1alpha1.MissionPhase) aiv1alpha1.TriggerFiringOutcome {
	switch p {
	case aiv1alpha1.MissionPhaseSucceeded:
		return aiv1alpha1.TriggerFiringSucceeded
	case aiv1alpha1.MissionPhaseFailed:
		return aiv1alpha1.TriggerFiringFailed
	}
	return aiv1alpha1.TriggerFiringLost
}

// tableMissionsAtLimit reports whether the Trigger's RoundTable is at its
// policies.maxMissions, counting the missions the limit holds or runs.
func (r *TriggerReconciler) tableMissionsAtLimit(ctx context.Context, trg *aiv1alpha1.Trigger) (bool, string, error) {
	table := aiv1alpha1.RefName(trg.Spec.RoundTableRef)
	rt := &aiv1alpha1.RoundTable{}
	if err := r.reader().Get(ctx, types.NamespacedName{Namespace: trg.Namespace, Name: table}, rt); err != nil {
		return false, "", client.IgnoreNotFound(err)
	}
	if rt.Spec.Policies == nil || rt.Spec.Policies.MaxMissions <= 0 {
		return false, "", nil
	}
	limit := int(rt.Spec.Policies.MaxMissions)
	missions := &aiv1alpha1.MissionList{}
	if err := r.reader().List(ctx, missions, client.InNamespace(trg.Namespace)); err != nil {
		return false, "", fmt.Errorf("list missions: %w", err)
	}
	n := 0
	for i := range missions.Items {
		m := &missions.Items[i]
		if aiv1alpha1.RefName(m.Spec.RoundTableRef) != table || !m.DeletionTimestamp.IsZero() {
			continue
		}
		if missionUnfinished(m) {
			n++
		}
	}
	if n >= limit {
		return true, fmt.Sprintf("RoundTable %s is at its mission limit (%d unfinished, policies.maxMissions %d)", table, n, limit), nil
	}
	return false, "", nil
}

// fireMission creates one Mission on the Trigger's RoundTable. Its name is
// the firing's run and an active run until it finishes. A Mission that
// already exists under the name is the dedupe: the key already fired.
func (r *TriggerReconciler) fireMission(ctx context.Context, f missionFiring) (triggerOutcome, error) {
	trg, mt := f.trg, f.trg.Spec.Target.Mission
	objective, err := renderTriggerTemplate("objective", mt.Objective, f.data)
	if err != nil {
		return f.skip(triggerTerm, "target.mission.objective template: %v", err)
	}
	if strings.TrimSpace(objective) == "" {
		return f.skip(triggerTerm, "target.mission.objective rendered empty")
	}
	criteria, err := renderTriggerTemplate("successCriteria", mt.SuccessCriteria, f.data)
	if err != nil {
		return f.skip(triggerTerm, "target.mission.successCriteria template: %v", err)
	}

	name := triggerMissionName(trg, f.key)
	existing := &aiv1alpha1.Mission{}
	switch err := r.reader().Get(ctx, types.NamespacedName{Namespace: trg.Namespace, Name: name}, existing); {
	case err == nil:
		f.firing.Run = name
		return f.deduped("duplicate dedupe key: mission %s exists", name)
	case !apierrors.IsNotFound(err):
		return triggerRetry, err
	}

	missions, err := r.listMissions(ctx, trg)
	if err != nil {
		return triggerRetry, err
	}
	var active []string
	for i := range missions {
		if missionUnfinished(&missions[i]) {
			active = append(active, missions[i].Name)
		}
	}
	switch trg.Spec.Concurrency.Policy {
	case aiv1alpha1.TriggerConcurrencyForbid:
		if len(active) > 0 {
			return f.skip(triggerHandled, "concurrency Forbid: mission %s is active", active[0])
		}
	case aiv1alpha1.TriggerConcurrencyReplace:
		// Rejected by validation.
		return f.skip(triggerTerm, "concurrency Replace is not supported with target.mission")
	default:
		if n := trg.Spec.Concurrency.MaxActive; n > 0 && int32(len(active)) >= n {
			return f.skip(triggerHandled, "concurrency Allow: maxActive %d reached", n)
		}
	}

	// A firing over the table's mission limit is skipped, not left Pending.
	if full, why, err := r.tableMissionsAtLimit(ctx, trg); err != nil {
		return triggerRetry, err
	} else if full {
		return f.skip(triggerHandled, "%s", why)
	}

	svc := gateway.NewService(r.knightResolver(), nil, gateway.Options{AllowedNamespaces: []string{"*"}},
		logf.Log.WithName("trigger-missions"))
	m, err := svc.BuildMission(ctx, trg.Namespace, missionRequest(trg, objective, criteria))
	switch {
	case errors.Is(err, gateway.ErrInvalid):
		return f.skip(triggerTerm, "mission target: %v", err)
	case err != nil:
		return triggerRetry, fmt.Errorf("build mission: %w", err)
	}
	m.Name = name
	m.Labels = map[string]string{
		aiv1alpha1.LabelTrigger:   trg.Name,
		aiv1alpha1.LabelCreatedBy: aiv1alpha1.CreatedByTrigger,
	}
	m.Annotations = map[string]string{aiv1alpha1.AnnotationTriggerKey: truncate(f.key, triggerKeyAnnotationMax)}
	if err := controllerutil.SetControllerReference(trg, m, r.Scheme); err != nil {
		return triggerRetry, err
	}
	if err := r.Create(ctx, m); apierrors.IsAlreadyExists(err) {
		f.firing.Run = name
		return f.deduped("duplicate dedupe key: mission %s exists", name)
	} else if err != nil {
		return triggerRetry, fmt.Errorf("create mission %s: %w", name, err)
	}

	f.firing.Run = name
	countTriggerFiring(f.table, trg, triggerOutcomeStarted)
	if err := r.recordFiring(ctx, f.nn, *f.firing, func(st *aiv1alpha1.TriggerStatus) {
		at := metav1.NewTime(f.now)
		st.LastFiredAt = &at
		metrics.TriggerLastFiredTimestampSeconds.WithLabelValues(f.table, trg.Name).Set(float64(f.now.Unix()))
		if !slices.Contains(st.ActiveRuns, name) {
			st.ActiveRuns = append(st.ActiveRuns, name)
		}
		if f.mutate != nil {
			f.mutate(st)
		}
	}); err != nil {
		// The Mission exists; a redelivery finds it by name and skips.
		logf.FromContext(ctx).Error(err, "Failed to record Trigger firing", "trigger", f.nn.String(), "mission", name)
	}
	logf.Log.WithName("trigger").Info("Created Mission", "trigger", f.nn.String(), "mission", name, "key", f.firing.Key)
	r.event(trg, corev1.EventTypeNormal, "Fired", "Created mission %s", name)
	return triggerHandled, nil
}

// collectMissions returns the names of the Trigger's unfinished Missions and
// gives the firings of finished or deleted Missions their outcome and final
// phase, updating trg's status in place. It returns when to look again (0:
// nothing is unfinished).
func (r *TriggerReconciler) collectMissions(ctx context.Context, trg *aiv1alpha1.Trigger, _ time.Time) ([]string, time.Duration, error) {
	missions, err := r.listMissions(ctx, trg)
	if err != nil {
		return nil, 0, err
	}
	byName := map[string]*aiv1alpha1.Mission{}
	var active []string
	for i := range missions {
		m := &missions[i]
		byName[m.Name] = m
		if missionUnfinished(m) {
			active = append(active, m.Name)
		}
	}
	for i := range trg.Status.RecentFirings {
		f := &trg.Status.RecentFirings[i]
		if f.Run == "" || f.Outcome != "" || f.SkipReason != "" {
			continue
		}
		m, ok := byName[f.Run]
		switch {
		case !ok:
			// Cleaned up (ttlSeconds) before its outcome was read.
			f.Outcome = aiv1alpha1.TriggerFiringLost
		case missionFinished(m.Status.Phase):
			f.Outcome = missionOutcome(m.Status.Phase)
			f.Phase = string(m.Status.Phase)
		}
	}
	if len(active) == 0 {
		return nil, 0, nil
	}
	return active, triggerMissionPoll, nil
}
