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
	"bytes"
	"context"
	"fmt"
	"maps"
	"strings"
	"text/template"

	"github.com/roundtable-k8s/roundtable/internal/util"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// validateKnightRefs checks that all knightRef values resolve to Knight CRs.
func (r *ChainReconciler) validateKnightRefs(ctx context.Context, chain *aiv1alpha1.Chain) error {
	check := func(stepName, knightRef string) error {
		if knightRef == "" {
			return fmt.Errorf("step %q has no knightRef", stepName)
		}
		knight := &aiv1alpha1.Knight{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      knightRef,
			Namespace: chain.Namespace,
		}, knight); err != nil {
			return fmt.Errorf("step %q references non-existent knight %q: %w", stepName, knightRef, err)
		}
		return nil
	}
	for _, step := range chain.Spec.Steps {
		if step.FallbackRef.Name != "" {
			if err := check(step.Name+" (fallback)", step.FallbackRef.Name); err != nil {
				return err
			}
		}
		if step.Loop == nil {
			if err := check(step.Name, step.KnightRef.Name); err != nil {
				return err
			}
			continue
		}
		for _, b := range step.Loop.Steps {
			if err := check(step.Name+"/"+b.Name, b.KnightRef.Name); err != nil {
				return err
			}
		}
	}
	return nil
}

// owningMissionInactive reports whether the chain belongs to a mission that no
// longer needs it reconciled: the mission is gone, being deleted, cleaning up,
// or already in a terminal phase (Succeeded/Failed/Expired). Mission cleanup
// deletes the mission's ephemeral knights before the chain is garbage-collected,
// so a chain can transiently reference knights that were legitimately removed.
// A missing knight while the mission is still active (pre-dispatch) is a real
// error and is NOT covered here.
func (r *ChainReconciler) owningMissionInactive(ctx context.Context, chain *aiv1alpha1.Chain) bool {
	missionName := chain.Labels[aiv1alpha1.LabelMission]
	if missionName == "" {
		return false
	}
	mission := &aiv1alpha1.Mission{}
	if err := r.Get(ctx, types.NamespacedName{Name: missionName, Namespace: chain.Namespace}, mission); err != nil {
		// Mission deleted entirely — the chain is an orphan awaiting GC.
		return apierrors.IsNotFound(err)
	}
	if mission.DeletionTimestamp != nil {
		return true
	}
	switch mission.Status.Phase {
	case aiv1alpha1.MissionPhaseCleaningUp,
		aiv1alpha1.MissionPhaseSucceeded,
		aiv1alpha1.MissionPhaseFailed,
		aiv1alpha1.MissionPhaseExpired:
		return true
	}
	return false
}

// validateDAG performs topological sort to detect cycles.
// validateTemplates pre-parses all step task templates to catch syntax errors early.
// Also warns about common mistakes like using lowercase field names.
func (r *ChainReconciler) validateTemplates(chain *aiv1alpha1.Chain) error {
	// Dry-run execute with mock data to catch field access errors
	mockSteps := make(map[string]map[string]string)
	for _, s := range chain.Spec.Steps {
		mockSteps[s.Name] = stepTemplateData(aiv1alpha1.ChainStepStatus{})
	}
	check := func(name, task string, extra map[string]any) error {
		if !strings.Contains(task, "{{") {
			return nil
		}
		tmpl, err := template.New("validate").Parse(task)
		if err != nil {
			return fmt.Errorf("step %q has invalid template: %w", name, err)
		}
		mockData := map[string]any{
			"Steps":  mockSteps,
			"Input":  "",
			"Fields": map[string]any{},
		}
		maps.Copy(mockData, extra)
		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, mockData); err != nil {
			return fmt.Errorf("step %q template execution error (hint: use .Steps.stepname.Output not steps.stepname.output): %w", name, err)
		}
		return nil
	}
	for _, step := range chain.Spec.Steps {
		var extra map[string]any
		if step.ForEach != nil {
			extra = map[string]any{"Item": map[string]any{}, "item": map[string]any{}, "ItemIndex": 0, "ItemJSON": "{}"}
		}
		if err := check(step.Name, step.Task, extra); err != nil {
			return err
		}
		if step.Loop == nil {
			continue
		}
		body := make(map[string]map[string]string, len(step.Loop.Steps))
		for _, b := range step.Loop.Steps {
			body[b.Name] = map[string]string{"Output": "", "Error": "", "Phase": ""}
		}
		loopData := map[string]any{"Loop": map[string]any{
			"Iteration": 1, "MaxIterations": 1, "Steps": body,
			"Last": map[string]string{"Step": "", "Phase": "", "Output": "", "Error": ""},
		}}
		for _, b := range step.Loop.Steps {
			if err := check(step.Name+"/"+b.Name, b.Task, loopData); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *ChainReconciler) validateDAG(chain *aiv1alpha1.Chain) error {
	// Convert ChainSteps to DAGNodes
	nodes := make([]util.DAGNode, len(chain.Spec.Steps))
	for i, step := range chain.Spec.Steps {
		nodes[i] = util.DAGNode{
			Name:      step.Name,
			DependsOn: step.DependsOn,
		}
	}
	return util.ValidateDAG(nodes)
}

// validateSpec runs every spec check in order and returns the first failure as
// a ChainValid condition reason and message, plus the error the reconcile
// returns. A valid spec returns a nil error.
func (r *ChainReconciler) validateSpec(ctx context.Context, chain *aiv1alpha1.Chain) (reason, message string, err error) {
	if aiv1alpha1.RefName(chain.Spec.RoundTableRef) == "" && aiv1alpha1.RefName(chain.Spec.MissionRef) == "" {
		return aiv1alpha1.ReasonMissingRoundTableRef,
			"Chain must have either roundTableRef or missionRef configured",
			fmt.Errorf("chain %s/%s missing roundTableRef or missionRef", chain.Namespace, chain.Name)
	}
	if err := r.validateKnightRefs(ctx, chain); err != nil {
		return aiv1alpha1.ReasonInvalidKnightRef, err.Error(), err
	}
	if err := r.validateDAG(chain); err != nil {
		return aiv1alpha1.ReasonCyclicDependency, err.Error(), err
	}
	if err := validateForEach(chain); err != nil {
		return aiv1alpha1.ReasonInvalidForEach, err.Error(), err
	}
	if reason, err := validateConditionsAndLoops(chain); err != nil {
		return reason, err.Error(), err
	}
	if _, err := parseSchedule(chain); err != nil {
		return aiv1alpha1.ReasonInvalidSchedule, err.Error(), err
	}
	if err := r.validateTemplates(chain); err != nil {
		return aiv1alpha1.ReasonInvalidTemplate, err.Error(), err
	}
	return "", "", nil
}

// rejectSpec records a failed validateSpec on the chain status and returns
// the error the reconcile returns. A knight that is missing because the
// owning mission already cleaned it up stops the reconcile quietly instead.
func (r *ChainReconciler) rejectSpec(ctx context.Context, chain *aiv1alpha1.Chain, reason, message string, err error) error {
	log := logf.FromContext(ctx)
	// A knight that disappears after the owning mission started cleanup
	// (or is gone / terminal) was deleted BY that cleanup — mission-scoped
	// ephemeral knights are removed before the chain itself is
	// garbage-collected. Reporting InvalidKnightRef then is post-cleanup
	// noise that looks like the dispatch failure cause, so stop
	// reconciling quietly instead of setting conditions and requeuing.
	if reason == aiv1alpha1.ReasonInvalidKnightRef && apierrors.IsNotFound(err) && r.owningMissionInactive(ctx, chain) {
		log.V(1).Info("Knight missing after owning mission cleanup, stopping reconcile",
			"mission", chain.Labels[aiv1alpha1.LabelMission])
		return nil
	}
	meta.SetStatusCondition(&chain.Status.Conditions, metav1.Condition{
		Type:               aiv1alpha1.ConditionChainValid,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: chain.Generation,
	})
	switch reason {
	case aiv1alpha1.ReasonMissingRoundTableRef:
		chain.Status.Phase = aiv1alpha1.ChainPhaseFailed
	case aiv1alpha1.ReasonInvalidSchedule:
		chain.Status.NextScheduledAt = nil
	}
	chain.Status.ObservedGeneration = chain.Generation
	if statusErr := r.Status().Update(ctx, chain); statusErr != nil {
		log.Error(statusErr, "Failed to update status during validation error")
	}
	return err
}
