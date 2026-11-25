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
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/chainrun"
)

// The trigger engine (#520): with spec.issuePickup.engine: trigger the issue
// pickup controller generates a Trigger with a github source from the
// issuePickup fields and stops its own poller for the table. The Trigger
// names, labels and owns its runs like the builtin poller does, so the
// post-run checks (pickup_verify.go, pickup_split.go), which the pickup
// controller keeps doing for both engines, find them, and an engine switch
// never starts a second run for an issue.

// pickupEngine is the table's issue pickup engine, defaulted.
func pickupEngine(spec *aiv1alpha1.IssuePickupSpec) aiv1alpha1.IssuePickupEngine {
	if spec != nil && spec.Engine == aiv1alpha1.IssuePickupEngineTrigger {
		return aiv1alpha1.IssuePickupEngineTrigger
	}
	return aiv1alpha1.IssuePickupEngineBuiltin
}

// pickupTriggerName is the generated Trigger's name, "<table>-issue-pickup".
func pickupTriggerName(rt *aiv1alpha1.RoundTable) string {
	return rt.Name + aiv1alpha1.IssuePickupTriggerSuffix
}

// pickupTable returns the RoundTable whose issue pickup generated trg (it is
// labelled with LabelPickupRoundTable and controlled by that RoundTable), or
// "".
func pickupTable(trg *aiv1alpha1.Trigger) string {
	return controllingTable(trg, aiv1alpha1.LabelPickupRoundTable)
}

// templateLiteral returns s for use inside a Trigger template: as is, or as
// a quoted template string when it contains template delimiters.
func templateLiteral(s string) string {
	if !strings.Contains(s, "{{") && !strings.Contains(s, "}}") {
		return s
	}
	return "{{ " + strconv.Quote(s) + " }}"
}

// pickupTriggerInput renders the {"repo", "issue"} input the PR pipeline
// expects, the same as the builtin poller's.
const pickupTriggerInput = `{"repo": {{ toJson .Event.Data.repo }}, "issue": {{ toJson .Event.Data.number }}}`

// pickupTriggerSpec builds the generated Trigger's spec from the table's
// issuePickup fields.
func pickupTriggerSpec(rt *aiv1alpha1.RoundTable, spec *aiv1alpha1.IssuePickupSpec) aiv1alpha1.TriggerSpec {
	labels := pickupLabels(spec)
	lit := templateLiteral
	claimComment := fmt.Sprintf("Picked up by the Round Table (RoundTable `%s/%s`): started Chain `{{ .Run.Name }}` (run `{{ .Run.ID }}`), "+
		"which plans, implements, reviews and merges a PR for this issue or labels it `%s`.\n\n"+
		"Follow it with `kubectl -n %s get chain {{ .Run.Name }}`. Add `%s` again after it finishes to re-run it.",
		lit(rt.Namespace), lit(rt.Name), lit(labels.needsHuman), lit(rt.Namespace), lit(labels.ready))
	handBackComment := fmt.Sprintf("Not picked up by the Round Table: this issue needs a human ({{ .Reason }}). "+
		"Removed `%s` and labelled `%s`. A knight's bot token cannot push workflow changes, "+
		"and secrets/auth changes need a human; dispatch it by hand if it is safe.",
		lit(labels.ready), lit(labels.needsHuman))
	ttl := int32(0) // issue pickup deletes finished runs after its post-run checks
	return aiv1alpha1.TriggerSpec{
		Source: aiv1alpha1.TriggerSource{GitHub: &aiv1alpha1.TriggerGitHubSource{
			Repositories: append([]string(nil), spec.Repos...),
			Kind:         aiv1alpha1.TriggerGitHubKindIssue,
			Query: aiv1alpha1.TriggerGitHubQuery{
				Labels: []string{labels.ready},
				State:  aiv1alpha1.TriggerGitHubStateOpen,
			},
			PollInterval: &metav1.Duration{Duration: pickupInterval(spec)},
			Claim: &aiv1alpha1.TriggerGitHubClaim{
				AddLabels:    []string{labels.inProgress},
				RemoveLabels: []string{labels.ready},
				Assignees:    append([]string(nil), spec.Assignees...),
				Comment:      claimComment,
			},
			MaxActivePerRepository:      max(spec.MaxActivePerRepo, 1),
			HoldWhilePullRequestLabeled: breakingLabel(spec),
			SkipIfBodyMatches: &aiv1alpha1.TriggerGitHubSkip{
				Patterns: append([]string(nil), spec.HumanPatterns...),
				HandBack: aiv1alpha1.TriggerGitHubHandBack{
					AddLabels:    []string{labels.needsHuman},
					RemoveLabels: []string{labels.ready},
					Comment:      handBackComment,
				},
			},
		}},
		Target: aiv1alpha1.TriggerTarget{ChainRef: spec.ChainRef},
		Input:  pickupTriggerInput,
		Concurrency: aiv1alpha1.TriggerConcurrency{
			Policy:    aiv1alpha1.TriggerConcurrencyAllow,
			MaxActive: max(spec.MaxActive, 1),
		},
		Suspend:                 spec.Suspend || rt.Spec.Suspend,
		RoundTableRef:           &corev1.LocalObjectReference{Name: rt.Name},
		TTLSecondsAfterFinished: &ttl,
	}
}

// reconcilePickupTrigger creates or updates the generated Trigger while the
// table runs on the trigger engine, and deletes it otherwise. A Trigger of
// that name the table does not control is never touched.
func (r *IssuePickupReconciler) reconcilePickupTrigger(ctx context.Context, rt *aiv1alpha1.RoundTable) error {
	log := logf.FromContext(ctx)
	spec := rt.Spec.IssuePickup
	name := pickupTriggerName(rt)
	want := spec != nil && pickupEngine(spec) == aiv1alpha1.IssuePickupEngineTrigger && rt.DeletionTimestamp.IsZero()

	existing := &aiv1alpha1.Trigger{}
	err := r.Get(ctx, types.NamespacedName{Namespace: rt.Namespace, Name: name}, existing)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("get Trigger %s: %w", name, err)
	}
	found := err == nil

	if !want {
		if !found || !metav1.IsControlledBy(existing, rt) || !existing.DeletionTimestamp.IsZero() {
			return nil
		}
		// Runs are owned by the RoundTable, not the Trigger: deleting it
		// leaves them running and the builtin poller takes over.
		if err := r.Delete(ctx, existing); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("delete Trigger %s: %w", name, err)
		}
		log.Info("Deleted issue pickup Trigger", "trigger", name)
		r.Recorder.Eventf(rt, corev1.EventTypeNormal, "IssuePickupTriggerDeleted",
			"Deleted Trigger %s: issue pickup runs on the builtin engine", name)
		return nil
	}

	if len(name) > 63 {
		return fmt.Errorf("engine trigger: Trigger name %q is longer than 63 characters; shorten the RoundTable name", name)
	}
	desired := pickupTriggerSpec(rt, spec)
	if !found {
		trg := &aiv1alpha1.Trigger{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: rt.Namespace,
				Labels: map[string]string{aiv1alpha1.LabelPickupRoundTable: rt.Name},
			},
			Spec: desired,
		}
		if err := controllerutil.SetControllerReference(rt, trg, r.Scheme); err != nil {
			return err
		}
		if err := r.Create(ctx, trg); err != nil {
			return fmt.Errorf("create Trigger %s: %w", name, err)
		}
		log.Info("Created issue pickup Trigger", "trigger", name)
		r.Recorder.Eventf(rt, corev1.EventTypeNormal, "IssuePickupTriggerCreated",
			"Created Trigger %s: issue pickup runs on the trigger engine", name)
		return nil
	}
	if !metav1.IsControlledBy(existing, rt) {
		return fmt.Errorf("engine trigger: Trigger %s exists and is not owned by this RoundTable", name)
	}
	if !existing.DeletionTimestamp.IsZero() {
		return nil // recreated once the old one is gone
	}
	if equality.Semantic.DeepEqual(existing.Spec, desired) && existing.Labels[aiv1alpha1.LabelPickupRoundTable] == rt.Name {
		return nil
	}
	existing.Spec = desired
	if existing.Labels == nil {
		existing.Labels = map[string]string{}
	}
	existing.Labels[aiv1alpha1.LabelPickupRoundTable] = rt.Name
	if err := r.Update(ctx, existing); err != nil {
		return fmt.Errorf("update Trigger %s: %w", name, err)
	}
	log.Info("Updated issue pickup Trigger", "trigger", name)
	return nil
}

// triggerEngineSucceeded reports whether c is a run the trigger engine
// started that succeeded and has no post-run check left.
func triggerEngineSucceeded(c *aiv1alpha1.Chain) bool {
	return c.Labels[chainrun.LabelRunSource] == chainrun.SourceTrigger &&
		!pickupChainActive(c) && c.Status.Phase == aiv1alpha1.ChainPhaseSucceeded &&
		!pickupNeedsVerify(c) && !pickupNeedsSplitClose(c)
}

// setPickupEngineStatus records the engine in use and, once the trigger
// engine has succeeded on the table, the BuiltinEngineDeprecated condition.
func setPickupEngineStatus(rt *aiv1alpha1.RoundTable, st *aiv1alpha1.IssuePickupStatus) {
	engine := pickupEngine(rt.Spec.IssuePickup)
	st.Engine = engine
	st.Trigger = ""
	if engine == aiv1alpha1.IssuePickupEngineTrigger {
		st.Trigger = pickupTriggerName(rt)
	}
	if st.TriggerEngineSucceededAt == nil {
		return
	}
	msg := "The trigger engine has run issue pickup successfully on this table; the builtin engine is deprecated. "
	if engine == aiv1alpha1.IssuePickupEngineTrigger {
		msg += "Keep spec.issuePickup.engine: trigger."
	} else {
		msg += "Set spec.issuePickup.engine: trigger."
	}
	meta.SetStatusCondition(&st.Conditions, metav1.Condition{
		Type:               aiv1alpha1.ConditionIssuePickupBuiltinDeprecated,
		Status:             metav1.ConditionTrue,
		Reason:             aiv1alpha1.ReasonIssuePickupTriggerEngineProven,
		Message:            msg,
		ObservedGeneration: rt.Generation,
	})
}
