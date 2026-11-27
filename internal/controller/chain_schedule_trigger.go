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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// ChainScheduleMode selects how Chain.spec.schedule is run.
type ChainScheduleMode string

const (
	// ChainScheduleModeInProcess runs Chain.spec.schedule with the operator's
	// in-process cron: each fire runs the Chain object itself. The default.
	ChainScheduleModeInProcess ChainScheduleMode = "inprocess"
	// ChainScheduleModeTrigger runs Chain.spec.schedule through a schedule
	// Trigger <chain>-schedule the chain controller owns: each fire starts a
	// copy of the Chain, like any Trigger with target.chainRef.
	ChainScheduleModeTrigger ChainScheduleMode = "trigger"
)

// ParseChainScheduleMode parses the --chain-schedule-mode flag. Empty means
// inprocess.
func ParseChainScheduleMode(s string) (ChainScheduleMode, error) {
	switch m := ChainScheduleMode(s); m {
	case "", ChainScheduleModeInProcess:
		return ChainScheduleModeInProcess, nil
	case ChainScheduleModeTrigger:
		return m, nil
	}
	return "", fmt.Errorf("invalid chain schedule mode %q: must be %q or %q", s, ChainScheduleModeInProcess, ChainScheduleModeTrigger)
}

// labelManagedBy is the app.kubernetes.io/managed-by label.
const labelManagedBy = "app.kubernetes.io/managed-by"

// chainScheduleTriggerSuffix names a Chain's schedule Trigger:
// <chain>-schedule.
const chainScheduleTriggerSuffix = "-schedule"

// chainScheduleTriggerName is the name of the schedule Trigger of a Chain.
func chainScheduleTriggerName(chain *aiv1alpha1.Chain) string {
	return chain.Name + chainScheduleTriggerSuffix
}

// triggerMode reports whether Chain schedules run through owned Triggers.
func (r *ChainReconciler) triggerMode() bool {
	return r.ScheduleMode == ChainScheduleModeTrigger
}

// desiredScheduleTriggerSpec is the spec of the Trigger that runs a Chain's
// schedule. spec.input is left empty: a firing then starts the copy with the
// Chain's current spec.input verbatim, never rendered as a Trigger template
// (a literal "{{" in the Chain input stays literal).
func desiredScheduleTriggerSpec(chain *aiv1alpha1.Chain) aiv1alpha1.TriggerSpec {
	var deadline *int64
	if chain.Spec.StartingDeadlineSeconds != nil {
		d := *chain.Spec.StartingDeadlineSeconds
		deadline = &d
	}
	var table *corev1.LocalObjectReference
	if chain.Spec.RoundTableRef != nil {
		table = chain.Spec.RoundTableRef.DeepCopy()
	}
	return aiv1alpha1.TriggerSpec{
		Source: aiv1alpha1.TriggerSource{Schedule: &aiv1alpha1.TriggerScheduleSource{
			Cron:                    chain.Spec.Schedule,
			TimeZone:                chain.Spec.TimeZone,
			StartingDeadlineSeconds: deadline,
		}},
		Target:        aiv1alpha1.TriggerTarget{ChainRef: &corev1.LocalObjectReference{Name: chain.Name}},
		Concurrency:   aiv1alpha1.TriggerConcurrency{Policy: aiv1alpha1.TriggerConcurrencyForbid},
		Suspend:       chain.Spec.Suspend,
		RoundTableRef: table,
	}
}

// reconcileScheduleTrigger runs a Chain's schedule in trigger mode: it drops
// any in-process cron entry, creates, updates or deletes the owned schedule
// Trigger, and mirrors the Trigger's lastScheduledAt and nextScheduledAt onto
// the Chain's status. It returns whether the Chain's status changed.
func (r *ChainReconciler) reconcileScheduleTrigger(ctx context.Context, chain *aiv1alpha1.Chain) (bool, error) {
	r.removeCronEntry(types.NamespacedName{Namespace: chain.Namespace, Name: chain.Name})

	if chain.Spec.Schedule == "" {
		if err := r.deleteScheduleTrigger(ctx, chain); err != nil {
			return false, err
		}
		return setNextScheduledAt(chain, nil), nil
	}

	trg, err := r.ensureScheduleTrigger(ctx, chain)
	if err != nil || trg == nil {
		if trg == nil && err == nil {
			// Either a Trigger of that name exists but is not ours, or ours is
			// being deleted: leave it alone in both cases.
			return setNextScheduledAt(chain, nil), nil
		}
		return false, err
	}

	changed := setNextScheduledAt(chain, trg.Status.NextScheduledAt)
	// Keep the last fire recorded so far (e.g. by the in-process scheduler)
	// until the CronJob has fired once.
	if last := trg.Status.LastScheduledAt; last != nil && !timesEqual(chain.Status.LastScheduledAt, last) {
		chain.Status.LastScheduledAt = last.DeepCopy()
		changed = true
	}
	return changed, nil
}

// ensureScheduleTrigger creates or updates the Chain's schedule Trigger. It
// returns nil, nil when a Trigger of that name exists that the Chain does not
// control (a Warning event says so) and when the Chain's own Trigger is being
// deleted (it is left alone until it is gone, then recreated).
func (r *ChainReconciler) ensureScheduleTrigger(ctx context.Context, chain *aiv1alpha1.Chain) (*aiv1alpha1.Trigger, error) {
	log := logf.FromContext(ctx)
	name := chainScheduleTriggerName(chain)
	want := desiredScheduleTriggerSpec(chain)

	trg := &aiv1alpha1.Trigger{}
	err := r.Get(ctx, types.NamespacedName{Namespace: chain.Namespace, Name: name}, trg)
	if apierrors.IsNotFound(err) {
		trg = &aiv1alpha1.Trigger{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: chain.Namespace,
				Labels:    map[string]string{labelManagedBy: triggerManagedBy},
			},
			Spec: want,
		}
		if err := controllerutil.SetControllerReference(chain, trg, r.Scheme); err != nil {
			return nil, err
		}
		if err := r.Create(ctx, trg); err != nil {
			return nil, fmt.Errorf("create schedule Trigger %s: %w", name, err)
		}
		log.Info("Created schedule Trigger", "trigger", name, "schedule", chain.Spec.Schedule)
		r.event(chain, corev1.EventTypeNormal, "ScheduleTriggerCreated", "Created schedule Trigger %s", name)
		return trg, nil
	}
	if err != nil {
		return nil, err
	}
	key := client.ObjectKeyFromObject(chain)
	if !metav1.IsControlledBy(trg, chain) {
		// Warn once per Chain (UID and generation): reconciles that follow
		// without a spec change would only repeat the event, while a Chain
		// recreated under the same name is a new object and warns again.
		want := conflictWarning{uid: chain.UID, generation: chain.Generation}
		if last, ok := r.conflictWarned.Load(key); !ok || last != want {
			r.conflictWarned.Store(key, want)
			r.event(chain, corev1.EventTypeWarning, "ScheduleTriggerConflict",
				"Trigger %s exists and is not controlled by this Chain; spec.schedule is not run. Rename or delete that Trigger", name)
		}
		return nil, nil
	}
	r.conflictWarned.Delete(key)
	if !trg.DeletionTimestamp.IsZero() {
		return nil, nil
	}
	if equality.Semantic.DeepEqual(trg.Spec, want) && trg.Labels[labelManagedBy] == triggerManagedBy {
		return trg, nil
	}
	trg.Spec = want
	if trg.Labels == nil {
		trg.Labels = map[string]string{}
	}
	trg.Labels[labelManagedBy] = triggerManagedBy
	if err := r.Update(ctx, trg); err != nil {
		return nil, fmt.Errorf("update schedule Trigger %s: %w", name, err)
	}
	log.Info("Updated schedule Trigger", "trigger", name, "schedule", chain.Spec.Schedule, "suspend", chain.Spec.Suspend)
	return trg, nil
}

// conflictWarning identifies the Chain object a ScheduleTriggerConflict event
// was recorded for. The UID tells a recreated Chain apart from the old one,
// whose generation may be the same.
type conflictWarning struct {
	uid        types.UID
	generation int64
}

// deleteScheduleTrigger deletes the Chain's schedule Trigger if the Chain
// controls it; a Trigger of that name it does not control is left alone.
func (r *ChainReconciler) deleteScheduleTrigger(ctx context.Context, chain *aiv1alpha1.Chain) error {
	// Callers reach here when the Chain has no schedule or is not in trigger
	// mode, so a recorded conflict warning no longer applies.
	r.conflictWarned.Delete(client.ObjectKeyFromObject(chain))
	trg := &aiv1alpha1.Trigger{}
	name := chainScheduleTriggerName(chain)
	if err := r.Get(ctx, types.NamespacedName{Namespace: chain.Namespace, Name: name}, trg); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(trg, chain) || !trg.DeletionTimestamp.IsZero() {
		return nil
	}
	if err := r.Delete(ctx, trg, client.Preconditions{UID: &trg.UID},
		client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("delete schedule Trigger %s: %w", name, err)
	}
	logf.FromContext(ctx).Info("Deleted schedule Trigger", "trigger", name)
	return nil
}

// event records an event when a recorder is set.
func (r *ChainReconciler) event(obj *aiv1alpha1.Chain, eventType, reason, format string, args ...any) {
	if r.Recorder != nil {
		r.Recorder.Eventf(obj, eventType, reason, format, args...)
	}
}

// setNextScheduledAt sets status.nextScheduledAt and reports a change.
func setNextScheduledAt(chain *aiv1alpha1.Chain, next *metav1.Time) bool {
	if timesEqual(chain.Status.NextScheduledAt, next) {
		return false
	}
	chain.Status.NextScheduledAt = next.DeepCopy()
	return true
}

// timesEqual compares optional times at the second precision they are stored
// with.
func timesEqual(a, b *metav1.Time) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Unix() == b.Unix()
}
