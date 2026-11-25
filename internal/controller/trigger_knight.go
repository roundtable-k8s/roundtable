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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/gateway"
	"github.com/roundtable-k8s/roundtable/pkg/metrics"
)

const (
	// DefaultGatewayTaskBucket is the gateway's task KV bucket (chart value
	// gateway.taskBucket).
	DefaultGatewayTaskBucket = "roundtable_gateway_tasks"
	// DefaultGatewayTaskTTL is how long the gateway keeps task records
	// (chart value gateway.taskTTL).
	DefaultGatewayTaskTTL = 7 * 24 * time.Hour
	// triggerTaskPoll is how often a Trigger with tasks in flight looks for
	// their results.
	triggerTaskPoll = 30 * time.Second
)

// taskService returns the gateway service knight-target firings send their
// tasks through, on the operator's NATS connection and the gateway's task KV
// bucket, so they are recorded where the gateway's own tasks are.
func (r *TriggerReconciler) taskService(ctx context.Context) (*gateway.Service, error) {
	nc, err := r.natsConn()
	if err != nil {
		return nil, err
	}
	r.tasksMu.Lock()
	defer r.tasksMu.Unlock()
	if r.tasks != nil && r.tasksConn == nc && !nc.IsClosed() {
		return r.tasks, nil
	}
	bucket, ttl := r.TaskBucket, r.TaskTTL
	if bucket == "" {
		bucket = DefaultGatewayTaskBucket
	}
	if ttl <= 0 {
		ttl = DefaultGatewayTaskTTL
	}
	bus, err := gateway.NewNATSBus(ctx, nc, bucket, ttl)
	if err != nil {
		return nil, err
	}
	svc := gateway.NewService(r.knightResolver(), bus, gateway.Options{
		AllowedNamespaces:  []string{"*"},
		DefaultKnightImage: r.DefaultKnightImage,
	}, logf.Log.WithName("trigger-tasks"))
	r.tasks, r.tasksConn = svc, nc
	return svc, nil
}

func (r *TriggerReconciler) knightResolver() *gateway.Resolver {
	return &gateway.Resolver{Client: r.reader()}
}

// checkKnightTarget reports why a knight target cannot receive tasks: the
// knight does not exist, or sits at another RoundTable than roundTableRef.
// An empty reason means it can.
func (r *TriggerReconciler) checkKnightTarget(ctx context.Context, trg *aiv1alpha1.Trigger) (reason, msg string, err error) {
	kt := trg.Spec.Target.Knight
	table, err := r.knightResolver().KnightTable(ctx, trg.Namespace, kt.Name)
	switch {
	case errors.Is(err, gateway.ErrNotFound):
		return aiv1alpha1.ReasonTriggerTargetNotFound, fmt.Sprintf("Knight %q not found", kt.Name), nil
	case errors.Is(err, gateway.ErrUnavailable):
		return aiv1alpha1.ReasonTriggerTargetNotFound, err.Error(), nil
	case err != nil:
		return "", "", err
	}
	if want := aiv1alpha1.RefName(trg.Spec.RoundTableRef); want != "" && table != want {
		at := "no RoundTable"
		if table != "" {
			at = fmt.Sprintf("RoundTable %q", table)
		}
		return aiv1alpha1.ReasonTriggerTargetWrongTable,
			fmt.Sprintf("Knight %q sits at %s, not at roundTableRef %q", kt.Name, at, want), nil
	}
	return "", "", nil
}

// triggerTaskIdempotencyKey is the gateway Idempotency-Key of a firing's
// task: one task per Trigger and dedupe key, kept as long as the gateway
// keeps its task records.
func triggerTaskIdempotencyKey(trg *aiv1alpha1.Trigger, key string) string {
	sum := sha256.Sum256([]byte(key))
	return "trigger/" + trg.Name + "/" + hex.EncodeToString(sum[:])
}

// knightFiring is what fire hands fireKnight.
type knightFiring struct {
	nn   types.NamespacedName
	trg  *aiv1alpha1.Trigger
	data triggerTemplateData
	key  string
	// firing is fire's firing record, which skip records.
	firing *aiv1alpha1.TriggerFiring
	now    time.Time
	table  string
	mutate func(*aiv1alpha1.TriggerStatus)
	// skip records a firing that sent nothing; deduped does for a duplicate
	// dedupe key.
	skip    func(outcome triggerOutcome, format string, args ...any) (triggerOutcome, error)
	deduped func(format string, args ...any) (triggerOutcome, error)
}

// fireKnight sends one task to the target knight through the gateway's
// single-task path. Its task ID is the firing's run and an active run until
// its result arrives. Dedupe uses the gateway's Idempotency-Key: a key that
// already sent a task sends nothing.
func (r *TriggerReconciler) fireKnight(ctx context.Context, f knightFiring) (triggerOutcome, error) {
	trg, kt := f.trg, f.trg.Spec.Target.Knight
	task, err := renderTriggerTemplate("task", kt.Task, f.data)
	if err != nil {
		return f.skip(triggerTerm, "target.knight.task template: %v", err)
	}
	if strings.TrimSpace(task) == "" {
		return f.skip(triggerTerm, "target.knight.task rendered empty")
	}

	if reason, msg, err := r.checkKnightTarget(ctx, trg); err != nil {
		return triggerRetry, err
	} else if reason != "" {
		// Reconcile reports it in the Ready condition; the event waits.
		return triggerRetry, errors.New(msg)
	}

	// Concurrency: the tasks without a result are the active runs. They
	// are tracked in status.activeRuns, read uncached with the Trigger.
	active := trg.Status.ActiveRuns
	switch trg.Spec.Concurrency.Policy {
	case aiv1alpha1.TriggerConcurrencyForbid:
		if len(active) > 0 {
			return f.skip(triggerHandled, "concurrency Forbid: task %s is active", active[0])
		}
	case aiv1alpha1.TriggerConcurrencyReplace:
		// Rejected by validation: a sent task cannot be withdrawn.
		return f.skip(triggerTerm, "concurrency Replace is not supported with target.knight")
	default:
		if n := trg.Spec.Concurrency.MaxActive; n > 0 && int32(len(active)) >= n {
			return f.skip(triggerHandled, "concurrency Allow: maxActive %d reached", n)
		}
	}

	svc, err := r.taskService(ctx)
	if err != nil {
		return triggerRetry, fmt.Errorf("gateway task service: %w", err)
	}
	rec, err := svc.Invoke(ctx, kt.Name, gateway.InvokeRequest{
		Task:           task,
		Model:          kt.Model,
		Namespace:      trg.Namespace,
		TimeoutSeconds: new(0),
		IdempotencyKey: triggerTaskIdempotencyKey(trg, f.key),
	})
	switch {
	case errors.Is(err, gateway.ErrUnprocessable):
		// The key already sent a different task text.
		return f.deduped("duplicate dedupe key: a task was already sent for it")
	case errors.Is(err, gateway.ErrInvalid):
		return f.skip(triggerTerm, "send task to knight %s: %v", kt.Name, err)
	case err != nil:
		return triggerRetry, fmt.Errorf("send task to knight %s: %w", kt.Name, err)
	case rec.Replayed:
		f.firing.Run = rec.TaskID
		return f.deduped("duplicate dedupe key: task %s exists", rec.TaskID)
	}

	firing := *f.firing
	firing.Run = rec.TaskID
	if rec.Done() {
		firing.Outcome = taskOutcome(rec)
	}
	countTriggerFiring(f.table, trg, triggerOutcomeStarted)
	if err := r.recordFiring(ctx, f.nn, firing, func(st *aiv1alpha1.TriggerStatus) {
		at := metav1.NewTime(f.now)
		st.LastFiredAt = &at
		metrics.TriggerLastFiredTimestampSeconds.WithLabelValues(f.table, trg.Name).Set(float64(f.now.Unix()))
		if !rec.Done() && !slices.Contains(st.ActiveRuns, rec.TaskID) {
			st.ActiveRuns = append(st.ActiveRuns, rec.TaskID)
		}
		if f.mutate != nil {
			f.mutate(st)
		}
	}); err != nil {
		// The task was sent; a redelivery replays its Idempotency-Key.
		logf.FromContext(ctx).Error(err, "Failed to record Trigger firing", "trigger", f.nn.String(), "task", rec.TaskID)
	}
	logf.Log.WithName("trigger").Info("Sent knight task", "trigger", f.nn.String(), "knight", kt.Name, "taskId", rec.TaskID, "key", firing.Key)
	r.event(trg, corev1.EventTypeNormal, "Fired", "Sent task %s to knight %s", rec.TaskID, kt.Name)
	return triggerHandled, nil
}

// taskOutcome maps a finished gateway task to a firing outcome.
func taskOutcome(rec *gateway.TaskRecord) aiv1alpha1.TriggerFiringOutcome {
	if rec.Status == gateway.TaskSucceeded {
		return aiv1alpha1.TriggerFiringSucceeded
	}
	return aiv1alpha1.TriggerFiringFailed
}

// collectTasks looks up the results of a knight target's active tasks
// (status.activeRuns): a task with a result, without a record, or without a
// result after timeoutSeconds is no longer active, and its firing gets the
// outcome. It updates trg's status in place and returns when to look again
// (0: nothing is in flight).
func (r *TriggerReconciler) collectTasks(ctx context.Context, trg *aiv1alpha1.Trigger, now time.Time) (time.Duration, error) {
	var tracked []string
	for _, id := range trg.Status.ActiveRuns {
		// Runs of an earlier chainRef target are not tasks.
		if strings.HasPrefix(id, gateway.TaskIDPrefix) {
			tracked = append(tracked, id)
		}
	}
	if len(tracked) == 0 {
		trg.Status.ActiveRuns = nil
		return 0, nil
	}
	svc, err := r.taskService(ctx)
	if err != nil {
		// Keep tracking them; look again later.
		trg.Status.ActiveRuns = tracked
		logf.FromContext(ctx).Error(err, "Could not read Trigger task results")
		return triggerTaskPoll, nil
	}
	timeout := time.Duration(trg.Spec.Target.Knight.EffectiveTimeoutSeconds()) * time.Second
	var active []string
	for _, id := range tracked {
		rec, err := svc.GetTask(ctx, id)
		var outcome aiv1alpha1.TriggerFiringOutcome
		switch {
		case errors.Is(err, gateway.ErrNotFound):
			outcome = aiv1alpha1.TriggerFiringLost
		case err != nil:
			return 0, fmt.Errorf("read task %s: %w", id, err)
		case rec.Done():
			outcome = taskOutcome(rec)
		case now.Sub(rec.CreatedAt) > timeout:
			outcome = aiv1alpha1.TriggerFiringLost
		default:
			active = append(active, id)
			continue
		}
		for i := range trg.Status.RecentFirings {
			if f := &trg.Status.RecentFirings[i]; f.Run == id && f.Outcome == "" {
				f.Outcome = outcome
			}
		}
		logf.FromContext(ctx).Info("Trigger task finished", "taskId", id, "outcome", outcome)
	}
	trg.Status.ActiveRuns = active
	if len(active) == 0 {
		return 0, nil
	}
	return triggerTaskPoll, nil
}
