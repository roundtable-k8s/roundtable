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

package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/chainrun"
)

// Suspend, resume and run-now act on every schedule, including one applied
// from Git: they only patch annotations the trigger controller honours, never
// the spec, so the Git copy (and Flux's field ownership) is left alone.

// scheduleRunWait is how long POST .../run waits for the trigger controller
// to handle the firing before answering 202 (a variable for tests).
var scheduleRunWait = 10 * time.Second

const (
	// schedulePatchRetries bounds how often an action re-reads the Trigger
	// after a write conflict.
	schedulePatchRetries = 5

	// ScheduleRunStarted, ScheduleRunPending and ScheduleRunSkipped are the
	// states of a run-now request.
	ScheduleRunStarted = "started"
	ScheduleRunPending = "pending"
	ScheduleRunSkipped = "skipped"
)

// SchedulesEnabled reports whether the schedule endpoints are served.
func (s *Service) SchedulesEnabled() bool { return s.Schedules != nil }

var errSchedulesDisabled = fmt.Errorf("%w: schedules are disabled on this gateway", ErrNotFound)

// ScheduleState is the answer to suspend and resume: the schedule's effective
// suspension after the change.
type ScheduleState struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	// Suspended is true while spec.suspend or the suspended annotation is set.
	Suspended bool `json:"suspended"`
	// SuspendedBy is "spec", "annotation" or "spec,annotation".
	SuspendedBy string `json:"suspendedBy,omitempty"`
	// Message explains a resume that left the schedule suspended.
	Message string `json:"message,omitempty"`
}

// ScheduleRun is the answer to POST .../run.
type ScheduleRun struct {
	// Schedule and Namespace name the schedule.
	Schedule  string `json:"schedule"`
	Namespace string `json:"namespace"`
	// Run is the Chain run the firing starts (or started).
	Run string `json:"run"`
	// Key is the firing's dedupe key, "manual:<id>".
	Key string `json:"key"`
	// State is started, pending (the controller has not handled it yet) or
	// skipped (see SkipReason).
	State      string `json:"state"`
	SkipReason string `json:"skipReason,omitempty"`
	Caller     string `json:"caller,omitempty"`
}

// fetchSchedule reads a Trigger with a schedule source; anything else is 404.
func (s *Service) fetchSchedule(ctx context.Context, namespace, name string) (*aiv1alpha1.Trigger, error) {
	if !s.SchedulesEnabled() {
		return nil, errSchedulesDisabled
	}
	ns, err := s.namespace(namespace)
	if err != nil {
		return nil, err
	}
	trg := &aiv1alpha1.Trigger{}
	if err := s.Schedules.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, trg); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("%w: schedule %s/%s", ErrNotFound, ns, name)
		}
		return nil, k8sError("get schedule", err)
	}
	if trg.Spec.Source.Schedule == nil {
		return nil, fmt.Errorf("%w: schedule %s/%s", ErrNotFound, ns, name)
	}
	return trg, nil
}

// patchSchedule applies mutate to the schedule's annotations with a merge
// patch guarded by its resourceVersion, re-reading it on conflicts. mutate
// returns false when nothing needs to change.
func (s *Service) patchSchedule(ctx context.Context, namespace, name string,
	mutate func(*aiv1alpha1.Trigger) (bool, error)) (*aiv1alpha1.Trigger, error) {
	for attempt := 0; ; attempt++ {
		trg, err := s.fetchSchedule(ctx, namespace, name)
		if err != nil {
			return nil, err
		}
		patch := client.MergeFromWithOptions(trg.DeepCopy(), client.MergeFromWithOptimisticLock{})
		if trg.Annotations == nil {
			trg.Annotations = map[string]string{}
		}
		changed, err := mutate(trg)
		if err != nil || !changed {
			return trg, err
		}
		err = s.Schedules.Patch(ctx, trg, patch)
		if err == nil {
			return trg, nil
		}
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("%w: schedule %s/%s", ErrNotFound, trg.Namespace, name)
		}
		if !apierrors.IsConflict(err) || attempt >= schedulePatchRetries {
			return nil, k8sError("patch schedule", err)
		}
	}
}

func scheduleState(trg *aiv1alpha1.Trigger) *ScheduleState {
	return &ScheduleState{Name: trg.Name, Namespace: trg.Namespace, Suspended: trg.Suspended(), SuspendedBy: trg.SuspendedBy()}
}

// callerOr is the request's caller, or "gateway" when auth recorded none.
func callerOr(ctx context.Context) string {
	if c := callerName(ctx); c != "" {
		return c
	}
	return "gateway"
}

// SuspendSchedule sets the suspended annotation (its value is the caller).
func (s *Service) SuspendSchedule(ctx context.Context, namespace, name string) (*ScheduleState, error) {
	caller := callerOr(ctx)
	trg, err := s.patchSchedule(ctx, namespace, name, func(t *aiv1alpha1.Trigger) (bool, error) {
		if t.SuspendedByAnnotation() {
			return false, nil
		}
		t.Annotations[aiv1alpha1.AnnotationTriggerSuspended] = caller
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	s.Log.Info("Suspended schedule", "namespace", trg.Namespace, "schedule", trg.Name, "caller", caller)
	return scheduleState(trg), nil
}

// ResumeSchedule removes the suspended annotation. It does not touch
// spec.suspend: a schedule suspended in its spec stays suspended, and the
// answer says so.
func (s *Service) ResumeSchedule(ctx context.Context, namespace, name string) (*ScheduleState, error) {
	trg, err := s.patchSchedule(ctx, namespace, name, func(t *aiv1alpha1.Trigger) (bool, error) {
		if _, ok := t.Annotations[aiv1alpha1.AnnotationTriggerSuspended]; !ok {
			return false, nil
		}
		delete(t.Annotations, aiv1alpha1.AnnotationTriggerSuspended)
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	s.Log.Info("Resumed schedule", "namespace", trg.Namespace, "schedule", trg.Name, "caller", callerOr(ctx))
	st := scheduleState(trg)
	if trg.Spec.Suspend {
		st.Message = "spec.suspend is still true: the schedule stays suspended until spec.suspend is cleared where the Trigger is defined"
	}
	return st, nil
}

// RunSchedule asks the trigger controller to fire the schedule once now (the
// run-now annotation) and waits briefly for it to be handled. The firing goes
// through the controller's normal path: input rendering, concurrency policy
// and run labels; it fires even while the schedule is suspended. The run's
// name is known up front, so a pending answer names it too.
func (s *Service) RunSchedule(ctx context.Context, namespace, name string) (*ScheduleRun, error) {
	caller := callerOr(ctx)
	id := newRunNowID(s.now())
	var chain string
	trg, err := s.patchSchedule(ctx, namespace, name, func(t *aiv1alpha1.Trigger) (bool, error) {
		if prev := t.Annotations[aiv1alpha1.AnnotationTriggerRunNow]; prev != "" {
			return false, &ConflictError{State: ScheduleRunPending,
				Reason: fmt.Sprintf("schedule %s/%s already has a run-now request (%s) waiting for the operator; retry shortly", t.Namespace, t.Name, prev)}
		}
		if !t.DeletionTimestamp.IsZero() {
			return false, &ConflictError{State: "deleting", Reason: fmt.Sprintf("schedule %s/%s is being deleted", t.Namespace, t.Name)}
		}
		if t.Spec.Target.ChainRef == nil {
			return false, &ConflictError{State: "unsupportedTarget",
				Reason: fmt.Sprintf("schedule %s/%s does not target a Chain; run now supports Chain targets only", t.Namespace, t.Name)}
		}
		chain = t.Spec.Target.ChainRef.Name
		if err := s.Schedules.Get(ctx, types.NamespacedName{Namespace: t.Namespace, Name: chain}, &aiv1alpha1.Chain{}); err != nil {
			if apierrors.IsNotFound(err) {
				return false, &ConflictError{State: "targetNotFound",
					Reason: fmt.Sprintf("schedule %s/%s targets Chain %q, which does not exist", t.Namespace, t.Name, chain)}
			}
			return false, k8sError("get target chain", err)
		}
		t.Annotations[aiv1alpha1.AnnotationTriggerRunNow] = id
		t.Annotations[aiv1alpha1.AnnotationTriggerRunNowCaller] = caller
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	key := aiv1alpha1.TriggerRunNowKey(id)
	out := &ScheduleRun{Schedule: trg.Name, Namespace: trg.Namespace, Key: key, Caller: caller,
		Run: chainrun.TriggerRunName(chain, trg.Name, key), State: ScheduleRunPending}
	s.Log.Info("Requested schedule run", "namespace", trg.Namespace, "schedule", trg.Name, "run", out.Run, "caller", caller)
	return out, s.waitScheduleRun(ctx, out, min(scheduleRunWait, s.Opts.MaxTimeout))
}

// waitScheduleRun polls the schedule's recent firings for the request's key
// until the controller handled it or the wait ends (state stays pending).
func (s *Service) waitScheduleRun(ctx context.Context, out *ScheduleRun, wait time.Duration) error {
	wctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	tick := time.NewTicker(s.Opts.Runs.PollInterval)
	defer tick.Stop()
	for {
		trg := &aiv1alpha1.Trigger{}
		err := s.Schedules.Get(ctx, types.NamespacedName{Namespace: out.Namespace, Name: out.Schedule}, trg)
		switch {
		case apierrors.IsNotFound(err):
			return fmt.Errorf("%w: schedule %s/%s was deleted", ErrNotFound, out.Namespace, out.Schedule)
		case err != nil:
			return k8sError("get schedule", err)
		}
		for _, f := range trg.Status.RecentFirings {
			if f.Key != out.Key {
				continue
			}
			if f.Run != "" {
				out.Run, out.State = f.Run, ScheduleRunStarted
			} else {
				out.State, out.SkipReason = ScheduleRunSkipped, f.SkipReason
			}
			return nil
		}
		select {
		case <-wctx.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return nil
		case <-tick.C:
		}
	}
}

// newRunNowID is "<unix ms>-<8 hex>": unique per request.
func newRunNowID(now time.Time) string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return strconv.FormatInt(now.UnixMilli(), 10) + "-" + hex.EncodeToString(b[:])
}

func (s *Service) handleSuspendSchedule(w http.ResponseWriter, r *http.Request) {
	st, err := s.SuspendSchedule(r.Context(), r.PathValue("namespace"), r.PathValue("name"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Service) handleResumeSchedule(w http.ResponseWriter, r *http.Request) {
	st, err := s.ResumeSchedule(r.Context(), r.PathValue("namespace"), r.PathValue("name"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Service) handleRunSchedule(w http.ResponseWriter, r *http.Request) {
	run, err := s.RunSchedule(r.Context(), r.PathValue("namespace"), r.PathValue("name"))
	if err != nil {
		writeError(w, err)
		return
	}
	code := http.StatusCreated
	switch run.State {
	case ScheduleRunPending:
		code = http.StatusAccepted
	case ScheduleRunSkipped:
		// Nothing started (concurrency policy, bad template): the request was
		// handled, but there is no run to point at.
		writeJSON(w, http.StatusConflict, map[string]any{"error": "conflict: " + run.SkipReason, "state": run.State, "run": run})
		return
	}
	w.Header().Set("Location", "/v1/schedules/"+run.Namespace+"/"+run.Schedule+"/runs/"+run.Run)
	writeJSON(w, code, run)
}
