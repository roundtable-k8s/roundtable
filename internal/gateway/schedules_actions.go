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
	"encoding/json"
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

	// ScheduleTargetMission is the target kind of a schedule that creates a
	// Mission (the chain and knight kinds are in schedules.go).
	ScheduleTargetMission = "mission"
)

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

// ScheduleRunNow is the answer to POST .../run.
type ScheduleRunNow struct {
	// Schedule and Namespace name the schedule.
	Schedule  string `json:"schedule"`
	Namespace string `json:"namespace"`
	// Target is the schedule's target kind: chain, knight or mission.
	Target string `json:"target"`
	// Run is the Chain run the firing starts (or started), the task ID for a
	// knight target or the Mission's name for a mission target; empty while a
	// knight or mission target's request is pending.
	Run string `json:"run,omitempty"`
	// Key is the firing's dedupe key, "manual:<id>".
	Key string `json:"key"`
	// State is started, pending (the controller has not handled it yet) or
	// skipped (see SkipReason).
	State      string `json:"state"`
	SkipReason string `json:"skipReason,omitempty"`
	Caller     string `json:"caller,omitempty"`
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

// TableSchedulesRequest is the body of POST /v1/schedules/suspend and /resume.
type TableSchedulesRequest struct {
	Table     string `json:"table" jsonschema:"table (RoundTable) whose schedules to change"`
	Namespace string `json:"namespace,omitempty" jsonschema:"namespace of the table; omit for the default"`
}

// ScheduleFailure is a schedule a bulk suspend or resume could not change.
type ScheduleFailure struct {
	Name  string `json:"name"`
	Error string `json:"error"`
}

// TableSchedulesResult is the answer to a bulk suspend or resume. Changed
// holds the schedules whose annotation was set or removed, Unchanged those
// that already were in the requested state.
type TableSchedulesResult struct {
	Changed   []*ScheduleState  `json:"changed"`
	Unchanged []*ScheduleState  `json:"unchanged"`
	Failed    []ScheduleFailure `json:"failed"`
}

// SuspendSchedules suspends every schedule of a table (annotation only).
func (s *Service) SuspendSchedules(ctx context.Context, req TableSchedulesRequest) (*TableSchedulesResult, error) {
	return s.changeTableSchedules(ctx, req, true)
}

// ResumeSchedules resumes every schedule of a table; schedules suspended in
// their spec stay suspended (their state carries the message).
func (s *Service) ResumeSchedules(ctx context.Context, req TableSchedulesRequest) (*TableSchedulesResult, error) {
	return s.changeTableSchedules(ctx, req, false)
}

func (s *Service) changeTableSchedules(ctx context.Context, req TableSchedulesRequest, suspend bool) (*TableSchedulesResult, error) {
	if req.Table == "" {
		return nil, fmt.Errorf("%w: table is required", ErrInvalid)
	}
	list, err := s.ListSchedules(ctx, ScheduleFilter{Namespace: req.Namespace, Table: req.Table})
	if err != nil {
		return nil, err
	}
	out := &TableSchedulesResult{Changed: []*ScheduleState{}, Unchanged: []*ScheduleState{}, Failed: []ScheduleFailure{}}
	for _, v := range list {
		// A schedule counts as changed when the annotation flips, which is
		// what the single-schedule functions patch on.
		had, err := s.hasSuspendAnnotation(ctx, v.Namespace, v.Name)
		if err != nil {
			out.Failed = append(out.Failed, ScheduleFailure{Name: v.Name, Error: err.Error()})
			continue
		}
		var st *ScheduleState
		if suspend {
			st, err = s.SuspendSchedule(ctx, v.Namespace, v.Name)
		} else {
			st, err = s.ResumeSchedule(ctx, v.Namespace, v.Name)
		}
		switch {
		case err != nil:
			out.Failed = append(out.Failed, ScheduleFailure{Name: v.Name, Error: err.Error()})
		case had == suspend:
			out.Unchanged = append(out.Unchanged, st)
		default:
			out.Changed = append(out.Changed, st)
		}
	}
	return out, nil
}

func (s *Service) hasSuspendAnnotation(ctx context.Context, namespace, name string) (bool, error) {
	trg, err := s.fetchSchedule(ctx, namespace, name)
	if err != nil {
		return false, err
	}
	return trg.SuspendedByAnnotation(), nil
}

// RunSchedule asks the trigger controller to fire the schedule once now (the
// run-now annotation) and waits briefly for it to be handled. The firing goes
// through the controller's normal path: input rendering, concurrency policy
// and run labels; it fires even while the schedule is suspended. Chain
// targets: the run's name is known up front, so a pending answer names it too.
// Knight targets: Run is the task ID; mission targets: the Mission's name.
// Both are empty while the request is pending.
func (s *Service) RunSchedule(ctx context.Context, namespace, name string) (*ScheduleRunNow, error) {
	caller := callerOr(ctx)
	id := newRunNowID(s.now())
	var chain, target string
	trg, err := s.patchSchedule(ctx, namespace, name, func(t *aiv1alpha1.Trigger) (bool, error) {
		if prev := t.Annotations[aiv1alpha1.AnnotationTriggerRunNow]; prev != "" {
			return false, &ConflictError{State: ScheduleRunPending,
				Reason: fmt.Sprintf("schedule %s/%s already has a run-now request (%s) waiting for the operator; retry shortly", t.Namespace, t.Name, prev)}
		}
		if !t.DeletionTimestamp.IsZero() {
			return false, &ConflictError{State: "deleting", Reason: fmt.Sprintf("schedule %s/%s is being deleted", t.Namespace, t.Name)}
		}
		switch {
		case t.Spec.Target.Knight != nil:
			target = ScheduleTargetKnight
		case t.Spec.Target.Mission != nil:
			target = ScheduleTargetMission
		default:
			target = ScheduleTargetChain
		}
		if ref := t.Spec.Target.ChainRef; ref != nil {
			chain = ref.Name
			if err := s.Schedules.Get(ctx, types.NamespacedName{Namespace: t.Namespace, Name: chain}, &aiv1alpha1.Chain{}); err != nil {
				if apierrors.IsNotFound(err) {
					return false, &ConflictError{State: "targetNotFound",
						Reason: fmt.Sprintf("schedule %s/%s targets Chain %q, which does not exist", t.Namespace, t.Name, chain)}
				}
				return false, k8sError("get target chain", err)
			}
		}
		t.Annotations[aiv1alpha1.AnnotationTriggerRunNow] = id
		t.Annotations[aiv1alpha1.AnnotationTriggerRunNowCaller] = caller
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	key := aiv1alpha1.TriggerRunNowKey(id)
	out := &ScheduleRunNow{Schedule: trg.Name, Namespace: trg.Namespace, Key: key, Target: target, Caller: caller, State: ScheduleRunPending}
	if chain != "" {
		// A Chain run's name derives from the firing's key; a knight task's
		// ID is only known once the controller has sent the task, and a
		// Mission's name is built by the controller.
		out.Run = chainrun.TriggerRunName(chain, trg.Name, key)
	}
	s.Log.Info("Requested schedule run", "namespace", trg.Namespace, "schedule", trg.Name, "run", out.Run, "caller", caller)
	return out, s.waitScheduleRun(ctx, out, min(scheduleRunWait, s.Opts.MaxTimeout))
}

// waitScheduleRun polls the schedule's recent firings for the request's key
// until the controller handled it or the wait ends (state stays pending).
func (s *Service) waitScheduleRun(ctx context.Context, out *ScheduleRunNow, wait time.Duration) error {
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
	// Location is set only when the run's name is known, and follows the
	// target kind: a task, a Mission, or a Chain run (served by the schedule
	// run routes).
	if run.Run != "" {
		switch run.Target {
		case ScheduleTargetKnight:
			w.Header().Set("Location", "/v1/tasks/"+run.Run)
		case ScheduleTargetMission:
			w.Header().Set("Location", "/v1/missions/"+run.Namespace+"/"+run.Run)
		default:
			w.Header().Set("Location", "/v1/schedules/"+run.Namespace+"/"+run.Schedule+"/runs/"+run.Run)
		}
	}
	writeJSON(w, code, run)
}

func (s *Service) handleTableSchedules(suspend bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req TableSchedulesRequest
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			writeError(w, fmt.Errorf("%w: body: %v", ErrInvalid, err))
			return
		}
		if dec.More() {
			writeError(w, fmt.Errorf("%w: body: trailing data after the JSON object", ErrInvalid))
			return
		}
		var res *TableSchedulesResult
		var err error
		if suspend {
			res, err = s.SuspendSchedules(r.Context(), req)
		} else {
			res, err = s.ResumeSchedules(r.Context(), req)
		}
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	}
}
