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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/uuid"

	"github.com/roundtable-k8s/roundtable/internal/deadletter"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

const (
	// maxDeadLetterListLimit bounds a dead-letter listing.
	maxDeadLetterListLimit = 100
	// maxPreviousFailureBytes bounds TaskPayload.PreviousFailure.
	maxPreviousFailureBytes = 2 << 10
)

// DeadLetterView is a dead-letter entry as the API returns it.
type DeadLetterView struct {
	TaskID         string               `json:"taskId"`
	Table          string               `json:"table,omitempty"`
	Knight         string               `json:"knight"`
	State          deadletter.State     `json:"state"`
	Reason         deadletter.Reason    `json:"reason"`
	Error          string               `json:"error,omitempty"`
	Attempt        int                  `json:"attempt"`
	RetryOf        string               `json:"retryOf,omitempty"`
	ChainName      string               `json:"chainName,omitempty"`
	StepName       string               `json:"stepName,omitempty"`
	RunID          string               `json:"runId,omitempty"`
	TraceID        string               `json:"traceId,omitempty"`
	DeadLetteredAt *time.Time           `json:"deadLetteredAt,omitempty"`
	UpdatedAt      time.Time            `json:"updatedAt"`
	RetriedAs      string               `json:"retriedAs,omitempty"`
	Task           *natspkg.TaskPayload `json:"task,omitempty"`
}

// DeadLetterFilter narrows ListDeadLetters.
type DeadLetterFilter struct {
	Knight string `json:"knight,omitempty" jsonschema:"only dead letters of this knight"`
	Reason string `json:"reason,omitempty" jsonschema:"only this reason: lost, failed, timeout, usage_limited or refused"`
	State  string `json:"state,omitempty" jsonschema:"only this state: open, retried or resolved"`
	Limit  int    `json:"limit,omitempty" jsonschema:"maximum number of entries to return (default and max 100)"`
}

// RetryRequest is the body of POST /v1/dead-letters/{taskId}/retry and the
// input of the retry_task MCP tool (TaskID comes from the path over HTTP).
type RetryRequest struct {
	TaskID string `json:"taskId,omitempty" jsonschema:"taskId of the dead-lettered task to retry"`
	// Table disambiguates a task ID that is dead-lettered in more than one RoundTable.
	Table string `json:"table,omitempty" jsonschema:"RoundTable of the entry; only needed if the taskId is dead-lettered in several"`
	// Force retries a chain-owned task outside its chain.
	Force bool `json:"force,omitempty" jsonschema:"retry a chain step on its own anyway (the chain does not continue)"`
	// Namespace of the Knight; defaults to the gateway's namespace.
	Namespace string `json:"namespace,omitempty" jsonschema:"namespace of the knight; omit for the default"`
}

// RetryResult is returned by RetryDeadLetter.
type RetryResult struct {
	TaskID  string `json:"taskId"`
	RetryOf string `json:"retryOf"`
	Attempt int    `json:"attempt"`
	Knight  string `json:"knight"`
}

// ListDeadLetters returns dead-letter entries, newest first, capped at the
// limit, and the number of entries matching the filters before the cap.
func (s *Service) ListDeadLetters(ctx context.Context, f DeadLetterFilter) ([]*DeadLetterView, int, error) {
	recs, err := s.Bus.DeadLetterRecords(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	limit := f.Limit
	if limit <= 0 || limit > maxDeadLetterListLimit {
		limit = maxDeadLetterListLimit
	}
	recs = slices.DeleteFunc(recs, func(r DeadLetterRecord) bool {
		return (f.Knight != "" && r.Knight != f.Knight) ||
			(f.Reason != "" && string(r.Reason) != f.Reason) ||
			(f.State != "" && string(r.State) != f.State)
	})
	slices.SortFunc(recs, func(a, b DeadLetterRecord) int { return deadLetteredTime(b).Compare(deadLetteredTime(a)) })
	total := len(recs)
	if len(recs) > limit {
		recs = recs[:limit]
	}
	out := make([]*DeadLetterView, 0, len(recs))
	for _, r := range recs {
		e, err := s.Bus.DeadLetterEntry(ctx, r.Subject)
		if err != nil {
			return nil, 0, fmt.Errorf("%w: %v", ErrUnavailable, err)
		}
		v := deadLetterView(r, e)
		v.Task = nil // the list stays small; GET one for the payload
		out = append(out, v)
	}
	return out, total, nil
}

// deadLetteredTime is when the entry was dead-lettered, falling back to the
// last update for records written before DeadLetteredAt existed.
func deadLetteredTime(r DeadLetterRecord) time.Time {
	if !r.DeadLetteredAt.IsZero() {
		return r.DeadLetteredAt
	}
	return r.UpdatedAt
}

// retrySuffix matches the "-r<N>" a retry appends to a task ID.
var retrySuffix = regexp.MustCompile(`-r\d+$`)

// GetDeadLetter returns one entry including its original task payload.
func (s *Service) GetDeadLetter(ctx context.Context, taskID, table string) (*DeadLetterView, error) {
	rec, e, err := s.findDeadLetter(ctx, taskID, table)
	if err != nil {
		return nil, err
	}
	return deadLetterView(*rec, e), nil
}

// findDeadLetter loads the state record and body of the entry for taskID.
func (s *Service) findDeadLetter(ctx context.Context, taskID, table string) (*DeadLetterRecord, *deadletter.Entry, error) {
	if !taskIDPattern.MatchString(taskID) {
		return nil, nil, fmt.Errorf("%w: dead letter %q", ErrNotFound, taskID)
	}
	recs, err := s.Bus.DeadLetterRecords(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	var matches []DeadLetterRecord
	for _, r := range recs {
		if strings.HasSuffix(r.Key, "."+taskID) && (table == "" || r.Key == table+"."+taskID) {
			matches = append(matches, r)
		}
	}
	switch len(matches) {
	case 0:
		return nil, nil, fmt.Errorf("%w: dead letter %q", ErrNotFound, taskID)
	case 1:
	default:
		return nil, nil, fmt.Errorf("%w: task %q is dead-lettered in several RoundTables; pass table", ErrInvalid, taskID)
	}
	e, err := s.Bus.DeadLetterEntry(ctx, matches[0].Subject)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	return &matches[0], e, nil
}

func deadLetterView(r DeadLetterRecord, e *deadletter.Entry) *DeadLetterView {
	_, id, _ := strings.Cut(r.Key, ".")
	v := &DeadLetterView{
		TaskID: id,
		Knight: r.Knight, State: r.State, Reason: r.Reason, Attempt: max(r.Attempt, 1),
		UpdatedAt: r.UpdatedAt, RetriedAs: r.RetriedAs,
	}
	if e == nil {
		return v
	}
	v.TaskID, v.Table, v.Error = e.TaskID, e.Table, e.Error
	v.RetryOf, v.ChainName, v.StepName, v.RunID = e.RetryOf, e.ChainName, e.StepName, e.RunID
	v.TraceID = e.TraceID
	v.Task = e.Task
	if !e.DeadLetteredAt.IsZero() {
		t := e.DeadLetteredAt
		v.DeadLetteredAt = &t
	}
	return v
}

// RetryDeadLetter republishes a dead-lettered task to its knight as a new task
// carrying the next attempt number, so the knight can look for partial work.
// It is a deliberate act: nothing retries on its own.
func (s *Service) RetryDeadLetter(ctx context.Context, req RetryRequest) (*RetryResult, error) {
	rec, e, err := s.findDeadLetter(ctx, req.TaskID, req.Table)
	if err != nil {
		return nil, err
	}
	if rec.State != deadletter.StateOpen {
		return nil, &ConflictError{State: string(rec.State), Reason: fmt.Sprintf("dead letter %s is %s, only open entries can be retried", req.TaskID, rec.State)}
	}
	if e == nil || e.Task == nil {
		return nil, fmt.Errorf("%w: dead letter %s has no original task payload to republish", ErrUnprocessable, req.TaskID)
	}
	if e.ChainName != "" && !req.Force {
		return nil, &ConflictError{State: string(rec.State),
			Reason: fmt.Sprintf("dead letter %s belongs to chain %s: rerun the chain or let its retryPolicy handle it (force retries the step on its own)", req.TaskID, e.ChainName)}
	}
	ns, err := s.namespace(req.Namespace)
	if err != nil {
		return nil, err
	}
	knightName := e.Knight
	if knightName == "" {
		knightName = rec.Knight
	}
	t, err := s.Resolver.resolve(ctx, ns, knightName)
	if err != nil {
		return nil, err
	}
	if t.Knight.Spec.Suspend {
		return nil, fmt.Errorf("%w: knight %s/%s is suspended", ErrUnavailable, ns, knightName)
	}

	if e.Table != "" && t.RoundTable != e.Table {
		return nil, fmt.Errorf("%w: knight %s/%s belongs to RoundTable %q, dead letter %s to %q",
			ErrInvalid, ns, knightName, t.RoundTable, req.TaskID, e.Table)
	}

	attempt := max(e.Attempt, 1) + 1
	// RetryOf holds the previous attempt's ID, not the original, so always
	// strip the trailing -rN from the task ID to avoid nesting (t-r2-r4).
	base := retrySuffix.ReplaceAllString(e.TaskID, "")
	newID := fmt.Sprintf("%s-r%d", base, attempt)
	if !taskIDPattern.MatchString(newID) {
		return nil, fmt.Errorf("%w: retry task id %q is not a valid task id", ErrUnprocessable, newID)
	}
	payload := *e.Task
	payload.TaskID = newID
	payload.Attempt = attempt
	payload.RetryOf = e.TaskID
	payload.PreviousFailure = truncateUTF8(previousFailure(e), maxPreviousFailureBytes)
	if payload.RunID == "" {
		payload.RunID = string(uuid.NewUUID())
	}

	// Claim the entry first: the compare-and-set on its revision is what stops
	// two retries (two clicks, two gateway replicas) from both publishing.
	claimed := rec.Record
	claimed.State, claimed.RetriedAs, claimed.UpdatedAt = deadletter.StateRetried, newID, s.now().UTC()
	if err := s.Bus.UpdateDeadLetterRecord(ctx, rec.Key, claimed, rec.Revision); err != nil {
		if errors.Is(err, errStaleRevision) {
			return nil, &ConflictError{State: string(deadletter.StateRetried), Reason: fmt.Sprintf("dead letter %s changed state while retrying", req.TaskID)}
		}
		return nil, fmt.Errorf("%w: claim dead letter: %v", ErrUnavailable, err)
	}

	task := &TaskRecord{
		TaskID: newID, RunID: payload.RunID, Knight: knightName, Namespace: ns, RoundTable: t.RoundTable,
		Status: TaskPending, Caller: callerName(ctx), RequestedModel: payload.Model, CreatedAt: s.now().UTC(),
		ResultSubject: natspkg.ResultSubject(t.SubjectPrefix, newID), ResultsStream: t.ResultsStream,
	}
	fail := func(cause error) error {
		// The rollback must land even if the caller has gone away.
		ctx := context.WithoutCancel(ctx)
		// Hand the entry back so the retry can be tried again. The claim is at
		// revision+1 unless someone raced us, in which case leave it.
		if cur, gerr := s.findDeadLetterByKey(ctx, rec.Key); gerr == nil && cur.State == deadletter.StateRetried && cur.RetriedAs == newID {
			reopened := rec.Record
			reopened.UpdatedAt = s.now().UTC()
			_ = s.Bus.UpdateDeadLetterRecord(ctx, rec.Key, reopened, cur.Revision)
		}
		return fmt.Errorf("%w: %v", ErrUnavailable, cause)
	}
	if err := s.Bus.PutRecord(ctx, task); err != nil {
		return nil, fail(fmt.Errorf("store task: %w", err))
	}
	subject := natspkg.TaskSubject(t.SubjectPrefix, t.Knight.Spec.Domain, knightName)
	if err := s.Bus.Publish(ctx, subject, payload, t.RoundTable); err != nil {
		task.Status, task.Error = TaskFailed, "publish failed: "+err.Error()
		done := s.now().UTC()
		task.CompletedAt = &done
		_ = s.Bus.PutRecord(context.WithoutCancel(ctx), task)
		return nil, fail(err)
	}
	s.Log.Info("Retried dead-lettered task", "knight", knightName, "taskId", newID, "retryOf", e.TaskID, "attempt", attempt)
	return &RetryResult{TaskID: newID, RetryOf: e.TaskID, Attempt: attempt, Knight: knightName}, nil
}

func (s *Service) findDeadLetterByKey(ctx context.Context, key string) (*DeadLetterRecord, error) {
	recs, err := s.Bus.DeadLetterRecords(ctx)
	if err != nil {
		return nil, err
	}
	for i := range recs {
		if recs[i].Key == key {
			return &recs[i], nil
		}
	}
	return nil, ErrNotFound
}

// previousFailure is the failure text handed to the retried task. Lost tasks
// often carry no error, so say what is known instead of nothing.
func previousFailure(e *deadletter.Entry) string {
	if e.Error != "" {
		return e.Error
	}
	reason := string(e.Reason)
	if reason == "" {
		reason = "unknown"
	}
	return fmt.Sprintf("previous attempt was dead-lettered (reason: %s) without an error message; it may have left partial work", reason)
}

// truncateUTF8 cuts s to at most n bytes without splitting a character.
func truncateUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !isRuneStart(s[n]) {
		n--
	}
	return s[:n]
}

func isRuneStart(b byte) bool { return b&0xC0 != 0x80 }

func (s *Service) handleListDeadLetters(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := DeadLetterFilter{Knight: q.Get("knight"), Reason: q.Get("reason"), State: q.Get("state")}
	limit, err := queryInt(r, "limit")
	if err != nil {
		writeError(w, err)
		return
	}
	if limit != nil {
		f.Limit = *limit
	}
	out, total, err := s.ListDeadLetters(r.Context(), f)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deadLetters": out, "total": total})
}

func (s *Service) handleGetDeadLetter(w http.ResponseWriter, r *http.Request) {
	v, err := s.GetDeadLetter(r.Context(), r.PathValue("taskId"), r.URL.Query().Get("table"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (s *Service) handleRetryDeadLetter(w http.ResponseWriter, r *http.Request) {
	var req RetryRequest
	// The body is optional ({"force": true} is its main use).
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, fmt.Errorf("%w: body: %v", ErrInvalid, err))
		return
	}
	req.TaskID = r.PathValue("taskId")
	if q := r.URL.Query().Get("table"); q != "" {
		req.Table = q
	}
	res, err := s.RetryDeadLetter(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Location", "/v1/tasks/"+res.TaskID)
	writeJSON(w, http.StatusAccepted, res)
}
