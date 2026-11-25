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
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/util/uuid"
	"sigs.k8s.io/controller-runtime/pkg/client"

	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

const (
	// maxTaskBytes bounds the prompt size.
	maxTaskBytes = 256 << 10
	// defaultListLimit and maxListLimit bound list_tasks.
	defaultListLimit = 20
	maxListLimit     = 200
)

// Task IDs become NATS subject tokens and KV keys, run IDs session keys: no
// wildcards, spaces or empty tokens.
var (
	taskIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-][A-Za-z0-9._-]{0,299}$`)
	runIDPattern  = regexp.MustCompile(`^[A-Za-z0-9_-][A-Za-z0-9._:-]{0,127}$`)
)

// Options configures a Service.
type Options struct {
	// Namespace is the default namespace for knights.
	Namespace string
	// AllowedNamespaces are the namespaces callers may target. The default
	// namespace is always allowed.
	AllowedNamespaces []string
	// DefaultTimeout is how long invoke waits when the caller doesn't say.
	DefaultTimeout time.Duration
	// MaxTimeout caps the caller's wait.
	MaxTimeout time.Duration
	// MaxInFlight caps concurrently waiting invocations (0 = unlimited).
	MaxInFlight int
	// DefaultKnightImage is the operator's DEFAULT_KNIGHT_IMAGE, used to
	// infer a knight's runtime for model validation.
	DefaultKnightImage string
	// Missions configures the mission endpoints (served when
	// Service.Missions is set).
	Missions MissionOptions
	// Runs configures the chain-run endpoints (served when Service.Runs is
	// set).
	Runs RunOptions
}

// Service implements the gateway operations shared by the HTTP API and MCP.
type Service struct {
	Resolver *Resolver
	Bus      Bus
	Opts     Options
	Log      logr.Logger
	// Missions creates, reads and deletes Missions. nil disables the
	// mission endpoints.
	Missions client.Client
	// Runs creates, reads and deletes run copies of opted-in Chains. nil
	// disables the chain-run endpoints.
	Runs client.Client

	now      func() time.Time
	inflight chan struct{}
}

// NewService builds a Service.
func NewService(resolver *Resolver, bus Bus, opts Options, log logr.Logger) *Service {
	if opts.DefaultTimeout <= 0 {
		opts.DefaultTimeout = 60 * time.Second
	}
	if opts.MaxTimeout < opts.DefaultTimeout {
		opts.MaxTimeout = opts.DefaultTimeout
	}
	opts.Missions.defaults()
	opts.Runs.defaults()
	s := &Service{Resolver: resolver, Bus: bus, Opts: opts, Log: log, now: time.Now}
	if opts.MaxInFlight > 0 {
		s.inflight = make(chan struct{}, opts.MaxInFlight)
	}
	return s
}

func (s *Service) namespace(ns string) (string, error) {
	if ns == "" || ns == s.Opts.Namespace {
		return s.Opts.Namespace, nil
	}
	if ns != "*" && (slices.Contains(s.Opts.AllowedNamespaces, ns) || slices.Contains(s.Opts.AllowedNamespaces, "*")) {
		return ns, nil
	}
	return "", fmt.Errorf("%w: namespace %q is not allowed", ErrInvalid, ns)
}

// ListKnights lists the knights in a namespace.
func (s *Service) ListKnights(ctx context.Context, namespace string) ([]KnightInfo, error) {
	ns, err := s.namespace(namespace)
	if err != nil {
		return nil, err
	}
	knights, err := s.Resolver.Knights(ctx, ns)
	if err != nil {
		return nil, err
	}
	out := make([]KnightInfo, 0, len(knights))
	for i := range knights {
		out = append(out, knightInfo(&knights[i], s.Opts.DefaultKnightImage))
	}
	slices.SortFunc(out, func(a, b KnightInfo) int { return strings.Compare(a.Name, b.Name) })
	return out, nil
}

// Invoke publishes a task to a knight and waits for its result up to the
// requested timeout. It always returns the task record; the record's status
// says whether the result arrived (Done) or the caller should poll GetTask.
func (s *Service) Invoke(ctx context.Context, knightName string, req InvokeRequest) (*TaskRecord, error) {
	if strings.TrimSpace(req.Task) == "" {
		return nil, fmt.Errorf("%w: task is required", ErrInvalid)
	}
	if len(req.Task) > maxTaskBytes {
		return nil, fmt.Errorf("%w: task exceeds %d bytes", ErrInvalid, maxTaskBytes)
	}
	if req.RunID != "" && !runIDPattern.MatchString(req.RunID) {
		return nil, fmt.Errorf("%w: runId must match %s", ErrInvalid, runIDPattern)
	}
	wait := s.Opts.DefaultTimeout
	if req.TimeoutSeconds != nil {
		if *req.TimeoutSeconds < 0 {
			return nil, fmt.Errorf("%w: timeoutSeconds must be >= 0", ErrInvalid)
		}
		wait = min(time.Duration(*req.TimeoutSeconds)*time.Second, s.Opts.MaxTimeout)
	}
	ns, err := s.namespace(req.Namespace)
	if err != nil {
		return nil, err
	}

	t, err := s.Resolver.resolve(ctx, ns, knightName)
	if err != nil {
		return nil, err
	}
	if t.Knight.Spec.Suspend {
		return nil, fmt.Errorf("%w: knight %s/%s is suspended", ErrUnavailable, ns, knightName)
	}
	model, err := knightpkg.ResolveModel(req.Model, t.ModelAliases,
		knightpkg.EffectiveImage(t.Knight, s.Opts.DefaultKnightImage))
	if err != nil {
		return nil, fmt.Errorf("%w: invalid model for knight %q: %v", ErrInvalid, knightName, err)
	}

	if s.inflight != nil {
		select {
		case s.inflight <- struct{}{}:
			defer func() { <-s.inflight }()
		default:
			return nil, ErrBusy
		}
	}

	runID := req.RunID
	if runID == "" {
		runID = string(uuid.NewUUID())
	}
	taskID := newTaskID(knightName)
	rec := &TaskRecord{
		TaskID:         taskID,
		RunID:          runID,
		Knight:         knightName,
		Namespace:      ns,
		RoundTable:     t.RoundTable,
		Status:         TaskPending,
		RequestedModel: model,
		CreatedAt:      s.now().UTC(),
		ResultSubject:  natspkg.ResultSubject(t.SubjectPrefix, taskID),
		ResultsStream:  t.ResultsStream,
	}

	watch, err := s.Bus.Watch(rec.ResultSubject)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	defer watch.Close()

	if err := s.Bus.PutRecord(ctx, rec); err != nil {
		return nil, fmt.Errorf("%w: store task: %v", ErrUnavailable, err)
	}
	payload := natspkg.TaskPayload{
		TaskID: taskID,
		RunID:  runID,
		Task:   req.Task,
		Model:  model,
	}
	subject := natspkg.TaskSubject(t.SubjectPrefix, t.Knight.Spec.Domain, knightName)
	if err := s.Bus.Publish(ctx, subject, payload, t.RoundTable); err != nil {
		rec.Status = TaskFailed
		rec.Error = "publish failed: " + err.Error()
		done := s.now().UTC()
		rec.CompletedAt = &done
		_ = s.Bus.PutRecord(context.WithoutCancel(ctx), rec)
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	s.Log.Info("Invoked knight", "knight", knightName, "namespace", ns, "taskId", taskID, "runId", runID, "model", model)

	if wait > 0 {
		wctx, cancel := context.WithTimeout(ctx, wait)
		data, err := watch.Next(wctx)
		cancel()
		if err == nil {
			s.complete(ctx, rec, data)
			return rec, nil
		}
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			s.Log.Error(err, "Waiting for result failed", "taskId", taskID)
		}
	}
	// The result may have landed between the subscription and now through a
	// path the watch missed (e.g. a reconnect): check the stream once.
	if data, err := s.Bus.LastResult(ctx, rec.ResultsStream, rec.ResultSubject); err == nil && data != nil {
		s.complete(ctx, rec, data)
	}
	return rec, nil
}

// GetTask returns a task, reading its result from the results stream if it
// hasn't been recorded yet.
func (s *Service) GetTask(ctx context.Context, taskID string) (*TaskRecord, error) {
	if !taskIDPattern.MatchString(taskID) {
		return nil, fmt.Errorf("%w: task %q", ErrNotFound, taskID)
	}
	rec, err := s.Bus.GetRecord(ctx, taskID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, fmt.Errorf("%w: task %q", ErrNotFound, taskID)
		}
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	if !rec.Done() {
		data, err := s.Bus.LastResult(ctx, rec.ResultsStream, rec.ResultSubject)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
		}
		if data != nil {
			s.complete(ctx, rec, data)
		}
	}
	return rec, nil
}

// TaskFilter narrows ListTasks.
type TaskFilter struct {
	Knight string     `json:"knight,omitempty" jsonschema:"only tasks for this knight"`
	RunID  string     `json:"runId,omitempty" jsonschema:"only tasks of this conversation (runId)"`
	Status TaskStatus `json:"status,omitempty" jsonschema:"only tasks in this status: pending, succeeded or failed"`
	Limit  int        `json:"limit,omitempty" jsonschema:"maximum number of tasks to return (default 20, max 200)"`
}

// ListTasks returns recorded tasks, newest first. Records are as stored:
// call GetTask to refresh a pending one.
func (s *Service) ListTasks(ctx context.Context, f TaskFilter) ([]*TaskRecord, error) {
	recs, err := s.Bus.ListRecords(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	limit := f.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}
	limit = min(limit, maxListLimit)
	out := make([]*TaskRecord, 0, min(len(recs), limit))
	for _, r := range recs {
		if (f.Knight != "" && r.Knight != f.Knight) ||
			(f.RunID != "" && r.RunID != f.RunID) ||
			(f.Status != "" && r.Status != f.Status) {
			continue
		}
		out = append(out, r)
	}
	slices.SortFunc(out, func(a, b *TaskRecord) int { return b.CreatedAt.Compare(a.CreatedAt) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// complete fills rec from a result message and stores it. A malformed result
// marks the task failed rather than leaving it pending forever.
func (s *Service) complete(ctx context.Context, rec *TaskRecord, data []byte) {
	var res natspkg.TaskResult
	if err := json.Unmarshal(data, &res); err != nil {
		rec.Status = TaskFailed
		rec.Error = "malformed result: " + err.Error()
	} else {
		rec.Output = res.GetOutput()
		rec.Model = res.Model
		st := res.Structured()
		rec.PRs, rec.Verdict = st.PRs, st.Verdict
		switch e := res.GetError(); {
		case e != "":
			rec.Status = TaskFailed
			rec.Error = e
		case strings.TrimSpace(rec.Output) == "":
			rec.Status = TaskFailed
			rec.Error = "knight returned empty output"
		default:
			rec.Status = TaskSucceeded
		}
	}
	done := s.now().UTC()
	rec.CompletedAt = &done
	if err := s.Bus.PutRecord(ctx, rec); err != nil {
		// Keep the result in the stream so GET can still find it.
		s.Log.Error(err, "Failed to store task result", "taskId", rec.TaskID)
		return
	}
	if err := s.Bus.Discard(ctx, rec.ResultsStream, rec.ResultSubject); err != nil {
		s.Log.Error(err, "Failed to discard recorded result from the results stream", "taskId", rec.TaskID)
	}
}

// newTaskID returns "invoke-<knight>-<unix ms>-<random>".
func newTaskID(knight string) string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("invoke-%s-%d-%s", knight, time.Now().UnixMilli(), hex.EncodeToString(b[:]))
}
