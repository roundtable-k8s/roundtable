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

// Package gateway is the synchronous front door to the knights: an HTTP API
// and an MCP (streamable HTTP) endpoint that publish a task on a knight's NATS
// subject and wait for its result on the RoundTable's results stream.
package gateway

import (
	"errors"
	"time"

	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// TaskStatus is the lifecycle state of an invoked task.
type TaskStatus string

const (
	// TaskPending means the task was published and no result has arrived yet.
	TaskPending TaskStatus = "pending"
	// TaskSucceeded means the knight returned non-empty output and no error.
	TaskSucceeded TaskStatus = "succeeded"
	// TaskFailed means the knight reported an error or returned empty output.
	TaskFailed TaskStatus = "failed"
)

// InvokeRequest is the body of POST /v1/knights/{name}/invoke and the input of
// the invoke_knight MCP tool.
type InvokeRequest struct {
	// Task is the prompt for the knight.
	Task string `json:"task" jsonschema:"the prompt to send to the knight"`
	// RunID continues an earlier conversation: pass the runId of a previous
	// invocation and the knight reuses that agent session. Empty starts a
	// fresh session.
	RunID string `json:"runId,omitempty" jsonschema:"runId of an earlier invocation to continue that conversation; omit to start a new one"`
	// Model overrides the knight's model for this task only (a model ID or a
	// RoundTable model alias).
	Model string `json:"model,omitempty" jsonschema:"model ID or RoundTable alias for this task only; omit to use the knight's model"`
	// TimeoutSeconds is how long to wait for the result before returning the
	// pending task. nil uses the server default; 0 returns immediately.
	TimeoutSeconds *int `json:"timeoutSeconds,omitempty" jsonschema:"seconds to wait for the result before returning a pending task (0 = do not wait)"`
	// Namespace of the Knight; defaults to the gateway's namespace.
	Namespace string `json:"namespace,omitempty" jsonschema:"namespace of the knight; omit for the default"`
}

// TaskRecord is what the gateway stores per invoked task (NATS KV) and
// returns from the API.
type TaskRecord struct {
	TaskID     string     `json:"taskId"`
	RunID      string     `json:"runId"`
	Knight     string     `json:"knight"`
	Namespace  string     `json:"namespace"`
	RoundTable string     `json:"roundTable,omitempty"`
	Status     TaskStatus `json:"status"`
	// Output is the knight's answer (set once the task finished).
	Output string `json:"output,omitempty"`
	// Error is the knight's error (status failed).
	Error string `json:"error,omitempty"`
	// RequestedModel is the resolved per-task model override sent with the task.
	RequestedModel string `json:"requestedModel,omitempty"`
	// Model is the model the knight reports having used.
	Model string `json:"model,omitempty"`
	// PRs and Verdict are the structured result fields (reported by the knight,
	// else recovered from the prose output).
	PRs         []natspkg.PRRef `json:"prs,omitempty"`
	Verdict     string          `json:"verdict,omitempty"`
	CreatedAt   time.Time       `json:"createdAt"`
	CompletedAt *time.Time      `json:"completedAt,omitempty"`

	// ResultSubject and ResultsStream locate the result on NATS.
	ResultSubject string `json:"resultSubject"`
	ResultsStream string `json:"resultsStream,omitempty"`
}

// Done reports whether the task has a result.
func (r *TaskRecord) Done() bool { return r.Status != TaskPending }

// KnightInfo is one entry of list_knights / GET /v1/knights.
type KnightInfo struct {
	Name       string `json:"name"`
	Namespace  string `json:"namespace"`
	Domain     string `json:"domain"`
	Model      string `json:"model,omitempty"`
	Runtime    string `json:"runtime,omitempty"`
	Phase      string `json:"phase,omitempty"`
	Ready      bool   `json:"ready"`
	Suspended  bool   `json:"suspended,omitempty"`
	RoundTable string `json:"roundTable,omitempty"`
}

// Errors the HTTP and MCP layers map to status codes.
var (
	ErrNotFound    = errors.New("not found")
	ErrInvalid     = errors.New("invalid request")
	ErrUnavailable = errors.New("unavailable")
	ErrBusy        = errors.New("too many in-flight invocations")
	ErrForbidden   = errors.New("forbidden")
	// ErrConflict maps to 409; see ConflictError.
	ErrConflict = errors.New("conflict")
	// ErrUnprocessable maps to 422.
	ErrUnprocessable = errors.New("unprocessable")
)

// ConflictError is a 409 that carries the dead-letter entry's current state.
type ConflictError struct {
	State  string
	Reason string
}

func (e *ConflictError) Error() string { return "conflict: " + e.Reason }

// Is makes errors.Is(err, ErrConflict) true.
func (e *ConflictError) Is(target error) bool { return target == ErrConflict }
