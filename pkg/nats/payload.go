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

package nats

// TaskPayload is the JSON payload published to NATS for a chain step or knight task.
type TaskPayload struct {
	// TaskID is the unique task identifier.
	TaskID string `json:"taskId"`

	// ChainName is the name of the chain this task belongs to (optional).
	ChainName string `json:"chainName,omitempty"`

	// StepName is the name of the chain step (optional).
	StepName string `json:"stepName,omitempty"`

	// RunID identifies the chain run this task belongs to (optional).
	// Knights should key fresh agent sessions on this value so context
	// never bleeds between runs.
	RunID string `json:"runId,omitempty"`

	// Task is the task description or instruction to execute.
	Task string `json:"task"`

	// Model overrides the knight's configured model for this task only
	// (optional). The operator has already resolved RoundTable aliases and
	// checked it suits the knight's runtime. Empty means the knight default.
	Model string `json:"model,omitempty"`

	// Attempt is the 1-based attempt number of this task (optional; absent
	// means 1). Set by a manual retry (#397); the dead-letter stream keys its
	// de-duplication on taskId and attempt.
	Attempt int `json:"attempt,omitempty"`

	// RetryOf is the task ID this task retries (optional, set by a manual retry).
	RetryOf string `json:"retryOf,omitempty"`
}

// TaskResult is the JSON payload received from NATS for a completed task.
// Supports both controller format (taskId/output) and pi-knight format (task_id/result).
type TaskResult struct {
	// TaskID is the task identifier (controller format).
	TaskID string `json:"taskId,omitempty"`

	// TaskID2 is the task identifier (pi-knight format using snake_case).
	TaskID2 string `json:"task_id,omitempty"`

	// Output is the task output (controller format).
	Output string `json:"output,omitempty"`

	// Result is the task result (pi-knight format).
	Result string `json:"result,omitempty"`

	// Error contains error details if the task failed.
	Error string `json:"error,omitempty"`

	// Success indicates task success (pi-knight format).
	Success *bool `json:"success,omitempty"`

	// Model is the model the knight actually used for the task (optional).
	Model string `json:"model,omitempty"`

	// PRs lists pull requests the task opened or updated (optional).
	PRs []PRRef `json:"prs,omitempty"`

	// Verdict is a review verdict: "ship_it", "needs_changes" or null/absent
	// (optional). Use Structured() to read it with a prose fallback.
	Verdict string `json:"verdict,omitempty"`

	// UsageLimited marks a plan-quota refusal: the knight refused the task
	// before running it because a plan window (claude-knight: "Task refused:
	// Claude plan ... (resets ...)") is at the knight reserve. Not a failure:
	// the caller should re-run or defer, not escalate (#321).
	UsageLimited bool `json:"usage_limited,omitempty"`

	// Refused names why the knight refused the task before running it
	// (optional). Today only pi-knight's "credits": the provider account is
	// out of credits, which does not reset on its own. The operator parses it
	// but treats the result as a plain failure.
	Refused string `json:"refused,omitempty"`
}

// GetTaskID returns the task ID from whichever field was populated.
// This handles compatibility between controller and pi-knight message formats.
func (r *TaskResult) GetTaskID() string {
	if r.TaskID != "" {
		return r.TaskID
	}
	return r.TaskID2
}

// GetOutput returns the output from whichever field was populated.
// This handles compatibility between controller and pi-knight message formats.
func (r *TaskResult) GetOutput() string {
	if r.Output != "" {
		return r.Output
	}
	return r.Result
}

// GetError returns the error message, checking both explicit error field and success boolean.
// This handles compatibility between controller and pi-knight message formats.
func (r *TaskResult) GetError() string {
	if r.Error != "" {
		return r.Error
	}
	if r.Success != nil && !*r.Success {
		return "task reported failure"
	}
	return ""
}
