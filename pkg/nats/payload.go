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

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"

	"github.com/roundtable-k8s/roundtable/pkg/nats/contract"
)

// The knight task contract (task, result and activity event) is defined by
// the JSON Schemas in roundtable-k8s/knights, vendored with Go types generated
// from them in pkg/nats/contract. The types in this package are the operator's
// view of those messages: plain values instead of pointers, lenient decoding
// of both spellings older knights send, and a few operator-only fields. Enum
// values come from the generated code, and contract_test.go fails when a
// schema property has no field here or a golden fixture does not survive a
// decode and re-encode through these types.

// Refusal is the reason a knight refused a task (TaskResult.Refused). It
// marshals as a string; it decodes a string, true (RefusedUnspecified), and
// false or null (empty), so a runtime that sends a boolean flag does not make
// the whole TaskResult fail to decode.
type Refusal string

// UnmarshalJSON accepts a string, a boolean or null.
func (r *Refusal) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	switch string(b) {
	case "null", "false":
		*r = ""
		return nil
	case "true":
		*r = RefusedUnspecified
		return nil
	}
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("refused: want a string, boolean or null: %w", err)
	}
	*r = Refusal(s)
	return nil
}

// Refusal reasons a knight may report in TaskResult.Refused.
const (
	// RefusedUnspecified: the knight sent refused: true without a reason. A
	// plain refusal, not a quota one. Not a contract enum value (credits,
	// quota): the operator accepts refused: true for older runtimes, but
	// re-encoding the TaskResult emits "refused".
	RefusedUnspecified Refusal = "refused"
	// RefusedCredits: the provider account is out of credits (pi-knight, e.g.
	// OpenRouter 402). Does not reset on its own; the operator treats it as a
	// plain failure.
	RefusedCredits = Refusal(contract.TaskResultRefusedCredits)
	// RefusedQuota: a plan quota window is used up (claude-knight). Resets
	// later; the operator defers the step like UsageLimited (#321).
	RefusedQuota = Refusal(contract.TaskResultRefusedQuota)
)

// Transient failure reasons a knight may report in TaskResult.TransientReason.
const (
	TransientOverloaded  = string(contract.TaskResultTransientReasonOverloaded)
	TransientProvider5xx = string(contract.TaskResultTransientReasonProvider5Xx)
	TransientNetwork     = string(contract.TaskResultTransientReasonNetwork)
)

// Billing modes a knight may report in TaskResult.Billing.
const (
	BillingAPI          = string(contract.TaskResultBillingApi)
	BillingSubscription = string(contract.TaskResultBillingSubscription)
	BillingLocal        = string(contract.TaskResultBillingLocal)
)

// TaskPayload is the JSON payload published to NATS for a chain step or knight
// task (contract.TaskPayload). It holds the fields the operator sends; the
// schema's snake_case and legacy aliases are knight-to-knight spellings the
// operator neither sends nor reads.
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

	// Effort overrides the knight's reasoning effort for this task only
	// (optional): low, medium, high, xhigh or max. Empty means the knight's
	// configured effort (KNIGHT_EFFORT, from Knight.spec.effort).
	// Operator-only: not in the knights contract schema yet.
	Effort string `json:"effort,omitempty"`

	// Attempt is the 1-based attempt number of this task (optional; absent
	// means 1). Set by a manual retry (#397); the dead-letter stream keys its
	// de-duplication on taskId and attempt.
	Attempt int `json:"attempt,omitempty"`

	// RetryOf is the task ID this task retries (optional, set by a manual retry).
	RetryOf string `json:"retryOf,omitempty"`

	// PreviousFailure is why the attempt being retried was dead-lettered
	// (optional, set by a manual retry, at most 2 KB). A knight reads it to
	// know what went wrong and checks for partial work before starting over.
	PreviousFailure string `json:"previousFailure,omitempty"`
}

// TokenUsage is the token count of a task. Input is uncached input only;
// CacheRead and CacheWrite (optional) are the cached input tokens read from
// and written to the prompt cache. Older knights send only Input and Output.
type TokenUsage struct {
	Input      int64 `json:"input,omitempty"`
	Output     int64 `json:"output,omitempty"`
	CacheRead  int64 `json:"cacheRead,omitempty"`
	CacheWrite int64 `json:"cacheWrite,omitempty"`
}

// UnmarshalJSON accepts any JSON number for each count (the contract allows
// a fractional one) and rounds it down, so a fractional count does not fail
// the whole TaskResult decode. NaN, negative and -Inf counts become 0 and
// counts at or above MaxInt64 (including +Inf) become MaxInt64.
func (t *TokenUsage) UnmarshalJSON(b []byte) error {
	var raw struct {
		Input      *float64 `json:"input"`
		Output     *float64 `json:"output"`
		CacheRead  *float64 `json:"cacheRead"`
		CacheWrite *float64 `json:"cacheWrite"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	floor := func(f *float64) int64 {
		switch {
		case f == nil || math.IsNaN(*f) || *f <= 0:
			return 0
		case *f >= math.MaxInt64: // also +Inf; float64(MaxInt64) is 2^63
			return math.MaxInt64
		}
		return int64(math.Floor(*f))
	}
	*t = TokenUsage{
		Input:      floor(raw.Input),
		Output:     floor(raw.Output),
		CacheRead:  floor(raw.CacheRead),
		CacheWrite: floor(raw.CacheWrite),
	}
	return nil
}

// TaskResult is the JSON payload received from NATS for a completed task
// (contract.TaskResult). Supports both the legacy controller format
// (taskId/output, operator-only) and the contract format (task_id/result).
// Every field is optional here: required contract fields that an older knight
// leaves out decode to their zero value instead of failing.
type TaskResult struct {
	// TaskID is the task identifier (legacy controller format, operator-only).
	TaskID string `json:"taskId,omitempty"`

	// TaskID2 is the task identifier (contract format, snake_case).
	TaskID2 string `json:"task_id,omitempty"`

	// Output is the task output (legacy controller format, operator-only).
	Output string `json:"output,omitempty"`

	// Result is the task result (contract format).
	Result string `json:"result,omitempty"`

	// ResultTruncated marks a Result cut to fit the NATS payload limit (head
	// and tail kept with an omission line between them).
	ResultTruncated bool `json:"result_truncated,omitempty"`

	// Knight is the knight that ran the task.
	Knight string `json:"knight,omitempty"`

	// DurationMs is the wall-clock run time in ms.
	DurationMs float64 `json:"duration_ms,omitempty"`

	// Timestamp is when the result was published (RFC 3339). Kept as a
	// string so an unparseable value does not fail the whole result.
	Timestamp string `json:"timestamp,omitempty"`

	// ToolCalls is the number of tool calls the agent made (pi-knight). A pointer so a knight that does not send it
	// (absent) is told apart from one that made no calls (0).
	ToolCalls *float64 `json:"tool_calls,omitempty"`

	// Turns is the number of agent turns (claude-knight).
	Turns float64 `json:"turns,omitempty"`

	// Runtime names the knight runtime (claude-knight: "claude-knight").
	Runtime string `json:"runtime,omitempty"`

	// Lost marks a task delivered to a knight that stopped before finishing
	// it (max_deliver=1); it was not re-run.
	Lost bool `json:"lost,omitempty"`

	// Attempt echoes the task's attempt number (manual retry only).
	Attempt int `json:"attempt,omitempty"`

	// RetryOf echoes the task's retryOf (manual retry only).
	RetryOf string `json:"retry_of,omitempty"`

	// Task is the original task, on failure results only, so the operator can
	// dead-letter and retry it. The contract sends a TaskPayload object; it is
	// kept raw because some runtimes sent the bare prompt string, and a type
	// mismatch must not fail decoding the whole result.
	Task json.RawMessage `json:"task,omitempty"`

	// Transient marks a failure caused by the model provider rather than the
	// task (overloaded, 5xx): the chain may retry it after a backoff (#481).
	Transient bool `json:"transient,omitempty"`

	// TransientReason says why a Transient failure happened: TransientOverloaded,
	// TransientProvider5xx or TransientNetwork.
	TransientReason string `json:"transientReason,omitempty"`

	// Error contains error details if the task failed.
	Error string `json:"error,omitempty"`

	// Success indicates task success (pi-knight format).
	Success *bool `json:"success,omitempty"`

	// Model is the model the knight actually used for the task (optional).
	Model string `json:"model,omitempty"`

	// Cost is the task's cost in USD (optional). For subscription-billed
	// models it is a notional cost; see Billing.
	Cost float64 `json:"cost,omitempty"`

	// Tokens is the task's token use (optional).
	Tokens *TokenUsage `json:"tokens,omitempty"`

	// Billing says how the model is billed: BillingAPI, BillingSubscription or
	// BillingLocal (optional). Subscription cost is notional, so dashboards separate it
	// from real spend.
	Billing string `json:"billing,omitempty"`

	// PRs lists pull requests the task opened or updated (optional).
	PRs []PRRef `json:"prs,omitempty"`

	// Verdict is a review verdict: "ship_it", "needs_changes" or null/absent
	// (optional). Use Structured() to read it with a prose fallback.
	Verdict string `json:"verdict,omitempty"`

	// UsageLimited marks a plan-quota refusal: the knight refused the task
	// before running it because a plan window (claude-knight: "Task refused:
	// Claude plan ... (resets ...)") is at the knight reserve. Not a failure:
	// the caller should re-run or defer, not escalate (#321). Deprecated in
	// the contract in favour of Refused == RefusedQuota; claude-knight sends
	// both during the transition. Use QuotaRefused to read either.
	UsageLimited bool `json:"usage_limited,omitempty"`

	// Refused names why the knight refused the task before running it
	// (optional): RefusedCredits or RefusedQuota. RefusedQuota is handled
	// like UsageLimited; RefusedCredits is a plain failure.
	Refused Refusal `json:"refused,omitempty"`
}

// QuotaRefused reports whether the knight refused the task because a plan
// quota window is used up: usage_limited (deprecated) or refused "quota".
func (r *TaskResult) QuotaRefused() bool {
	return r.UsageLimited || r.Refused == RefusedQuota
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
