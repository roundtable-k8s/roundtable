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

import "fmt"

// Task activity events (docs/design/task-activity-events.md).
//
// Knights publish these on core NATS (fire-and-forget) while a task runs so
// dashboards can show live progress. They are never stored in JetStream.

// ActivityVersion is the current value of ActivityEvent.V.
const ActivityVersion = 1

// Bounds every publisher must respect.
const (
	// MaxActivityEventBytes is the maximum serialized size of one event.
	MaxActivityEventBytes = 8 * 1024
	// MaxActivityPreviewChars caps Preview and OutputPreview.
	MaxActivityPreviewChars = 500
	// MaxActivityArgsChars caps ArgsSummary.
	MaxActivityArgsChars = 200
	// ActivityRatePerSecond is the per-task event rate above which tool and
	// assistant events may be sampled. Heartbeats and lifecycle events
	// (task.started, task.heartbeat, task.finished) are never dropped.
	ActivityRatePerSecond = 20
)

// ActivityType identifies the kind of activity event.
type ActivityType string

const (
	ActivityTaskStarted      ActivityType = "task.started"
	ActivityTaskHeartbeat    ActivityType = "task.heartbeat"
	ActivityToolStarted      ActivityType = "tool.started"
	ActivityToolFinished     ActivityType = "tool.finished"
	ActivityAssistantMessage ActivityType = "assistant.message"
	ActivityTaskFinished     ActivityType = "task.finished"
)

// ActivityTypes lists every defined event type.
var ActivityTypes = []ActivityType{
	ActivityTaskStarted, ActivityTaskHeartbeat, ActivityToolStarted,
	ActivityToolFinished, ActivityAssistantMessage, ActivityTaskFinished,
}

// ActivitySubject constructs the subject a knight publishes task activity on.
// Format: {prefix}.activity.{knight}.{taskID}
func ActivitySubject(prefix, knight, taskID string) string {
	return fmt.Sprintf("%s.activity.%s.%s", prefix, knight, taskID)
}

// ActivitySubjectWildcard matches all activity of every knight (for consumers).
// Format: {prefix}.activity.>
func ActivitySubjectWildcard(prefix string) string {
	return fmt.Sprintf("%s.activity.>", prefix)
}

// ActivityPublishSubject is the per-knight publish permission (authz, #170).
// Format: {prefix}.activity.{knight}.>
func ActivityPublishSubject(prefix, knight string) string {
	return fmt.Sprintf("%s.activity.%s.>", prefix, knight)
}

// IntrospectPublishSubject is the per-knight introspect reply permission (authz, #170).
// Format: {prefix}.introspect.{knight}.>
func IntrospectPublishSubject(prefix, knight string) string {
	return fmt.Sprintf("%s.introspect.%s.>", prefix, knight)
}

// ActivityEvent is one task activity event. Common fields are always present
// unless marked optional; type-specific fields are only set for their types.
type ActivityEvent struct {
	// Common fields.
	V         int          `json:"v"`
	Type      ActivityType `json:"type"`
	Timestamp string       `json:"ts"` // RFC 3339, UTC, millisecond precision
	Knight    string       `json:"knight"`
	Pod       string       `json:"pod"`            // POD_NAME
	Node      string       `json:"node,omitempty"` // NODE_NAME
	TaskID    string       `json:"taskId"`
	RunID     string       `json:"runId,omitempty"`
	Chain     string       `json:"chainName,omitempty"`
	Step      string       `json:"stepName,omitempty"`
	Model     string       `json:"model,omitempty"`
	// Seq is per-task, starts at 1 and increases by one for every event
	// published for the task, so consumers can detect gaps.
	Seq       int64 `json:"seq"`
	Truncated bool  `json:"truncated,omitempty"`

	// task.started
	Slot        *int   `json:"slot,omitempty"`
	Concurrency *int   `json:"concurrency,omitempty"`
	QueuedForMs *int64 `json:"queuedFor,omitempty"` // publish -> pickup, ms, when known

	// task.heartbeat and task.finished
	Turns        *int     `json:"turns,omitempty"`
	InputTokens  *int64   `json:"inputTokens,omitempty"`
	OutputTokens *int64   `json:"outputTokens,omitempty"`
	CostUSD      *float64 `json:"costUsd,omitempty"`
	// CurrentTool is the tool in flight at heartbeat time, if any.
	CurrentTool string `json:"currentTool,omitempty"`

	// tool.started / tool.finished
	Tool          string `json:"tool,omitempty"`
	ToolCallID    string `json:"toolCallId,omitempty"`
	ArgsSummary   string `json:"argsSummary,omitempty"`
	DurationMs    *int64 `json:"durationMs,omitempty"` // tool.finished, task.finished
	IsError       *bool  `json:"isError,omitempty"`
	OutputPreview string `json:"outputPreview,omitempty"`

	// assistant.message
	Preview string `json:"preview,omitempty"`

	// task.finished
	Success    *bool  `json:"success,omitempty"`
	ErrorClass string `json:"errorClass,omitempty"`
}
