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

// Package deadletter captures lost and failed knight tasks into a per-table
// JetStream dead-letter stream, so no task disappears (#396).
//
// Per RoundTable it keeps:
//   - <tasksStream>_ADVISORIES, a Limits stream that stores the JetStream
//     max-deliveries advisories of the tasks stream, so a task whose knight
//     died is recorded even when no pod of that knight is up to hear it;
//   - <tasksStream>_DLQ on <prefix>.dlq.>, the dead-letter stream itself;
//   - a durable consumer on the advisories stream that turns each advisory
//     into a dead-letter entry and deletes the never-acked original from the
//     tasks stream;
//   - a core subscription on <prefix>.results.> that dead-letters every
//     success:false result and resolves an open entry when a success:true
//     result for the same task arrives later (pi-knight#84);
//   - entry state in the KV bucket roundtable-dead-letters, key <table>.<taskId>.
//
// Nothing here re-runs a task: execution stays at most once (decision 12).
package deadletter

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/trace"

	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

const (
	// KVBucket holds the state of every dead-letter entry.
	KVBucket = "roundtable-dead-letters"
	// ConsumerName is the operator's durable consumer on each advisories stream.
	ConsumerName = "roundtable-dead-letter"
	// DefaultMaxAge is the dead-letter stream retention when the RoundTable
	// does not set spec.nats.deadLetterMaxAge.
	DefaultMaxAge = 30 * 24 * time.Hour
	// AdvisoryMaxAge is the advisories stream retention.
	AdvisoryMaxAge = 7 * 24 * time.Hour
	// DuplicateWindow is the dead-letter stream's Nats-Msg-Id window.
	DuplicateWindow = 10 * time.Minute

	// advisoryPrefix is the subject prefix of JetStream max-deliveries
	// advisories: <prefix>.<stream>.<consumer>.
	advisoryPrefix = "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES"
)

// Reason says why a task was dead-lettered.
type Reason string

// Dead-letter reasons.
const (
	ReasonLost         Reason = "lost"
	ReasonFailed       Reason = "failed"
	ReasonTimeout      Reason = "timeout"
	ReasonUsageLimited Reason = "usage_limited"
	ReasonRefused      Reason = "refused"
)

// State is the lifecycle state of a dead-letter entry.
type State string

// Entry states.
const (
	// StateOpen is set on capture.
	StateOpen State = "open"
	// StateResolved is set when a success:true result for the same task
	// arrives after (or shortly before) the entry was captured.
	StateResolved State = "resolved"
	// StateRetried is set by the retry API (#397).
	StateRetried State = "retried"
)

// Entry is the JSON body of a message on <prefix>.dlq.<knight>.<taskId>.
type Entry struct {
	TaskID    string `json:"taskId"`
	Table     string `json:"table"`
	Knight    string `json:"knight"`
	Reason    Reason `json:"reason"`
	Error     string `json:"error"`
	Attempt   int    `json:"attempt"`
	RetryOf   string `json:"retryOf"`
	ChainName string `json:"chainName"`
	StepName  string `json:"stepName"`
	RunID     string `json:"runId"`
	// TraceID is the trace of the original task (from its traceparent
	// header), when it had a valid one.
	TraceID        string               `json:"traceId,omitempty"`
	Task           *natspkg.TaskPayload `json:"task"`
	StreamSeq      uint64               `json:"streamSeq"`
	DeadLetteredAt time.Time            `json:"deadLetteredAt"`
}

// Record is the KV value kept per entry under <table>.<taskId>.
type Record struct {
	State     State     `json:"state"`
	Reason    Reason    `json:"reason"`
	Knight    string    `json:"knight"`
	Attempt   int       `json:"attempt"`
	Subject   string    `json:"subject"`
	UpdatedAt time.Time `json:"updatedAt"`
	// DeadLetteredAt is when the task was dead-lettered; unlike UpdatedAt it
	// does not move when the entry is retried or resolved. Zero on records
	// written before the field existed.
	DeadLetteredAt time.Time `json:"deadLetteredAt,omitzero"`
	// RetriedAs is the task ID the retry API published (state retried).
	RetriedAs string `json:"retriedAs,omitempty"`
}

// AdvisoriesStreamName is the stream storing the tasks stream's max-deliveries advisories.
func AdvisoriesStreamName(tasksStream string) string { return tasksStream + "_ADVISORIES" }

// StreamName is the table's dead-letter stream.
func StreamName(tasksStream string) string { return tasksStream + "_DLQ" }

// AdvisorySubject matches every max-deliveries advisory of tasksStream.
func AdvisorySubject(tasksStream string) string {
	return fmt.Sprintf("%s.%s.*", advisoryPrefix, tasksStream)
}

// StreamSubject is the dead-letter stream's subject filter.
func StreamSubject(prefix string) string { return prefix + ".dlq.>" }

// Subject is the subject of one dead-letter entry.
func Subject(prefix, knight, taskID string) string {
	return fmt.Sprintf("%s.dlq.%s.%s", prefix, token(knight), token(taskID))
}

// MsgID is the Nats-Msg-Id of an entry, so the advisory and the knight's own
// lost result for the same attempt produce one entry.
func MsgID(taskID string, attempt int) string {
	return fmt.Sprintf("dlq-%s-%d", taskID, normAttempt(attempt))
}

// Key is the KV key of an entry's state.
func Key(table, taskID string) string { return token(table) + "." + token(taskID) }

func normAttempt(a int) int {
	if a < 1 {
		return 1
	}
	return a
}

// token makes s safe as one or more NATS subject / KV key tokens: characters
// outside [A-Za-z0-9_=-] and '.' become '_', and empty dot-separated tokens
// become '_'. Dots are kept because task IDs already use them in result subjects.
func token(s string) string {
	if s == "" {
		return "_"
	}
	b := []rune(s)
	for i, r := range b {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '_', r == '-', r == '=', r == '.':
		default:
			b[i] = '_'
		}
	}
	parts := strings.Split(string(b), ".")
	for i, p := range parts {
		if p == "" {
			parts[i] = "_"
		}
	}
	return strings.Join(parts, ".")
}

// knightFromSubject returns the knight token of <prefix>.tasks.<domain>.<knight>.
func knightFromSubject(subject string) string {
	if i := strings.LastIndex(subject, "."); i >= 0 {
		return strings.ToLower(subject[i+1:])
	}
	return strings.ToLower(subject)
}

// knightFromConsumer maps a runtime's durable consumer name (<Knight>-consumer)
// back to the knight name.
func knightFromConsumer(consumer string) string {
	return strings.ToLower(strings.TrimSuffix(consumer, "-consumer"))
}

// advisory is the subset of io.nats.jetstream.advisory.v1.max_deliver we use.
type advisory struct {
	Stream     string `json:"stream"`
	Consumer   string `json:"consumer"`
	StreamSeq  uint64 `json:"stream_seq"`
	Deliveries uint64 `json:"deliveries"`
	// Timestamp is when the advisory was raised.
	Timestamp time.Time `json:"timestamp"`
}

// result is a knight result as published on <prefix>.results.<taskId>.
type result struct {
	natspkg.TaskResult
}

// succeeded reports whether r is a success. A result with neither success nor
// error set is not dead-lettered: only explicit failures are.
func (r *result) succeeded() bool {
	if r.Success != nil {
		return *r.Success
	}
	return r.Error == ""
}

// refused reports whether the knight refused the task (a refusal code, or
// refused: true, which decodes to natspkg.RefusedUnspecified).
func (r *result) refused() bool {
	return r.Refused != ""
}

// reason classifies a failed result.
func (r *result) reason() Reason {
	errText := strings.ToLower(r.GetError())
	switch {
	case r.Lost:
		return ReasonLost
	case r.QuotaRefused():
		return ReasonUsageLimited
	case r.refused():
		return ReasonRefused
	case strings.Contains(errText, "timed out") || strings.Contains(errText, "timeout"):
		return ReasonTimeout
	default:
		return ReasonFailed
	}
}

// task decodes the result's original task: a TaskPayload object, or a bare
// prompt string. Nil when absent or unreadable.
func (r *result) task(taskID string) *natspkg.TaskPayload {
	raw := strings.TrimSpace(string(r.Task))
	if raw == "" || raw == "null" {
		return nil
	}
	var p natspkg.TaskPayload
	if err := json.Unmarshal(r.Task, &p); err == nil {
		if p.TaskID == "" {
			p.TaskID = taskID
		}
		return &p
	}
	var s string
	if err := json.Unmarshal(r.Task, &s); err == nil && s != "" {
		return &natspkg.TaskPayload{TaskID: taskID, Task: s}
	}
	return nil
}

// TraceIDFromHeaders returns the 32-hex trace ID of the W3C traceparent header
// in h, or "" when it is absent or not a valid version-00 traceparent with a
// non-zero trace ID.
func TraceIDFromHeaders(h nats.Header) string {
	parts := strings.Split(h.Get("traceparent"), "-")
	if len(parts) != 4 || parts[0] != "00" || len(parts[1]) != 32 || len(parts[2]) != 16 || len(parts[3]) != 2 {
		return ""
	}
	tid, err := trace.TraceIDFromHex(parts[1])
	if err != nil || !tid.IsValid() {
		return ""
	}
	return parts[1]
}

// applyTask copies the task's chain fields and attempt into e.
func (e *Entry) applyTask(p *natspkg.TaskPayload) {
	e.Task = p
	if p == nil {
		return
	}
	if e.TaskID == "" {
		e.TaskID = p.TaskID
	}
	e.ChainName = p.ChainName
	e.StepName = p.StepName
	e.RunID = p.RunID
	e.Attempt = p.Attempt
	e.RetryOf = p.RetryOf
}
