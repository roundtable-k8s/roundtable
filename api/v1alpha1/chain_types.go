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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// AnnotationChainRun starts a brand-new chain run (new runId, steps reset)
	// whenever its value differs from status.lastRunTrigger.
	AnnotationChainRun = "ai.roundtable.io/run"

	// AnnotationChainRetryFailed resumes the current run from its failed
	// step(s) only, whenever its value differs from status.lastRetryTrigger.
	AnnotationChainRetryFailed = "ai.roundtable.io/retry-failed"

	// LabelInvokable set to "true" opts a Chain in to runs started through
	// the invoke gateway (POST /v1/chains/{name}/runs, the run_chain tool).
	LabelInvokable = "ai.roundtable.io/invokable"
)

// ChainSpec defines the desired state of a Chain — a declarative multi-knight task pipeline.
type ChainSpec struct {
	// description is a human-readable summary of what this chain accomplishes.
	// +optional
	Description string `json:"description,omitempty"`

	// steps defines the ordered list of pipeline steps.
	// Steps execute sequentially unless parallel grouping is used via `parallel`.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	Steps []ChainStep `json:"steps"`

	// timeoutSeconds is the overall chain timeout in seconds. The entire chain
	// is failed if exceeded.
	// +kubebuilder:default=600
	// +kubebuilder:validation:Minimum=30
	// +kubebuilder:validation:Maximum=86400
	// +optional
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`

	// maxQueueWaitSeconds is how long a published step task may wait for its
	// knight to pick it up before the step fails. The per-step timeout only
	// starts counting once the knight has picked the task up, so a busy (or
	// dead) knight is bounded by this guard instead. Defaults to 1800 when unset.
	// +kubebuilder:validation:Minimum=30
	// +kubebuilder:validation:Maximum=86400
	// +optional
	MaxQueueWaitSeconds int32 `json:"maxQueueWaitSeconds,omitempty"`

	// schedule is an optional cron expression to trigger this chain on a recurring basis.
	// Uses standard cron syntax (e.g., "0 */6 * * *").
	// +optional
	Schedule string `json:"schedule,omitempty"`

	// timeZone is the IANA time zone name (e.g. "America/New_York") the
	// schedule is evaluated in, so "0 7 * * *" fires at 07:00 local time on
	// both sides of a daylight-saving change. Defaults to UTC when unset.
	// "Local" is not accepted.
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:Pattern=`^[A-Za-z][A-Za-z0-9_+\-]*(/[A-Za-z0-9_+\-]+)*$`
	// +optional
	TimeZone string `json:"timeZone,omitempty"`

	// startingDeadlineSeconds bounds catch-up of missed scheduled runs.
	// If the controller was down when a scheduled run should have fired, the
	// run is triggered late only if fewer than this many seconds have passed
	// since the missed schedule time. If not set, a missed run is always
	// caught up (at most one catch-up run is triggered).
	// +kubebuilder:validation:Minimum=1
	// +optional
	StartingDeadlineSeconds *int64 `json:"startingDeadlineSeconds,omitempty"`

	// input provides initial data passed to the first step(s) as JSON.
	// +optional
	Input string `json:"input,omitempty"`

	// outputKnightRef names the Knight that writes chain artifacts when steps
	// have outputPath set. Defaults to the knight "gawain".
	// +optional
	OutputKnightRef *corev1.LocalObjectReference `json:"outputKnightRef,omitempty"`

	// roundTableRef names the RoundTable this chain belongs to (same namespace).
	// It supplies the NATS subjects and streams the steps are published on.
	// +optional
	RoundTableRef *corev1.LocalObjectReference `json:"roundTableRef,omitempty"`

	// suspend, if true, prevents scheduled runs and disallows new executions.
	// +kubebuilder:default=false
	// +optional
	Suspend bool `json:"suspend,omitempty"`

	// retryPolicy configures retry behavior for failed steps.
	// +optional
	RetryPolicy *ChainRetryPolicy `json:"retryPolicy,omitempty"`

	// missionRef names the Mission that owns a mission-scoped chain. It is set
	// by the mission controller; the chain controller resolves NATS config
	// from the mission's RoundTable through it.
	// +optional
	MissionRef *corev1.LocalObjectReference `json:"missionRef,omitempty"`

	// notify configures a completion notification fired exactly once per run
	// when the chain reaches a terminal phase (Succeeded, Failed,
	// PartiallySucceeded).
	// +optional
	Notify *NotifySpec `json:"notify,omitempty"`
}

// ChainStep defines a single step in the pipeline.
type ChainStep struct {
	// name is a unique identifier for this step within the chain.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// knightRef names the Knight (same namespace) that executes this step.
	// Required unless the step is a loop (whose body steps name their own
	// knights).
	// +optional
	KnightRef corev1.LocalObjectReference `json:"knightRef,omitzero"`

	// task is the task prompt or instruction to send to the knight.
	// Supports Go template syntax with access to prior step outputs: {{ .Steps.step_name.Output }}
	// Required unless the step is a loop.
	// +optional
	Task string `json:"task,omitempty"`

	// dependsOn lists step names that must complete successfully before this step runs.
	// If empty, the step runs immediately (or after the previous step in sequence).
	// +optional
	DependsOn []string `json:"dependsOn,omitempty"`

	// timeoutSeconds is the per-step timeout in seconds, counted from knight
	// pickup. Overrides the knight's default taskTimeoutSeconds.
	// +kubebuilder:default=120
	// +kubebuilder:validation:Minimum=10
	// +kubebuilder:validation:Maximum=3600
	// +optional
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`

	// outputKey is the key name under which this step's output is stored for downstream steps.
	// Defaults to the step name if not specified.
	// +optional
	OutputKey string `json:"outputKey,omitempty"`

	// outputPath is an optional file path where this step's output should be written.
	// Supports Go template variables: {{ .Date }} (YYYY-MM-DD), {{ .Chain }} (chain name), {{ .Step }} (step name).
	// When set, the controller dispatches a write task to the outputKnightRef knight after the step succeeds.
	// +optional
	OutputPath string `json:"outputPath,omitempty"`

	// continueOnFailure allows downstream steps to proceed even if this step fails.
	// +kubebuilder:default=false
	// +optional
	ContinueOnFailure bool `json:"continueOnFailure,omitempty"`

	// retry configures per-step retry behavior, overriding the chain-level retryPolicy.
	// For a forEach step it applies to each item individually.
	// +optional
	Retry *StepRetry `json:"retry,omitempty"`

	// model overrides the knight's spec.model for this step only. It is either
	// a model ID or the name of an alias from the chain's RoundTable
	// spec.modelAliases (an alias name wins over a literal model ID). The
	// resolved model must suit the knight's runtime: a Claude model ("opus",
	// "sonnet", "haiku", "claude-...") on a claude-knight, a "provider/model"
	// on a pi-knight. A mismatch fails the step without dispatching it.
	// Empty means the knight's default model.
	// +kubebuilder:validation:MaxLength=256
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._:/@\[\]-]*$`
	// +optional
	Model string `json:"model,omitempty"`

	// effort overrides the knight's reasoning effort for this step.
	// Empty means the knight's own spec.effort. Sent to the knight in the task
	// payload; the runtime maps it onto its own setting.
	// +kubebuilder:validation:Enum=low;medium;high;xhigh;max
	// +optional
	Effort string `json:"effort,omitempty"`

	// fallbackRef names the Knight (same namespace) that re-runs this step
	// when the primary knight refuses the task for plan quota (#321: a
	// claude-knight plan-reserve refusal, "Task refused: Claude plan ...
	// (resets ...)"). The re-run uses NO model override - the
	// cheap/fast/deep aliases resolve to Claude models, so the fallback runs
	// its own default model. For a loop step it applies to the loop's body
	// steps. Unset - or a refusal on the fallback too - the step is deferred
	// until the plan window resets and the chain requeues then (it neither
	// fails nor escalates).
	// +optional
	FallbackRef corev1.LocalObjectReference `json:"fallbackRef,omitzero"`

	// forEach fans this step out: it runs once per element of a JSON list taken
	// from the chain input or a prior step's output. The task template gets the
	// element as {{ .Item }} (alias {{ .item }}), its compact JSON as
	// {{ .ItemJSON }} and its zero-based position as {{ .ItemIndex }}. Every
	// item is its own task with its own taskId and a per-item runId. When all
	// items are done the step output is the joined JSON array of per-item
	// results ({index, item, phase, output, error}), which downstream steps read
	// through {{ .Steps.<name>.Output }}.
	// +optional
	ForEach *ChainForEach `json:"forEach,omitempty"`

	// maxParallel caps how many items of a forEach step are in flight at once.
	// Defaults to the knight's concurrency. Only valid together with forEach.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +optional
	MaxParallel int32 `json:"maxParallel,omitempty"`

	// failurePolicy decides what a failed item does to a forEach step.
	// failFast (default): no further items are dispatched once an item fails
	// (after its retries); in-flight items finish, the rest are Skipped and the
	// step fails. continue: every item runs; the step succeeds unless every
	// item failed, and a chain with failed items ends PartiallySucceeded.
	// Only valid together with forEach.
	// +optional
	FailurePolicy ChainFailurePolicy `json:"failurePolicy,omitempty"`

	// when is a CEL expression that gates the step. It is evaluated once the
	// step's dependencies are done; false skips the step (phase Skipped,
	// reason WhenFalse) without dispatching anything, and a skipped step
	// satisfies its dependents' dependsOn. An evaluation error fails the step.
	// Variables: input (string) and steps, a map from step name to
	// {phase, output, error, reason, outcome, iterations}; outputs are the
	// full outputs (read back from the chain-outputs KV bucket), not the
	// truncated status copies. Besides standard CEL (contains, startsWith,
	// matches, size, exists, ...) the extended string library and
	// json(string) (parses a JSON document, tolerating a fenced ```json
	// block) are available. Examples:
	//   steps.recon.output.contains("22/tcp open")
	//   steps.review.phase == "Succeeded" && !steps.review.output.contains("Verdict: ship it")
	//   steps.fixloop.outcome == "Exhausted"
	// +kubebuilder:validation:MaxLength=4096
	// +optional
	When string `json:"when,omitempty"`

	// resumeRunId is a CEL expression, with the same variables as when, that
	// yields the runId the step's task carries instead of the chain run's
	// runId. Knights key their agent session by runId, so this lets a step
	// resume the session of an earlier run, e.g. json(input).runId for a
	// follow-up chain fed the original run's ID. An empty result keeps the
	// chain run's runId; an evaluation error fails the step. Not valid on
	// forEach or loop steps.
	// +kubebuilder:validation:MaxLength=1024
	// +optional
	ResumeRunID string `json:"resumeRunId,omitempty"`

	// loop makes this step a bounded loop: its body steps run in order, round
	// after round, until the until expression holds or maxIterations rounds
	// have run. A loop step has no knightRef, task, forEach, model, retry or
	// outputPath of its own; its output is the output of the last step that
	// ran in it.
	// +optional
	Loop *ChainLoop `json:"loop,omitempty"`
}

// ChainLoopExhaustedPolicy decides what an exhausted loop does to its step.
// +kubebuilder:validation:Enum=fail;continue
type ChainLoopExhaustedPolicy string

const (
	// ChainLoopExhaustedFail fails the loop step when maxIterations rounds
	// ran without until holding.
	ChainLoopExhaustedFail ChainLoopExhaustedPolicy = "fail"
	// ChainLoopExhaustedContinue lets the loop step succeed with outcome
	// Exhausted so later steps can branch on it with when; the chain then
	// ends PartiallySucceeded at best.
	ChainLoopExhaustedContinue ChainLoopExhaustedPolicy = "continue"
)

// ChainLoop is a bounded loop over an ordered list of body steps.
//
// The loop tracks "last": the most recent step in its flow, i.e. the from
// step on entry, then each body step as it finishes. until is evaluated
// against last on entry (only when from is set) and after each full round.
type ChainLoop struct {
	// from names the step (listed in dependsOn) whose result the loop starts
	// from, e.g. the first review. It seeds last, so until is checked before
	// the first round (a review that already passes runs zero rounds) and the
	// first body step sees it as {{ .Loop.Last.Output }}.
	// +optional
	From string `json:"from,omitempty"`

	// until is a CEL expression; the loop ends with outcome Converged as soon
	// as it is true. Variables: everything when gets (input, steps), plus
	// last ({step, phase, output, error}), iteration (the round just
	// finished, 0 on entry), maxIterations and body, a map from body step
	// name to {phase, output, error, iteration}. Example:
	//   last.output.contains("Verdict: ship it")
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=4096
	Until string `json:"until"`

	// maxIterations bounds the number of rounds.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=20
	// +optional
	MaxIterations int32 `json:"maxIterations,omitempty"`

	// onExhausted decides the step's phase when maxIterations rounds ran
	// without until holding: fail (default) fails the step; continue
	// succeeds it with outcome Exhausted so dependents can branch on
	// steps.<name>.outcome. Either way the outcome is Exhausted.
	// +optional
	OnExhausted ChainLoopExhaustedPolicy `json:"onExhausted,omitempty"`

	// steps is the loop body, run in order each round. Body tasks are
	// published with the chain's runId, so a knight that already worked on
	// this run (e.g. the coder of an earlier implement step) resumes the same
	// agent session. Body task templates get, besides .Steps and .Input,
	// .Loop.Iteration (1-based), .Loop.MaxIterations, .Loop.Last (the
	// previous step in the flow: {Step, Phase, Output, Error}) and
	// .Loop.Steps.<body>.Output/.Error (latest round each ran in).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=5
	Steps []ChainLoopStep `json:"steps"`
}

// ChainLoopStep is one step of a loop body.
type ChainLoopStep struct {
	// name identifies the body step within its loop.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9_-]*$`
	Name string `json:"name"`

	// knightRef names the Knight that runs this body step.
	// +kubebuilder:validation:Required
	KnightRef corev1.LocalObjectReference `json:"knightRef"`

	// task is the task template (see ChainLoop.steps for the extra .Loop data).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Task string `json:"task"`

	// timeoutSeconds is the per-task timeout in seconds, counted from knight
	// pickup.
	// +kubebuilder:default=120
	// +kubebuilder:validation:Minimum=10
	// +kubebuilder:validation:Maximum=3600
	// +optional
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`

	// model overrides the knight's model for this body step (same rules as
	// ChainStep.model, including RoundTable model aliases).
	// +kubebuilder:validation:MaxLength=256
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._:/@\[\]-]*$`
	// +optional
	Model string `json:"model,omitempty"`

	// effort overrides the knight's reasoning effort for this body step (same as ChainStep.effort).
	// Empty means the knight's own spec.effort. Sent to the knight in the task
	// payload; the runtime maps it onto its own setting.
	// +kubebuilder:validation:Enum=low;medium;high;xhigh;max
	// +optional
	Effort string `json:"effort,omitempty"`

	// retry configures retries of a failed body task (per round), overriding
	// the chain-level retryPolicy.
	// +optional
	Retry *StepRetry `json:"retry,omitempty"`
}

// ChainLoopOutcome is how a loop ended.
type ChainLoopOutcome string

const (
	// ChainLoopConverged: until held (possibly on entry, after zero rounds).
	ChainLoopConverged ChainLoopOutcome = "Converged"
	// ChainLoopExhausted: maxIterations rounds ran without until holding.
	ChainLoopExhausted ChainLoopOutcome = "Exhausted"
	// ChainLoopFailed: a body step failed (after its retries) or until could
	// not be evaluated.
	ChainLoopFailed ChainLoopOutcome = "Failed"
)

// ChainFailurePolicy controls how item failures affect a forEach step.
// +kubebuilder:validation:Enum=failFast;continue
type ChainFailurePolicy string

const (
	ChainFailurePolicyFailFast ChainFailurePolicy = "failFast"
	ChainFailurePolicyContinue ChainFailurePolicy = "continue"
)

// ChainForEach selects the list a forEach step fans out over.
type ChainForEach struct {
	// from names the JSON document holding the list: "input" (spec.input) or
	// "steps.<stepName>" (that step's output; the step must be listed in
	// dependsOn). Prose around the JSON is tolerated when the JSON sits in a
	// fenced ```json code block.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^(input|steps\.[A-Za-z0-9_-]+)$`
	From string `json:"from"`

	// path is a JSONPath expression (kubectl syntax, e.g. "{.issues}" or
	// ".issues[*].number") evaluated against the document. A single list
	// result is the item list; otherwise the values found form the list. When
	// empty the document itself must be a JSON array.
	// +optional
	Path string `json:"path,omitempty"`

	// maxItems is the largest list the step accepts. A longer list fails the
	// step instead of being silently truncated.
	// +kubebuilder:default=50
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=200
	// +optional
	MaxItems int32 `json:"maxItems,omitempty"`

	// uniqueBy names a key every item (a JSON object) must have. Items whose
	// value under that key repeats an earlier item's are dropped, so the list
	// fans out once per distinct value (e.g. "number": one task per issue).
	// An item that is not an object or lacks the key fails the step.
	// +kubebuilder:validation:Pattern=`^[A-Za-z_][A-Za-z0-9_-]*$`
	// +optional
	UniqueBy string `json:"uniqueBy,omitempty"`
}

// StepRetry configures retry behavior for an individual step.
type StepRetry struct {
	// maxAttempts is the maximum number of retry attempts for this step.
	// +kubebuilder:default=0
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=10
	// +optional
	MaxAttempts int32 `json:"maxAttempts,omitempty"`

	// backoffSeconds is the delay between retries in seconds.
	// +kubebuilder:default=30
	// +kubebuilder:validation:Minimum=1
	// +optional
	BackoffSeconds int32 `json:"backoffSeconds,omitempty"`
}

// ChainRetryPolicy configures retry behavior for failed steps.
type ChainRetryPolicy struct {
	// maxRetries is the maximum number of retries per step.
	// +kubebuilder:default=0
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=5
	// +optional
	MaxRetries int32 `json:"maxRetries,omitempty"`

	// backoffSeconds is the delay between retries in seconds.
	// +kubebuilder:default=30
	// +optional
	BackoffSeconds int32 `json:"backoffSeconds,omitempty"`
}

// ChainPhase represents the current lifecycle phase of the Chain.
// +kubebuilder:validation:Enum=Idle;Running;Succeeded;Failed;Suspended;PartiallySucceeded
type ChainPhase string

const (
	ChainPhaseIdle               ChainPhase = "Idle"
	ChainPhaseRunning            ChainPhase = "Running"
	ChainPhaseSucceeded          ChainPhase = "Succeeded"
	ChainPhaseFailed             ChainPhase = "Failed"
	ChainPhaseSuspended          ChainPhase = "Suspended"
	ChainPhasePartiallySucceeded ChainPhase = "PartiallySucceeded"
)

// ChainStepPhase represents the status of an individual step.
// +kubebuilder:validation:Enum=Pending;Running;Succeeded;Failed;Skipped;Deferred
type ChainStepPhase string

const (
	ChainStepPhasePending   ChainStepPhase = "Pending"
	ChainStepPhaseRunning   ChainStepPhase = "Running"
	ChainStepPhaseSucceeded ChainStepPhase = "Succeeded"
	ChainStepPhaseFailed    ChainStepPhase = "Failed"
	ChainStepPhaseSkipped   ChainStepPhase = "Skipped"
	// Deferred parks a step the knight refused for plan quota (#321): the
	// chain requeues until the plan window resets (deferUntil), then the
	// step runs again. It is neither a failure nor an escalation.
	ChainStepPhaseDeferred ChainStepPhase = "Deferred"
)

// TokenTotals is a sum of input, cached input and output tokens.
type TokenTotals struct {
	// input is the number of uncached input tokens. Cached input is counted
	// in cacheRead and cacheWrite, not here.
	// +optional
	Input int64 `json:"input,omitempty"`

	// output is the number of output tokens.
	// +optional
	Output int64 `json:"output,omitempty"`

	// cacheRead is the number of input tokens read from the prompt cache.
	// +optional
	CacheRead int64 `json:"cacheRead,omitempty"`

	// cacheWrite is the number of input tokens written to the prompt cache.
	// +optional
	CacheWrite int64 `json:"cacheWrite,omitempty"`
}

// IsZero reports whether t counts no tokens.
func (t TokenTotals) IsZero() bool {
	return t.Input <= 0 && t.Output <= 0 && t.CacheRead <= 0 && t.CacheWrite <= 0
}

// Add returns t plus o, allocating t when nil.
func (t *TokenTotals) Add(o TokenTotals) *TokenTotals {
	if t == nil {
		t = &TokenTotals{}
	}
	t.Input += o.Input
	t.Output += o.Output
	t.CacheRead += o.CacheRead
	t.CacheWrite += o.CacheWrite
	return t
}

// AnnotationRunTemplate names the template Chain a run copy was started from
// (by issue pickup, a Trigger or the gateway). Metrics label the run by it, so
// their cardinality stays bounded.
const AnnotationRunTemplate = "ai.roundtable.io/run-template"

// ChainStepStatus tracks the execution status of an individual step.
type ChainStepStatus struct {
	// name matches the step name from the spec.
	Name string `json:"name"`

	// phase is the current execution phase of this step.
	// +optional
	Phase ChainStepPhase `json:"phase,omitempty"`

	// taskID is the unique NATS task identifier for this step's current execution.
	// Used to poll for the exact result message, preventing stale result replay.
	// +optional
	TaskID string `json:"taskId,omitempty"`

	// startedAt is when the step's task was published (queued) to the knight.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// taskSeq is the tasks-stream sequence number of the published task. It is
	// used to detect knight pickup (the knight's consumer acknowledging it).
	// +optional
	TaskSeq uint64 `json:"taskSeq,omitempty"`

	// pickedUpAt is when the knight picked the task up. The step timeout is
	// measured from here, not from startedAt.
	// +optional
	PickedUpAt *metav1.Time `json:"pickedUpAt,omitempty"`

	// completedAt is when the step finished execution.
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`

	// output is the result data from this step (truncated if large).
	// +optional
	Output string `json:"output,omitempty"`

	// error contains the error message if the step failed.
	// +optional
	Error string `json:"error,omitempty"`

	// retries is the number of retry attempts made.
	// +optional
	Retries int32 `json:"retries,omitempty"`

	// model is the model requested for the step's current task after alias
	// resolution (empty when the step has no override). It is replaced by the
	// model the knight reports having used, when the knight reports one.
	// +optional
	Model string `json:"model,omitempty"`

	// knight is the Knight running the step's current task: the spec's
	// knightRef, or the fallbackRef while a plan-quota refusal is re-run on
	// the fallback knight. Empty before the step's first dispatch.
	// +optional
	Knight string `json:"knight,omitempty"`

	// costUSD is the step's cost in USD, summed over its retries and fan-out
	// items or loop rounds. Subscription-billed models report a notional cost.
	// +optional
	CostUSD string `json:"costUSD,omitempty"`

	// tokens is the step's token use, summed like costUSD.
	// +optional
	Tokens *TokenTotals `json:"tokens,omitempty"`

	// providerRetries is the number of retries made after a transient model
	// provider failure (HTTP 529/503/502/500, overloaded), #481.
	// +optional
	ProviderRetries int32 `json:"providerRetries,omitempty"`

	// transientReason is why the last transient provider failure happened:
	// overloaded, provider_5xx or network.
	// +optional
	TransientReason string `json:"transientReason,omitempty"`

	// deferUntil is when a Deferred step (a plan-quota refusal or a provider
	// overload backoff) runs again.
	// +optional
	DeferUntil *metav1.Time `json:"deferUntil,omitempty"`

	// fanOut tracks the items of a forEach step. Unset for ordinary steps.
	// +optional
	FanOut *ChainFanOutStatus `json:"fanOut,omitempty"`

	// reason is a short machine-readable reason for the step's phase, e.g.
	// WhenFalse (skipped by its when expression) or ChainFailed (skipped
	// because the chain failed first).
	// +optional
	Reason string `json:"reason,omitempty"`

	// loop tracks a loop step. Unset for ordinary steps.
	// +optional
	Loop *ChainLoopStatus `json:"loop,omitempty"`
}

// ChainLoopStatus is the bounded state of a loop step: the round counter,
// the outcome and one entry per body step holding its latest round only.
type ChainLoopStatus struct {
	// iteration is the current (or last) round, 1-based; 0 before the first.
	// +optional
	Iteration int32 `json:"iteration,omitempty"`

	// maxIterations is the round cap for this run. It starts at
	// spec.loop.maxIterations and grows by that much each time
	// ai.roundtable.io/retry-failed resumes an exhausted loop.
	// +optional
	MaxIterations int32 `json:"maxIterations,omitempty"`

	// outcome is set once the loop ends: Converged, Exhausted or Failed.
	// +optional
	Outcome ChainLoopOutcome `json:"outcome,omitempty"`

	// steps holds the latest state of each body step, in body order.
	// +optional
	Steps []ChainLoopStepStatus `json:"steps,omitempty"`

	// history is a bounded, append-only record of body task executions,
	// oldest first: one entry each time a body task finishes (a retried
	// attempt adds one entry per attempt). Unlike steps, which only holds the
	// latest round of each body step, history keeps earlier rounds so their
	// outcomes stay visible. At most the last min(maxIterations, 20) rounds
	// are kept and outputSummary is truncated to 1 KiB. Loop semantics and
	// template data (.Loop.Steps) are unaffected.
	// +optional
	History []ChainLoopHistoryEntry `json:"history,omitempty"`
}

// ChainLoopHistoryEntry is one finished body task execution in a loop round.
type ChainLoopHistoryEntry struct {
	// round is the 1-based loop round the task ran in.
	Round int32 `json:"round"`

	// step is the body step name.
	Step string `json:"step"`

	// phase is how the execution ended: Succeeded or Failed.
	Phase ChainStepPhase `json:"phase"`

	// taskId is the NATS task identifier of the execution.
	// +optional
	TaskID string `json:"taskId,omitempty"`

	// startedAt is when the task was published.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// finishedAt is when the task finished.
	// +optional
	FinishedAt *metav1.Time `json:"finishedAt,omitempty"`

	// error is the (truncated) failure message of a failed execution.
	// +optional
	Error string `json:"error,omitempty"`

	// outputSummary is the task output truncated to 1 KiB.
	// +optional
	OutputSummary string `json:"outputSummary,omitempty"`
}

// ChainLoopStepStatus is the state of a loop body step in its latest round.
type ChainLoopStepStatus struct {
	// name matches the body step name.
	Name string `json:"name"`

	// iteration is the round this entry belongs to.
	// +optional
	Iteration int32 `json:"iteration,omitempty"`

	// phase is the body step's phase in that round.
	// +optional
	Phase ChainStepPhase `json:"phase,omitempty"`

	// taskID is the NATS task identifier of the body step's current task.
	// +optional
	TaskID string `json:"taskId,omitempty"`

	// taskSeq is the tasks-stream sequence of the published task.
	// +optional
	TaskSeq uint64 `json:"taskSeq,omitempty"`

	// startedAt is when the task was published.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// pickedUpAt is when the knight picked the task up.
	// +optional
	PickedUpAt *metav1.Time `json:"pickedUpAt,omitempty"`

	// completedAt is when the task finished.
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`

	// output is the task's result (truncated; the full output is in the
	// chain-outputs KV bucket under <chain>.<loopStep>.<bodyStep>).
	// +optional
	Output string `json:"output,omitempty"`

	// error is the task's error message (truncated).
	// +optional
	Error string `json:"error,omitempty"`

	// retries is the number of retry attempts made in this round.
	// +optional
	Retries int32 `json:"retries,omitempty"`

	// model is the model requested for (or reported by) the task.
	// +optional
	Model string `json:"model,omitempty"`

	// knight is the Knight running the task: the body step's knightRef, or
	// the loop step's fallbackRef while a plan-quota refusal is re-run on
	// the fallback knight.
	// +optional
	Knight string `json:"knight,omitempty"`
}

// ChainFanOutStatus is the bounded per-item state of a forEach step.
//
// Every item's phase is kept in itemPhases (one character per item) so a
// resumed or retried fan-out never redoes finished items. Detailed entries are
// kept for every in-flight item and for at most a fixed number of finished
// ones (failed items first, then the most recently finished); the rest are
// only counted in omittedItems and their outputs are read back from the
// chain-outputs NATS KV bucket when the step joins.
type ChainFanOutStatus struct {
	// total is the number of items in the list.
	Total int32 `json:"total"`

	// itemsHash fingerprints the resolved list; a different list on a later
	// reconcile of the same run fails the step instead of mixing item sets.
	// +optional
	ItemsHash string `json:"itemsHash,omitempty"`

	// itemPhases holds one character per item, in list order:
	// P=Pending, R=Running, S=Succeeded, F=Failed, K=Skipped.
	// +optional
	ItemPhases string `json:"itemPhases,omitempty"`

	// pending is the number of items not dispatched yet.
	// +optional
	Pending int32 `json:"pending,omitempty"`
	// running is the number of items in flight.
	// +optional
	Running int32 `json:"running,omitempty"`
	// succeeded is the number of items that succeeded.
	// +optional
	Succeeded int32 `json:"succeeded,omitempty"`
	// failed is the number of items that failed (after their retries).
	// +optional
	Failed int32 `json:"failed,omitempty"`
	// skipped is the number of items that were never run.
	// +optional
	Skipped int32 `json:"skipped,omitempty"`

	// items are the detailed per-item entries (bounded; see omittedItems).
	// +optional
	Items []ChainItemStatus `json:"items,omitempty"`

	// omittedItems counts finished items whose detailed entry was dropped to
	// keep the status bounded.
	// +optional
	OmittedItems int32 `json:"omittedItems,omitempty"`
}

// ChainItemStatus is the execution state of one forEach item.
type ChainItemStatus struct {
	// index is the item's zero-based position in the list.
	Index int32 `json:"index"`

	// phase is the item's execution phase.
	// +optional
	Phase ChainStepPhase `json:"phase,omitempty"`

	// runId is the per-item run identity sent to the knight.
	// +optional
	RunID string `json:"runId,omitempty"`

	// taskID is the NATS task identifier of the item's current execution.
	// +optional
	TaskID string `json:"taskId,omitempty"`

	// taskSeq is the tasks-stream sequence of the published item task.
	// +optional
	TaskSeq uint64 `json:"taskSeq,omitempty"`

	// startedAt is when the item task was published.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// pickedUpAt is when the knight picked the item task up.
	// +optional
	PickedUpAt *metav1.Time `json:"pickedUpAt,omitempty"`

	// completedAt is when the item finished.
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`

	// output is the item's result (truncated).
	// +optional
	Output string `json:"output,omitempty"`

	// error is the item's error message (truncated).
	// +optional
	Error string `json:"error,omitempty"`

	// retries is the number of retry attempts made for this item.
	// +optional
	Retries int32 `json:"retries,omitempty"`
}

// TraceContext is a W3C trace context (https://www.w3.org/TR/trace-context/)
// in its header form.
type TraceContext struct {
	// traceparent is the W3C traceparent value, e.g.
	// 00-<32 hex trace id>-<16 hex span id>-<2 hex flags>.
	// +optional
	Traceparent string `json:"traceparent,omitempty"`

	// tracestate is the W3C tracestate value (vendor-specific trace data).
	// +optional
	Tracestate string `json:"tracestate,omitempty"`
}

// ChainStatus defines the observed state of Chain.
type ChainStatus struct {
	// phase is the current lifecycle phase of the chain.
	// +optional
	Phase ChainPhase `json:"phase,omitempty"`

	// stepStatuses tracks the status of each step.
	// +optional
	StepStatuses []ChainStepStatus `json:"stepStatuses,omitempty"`

	// startedAt is when the current chain run began.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// completedAt is when the current chain run finished.
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`

	// costUSD is the cost in USD of the current (or most recent) run: the sum
	// of its steps' costUSD.
	// +optional
	CostUSD string `json:"costUSD,omitempty"`

	// tokens is the token use of the current (or most recent) run.
	// +optional
	Tokens *TokenTotals `json:"tokens,omitempty"`

	// runsCompleted is the total number of successful chain runs.
	// +optional
	RunsCompleted int64 `json:"runsCompleted,omitempty"`

	// runsFailed is the total number of failed chain runs.
	// +optional
	RunsFailed int64 `json:"runsFailed,omitempty"`

	// lastScheduledAt is when the chain was last triggered by its cron schedule.
	// +optional
	LastScheduledAt *metav1.Time `json:"lastScheduledAt,omitempty"`

	// nextScheduledAt is the next time the cron schedule will fire, evaluated
	// in spec.timeZone. Unset when the chain has no schedule or is suspended.
	// +optional
	NextScheduledAt *metav1.Time `json:"nextScheduledAt,omitempty"`

	// runId uniquely identifies the current (or most recent) chain run.
	// It is embedded in task IDs and NATS KV entries so results produced by
	// a previous run can never be attributed to the current one.
	// +optional
	RunID string `json:"runId,omitempty"`

	// traceContext is the W3C trace context of the current (or most recent)
	// run's root span. Every reconcile of the run parents its spans from it,
	// so the run is one trace. Unset when tracing is off. A new run (or a
	// retry-failed resume) replaces it.
	// +optional
	TraceContext *TraceContext `json:"traceContext,omitempty"`

	// traceID is the trace ID of the run's trace (32 hex characters), for
	// linking to a trace backend. Unset when tracing is off.
	// +optional
	TraceID string `json:"traceID,omitempty"`

	// lastRunTrigger is the last value of the ai.roundtable.io/run annotation
	// that was handled. A run is only started when the annotation differs.
	// +optional
	LastRunTrigger string `json:"lastRunTrigger,omitempty"`

	// lastRetryTrigger is the last value of the ai.roundtable.io/retry-failed
	// annotation that was handled.
	// +optional
	LastRetryTrigger string `json:"lastRetryTrigger,omitempty"`

	// observedGeneration is the most recent generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// conditions represent the current state of the Chain resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=ch,categories=roundtable
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Schedule",type=string,JSONPath=`.spec.schedule`
// +kubebuilder:printcolumn:name="TimeZone",type=string,JSONPath=`.spec.timeZone`
// +kubebuilder:printcolumn:name="Next",type=date,JSONPath=`.status.nextScheduledAt`
// +kubebuilder:printcolumn:name="Runs",type=integer,JSONPath=`.status.runsCompleted`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Chain is the Schema for the chains API.
// A Chain represents a declarative multi-knight task pipeline in the Round Table,
// with support for parallel fan-out, sequential dependencies, data passing between
// steps, and optional cron scheduling.
type Chain struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Chain
	// +required
	Spec ChainSpec `json:"spec"`

	// status defines the observed state of Chain
	// +optional
	Status ChainStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ChainList contains a list of Chain
type ChainList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Chain `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Chain{}, &ChainList{})
}
