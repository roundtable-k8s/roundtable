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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// AnnotationChainRun starts a brand-new chain run (new runId, steps reset)
	// whenever its value differs from status.lastRunTrigger.
	AnnotationChainRun = "ai.roundtable.io/run"

	// AnnotationChainRetryFailed resumes the current run from its failed
	// step(s) only, whenever its value differs from status.lastRetryTrigger.
	AnnotationChainRetryFailed = "ai.roundtable.io/retry-failed"
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

	// timeout is the overall chain timeout in seconds. The entire chain is failed if exceeded.
	// +kubebuilder:default=600
	// +kubebuilder:validation:Minimum=30
	// +kubebuilder:validation:Maximum=86400
	// +optional
	Timeout int32 `json:"timeout,omitempty"`

	// maxQueueWait is how long (seconds) a published step task may wait for its
	// knight to pick it up before the step fails. The per-step timeout only
	// starts counting once the knight has picked the task up, so a busy (or
	// dead) knight is bounded by this guard instead. Defaults to 1800 when unset.
	// +kubebuilder:validation:Minimum=30
	// +kubebuilder:validation:Maximum=86400
	// +optional
	MaxQueueWait int32 `json:"maxQueueWait,omitempty"`

	// schedule is an optional cron expression to trigger this chain on a recurring basis.
	// Uses standard cron syntax (e.g., "0 */6 * * *").
	// +optional
	Schedule string `json:"schedule,omitempty"`

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

	// outputKnight is the knight responsible for writing chain artifacts when steps have outputPath set.
	// Defaults to "gawain" if not specified.
	// +kubebuilder:default="gawain"
	// +optional
	OutputKnight string `json:"outputKnight,omitempty"`

	// roundTableRef references the RoundTable this chain belongs to.
	// If omitted, the chain operates in the default namespace NATS prefix.
	// +optional
	RoundTableRef string `json:"roundTableRef,omitempty"`

	// suspended, if true, prevents scheduled runs and disallows new executions.
	// +kubebuilder:default=false
	// +optional
	Suspended bool `json:"suspended,omitempty"`

	// retryPolicy configures retry behavior for failed steps.
	// +optional
	RetryPolicy *ChainRetryPolicy `json:"retryPolicy,omitempty"`

	// missionRef is set by the mission controller when creating mission-scoped chains.
	// The chain controller uses this to resolve NATS config from the mission's RoundTable.
	// +optional
	MissionRef string `json:"missionRef,omitempty"`

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

	// knightRef is the name of the Knight to execute this step.
	// +kubebuilder:validation:Required
	KnightRef string `json:"knightRef"`

	// task is the task prompt or instruction to send to the knight.
	// Supports Go template syntax with access to prior step outputs: {{ .Steps.step_name.Output }}
	// +kubebuilder:validation:Required
	Task string `json:"task"`

	// dependsOn lists step names that must complete successfully before this step runs.
	// If empty, the step runs immediately (or after the previous step in sequence).
	// +optional
	DependsOn []string `json:"dependsOn,omitempty"`

	// timeout is the per-step timeout in seconds. Overrides the knight's default taskTimeout.
	// +kubebuilder:default=120
	// +kubebuilder:validation:Minimum=10
	// +kubebuilder:validation:Maximum=3600
	// +optional
	Timeout int32 `json:"timeout,omitempty"`

	// outputKey is the key name under which this step's output is stored for downstream steps.
	// Defaults to the step name if not specified.
	// +optional
	OutputKey string `json:"outputKey,omitempty"`

	// outputPath is an optional file path where this step's output should be written.
	// Supports Go template variables: {{ .Date }} (YYYY-MM-DD), {{ .Chain }} (chain name), {{ .Step }} (step name).
	// When set, the controller dispatches a write task to the outputKnight after the step succeeds.
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
}

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
// +kubebuilder:validation:Enum=Pending;Running;Succeeded;Failed;Skipped
type ChainStepPhase string

const (
	ChainStepPhasePending   ChainStepPhase = "Pending"
	ChainStepPhaseRunning   ChainStepPhase = "Running"
	ChainStepPhaseSucceeded ChainStepPhase = "Succeeded"
	ChainStepPhaseFailed    ChainStepPhase = "Failed"
	ChainStepPhaseSkipped   ChainStepPhase = "Skipped"
)

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

	// fanOut tracks the items of a forEach step. Unset for ordinary steps.
	// +optional
	FanOut *ChainFanOutStatus `json:"fanOut,omitempty"`
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

	// pending, running, succeeded, failed and skipped summarise itemPhases.
	// +optional
	Pending int32 `json:"pending,omitempty"`
	// +optional
	Running int32 `json:"running,omitempty"`
	// +optional
	Succeeded int32 `json:"succeeded,omitempty"`
	// +optional
	Failed int32 `json:"failed,omitempty"`
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

	// runsCompleted is the total number of successful chain runs.
	// +optional
	RunsCompleted int64 `json:"runsCompleted,omitempty"`

	// runsFailed is the total number of failed chain runs.
	// +optional
	RunsFailed int64 `json:"runsFailed,omitempty"`

	// lastScheduledAt is when the chain was last triggered by its cron schedule.
	// +optional
	LastScheduledAt *metav1.Time `json:"lastScheduledAt,omitempty"`

	// runId uniquely identifies the current (or most recent) chain run.
	// It is embedded in task IDs and NATS KV entries so results produced by
	// a previous run can never be attributed to the current one.
	// +optional
	RunID string `json:"runId,omitempty"`

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
// +kubebuilder:printcolumn:name="Steps",type=integer,JSONPath=`.spec.steps`,priority=1
// +kubebuilder:printcolumn:name="Schedule",type=string,JSONPath=`.spec.schedule`
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
