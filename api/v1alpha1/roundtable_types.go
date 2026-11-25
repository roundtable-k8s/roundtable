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
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RoundTableSpec defines the desired state of a RoundTable — a fleet of knights
// with shared configuration and policies.
type RoundTableSpec struct {
	// description is a human-readable description of this round table's purpose.
	// +optional
	Description string `json:"description,omitempty"`

	// nats configures the shared NATS infrastructure for all knights in this table.
	// +kubebuilder:validation:Required
	NATS RoundTableNATS `json:"nats"`

	// defaults defines default configuration applied to all knights in this table.
	// Individual knight specs can override these values.
	// +optional
	Defaults *RoundTableDefaults `json:"defaults,omitempty"`

	// policies defines fleet-level operational policies.
	// +optional
	Policies *RoundTablePolicies `json:"policies,omitempty"`

	// knightSelector is a label selector for Knights that belong to this table.
	// Knights matching this selector are automatically managed by this RoundTable.
	// +optional
	KnightSelector *metav1.LabelSelector `json:"knightSelector,omitempty"`

	// secrets references shared secrets available to all knights in this table.
	// +optional
	Secrets []corev1.LocalObjectReference `json:"secrets,omitempty"`

	// github configures GitHub credentials inherited by the table's knights
	// (including mission knights). A Knight's own
	// spec.github.tokenSecretRef takes precedence.
	// +optional
	GitHub *GitHubConfig `json:"github,omitempty"`

	// issuePickup polls GitHub for issues labelled ready and starts one copy
	// of a Chain (the PR pipeline) per issue. Unset means off.
	// +optional
	IssuePickup *IssuePickupSpec `json:"issuePickup,omitempty"`

	// telemetry overrides the operator-wide OpenTelemetry settings (Helm
	// telemetry.*) for the knights in this table. A knight's own spec.env
	// still wins over it.
	// +optional
	Telemetry *RoundTableTelemetry `json:"telemetry,omitempty"`

	// vault configures the shared Obsidian vault for the table's standing
	// knights (those labelled ai.roundtable.io/round-table). A knight that sets
	// its own spec.vault keeps it; the two are not merged.
	// +optional
	Vault *KnightVault `json:"vault,omitempty"`

	// sharedWorkspace configures a shared RWX PVC mounted on all knights for collaborative work.
	// Knights can read/write files here to pass artifacts between chain steps.
	// +optional
	SharedWorkspace *SharedWorkspaceConfig `json:"sharedWorkspace,omitempty"`

	// knightTemplates defines reusable knight configurations that missions can reference.
	// Templates provide defaults for domain, model, skills, NATS config, image, workspace, etc.
	// Missions can override specific fields using specOverrides.
	// +optional
	KnightTemplates map[string]KnightSpec `json:"knightTemplates,omitempty"`

	// modelAliases maps intent names to model IDs so chain steps can say
	// "model: deep" instead of a concrete model, e.g.
	// {deep: opus, fast: haiku, review: openrouter/z-ai/glm-5.3}.
	// A step's model is looked up here first (one level, no chaining); the
	// resolved value is then validated against the target knight's runtime, so
	// an alias only works on knights whose runtime supports its model.
	// +kubebuilder:validation:MaxProperties=64
	// +optional
	ModelAliases map[string]string `json:"modelAliases,omitempty"`

	// prFeedback watches knight-authored pull requests for a red CI run or a
	// merge-queue rejection and feeds the failure back to the PR's author
	// knight through a feedback Chain. Off unless enabled.
	// +optional
	PRFeedback *PRFeedbackSpec `json:"prFeedback,omitempty"`

	// suspend, if true, suspends all knights in this table.
	// +kubebuilder:default=false
	// +optional
	Suspend bool `json:"suspend,omitempty"`

	// ephemeral marks this RoundTable as mission-owned. Ephemeral tables are
	// excluded from fleet-wide aggregation and are garbage collected with their mission.
	// +kubebuilder:default=false
	// +optional
	Ephemeral bool `json:"ephemeral,omitempty"`

	// missionRef names the Mission that owns this ephemeral table. It is set
	// by the mission controller.
	// +optional
	MissionRef *corev1.LocalObjectReference `json:"missionRef,omitempty"`
}

// SharedWorkspaceConfig configures a shared RWX volume for collaborative knight work.
type SharedWorkspaceConfig struct {
	// claimName is the PVC name for the shared workspace.
	// +kubebuilder:validation:Required
	ClaimName string `json:"claimName"`

	// mountPath is where the shared workspace is mounted in knight pods.
	// +kubebuilder:default="/shared"
	// +optional
	MountPath string `json:"mountPath,omitempty"`
}

// RoundTableNATS configures the shared NATS infrastructure for a round table.
type RoundTableNATS struct {
	// url is the NATS server URL.
	// +kubebuilder:default="nats://nats.database.svc:4222"
	// +optional
	URL string `json:"url,omitempty"`

	// subjectPrefix is the NATS subject prefix for this table (e.g., "fleet-a").
	// All knights in this table use subjects under this prefix.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	SubjectPrefix string `json:"subjectPrefix"`

	// tasksStream is the JetStream stream name for tasks.
	// +kubebuilder:validation:Required
	TasksStream string `json:"tasksStream"`

	// resultsStream is the JetStream stream name for results.
	// +kubebuilder:validation:Required
	ResultsStream string `json:"resultsStream"`

	// createStreams, if true, tells the controller to create/update the JetStream streams.
	// +kubebuilder:default=false
	// +optional
	CreateStreams bool `json:"createStreams,omitempty"`

	// streamRetention configures the retention policy for auto-created streams.
	// +kubebuilder:default="WorkQueue"
	// +kubebuilder:validation:Enum=Limits;Interest;WorkQueue
	// +optional
	StreamRetention string `json:"streamRetention,omitempty"`

	// deadLetterMaxAge is how long entries are kept in the table's dead-letter
	// stream (<tasksStream>_DLQ), which records lost and failed tasks. Only
	// used when createStreams is true. Defaults to 720h (30 days).
	// +optional
	DeadLetterMaxAge *metav1.Duration `json:"deadLetterMaxAge,omitempty"`

	// eventsStream is the JetStream stream that captures the table's events
	// (<subjectPrefix>.events.>), which NATS-source Triggers read. Created
	// with Limits retention when createStreams is true. Defaults to
	// <tasksStream>_EVENTS.
	// +optional
	EventsStream string `json:"eventsStream,omitempty"`

	// eventsMaxAge is how long events are kept in the events stream. Only
	// used when createStreams is true. Defaults to 168h (7 days).
	// +optional
	EventsMaxAge *metav1.Duration `json:"eventsMaxAge,omitempty"`
}

// DefaultEventsMaxAge is the events stream retention when
// spec.nats.eventsMaxAge is unset.
const DefaultEventsMaxAge = 7 * 24 * time.Hour

// EventsStreamName is spec.nats.eventsStream, or <tasksStream>_EVENTS when
// unset.
func (n *RoundTableNATS) EventsStreamName() string {
	if n.EventsStream != "" {
		return n.EventsStream
	}
	return n.TasksStream + "_EVENTS"
}

// EventsRetention is spec.nats.eventsMaxAge, or DefaultEventsMaxAge.
func (n *RoundTableNATS) EventsRetention() time.Duration {
	if n.EventsMaxAge != nil && n.EventsMaxAge.Duration > 0 {
		return n.EventsMaxAge.Duration
	}
	return DefaultEventsMaxAge
}

// RoundTableTelemetry overrides the operator-wide telemetry settings for one
// table's knights. Precedence: knight env > table telemetry > chart values.
type RoundTableTelemetry struct {
	// otlpEndpoint replaces OTEL_EXPORTER_OTLP_ENDPOINT.
	// +optional
	OTLPEndpoint string `json:"otlpEndpoint,omitempty"`

	// otlpProtocol replaces OTEL_EXPORTER_OTLP_PROTOCOL.
	// +kubebuilder:validation:Enum=grpc;"http/protobuf"
	// +optional
	OTLPProtocol string `json:"otlpProtocol,omitempty"`

	// samplingRatio is a decimal between 0 and 1; it sets the
	// parentbased_traceidratio sampler argument.
	// +kubebuilder:validation:Pattern=`^(0(\.[0-9]+)?|1(\.0+)?)$`
	// +optional
	SamplingRatio *string `json:"samplingRatio,omitempty"`

	// disabled turns telemetry off (OTEL_SDK_DISABLED=true) for this table's
	// knights.
	// +optional
	Disabled bool `json:"disabled,omitempty"`

	// captureContent sets whether prompt and response text is captured in
	// traces and activity previews. nil inherits the chart value; false sets
	// ROUNDTABLE_TELEMETRY_CAPTURE_CONTENT=false and ACTIVITY_CONTENT=none.
	// +optional
	CaptureContent *bool `json:"captureContent,omitempty"`
}

// RoundTableDefaults defines default configuration inherited by knights in this table.
type RoundTableDefaults struct {
	// model is the default AI model for the table's standing knights (those
	// labelled ai.roundtable.io/round-table). A knight that sets its own
	// spec.model keeps it; with neither set the operator default
	// ("openrouter/deepseek/deepseek-v3.2") applies.
	// +optional
	Model string `json:"model,omitempty"`

	// image is the default container image for the table's standing knights.
	// A knight that sets its own spec.image keeps it; with neither set the
	// operator's DEFAULT_KNIGHT_IMAGE applies.
	// +optional
	Image string `json:"image,omitempty"`

	// taskTimeoutSeconds is the default task timeout in seconds for the table's
	// standing knights. A knight that sets its own spec.taskTimeoutSeconds
	// keeps it; with neither set 120 applies.
	// +kubebuilder:validation:Minimum=30
	// +kubebuilder:validation:Maximum=3600
	// +optional
	TaskTimeoutSeconds int32 `json:"taskTimeoutSeconds,omitempty"`

	// concurrency is the default max concurrent tasks per pod for the table's
	// standing knights. A knight that sets its own spec.concurrency keeps it;
	// with neither set 2 applies.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10
	// +optional
	Concurrency int32 `json:"concurrency,omitempty"`

	// resources are the default compute resource requests and limits (cpu,
	// memory, ephemeral-storage, ...) for the app container of the table's
	// standing knights (those labelled ai.roundtable.io/round-table). A knight
	// that sets its own spec.resources keeps it; resources are not merged.
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// egress is the default egress allowlist for the table's standing knights.
	// A knight that sets its own spec.egress keeps it; allowlists are not
	// merged. There is no table-level opt-out: opting out is explicit on the
	// Knight.
	// +optional
	Egress *KnightEgress `json:"egress,omitempty"`

	// runtimeClassName is the default RuntimeClass (for example "gvisor") for
	// the table's standing knights. A knight that sets its own
	// spec.runtimeClassName keeps it.
	// +optional
	RuntimeClassName *string `json:"runtimeClassName,omitempty"`

	// workspace is the default workspace storage (mode and size) for the
	// table's standing knights. A knight that sets its own spec.workspace
	// keeps it. existingClaim is never inherited: a claim is per knight.
	// +optional
	Workspace *KnightWorkspace `json:"workspace,omitempty"`

	// memory is the default persistent memory volume for the table's
	// standing knights. A knight that sets its own spec.memory keeps it.
	// Unlike workspace, existingClaim is inherited: point it at one
	// ReadWriteMany claim to give the table's knights shared memory, each
	// writing only its own sub-directory.
	// +optional
	Memory *KnightMemory `json:"memory,omitempty"`

	// arsenal configures the default skill arsenal for knights.
	// +optional
	Arsenal *KnightArsenal `json:"arsenal,omitempty"`
}

// RoundTablePolicies defines fleet-level operational policies.
type RoundTablePolicies struct {
	// maxConcurrentTasks is the maximum total concurrent tasks across all knights in this table.
	// 0 means unlimited.
	// +kubebuilder:default=0
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxConcurrentTasks int32 `json:"maxConcurrentTasks,omitempty"`

	// maxConcurrentPlanTasks caps how many tasks the table's plan-backed
	// knights (see Knight spec.planBacked) can run at once, so parallel
	// sessions sharing one Claude plan token don't exhaust its rate limit.
	// The operator enforces it on capacity: the sum of replicas x concurrency
	// over plan-backed knights stays within the cap, by lowering each
	// knight's replicas/maxReplicas. Every running plan-backed knight keeps
	// at least one pod, so the cap can be exceeded only by those first pods.
	// 0 means unlimited.
	// +kubebuilder:default=0
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxConcurrentPlanTasks int32 `json:"maxConcurrentPlanTasks,omitempty"`

	// costBudgetUSD is the maximum cumulative cost in USD across all knights.
	// When reached, all knights are suspended. "0" means unlimited.
	// +kubebuilder:default="0"
	// +optional
	CostBudgetUSD string `json:"costBudgetUSD,omitempty"`

	// costResetSchedule is a cron expression for resetting the cost counter (e.g., "0 0 1 * *" for monthly).
	// +optional
	CostResetSchedule string `json:"costResetSchedule,omitempty"`

	// maxKnights is the maximum number of knights allowed in this table.
	// 0 means unlimited.
	// +kubebuilder:default=0
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxKnights int32 `json:"maxKnights,omitempty"`

	// maxMissions is the maximum number of concurrent active missions.
	// +kubebuilder:default=5
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxMissions int32 `json:"maxMissions,omitempty"`
}

// RoundTablePhase represents the current lifecycle phase of the RoundTable.
// +kubebuilder:validation:Enum=Provisioning;Ready;Degraded;Suspended;OverBudget
type RoundTablePhase string

const (
	RoundTablePhaseProvisioning RoundTablePhase = "Provisioning"
	RoundTablePhaseReady        RoundTablePhase = "Ready"
	RoundTablePhaseDegraded     RoundTablePhase = "Degraded"
	RoundTablePhaseSuspended    RoundTablePhase = "Suspended"
	RoundTablePhaseOverBudget   RoundTablePhase = "OverBudget"
)

// RoundTableKnightSummary provides an aggregated view of a knight's status.
type RoundTableKnightSummary struct {
	// name is the knight name.
	Name string `json:"name"`

	// ready indicates whether this knight is ready.
	// +optional
	Ready bool `json:"ready,omitempty"`

	// phase is the knight's current phase.
	// +optional
	Phase KnightPhase `json:"phase,omitempty"`
}

// RoundTableStatus defines the observed state of RoundTable.
type RoundTableStatus struct {
	// phase is the current lifecycle phase of the round table.
	// +optional
	Phase RoundTablePhase `json:"phase,omitempty"`

	// knightsReady is the number of knights in Ready phase.
	// +optional
	KnightsReady int32 `json:"knightsReady,omitempty"`

	// knightsTotal is the total number of knights in this table.
	// +optional
	KnightsTotal int32 `json:"knightsTotal,omitempty"`

	// knights provides a summary of each knight's status.
	// +optional
	Knights []RoundTableKnightSummary `json:"knights,omitempty"`

	// totalTasksCompleted is the aggregate tasks completed across all knights.
	// +optional
	TotalTasksCompleted int64 `json:"totalTasksCompleted,omitempty"`

	// totalCost is the aggregate cost in USD across all knights since last reset.
	// +optional
	TotalCost string `json:"totalCost,omitempty"`

	// activeMissions is the number of currently active missions under this table.
	// +optional
	ActiveMissions int32 `json:"activeMissions,omitempty"`

	// issuePickup is the observed state of spec.issuePickup.
	// +optional
	IssuePickup *IssuePickupStatus `json:"issuePickup,omitempty"`

	// observedGeneration is the most recent generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// prFeedback is the state of the PR feedback watcher (spec.prFeedback).
	// +optional
	PRFeedback *PRFeedbackStatus `json:"prFeedback,omitempty"`

	// conditions represent the current state of the RoundTable resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=rt,categories=roundtable
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.knightsReady`,description="Knights in phase Ready"
// +kubebuilder:printcolumn:name="Total",type=integer,JSONPath=`.status.knightsTotal`
// +kubebuilder:printcolumn:name="Tasks",type=integer,JSONPath=`.status.totalTasksCompleted`
// +kubebuilder:printcolumn:name="Cost",type=string,JSONPath=`.status.totalCost`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// RoundTable is the Schema for the roundtables API.
// A RoundTable represents a fleet of knights with shared configuration,
// NATS infrastructure, and operational policies. It is the top-level
// organizational resource in the Round Table operator.
type RoundTable struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of RoundTable
	// +required
	Spec RoundTableSpec `json:"spec"`

	// status defines the observed state of RoundTable
	// +optional
	Status RoundTableStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// RoundTableList contains a list of RoundTable
type RoundTableList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []RoundTable `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RoundTable{}, &RoundTableList{})
}

// PRFeedbackSpec configures the PR feedback watcher: the operator polls
// GitHub for knight-authored pull requests whose CI went red or that the
// merge queue rejected, and feeds each failure to the PR's author knight
// through a feedback Chain (see docs/pr-feedback.md).
type PRFeedbackSpec struct {
	// enabled turns the watcher on.
	// +kubebuilder:default=false
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// repos are the GitHub repositories to watch, as owner/name.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=50
	// +kubebuilder:validation:items:Pattern=`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`
	// +listType=set
	Repos []string `json:"repos"`

	// authors are the GitHub logins whose pull requests are watched (REST
	// form, e.g. "dunce-bot[bot]" for a GitHub App).
	// +kubebuilder:default={"dunce-bot[bot]"}
	// +kubebuilder:validation:MaxItems=20
	// +listType=set
	// +optional
	Authors []string `json:"authors,omitempty"`

	// chainRef names the feedback Chain (same namespace) the watcher feeds:
	// it sets the Chain's spec.input to the failure and starts a run. The
	// chart renders one as prPipeline.feedback.
	// +kubebuilder:default={name: "pr-pipeline-feedback"}
	// +optional
	ChainRef *corev1.LocalObjectReference `json:"chainRef,omitempty"`

	// tokenSecretRef selects the Secret key holding the GitHub token the
	// operator uses. It needs read access to pull requests, checks and
	// Actions runs (public repositories need no extra permission) and write
	// access to issues and pull requests (labels, comments, flake issues).
	// Defaults to the table's spec.github.tokenSecretRef. The Secret is
	// re-read on every poll, so rotated tokens are picked up.
	// +optional
	TokenSecretRef *corev1.SecretKeySelector `json:"tokenSecretRef,omitempty"`

	// maxAttempts caps the fix rounds per pull request. The next failure
	// after the last round labels the pull request with escalationLabel
	// instead of feeding it back again.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10
	// +optional
	MaxAttempts int32 `json:"maxAttempts,omitempty"`

	// pollIntervalSeconds is how often GitHub is polled.
	// +kubebuilder:default=300
	// +kubebuilder:validation:Minimum=60
	// +kubebuilder:validation:Maximum=86400
	// +optional
	PollIntervalSeconds int32 `json:"pollIntervalSeconds,omitempty"`

	// fallbackKnightRef names the knight that gets failures of pull requests
	// the watcher cannot trace back to a chain run (in a fresh session).
	// Unset: such pull requests are ignored.
	// +optional
	FallbackKnightRef *corev1.LocalObjectReference `json:"fallbackKnightRef,omitempty"`

	// escalationLabel is added to a pull request whose fix rounds ran out.
	// Pull requests carrying it are no longer watched.
	// +kubebuilder:default="needs-human"
	// +kubebuilder:validation:MaxLength=50
	// +optional
	EscalationLabel string `json:"escalationLabel,omitempty"`

	// flakeLabel labels the issues opened for flaky checks.
	// +kubebuilder:default="flaky-test"
	// +kubebuilder:validation:MaxLength=50
	// +optional
	FlakeLabel string `json:"flakeLabel,omitempty"`

	// ignoreChecks are check names the watcher never treats as a failure:
	// no fix round, no flake issue. Use them for policy checks that fail
	// until a human acts (merge-guard fails while a PR is on hold or has no
	// ship-it verdict, then passes on the same commit). An empty list
	// ignores nothing.
	// +kubebuilder:default={"merge-guard"}
	// +kubebuilder:validation:MaxItems=20
	// +kubebuilder:validation:items:MaxLength=100
	// +listType=set
	// +optional
	IgnoreChecks []string `json:"ignoreChecks,omitempty"`
}

// PRFeedbackPhase is where a watched pull request stands.
// +kubebuilder:validation:Enum=Watching;Queued;Fixing;Escalated
type PRFeedbackPhase string

const (
	// PRFeedbackWatching: no unhandled failure.
	PRFeedbackWatching PRFeedbackPhase = "Watching"
	// PRFeedbackQueued: a failure waits for the feedback Chain to be free.
	PRFeedbackQueued PRFeedbackPhase = "Queued"
	// PRFeedbackFixing: the feedback Chain is running for this pull request.
	PRFeedbackFixing PRFeedbackPhase = "Fixing"
	// PRFeedbackEscalated: fix rounds ran out; the pull request was labelled.
	PRFeedbackEscalated PRFeedbackPhase = "Escalated"
)

// PRFeedbackStatus is the observed state of the PR feedback watcher.
type PRFeedbackStatus struct {
	// lastPollTime is when GitHub was last polled successfully.
	// +optional
	LastPollTime *metav1.Time `json:"lastPollTime,omitempty"`

	// lastError is the error of the last poll, empty when it succeeded.
	// +optional
	LastError string `json:"lastError,omitempty"`

	// activeRun is the run trigger the watcher put on the feedback Chain
	// and is waiting for, as <repo>#<number>@<trigger>.
	// +optional
	ActiveRun string `json:"activeRun,omitempty"`

	// pullRequests are the watched pull requests with a failure history.
	// Merged and closed pull requests are dropped.
	// +kubebuilder:validation:MaxItems=200
	// +optional
	PullRequests []PRFeedbackPullRequest `json:"pullRequests,omitempty"`
}

// PRFeedbackPullRequest is the watcher's record of one pull request.
type PRFeedbackPullRequest struct {
	// repo is the repository, owner/name.
	Repo string `json:"repo"`
	// number is the pull request number.
	Number int64 `json:"number"`
	// phase is where the pull request stands.
	Phase PRFeedbackPhase `json:"phase"`
	// knight is the author knight the failures go to.
	// +optional
	Knight string `json:"knight,omitempty"`
	// runId is the chain run that opened the pull request; the fix task
	// carries it so the knight resumes that session.
	// +optional
	RunID string `json:"runId,omitempty"`
	// issue is the issue the chain run worked on, if known.
	// +optional
	Issue int64 `json:"issue,omitempty"`
	// attempts is the number of fix rounds started.
	// +optional
	Attempts int32 `json:"attempts,omitempty"`
	// lastFailure identifies the last failure handled (a merge-group run ID
	// or the head SHA's failed checks), so a failure is fed back once.
	// +optional
	LastFailure string `json:"lastFailure,omitempty"`
	// failureHeadSHA is the pull request's head commit when lastFailure was
	// seen. A later pass on the same commit marks the failed checks flaky.
	// +optional
	FailureHeadSHA string `json:"failureHeadSHA,omitempty"`
	// failedChecks are the check or job names of lastFailure.
	// +optional
	FailedChecks []string `json:"failedChecks,omitempty"`
	// failureKind is the kind of lastFailure: merge-queue (a failed
	// merge_group run), ci (failed checks on the head commit) or conflict
	// (the pull request has merge conflicts with its base branch).
	// +kubebuilder:validation:Enum=merge-queue;ci;conflict
	// +optional
	FailureKind string `json:"failureKind,omitempty"`
	// failureURL links the failed run or check.
	// +optional
	FailureURL string `json:"failureURL,omitempty"`
	// message is a human-readable note on the last transition.
	// +optional
	Message string `json:"message,omitempty"`
	// updatedAt is when this record last changed.
	// +optional
	UpdatedAt *metav1.Time `json:"updatedAt,omitempty"`
}
