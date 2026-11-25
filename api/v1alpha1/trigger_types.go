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
	// LabelTrigger marks a Chain run started by a Trigger; the value is the
	// Trigger's name.
	LabelTrigger = "ai.roundtable.io/trigger"

	// AnnotationTriggerKey is the dedupe key of the event that started a
	// Trigger's run.
	AnnotationTriggerKey = "ai.roundtable.io/trigger-key"

	// AnnotationTriggerSubject is the NATS subject of the event that started
	// a Trigger's run (NATS and webhook sources).
	AnnotationTriggerSubject = "ai.roundtable.io/trigger-subject"

	// LabelWebhookSecret marks a Secret the gateway may read as a webhook
	// Trigger's credential. The gateway refuses a referenced Secret without
	// it.
	LabelWebhookSecret = "ai.roundtable.io/webhook-secret"

	// FinalizerTrigger lets the operator delete a Trigger's JetStream
	// consumer before the Trigger goes away.
	FinalizerTrigger = "ai.roundtable.io/trigger-consumer"

	// AnnotationTriggerGitHubRepo is the owner/name of the GitHub item a
	// github-source run was started for.
	AnnotationTriggerGitHubRepo = "ai.roundtable.io/trigger-github-repo"

	// AnnotationTriggerClaimed is set on a github-source run to the run's
	// ai.roundtable.io/run value once the item's claim was applied for that
	// run (labels, assignees and comment), so a retried claim never comments
	// twice and an item selected again after its run finished is a re-run.
	AnnotationTriggerClaimed = "ai.roundtable.io/trigger-claimed"

	// AnnotationTriggerItemRuns counts the runs a github source with a
	// state selector (checks, mergeable, mergeQueue) started for an item:
	// one per failure, compared with maxRunsPerItem.
	AnnotationTriggerItemRuns = "ai.roundtable.io/trigger-item-runs"

	// AnnotationTriggerHandedBack is set on an item's run to the event key
	// for which source.github.onMaxRuns was applied. The next time the item
	// is selected (a human removed the hand-back label) its run count starts
	// over.
	AnnotationTriggerHandedBack = "ai.roundtable.io/trigger-handed-back"

	// TriggerMaxRecentFirings bounds status.recentFirings.
	TriggerMaxRecentFirings = 10
)

// TriggerConcurrencyPolicy says what a firing does while earlier runs of the
// same Trigger are still active.
// +kubebuilder:validation:Enum=Allow;Forbid;Replace
type TriggerConcurrencyPolicy string

const (
	// TriggerConcurrencyAllow starts a new run next to active ones, up to
	// maxActive.
	TriggerConcurrencyAllow TriggerConcurrencyPolicy = "Allow"
	// TriggerConcurrencyForbid skips the firing while a run is active.
	TriggerConcurrencyForbid TriggerConcurrencyPolicy = "Forbid"
	// TriggerConcurrencyReplace deletes the active runs and starts a new one.
	TriggerConcurrencyReplace TriggerConcurrencyPolicy = "Replace"
)

// TriggerSource is where a Trigger's events come from. Exactly one source is
// set.
// +kubebuilder:validation:XValidation:rule="[has(self.nats), has(self.schedule), has(self.webhook), has(self.github)].filter(x, x).size() == 1",message="set exactly one of source.nats, source.schedule, source.webhook or source.github"
type TriggerSource struct {
	// nats fires on each message published to a subject on the RoundTable's
	// events stream (<subjectPrefix>.events.>).
	// +optional
	NATS *TriggerNATSSource `json:"nats,omitempty"`

	// schedule fires on a cron schedule.
	// +optional
	Schedule *TriggerScheduleSource `json:"schedule,omitempty"`

	// webhook fires on each authenticated POST to the gateway's
	// /v1/hooks/<namespace>/<name>. The gateway publishes the request body
	// to <subjectPrefix>.events.webhook.<namespace>.<name> on the
	// RoundTable's events stream, which the Trigger reads like a NATS source.
	// +optional
	Webhook *TriggerWebhookSource `json:"webhook,omitempty"`

	// github polls GitHub for issues or pull requests matching a query and
	// fires once per item. The operator publishes each item to
	// <subjectPrefix>.events.github.<namespace>.<name> on the RoundTable's
	// events stream, which the Trigger reads like a NATS source.
	// +optional
	GitHub *TriggerGitHubSource `json:"github,omitempty"`
}

// TriggerGitHubKind is what a GitHub source selects.
// +kubebuilder:validation:Enum=issue;pullRequest
type TriggerGitHubKind string

const (
	// TriggerGitHubKindIssue selects issues (not pull requests).
	TriggerGitHubKindIssue TriggerGitHubKind = "issue"
	// TriggerGitHubKindPullRequest selects pull requests.
	TriggerGitHubKindPullRequest TriggerGitHubKind = "pullRequest"
)

// TriggerGitHubState is the state of the items a GitHub source selects.
// +kubebuilder:validation:Enum=open;closed;all
type TriggerGitHubState string

const (
	// TriggerGitHubStateOpen selects open items.
	TriggerGitHubStateOpen TriggerGitHubState = "open"
	// TriggerGitHubStateClosed selects closed items.
	TriggerGitHubStateClosed TriggerGitHubState = "closed"
	// TriggerGitHubStateAll selects open and closed items.
	TriggerGitHubStateAll TriggerGitHubState = "all"
)

// TriggerGitHubChecks selects pull requests by the checks on their head
// commit.
// +kubebuilder:validation:Enum=failing
type TriggerGitHubChecks string

const (
	// TriggerGitHubChecksFailing selects pull requests whose head commit has
	// a check whose latest attempt failed or timed out.
	TriggerGitHubChecksFailing TriggerGitHubChecks = "failing"
)

// TriggerGitHubMergeable selects pull requests by their merge state.
// +kubebuilder:validation:Enum=conflicting
type TriggerGitHubMergeable string

const (
	// TriggerGitHubMergeableConflicting selects pull requests with merge
	// conflicts with their base branch (GitHub mergeable_state "dirty").
	TriggerGitHubMergeableConflicting TriggerGitHubMergeable = "conflicting"
)

// TriggerGitHubMergeQueue selects pull requests by their latest merge queue
// attempt.
// +kubebuilder:validation:Enum=rejected
type TriggerGitHubMergeQueue string

const (
	// TriggerGitHubMergeQueueRejected selects pull requests whose newest
	// merge group has a failed workflow run.
	TriggerGitHubMergeQueueRejected TriggerGitHubMergeQueue = "rejected"
)

// TriggerGitHubSource polls GitHub's REST API with the RoundTable's token
// (spec.github.tokenSecretRef of roundTableRef). Each matching item is one
// event whose default dedupe key is "<owner/name>#<number>".
//
// With a state selector (checks, mergeable or mergeQueue) the source selects
// open pull requests with a failure instead, and fires once per failure: the
// event key is "<owner/name>#<number>@<kind>:<head sha>" (kind is conflict,
// ci or merge-queue; a merge-queue key ends with ":<merge group sha>"), so a
// failure fires once and a new push can fire again. Each pull request has
// one run, started again for each new failure once it finished.
// +kubebuilder:validation:XValidation:rule="(has(self.query.labels) && size(self.query.labels) > 0) || has(self.checks) || has(self.mergeable) || has(self.mergeQueue)",message="set query.labels, or a state selector (checks, mergeable, mergeQueue)"
// +kubebuilder:validation:XValidation:rule="!(has(self.checks) || has(self.mergeable) || has(self.mergeQueue)) || (has(self.kind) && self.kind == 'pullRequest')",message="checks, mergeable and mergeQueue need kind: pullRequest"
// +kubebuilder:validation:XValidation:rule="!(has(self.checks) || has(self.mergeable) || has(self.mergeQueue)) || !has(self.query.state) || self.query.state == 'open'",message="checks, mergeable and mergeQueue select open pull requests: leave query.state open"
// +kubebuilder:validation:XValidation:rule="!has(self.maxRunsPerItem) || self.maxRunsPerItem == 0 || ((has(self.checks) || has(self.mergeable) || has(self.mergeQueue)) && has(self.onMaxRuns) && has(self.onMaxRuns.addLabels) && size(self.onMaxRuns.addLabels) > 0)",message="maxRunsPerItem needs a state selector and onMaxRuns.addLabels"
// +kubebuilder:validation:XValidation:rule="!has(self.ignoreChecks) || has(self.checks) || has(self.mergeQueue)",message="ignoreChecks needs checks or mergeQueue"
type TriggerGitHubSource struct {
	// repositories are the repositories polled, as "owner/name".
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=50
	// +kubebuilder:validation:items:Pattern=`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`
	// +kubebuilder:validation:items:MaxLength=140
	// +listType=set
	// +required
	Repositories []string `json:"repositories"`

	// kind selects issues or pull requests. Defaults to issue.
	// +kubebuilder:default=issue
	// +optional
	Kind TriggerGitHubKind `json:"kind,omitempty"`

	// query selects the items.
	// +required
	Query TriggerGitHubQuery `json:"query"`

	// pollInterval is how often each repository is listed. Defaults to 60s;
	// at least 30s.
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('30s')",message="pollInterval must be at least 30s"
	// +optional
	PollInterval *metav1.Duration `json:"pollInterval,omitempty"`

	// claim marks an item as taken once its run was created, so it is not
	// picked again: typically remove the selecting label and add an
	// in-progress one. With a claim, an item that matches again after its
	// run finished (the selecting label was put back) starts that run again.
	// +optional
	Claim *TriggerGitHubClaim `json:"claim,omitempty"`

	// maxActivePerRepository caps active runs per repository, next to
	// concurrency.maxActive for the whole Trigger. An item over the cap is
	// held and starts on a later poll. 0 means no cap. Applies under the
	// Allow concurrency policy.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=50
	// +optional
	MaxActivePerRepository int32 `json:"maxActivePerRepository,omitempty"`

	// holdWhilePullRequestLabeled holds a repository while it has an open
	// pull request with this label: its items start no new run until the
	// pull request is merged or closed (for example a pull request that
	// changes an API). If the pull requests cannot be listed, the repository
	// is held too.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=50
	// +optional
	HoldWhilePullRequestLabeled string `json:"holdWhilePullRequestLabeled,omitempty"`

	// skipIfBodyMatches hands items that must not start a run back instead:
	// items whose title or body matches a pattern, or that already carry a
	// handBack.addLabels label.
	// +optional
	SkipIfBodyMatches *TriggerGitHubSkip `json:"skipIfBodyMatches,omitempty"`

	// author selects items opened by this GitHub login (REST form, e.g.
	// "dunce-bot[bot]" for a GitHub App), compared case-insensitively.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=100
	// +optional
	Author string `json:"author,omitempty"`

	// checks: failing selects pull requests (kind: pullRequest) whose head
	// commit has a check whose latest attempt failed or timed out. The
	// event's failedChecks lists them.
	// +optional
	Checks TriggerGitHubChecks `json:"checks,omitempty"`

	// ignoreChecks are check (and merge queue workflow) names that never
	// count as failed, for example a policy check that fails until a human
	// acts.
	// +kubebuilder:validation:MaxItems=20
	// +kubebuilder:validation:items:MaxLength=100
	// +listType=set
	// +optional
	IgnoreChecks []string `json:"ignoreChecks,omitempty"`

	// mergeable: conflicting selects pull requests (kind: pullRequest) with
	// merge conflicts with their base branch. A conflict fires before failed
	// checks: the pull request cannot merge until it is rebased.
	// +optional
	Mergeable TriggerGitHubMergeable `json:"mergeable,omitempty"`

	// mergeQueue: rejected selects pull requests (kind: pullRequest) whose
	// newest merge queue attempt (merge_group workflow runs) failed. Failed
	// checks on the head commit fire first.
	// +optional
	MergeQueue TriggerGitHubMergeQueue `json:"mergeQueue,omitempty"`

	// maxRunsPerItem caps the runs started for one pull request (one per
	// failure) while its run is kept (ttlSecondsAfterFinished). The next
	// failure after the last run applies onMaxRuns instead. 0 means no cap.
	// Needs a state selector.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=50
	// +optional
	MaxRunsPerItem int32 `json:"maxRunsPerItem,omitempty"`

	// onMaxRuns is applied to a pull request whose runs ran out
	// (maxRunsPerItem). Items carrying one of its addLabels are not
	// selected; when a human removes the label the item's run count starts
	// over. Its comment also sees .Run (.Run.Name, .Run.Attempt: the runs
	// started).
	// +optional
	OnMaxRuns *TriggerGitHubHandBack `json:"onMaxRuns,omitempty"`
}

// TriggerGitHubSkip selects items to hand back instead of starting a run.
type TriggerGitHubSkip struct {
	// patterns are RE2 regular expressions matched against an item's title
	// and body.
	// +kubebuilder:validation:MaxItems=20
	// +optional
	Patterns []string `json:"patterns,omitempty"`

	// handBack is applied to a skipped item. Remove the selecting label so
	// it is handed back once.
	// +required
	HandBack TriggerGitHubHandBack `json:"handBack"`
}

// TriggerGitHubHandBack is the labelling and comment applied to a skipped
// item.
// +kubebuilder:validation:XValidation:rule="(has(self.addLabels) && size(self.addLabels) > 0) || (has(self.removeLabels) && size(self.removeLabels) > 0)",message="set handBack.addLabels or handBack.removeLabels"
type TriggerGitHubHandBack struct {
	// addLabels are added to the item. An item that already carries one of
	// them is handed back as well.
	// +kubebuilder:validation:MaxItems=10
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=50
	// +listType=set
	// +optional
	AddLabels []string `json:"addLabels,omitempty"`

	// removeLabels are removed from the item, last.
	// +kubebuilder:validation:MaxItems=10
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=50
	// +listType=set
	// +optional
	RemoveLabels []string `json:"removeLabels,omitempty"`

	// comment is a Go template posted on the item. It sees .Event (as in
	// spec.input), .Trigger and .Reason (why the item was skipped). Empty
	// posts no comment.
	// +kubebuilder:validation:MaxLength=4096
	// +optional
	Comment string `json:"comment,omitempty"`
}

// TriggerGitHubQuery is what GitHub's "list repository issues" endpoint can
// filter on.
type TriggerGitHubQuery struct {
	// labels an item must all carry. Required unless a state selector
	// (checks, mergeable, mergeQueue) is set.
	// +kubebuilder:validation:MaxItems=10
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=50
	// +listType=set
	// +optional
	Labels []string `json:"labels,omitempty"`

	// state is open, closed or all. Defaults to open.
	// +kubebuilder:default=open
	// +optional
	State TriggerGitHubState `json:"state,omitempty"`
}

// TriggerGitHubClaim is the labelling applied to an item after its run was
// created.
// +kubebuilder:validation:XValidation:rule="(has(self.addLabels) && size(self.addLabels) > 0) || (has(self.removeLabels) && size(self.removeLabels) > 0)",message="set claim.addLabels or claim.removeLabels"
type TriggerGitHubClaim struct {
	// addLabels are added to the item (created in the repository if
	// missing).
	// +kubebuilder:validation:MaxItems=10
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=50
	// +listType=set
	// +optional
	AddLabels []string `json:"addLabels,omitempty"`

	// removeLabels are removed from the item, last.
	// +kubebuilder:validation:MaxItems=10
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=50
	// +listType=set
	// +optional
	RemoveLabels []string `json:"removeLabels,omitempty"`

	// assignees are GitHub logins assigned to the item. GitHub ignores
	// logins that cannot be assigned.
	// +kubebuilder:validation:MaxItems=10
	// +listType=set
	// +optional
	Assignees []string `json:"assignees,omitempty"`

	// comment is a Go template posted on the item once per run, after the
	// labels were added. It sees .Event (as in spec.input), .Trigger and
	// .Run (.Run.Name, and .Run.ID: the run's ai.roundtable.io/run value).
	// Empty posts no comment.
	// +kubebuilder:validation:MaxLength=4096
	// +optional
	Comment string `json:"comment,omitempty"`
}

// TriggerNATSSource reads events from the RoundTable's events stream.
type TriggerNATSSource struct {
	// subject is the subject filter (wildcards allowed). It must sit under
	// the table's "<subjectPrefix>.events.", for example
	// "fleet-a.events.deploys.>".
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:Pattern=`^[^\s]+$`
	// +required
	Subject string `json:"subject"`
}

// TriggerWebhookSource receives events through the gateway's hook endpoint.
type TriggerWebhookSource struct {
	// auth is the credential a sender must present. The gateway accepts only
	// this credential on the Trigger's hook URL, never its own API tokens.
	// +required
	Auth TriggerWebhookAuth `json:"auth"`

	// eventType, when set, fires only on events of this type: the Ce-Type
	// header (CloudEvents), else X-GitHub-Event. Other events are stored on
	// the stream and acknowledged without firing.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	EventType string `json:"eventType,omitempty"`
}

// TriggerWebhookAuth selects how the gateway authenticates a sender. Exactly
// one is set. The referenced Secret is in the Trigger's namespace and must be
// labelled ai.roundtable.io/webhook-secret: "true".
// +kubebuilder:validation:XValidation:rule="[has(self.bearerTokenSecretRef), has(self.hmacSecretRef)].filter(x, x).size() == 1",message="set exactly one of auth.bearerTokenSecretRef or auth.hmacSecretRef"
type TriggerWebhookAuth struct {
	// bearerTokenSecretRef selects the Secret key holding the token senders
	// send as "Authorization: Bearer <token>" (at least 16 characters).
	// +optional
	BearerTokenSecretRef *corev1.SecretKeySelector `json:"bearerTokenSecretRef,omitempty"`

	// hmacSecretRef selects the Secret key holding the HMAC-SHA256 key
	// senders sign the raw request body with, sent as
	// "X-Hub-Signature-256: sha256=<hex>" (GitHub style) or
	// "X-Signature: sha256=<hex>" (Flux generic-hmac).
	// +optional
	HMACSecretRef *corev1.SecretKeySelector `json:"hmacSecretRef,omitempty"`
}

// TriggerScheduleSource fires on a cron schedule. Catch-up follows
// Chain.spec.schedule: a fire missed while the operator was down runs once,
// late, within startingDeadlineSeconds.
type TriggerScheduleSource struct {
	// cron is a standard five-field cron expression, e.g. "0 7 * * 1-5".
	// +kubebuilder:validation:MinLength=1
	// +required
	Cron string `json:"cron"`

	// timeZone is the IANA time zone the schedule is evaluated in. Defaults
	// to UTC. "Local" is not accepted.
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:Pattern=`^[A-Za-z][A-Za-z0-9_+\-]*(/[A-Za-z0-9_+\-]+)*$`
	// +optional
	TimeZone string `json:"timeZone,omitempty"`

	// startingDeadlineSeconds bounds catch-up: a missed fire still runs only
	// if fewer than this many seconds have passed since it was due. Unset
	// always catches up (at most one run).
	// +kubebuilder:validation:Minimum=1
	// +optional
	StartingDeadlineSeconds *int64 `json:"startingDeadlineSeconds,omitempty"`
}

// TriggerTarget is what a firing starts.
type TriggerTarget struct {
	// chainRef names the Chain (same namespace) a firing starts a copy of.
	// +required
	ChainRef corev1.LocalObjectReference `json:"chainRef"`
}

// TriggerConcurrency bounds how many runs of a Trigger are active at once.
type TriggerConcurrency struct {
	// policy is Allow (start next to active runs, up to maxActive), Forbid
	// (skip while a run is active) or Replace (delete the active runs, then
	// start). Defaults to Allow.
	// +kubebuilder:default=Allow
	// +optional
	Policy TriggerConcurrencyPolicy `json:"policy,omitempty"`

	// maxActive caps active runs under Allow; a firing at the cap is
	// skipped. 0 means no cap. Ignored by Forbid and Replace.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxActive int32 `json:"maxActive,omitempty"`
}

// TriggerSpec defines the desired state of Trigger.
// +kubebuilder:validation:XValidation:rule="!has(self.source.nats) || has(self.roundTableRef)",message="roundTableRef is required for source.nats"
// +kubebuilder:validation:XValidation:rule="!has(self.source.webhook) || has(self.roundTableRef)",message="roundTableRef is required for source.webhook"
// +kubebuilder:validation:XValidation:rule="!has(self.source.github) || has(self.roundTableRef)",message="roundTableRef is required for source.github"
type TriggerSpec struct {
	// source is where events come from.
	// +required
	Source TriggerSource `json:"source"`

	// target is what each firing starts.
	// +required
	Target TriggerTarget `json:"target"`

	// input is a Go template rendered into the run's spec.input. The event
	// is available as .Event: .Event.Subject, .Event.Data (the payload
	// parsed as JSON, or the raw string), .Event.Headers (map of header name
	// to value) and .Event.Time; .Trigger has .Name and .Namespace. A
	// missing map key is an error. toJson encodes a value as JSON. Empty
	// keeps the target Chain's own spec.input.
	// +optional
	Input string `json:"input,omitempty"`

	// dedupeKey is a Go template (same data as input) whose result
	// identifies the event: an event whose key matches a run that still
	// exists starts nothing. Defaults to the Nats-Msg-Id header (else the
	// stream sequence) for NATS and webhook, the scheduled time for
	// schedule, and "<owner/name>#<number>" for github.
	// +optional
	DedupeKey string `json:"dedupeKey,omitempty"`

	// concurrency bounds how many runs are active at once.
	// +optional
	Concurrency TriggerConcurrency `json:"concurrency,omitempty"`

	// suspend stops firing. A NATS, webhook or github source's consumer is
	// deleted, so events published while suspended do not fire on resume (a
	// github source stops polling and picks up matching items again on
	// resume).
	// +optional
	Suspend bool `json:"suspend,omitempty"`

	// roundTableRef names the RoundTable (same namespace) whose events
	// stream a NATS, webhook or github source reads, and whose
	// spec.github.tokenSecretRef a github source polls with. Required for
	// source.nats, source.webhook and source.github.
	// +optional
	RoundTableRef *corev1.LocalObjectReference `json:"roundTableRef,omitempty"`

	// ttlSecondsAfterFinished deletes a finished run this long after it
	// completed. Its dedupe key is forgotten then too. 0 keeps finished runs.
	// +kubebuilder:default=86400
	// +kubebuilder:validation:Minimum=0
	// +optional
	TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty"`
}

// TriggerFiring records one event a Trigger handled.
type TriggerFiring struct {
	// time is when the event was handled.
	Time metav1.Time `json:"time"`

	// key is the event's dedupe key.
	// +optional
	Key string `json:"key,omitempty"`

	// run is the Chain the firing started.
	// +optional
	Run string `json:"run,omitempty"`

	// skipReason says why the firing started nothing.
	// +optional
	SkipReason string `json:"skipReason,omitempty"`
}

// TriggerStatus defines the observed state of Trigger.
type TriggerStatus struct {
	// observedGeneration is the generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// lastFiredAt is when a firing last started a run.
	// +optional
	LastFiredAt *metav1.Time `json:"lastFiredAt,omitempty"`

	// lastScheduledAt is the last schedule time handled (schedule source).
	// +optional
	LastScheduledAt *metav1.Time `json:"lastScheduledAt,omitempty"`

	// nextScheduledAt is the next schedule time (schedule source).
	// +optional
	NextScheduledAt *metav1.Time `json:"nextScheduledAt,omitempty"`

	// recentFirings are the latest firings, newest first (at most 10).
	// +optional
	RecentFirings []TriggerFiring `json:"recentFirings,omitempty"`

	// activeRuns are the names of the Trigger's runs that are running or
	// pending.
	// +optional
	ActiveRuns []string `json:"activeRuns,omitempty"`

	// stream is the JetStream stream the NATS, webhook or github source's
	// consumer is on.
	// +optional
	Stream string `json:"stream,omitempty"`

	// consumer is the NATS, webhook or github source's durable consumer.
	// +optional
	Consumer string `json:"consumer,omitempty"`

	// lastPolledAt is when a github source last listed its repositories
	// successfully.
	// +optional
	LastPolledAt *metav1.Time `json:"lastPolledAt,omitempty"`

	// conditions: Ready is True while the Trigger is listening; GitHubReady
	// (github source) is True while polling works.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=trg,categories=roundtable
// +kubebuilder:printcolumn:name="Chain",type=string,JSONPath=`.spec.target.chainRef.name`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=='Ready')].status`
// +kubebuilder:printcolumn:name="Last Fired",type=date,JSONPath=`.status.lastFiredAt`
// +kubebuilder:printcolumn:name="Suspend",type=boolean,JSONPath=`.spec.suspend`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="self.metadata.name.size() <= 63",message="metadata.name must be at most 63 characters: it labels the Trigger's runs"

// Trigger starts a run of a Chain with input rendered from an event: a
// message on the RoundTable's events stream, a webhook delivered through the
// gateway, a cron schedule, or a GitHub issue or pull request matching a
// query.
type Trigger struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Trigger
	// +required
	Spec TriggerSpec `json:"spec"`

	// status defines the observed state of Trigger
	// +optional
	Status TriggerStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// TriggerList contains a list of Trigger
type TriggerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Trigger `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Trigger{}, &TriggerList{})
}
