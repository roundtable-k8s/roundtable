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
	// LabelPickupRoundTable marks a Chain created by a RoundTable's issue
	// pickup; the value is the RoundTable name.
	LabelPickupRoundTable = "ai.roundtable.io/pickup-roundtable"

	// AnnotationPickupRepo is the owner/name of the picked-up issue's repo.
	AnnotationPickupRepo = "ai.roundtable.io/pickup-repo"

	// AnnotationPickupIssue is the picked-up issue number.
	AnnotationPickupIssue = "ai.roundtable.io/pickup-issue"

	// AnnotationPickupClaimed is set to the run trigger once the issue has
	// been claimed on GitHub for that run (labels swapped and commented), so
	// a retried poll never comments twice.
	AnnotationPickupClaimed = "ai.roundtable.io/pickup-claimed"

	// AnnotationPickupVerified is set to the run trigger once the operator has
	// checked on GitHub that the run's PRs are merged or queued (#301), or,
	// for a failed run, has handed its issue back with needs-human (#314).
	AnnotationPickupVerified = "ai.roundtable.io/pickup-verified"

	// AnnotationPickupParentDone is set to the run trigger once the operator
	// has settled the parent issue of a split run (#319): closed it after all
	// sub-issues were completed, or handed it back with needs-human.
	AnnotationPickupParentDone = "ai.roundtable.io/pickup-parent-done"

	// IssuePickupTriggerSuffix names the Trigger generated for a table with
	// spec.issuePickup.engine: trigger: "<table>-issue-pickup".
	IssuePickupTriggerSuffix = "-issue-pickup"
)

// IssuePickupEngine selects what polls GitHub for a table's ready issues.
// +kubebuilder:validation:Enum=builtin;trigger
type IssuePickupEngine string

const (
	// IssuePickupEngineBuiltin is the issue pickup controller's own poller.
	IssuePickupEngineBuiltin IssuePickupEngine = "builtin"
	// IssuePickupEngineTrigger runs pickup on a generated Trigger with a
	// github source ("<table>-issue-pickup").
	IssuePickupEngineTrigger IssuePickupEngine = "trigger"
)

// IssuePickupPostRun selects who does the PR checks after a pickup run.
// +kubebuilder:validation:Enum=builtin;chain
type IssuePickupPostRun string

const (
	// IssuePickupPostRunBuiltin: the operator verifies that the run's PRs are
	// merged or queued (posting a missing review), hands a failed run's issue
	// back, and closes a split run's parent issue.
	IssuePickupPostRunBuiltin IssuePickupPostRun = "builtin"
	// IssuePickupPostRunChain: steps of the chain do that work. The operator
	// parses no step output; it only releases an issue a finished run left
	// in progress (see IssuePickupSpec.PostRun).
	IssuePickupPostRunChain IssuePickupPostRun = "chain"
)

// IssuePickupSpec configures polling GitHub for issues labelled ready and
// starting one copy of a Chain (the PR pipeline) per issue. The operator uses
// the RoundTable's spec.github.tokenSecretRef token; it needs read/write on
// issues in every listed repo.
// +kubebuilder:validation:XValidation:rule="self.readyLabel != self.inProgressLabel && self.readyLabel != self.needsHumanLabel",message="readyLabel must differ from inProgressLabel and needsHumanLabel"
type IssuePickupSpec struct {
	// engine selects what picks up ready issues. builtin (the default) is
	// the issue pickup controller's own poller. trigger generates a Trigger
	// named "<table>-issue-pickup" (owned by the RoundTable) with a github
	// source built from these fields, and the builtin poller only does the
	// post-run checks (merge verification, failed-run hand-back, split
	// parents) and cleanup for the table. Switching back to builtin deletes
	// the generated Trigger. Both engines name a run after its issue, so a
	// switch never starts a second run for an issue.
	// +kubebuilder:default=builtin
	// +optional
	Engine IssuePickupEngine `json:"engine,omitempty"`

	// postRun selects who checks a finished run on GitHub. builtin (the
	// default): the operator parses the run's "PRs:" and "Verdict:" lines,
	// checks every PR is merged or queued (queueing it or posting the
	// missing review when it can), hands failed runs back with
	// needsHumanLabel and closes the parent issue of a split run once its
	// sub-issues are done. chain: the chain's own steps do all of that (the
	// chart's PR pipeline with prPipeline.postRun: chain) and the operator
	// only releases the issue: when a finished run did not succeed, or left
	// needsHumanLabel on the issue, and the issue is still open with
	// inProgressLabel, it gets needsHumanLabel and loses inProgressLabel, so
	// a failed verify or handback step never leaves it in progress. See
	// docs/pr-pipeline.md.
	// +kubebuilder:default=builtin
	// +optional
	PostRun IssuePickupPostRun `json:"postRun,omitempty"`

	// suspend stops polling. Chains already started keep running.
	// +kubebuilder:default=false
	// +optional
	Suspend bool `json:"suspend,omitempty"`

	// repos are the GitHub repositories to poll, as owner/name.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=50
	// +kubebuilder:validation:items:Pattern=`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`
	// +listType=set
	// +required
	Repos []string `json:"repos"`

	// chainRef names the Chain in the RoundTable's namespace that is copied
	// for every picked-up issue (normally the PR pipeline, pr-pipeline). The
	// copy gets spec.input {"repo": "<owner/name>", "issue": <n>} and is
	// started with the ai.roundtable.io/run annotation. The template itself
	// is never run by pickup.
	// +kubebuilder:validation:XValidation:rule="size(self.name) > 0",message="chainRef.name is required"
	// +required
	ChainRef corev1.LocalObjectReference `json:"chainRef"`

	// readyLabel marks issues to pick up.
	// +kubebuilder:default="rt:ready"
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=50
	// +optional
	ReadyLabel string `json:"readyLabel,omitempty"`

	// inProgressLabel replaces readyLabel when an issue is claimed.
	// +kubebuilder:default="rt:in-progress"
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=50
	// +optional
	InProgressLabel string `json:"inProgressLabel,omitempty"`

	// needsHumanLabel marks issues a knight must not take. A ready issue with
	// this label, or whose title or body matches humanPatterns, is skipped:
	// readyLabel is removed, needsHumanLabel added and a comment says why.
	// +kubebuilder:default="needs-human"
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=50
	// +optional
	NeedsHumanLabel string `json:"needsHumanLabel,omitempty"`

	// breakingLabel marks open pull requests that change an API, CRD or
	// contract (the breaking-change protocol, #260). While a repo has an open
	// pull request with this label, pickup starts no new Chain for it: ready
	// issues stay ready and are picked up after the breaking PR is merged or
	// closed.
	// +kubebuilder:default="breaking"
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=50
	// +optional
	BreakingLabel string `json:"breakingLabel,omitempty"`

	// humanPatterns are RE2 regular expressions matched against an issue's
	// title and body. A match means the work needs a human (the bot token
	// cannot push workflow changes, and secrets/auth changes need review).
	// +kubebuilder:default={"\\.github/workflows","(?i)\\bsecrets?\\b","(?i)\\b(auth|oauth|authn|authz|authentication|authori[sz]ation|credentials?)\\b"}
	// +kubebuilder:validation:MaxItems=20
	// +optional
	HumanPatterns []string `json:"humanPatterns,omitempty"`

	// assignees are GitHub logins assigned to a claimed issue. GitHub
	// ignores logins that cannot be assigned (GitHub App bots cannot be).
	// +kubebuilder:validation:MaxItems=10
	// +listType=set
	// +optional
	Assignees []string `json:"assignees,omitempty"`

	// intervalSeconds is the time between polls.
	// +kubebuilder:default=300
	// +kubebuilder:validation:Minimum=60
	// +kubebuilder:validation:Maximum=86400
	// +optional
	IntervalSeconds int32 `json:"intervalSeconds,omitempty"`

	// maxActivePerRepo caps unfinished pickup Chains per repository.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=20
	// +optional
	MaxActivePerRepo int32 `json:"maxActivePerRepo,omitempty"`

	// maxActive caps unfinished pickup Chains for the whole RoundTable.
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=50
	// +optional
	MaxActive int32 `json:"maxActive,omitempty"`

	// fairShare shares maxActive across the repositories round-robin, so one
	// repository cannot fill every slot while another waits. Only the trigger
	// engine honors it. Default false.
	// +optional
	FairShare bool `json:"fairShare,omitempty"`

	// ttlSecondsAfterFinished deletes a finished pickup Chain this long after
	// it completed. 0 keeps finished Chains.
	// +kubebuilder:default=604800
	// +kubebuilder:validation:Minimum=0
	// +optional
	TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty"`
}

// IssuePickupStatus is the observed state of a RoundTable's issue pickup.
type IssuePickupStatus struct {
	// lastPollTime is when GitHub was last polled.
	// +optional
	LastPollTime *metav1.Time `json:"lastPollTime,omitempty"`

	// active is the number of unfinished pickup Chains.
	// +optional
	Active int32 `json:"active,omitempty"`

	// pickedTotal counts issues claimed (and Chain runs started).
	// +optional
	PickedTotal int64 `json:"pickedTotal,omitempty"`

	// skippedTotal counts ready issues handed back to a human.
	// +optional
	SkippedTotal int64 `json:"skippedTotal,omitempty"`

	// lastError is the last poll's error ("" when it succeeded).
	// +optional
	LastError string `json:"lastError,omitempty"`

	// engine is the pickup engine in use: builtin or trigger.
	// +optional
	Engine IssuePickupEngine `json:"engine,omitempty"`

	// trigger is the generated Trigger the trigger engine runs on.
	// +optional
	Trigger string `json:"trigger,omitempty"`

	// triggerEngineSucceededAt is when a run started by the trigger engine
	// was first seen succeeded on this table. From then on the
	// BuiltinEngineDeprecated condition is set.
	// +optional
	TriggerEngineSucceededAt *metav1.Time `json:"triggerEngineSucceededAt,omitempty"`

	// conditions: BuiltinEngineDeprecated is True once the trigger engine
	// has run issue pickup successfully on this table (absent before).
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}
