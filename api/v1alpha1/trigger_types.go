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
	// a Trigger's run (NATS source only).
	AnnotationTriggerSubject = "ai.roundtable.io/trigger-subject"

	// FinalizerTrigger lets the operator delete a Trigger's JetStream
	// consumer before the Trigger goes away.
	FinalizerTrigger = "ai.roundtable.io/trigger-consumer"

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
// set. webhook and github are reserved and rejected until implemented.
// +kubebuilder:validation:XValidation:rule="[has(self.nats), has(self.schedule), has(self.webhook), has(self.github)].filter(x, x).size() == 1",message="set exactly one of source.nats, source.schedule, source.webhook or source.github"
// +kubebuilder:validation:XValidation:rule="!has(self.webhook)",message="source.webhook is not implemented yet (#176)"
// +kubebuilder:validation:XValidation:rule="!has(self.github)",message="source.github is not implemented yet"
type TriggerSource struct {
	// nats fires on each message published to a subject on the RoundTable's
	// events stream (<subjectPrefix>.events.>).
	// +optional
	NATS *TriggerNATSSource `json:"nats,omitempty"`

	// schedule fires on a cron schedule.
	// +optional
	Schedule *TriggerScheduleSource `json:"schedule,omitempty"`

	// webhook is reserved for the gateway webhook source (#176).
	// +optional
	Webhook *TriggerReservedSource `json:"webhook,omitempty"`

	// github is reserved for the GitHub source.
	// +optional
	GitHub *TriggerReservedSource `json:"github,omitempty"`
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

// TriggerReservedSource is a placeholder for a source that is not
// implemented yet.
type TriggerReservedSource struct{}

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
	// stream sequence) for NATS, and the scheduled time for schedule.
	// +optional
	DedupeKey string `json:"dedupeKey,omitempty"`

	// concurrency bounds how many runs are active at once.
	// +optional
	Concurrency TriggerConcurrency `json:"concurrency,omitempty"`

	// suspend stops firing. A NATS source's consumer is deleted, so events
	// published while suspended do not fire on resume.
	// +optional
	Suspend bool `json:"suspend,omitempty"`

	// roundTableRef names the RoundTable (same namespace) whose events
	// stream a NATS source reads. Required for source.nats.
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

	// stream is the JetStream stream the NATS source's consumer is on.
	// +optional
	Stream string `json:"stream,omitempty"`

	// consumer is the NATS source's durable consumer.
	// +optional
	Consumer string `json:"consumer,omitempty"`

	// conditions: Ready is True while the Trigger is listening.
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
// message on the RoundTable's events stream or a cron schedule.
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
