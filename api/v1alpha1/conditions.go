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

// Condition types follow Kubernetes API conventions:
// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties
//
// Type names should be:
// - Adjectives (e.g., "Ready", "Available") or past-tense verbs (e.g., "Succeeded", "Degraded")
// - PascalCase for multi-word types
// - Describe the current state, not an action in progress
//
// Reason values should be:
// - PascalCase
// - Brief identifier for the condition
//
// Status values should be:
// - metav1.ConditionTrue, metav1.ConditionFalse, or metav1.ConditionUnknown

const (
	// ===== Knight Condition Types =====

	// ConditionKnightAvailable indicates whether the knight is available to accept tasks.
	// Status=True means the knight pod is running and NATS consumer is active.
	// Status=False means the knight is suspended, degraded, or provisioning.
	ConditionKnightAvailable = "Available"

	// ConditionKnightRuntimeAvailable indicates whether the runtime backend
	// selected by spec.runtime is enabled in this operator.
	// Status=False means the knight cannot be scheduled until the backend is
	// available (e.g. runtime: sandbox without the agent-sandbox v1beta1 API).
	ConditionKnightRuntimeAvailable = "RuntimeAvailable"

	// ConditionKnightAutoscaled reports whether a KEDA ScaledObject scales
	// the knight. It is only set when autoscaling is requested
	// (spec.maxReplicas > 1 or lifecycle.suspendPolicy: auto).
	// Status=False means it was requested but can't be honoured (KEDA's
	// keda.sh/v1alpha1 ScaledObject isn't served, or the runtime doesn't
	// support it); the knight then runs spec.replicas pods.
	ConditionKnightAutoscaled = "Autoscaled"

	// ConditionKnightPlanCapLimited is True when the RoundTable's
	// policies.maxConcurrentPlanTasks lowered this plan-backed knight's
	// replicas or maxReplicas. Only set on plan-backed knights in a table
	// with a cap.
	ConditionKnightPlanCapLimited = "PlanCapLimited"

	// ConditionKnightToolsImageReady reports whether the tools image for the
	// current spec.tools is published. Only set on knights with Nix tools.
	// Status=False means the pod rollout is held on its current template
	// (ToolsImageBuilding) or the build failed (ToolsImageBuildFailed); fix
	// spec.tools or the builder and the operator retries.
	ConditionKnightToolsImageReady = "ToolsImageReady"

	// ConditionKnightEgressRestricted reports whether the knight's
	// spec.egress allowlist is enforced. Only set on knights with spec.egress.
	// Status=True means a CiliumNetworkPolicy limits egress to DNS, NATS and
	// the allowed hosts. Status=False means the hostname allowlist can't be
	// enforced (FQDNPolicyUnavailable: a NetworkPolicy allows the allowed
	// ports to any destination) or the policy failed to apply.
	ConditionKnightEgressRestricted = "EgressRestricted"

	// ConditionKnightIsolated reports how isolated the knight actually is.
	// Status=True (reason Isolated) means a RuntimeClass is applied and a
	// CiliumNetworkPolicy with hostname rules limits its egress. Status=False
	// reasons: RuntimeClassNotFound (the knight stays parked), OptedOut
	// (spec.isolation, the message carries its reason), FQDNPolicyUnavailable,
	// EgressPolicyFailed, or IsolationDisabled (no RuntimeClass or no egress
	// policy applies, e.g. the install turned the defaults off).
	ConditionKnightIsolated = "Isolated"

	// ConditionKnightMemoryShared reports whether the memory PVC the operator
	// created (spec.memory without existingClaim) can be shared by all of the
	// knight's pods. Only set when the knight can run more than one pod
	// (replicas or maxReplicas above 1) and the claim is bound.
	// Status=False (MemoryNotRWX) means the claim was bound without
	// ReadWriteMany: pods on other nodes can't mount it. Use a storageClassName
	// that supports ReadWriteMany.
	ConditionKnightMemoryShared = "MemoryShared"

	// ConditionKnightWorkspaceReady reports the workspace PVC when
	// spec.workspace.fromSnapshot is set. False (SnapshotNotReady,
	// SnapshotAPIUnavailable) while the PVC can't be created yet; True
	// (RestoredFromSnapshot) when the PVC was created from the snapshot; True
	// (FromSnapshotIgnored) when the PVC already existed, so fromSnapshot
	// didn't apply. Absent otherwise.
	ConditionKnightWorkspaceReady = "WorkspaceReady"

	// ConditionKnightWorkspaceSnapshots reports the outcome of the
	// ai.roundtable.io/snapshot annotation. Absent while the annotation is
	// unused and the knight has no snapshots. False when snapshots can't be
	// taken (SnapshotAPIUnavailable, WorkspaceNotPersistent, SnapshotFailed).
	ConditionKnightWorkspaceSnapshots = "WorkspaceSnapshots"

	// ConditionKnightDeprecatedFieldsUsed is True when the Knight sets a
	// deprecated, ignored spec field (currently spec.nats.maxDeliver above 1).
	// The condition is absent otherwise.
	ConditionKnightDeprecatedFieldsUsed = "DeprecatedFieldsUsed"

	// ConditionKnightDegraded is True when part of the spec cannot be honoured.
	ConditionKnightDegraded = "Degraded"

	// ConditionKnightMCPHeadersReady reports whether every Secret key named by
	// spec.mcpServers[].headersFrom exists. False (MCPHeaderSecretMissing)
	// names the missing Secret/key (MCPHeaderDuplicate: a header repeated in
	// a different case); the pod would otherwise fail to start
	// with CreateContainerConfigError. Absent when no headersFrom are
	// rendered (no headers, or a runtime other than claude-knight).
	ConditionKnightMCPHeadersReady = "MCPHeadersReady"

	// ReasonMCPHeaderSecretsFound means every referenced Secret key exists.
	ReasonMCPHeaderSecretsFound = "SecretsFound"

	// ReasonMCPHeaderSecretMissing means a referenced Secret or key is missing.
	ReasonMCPHeaderSecretMissing = "MCPHeaderSecretMissing"

	// ReasonMCPHeaderDuplicate means two headersFrom entries of one server
	// differ only in case; only the first is rendered.
	ReasonMCPHeaderDuplicate = "MCPHeaderDuplicate"

	// ReasonMCPHeaderSecretUnreadable means a referenced Secret could not be
	// read (other than not found), e.g. RBAC or an API error.
	ReasonMCPHeaderSecretUnreadable = "MCPHeaderSecretUnreadable"

	// ReasonMCPServersUnsupported means spec.mcpServers is set on a runtime
	// that has no MCP client (anything but claude-knight); nothing is rendered.
	ReasonMCPServersUnsupported = "MCPServersUnsupported"

	// ReasonKnightDeprecatedFieldsUsed is the reason (and event reason base)
	// for ConditionKnightDeprecatedFieldsUsed.
	ReasonKnightDeprecatedFieldsUsed = "DeprecatedFieldsUsed"

	// ConditionKnightConsumerReady reports whether the knight's durable task
	// consumer (<Knight>-consumer on spec.nats.stream) exists with the
	// expected config. The operator creates, updates and adopts it, so it
	// exists while the knight is scaled to zero, and deletes it with the
	// Knight. Absent when the operator runs without NATS.
	ConditionKnightConsumerReady = "ConsumerReady"

	// ReasonConsumerReady means the consumer exists with the expected config.
	ReasonConsumerReady = "ConsumerReady"

	// ReasonConsumerNATSUnavailable means the operator could not reach NATS.
	ReasonConsumerNATSUnavailable = "NATSUnavailable"

	// ReasonConsumerStreamNotFound means spec.nats.stream does not exist.
	ReasonConsumerStreamNotFound = "StreamNotFound"

	// ReasonConsumerFailed means JetStream rejected the consumer create or
	// update.
	ReasonConsumerFailed = "ConsumerFailed"

	// ===== RoundTable Condition Types =====

	// ConditionRoundTableAvailable indicates whether the RoundTable is operational.
	// Status=True means all knights are ready and within budget.
	// Status=False means knights are degraded, suspended, or over budget.
	ConditionRoundTableAvailable = "Available"

	// ConditionNATSReady indicates whether NATS JetStream streams are configured.
	// Status=True means streams are created and healthy.
	// Status=False means stream creation failed or streams are unhealthy.
	ConditionNATSReady = "NATSReady"

	// ===== Chain Condition Types =====

	// ConditionChainValid indicates whether the chain spec passed validation.
	// Status=True means all steps, knight refs, templates, and DAG are valid.
	// Status=False means validation failed (cyclic deps, invalid refs, bad templates).
	ConditionChainValid = "Valid"

	// ConditionChainComplete indicates whether the chain execution finished.
	// Status=True means all steps completed (succeeded, partially succeeded, or failed).
	// Status=False means chain is still running or pending.
	ConditionChainComplete = "Complete"

	// ===== Mission Condition Types =====

	// ConditionMissionComplete indicates whether the mission finished execution.
	// Status=True means mission succeeded, failed, timed out, or was over budget.
	// Status=False means mission is still running.
	ConditionMissionComplete = "Complete"

	// ConditionBriefingPublished indicates whether the mission briefing was published.
	// Status=True means briefing was successfully sent to all knights via NATS.
	// Status=False means briefing publish failed or no briefing was configured.
	ConditionBriefingPublished = "BriefingPublished"

	// ConditionCleanupComplete indicates whether mission cleanup finished.
	// Status=True means all ephemeral resources were deleted.
	// Status=False means cleanup is in progress.
	ConditionCleanupComplete = "CleanupComplete"

	// ===== Shared Condition Types (Chain + Mission) =====

	// ConditionNotificationSent indicates the state of the spec.notify
	// completion webhook for the current run.
	// Status=True means the notification was delivered (2xx) — never fired again.
	// Status=Unknown means delivery failed and is being retried with backoff.
	// Status=False means delivery permanently failed (retry window exhausted)
	// or the URL was rejected by the operator allowlist.
	ConditionNotificationSent = "NotificationSent"
)

const (
	// ===== Knight Condition Reasons =====

	// ReasonKnightReady indicates the knight is fully operational.
	ReasonKnightReady = "KnightReady"

	// ReasonKnightProvisioning indicates the knight deployment is being created.
	ReasonKnightProvisioning = "Provisioning"

	// ReasonKnightSuspended indicates the knight was manually suspended.
	ReasonKnightSuspended = "Suspended"

	// ReasonKnightReconcileError indicates the knight reconcile encountered an error.
	ReasonKnightReconcileError = "ReconcileError"

	// ReasonRuntimeBackendAvailable indicates the knight's runtime backend is enabled.
	ReasonRuntimeBackendAvailable = "BackendAvailable"

	// ReasonSandboxUnavailable indicates the knight requests runtime: sandbox but
	// the operator started without the sandbox backend.
	ReasonSandboxUnavailable = "SandboxUnavailable"

	// ReasonRuntimeClassNotFound indicates the RuntimeClass named by the
	// knight's spec.runtimeClassName (or the table default) does not exist.
	ReasonRuntimeClassNotFound = "RuntimeClassNotFound"

	// ReasonToolsImageBuilding indicates the tools image for the new spec is
	// still building; the pod keeps its current template meanwhile.
	ReasonToolsImageBuilding = "ToolsImageBuilding"

	// ReasonToolsImageBuildFailed indicates the tools image build Job failed;
	// the pod is not rolled.
	ReasonToolsImageBuildFailed = "ToolsImageBuildFailed"

	// ReasonToolsImagePublished indicates the tools image is published.
	ReasonToolsImagePublished = "ToolsImagePublished"

	// ReasonKnightScaledToZero indicates KEDA scaled an autoscaled knight to
	// zero pods because it had no pending tasks. It wakes on the next task.
	ReasonKnightScaledToZero = "ScaledToZero"

	// ReasonKnightWaking indicates an autoscaled knight at zero ready
	// replicas has tasks queued on its consumer and its pod is starting. The
	// phase stays Suspended; only the Available reason changes.
	ReasonKnightWaking = "Waking"

	// ReasonScaledObjectReady indicates the KEDA ScaledObject is rendered.
	ReasonScaledObjectReady = "ScaledObjectReady"

	// ReasonKEDAUnavailable indicates autoscaling was requested but the
	// operator found no keda.sh/v1alpha1 ScaledObject API at startup.
	ReasonKEDAUnavailable = "KEDAUnavailable"

	// ReasonAutoscalingUnsupported indicates autoscaling was requested on a
	// runtime the operator can't autoscale (runtime: sandbox).
	ReasonAutoscalingUnsupported = "UnsupportedRuntime"

	// ReasonSnapshotAPIUnavailable indicates the operator found no
	// snapshot.storage.k8s.io/v1 VolumeSnapshot API at startup.
	ReasonSnapshotAPIUnavailable = "SnapshotAPIUnavailable"

	// ReasonWorkspaceNotPersistent indicates a snapshot was requested for an
	// ephemeral workspace.
	ReasonWorkspaceNotPersistent = "WorkspaceNotPersistent"

	// ReasonSnapshotFailed indicates creating or pruning VolumeSnapshots failed.
	ReasonSnapshotFailed = "SnapshotFailed"

	// ReasonSnapshotNotReady indicates spec.workspace.fromSnapshot names a
	// VolumeSnapshot that doesn't exist or isn't readyToUse, so the workspace
	// PVC isn't created yet.
	ReasonSnapshotNotReady = "SnapshotNotReady"

	// ReasonFromSnapshotIgnored indicates the workspace PVC already existed, so
	// spec.workspace.fromSnapshot was not applied.
	ReasonFromSnapshotIgnored = "FromSnapshotIgnored"

	// ReasonRestoredFromSnapshot indicates the workspace PVC was created from
	// spec.workspace.fromSnapshot.
	ReasonRestoredFromSnapshot = "RestoredFromSnapshot"

	// ReasonSnapshotsReady indicates the workspace snapshots are in order.
	ReasonSnapshotsReady = "SnapshotsReady"

	// ReasonMemoryNotRWX indicates the knight's memory PVC was bound without
	// ReadWriteMany although the knight can run more than one pod.
	ReasonMemoryNotRWX = "MemoryNotRWX"

	// ReasonMemoryRWX indicates the knight's memory PVC is bound ReadWriteMany.
	ReasonMemoryRWX = "MemoryRWX"

	// ReasonPlanCapApplied indicates the plan-task cap lowered the knight's scale.
	ReasonPlanCapApplied = "PlanCapApplied"

	// ReasonWithinPlanCap indicates the knight got all the replicas it asked for.
	ReasonWithinPlanCap = "WithinPlanCap"

	// ReasonFQDNPolicyApplied indicates the CiliumNetworkPolicy enforcing the
	// hostname allowlist is applied.
	ReasonFQDNPolicyApplied = "FQDNPolicyApplied"

	// ReasonFQDNPolicyUnavailable indicates the cilium.io/v2
	// CiliumNetworkPolicy API isn't served, so the operator rendered a
	// NetworkPolicy without hostname rules.
	ReasonFQDNPolicyUnavailable = "FQDNPolicyUnavailable"

	// ReasonEgressPolicyFailed indicates the egress policy could not be applied.
	ReasonEgressPolicyFailed = "EgressPolicyFailed"

	// ReasonIsolated indicates the knight runs under a RuntimeClass and
	// behind an egress policy with hostname rules.
	ReasonIsolated = "Isolated"

	// ReasonIsolationOptedOut indicates the knight opts out of isolation in
	// spec.isolation.
	ReasonIsolationOptedOut = "OptedOut"

	// ReasonIsolationDisabled indicates no RuntimeClass or no egress policy
	// applies to the knight without an opt-out (the operator defaults are off,
	// or the table sets runtimeClassName: "").
	ReasonIsolationDisabled = "IsolationDisabled"

	// ===== RoundTable Condition Reasons =====

	// ReasonAllKnightsReady indicates all knights in the roundtable are ready.
	ReasonAllKnightsReady = "AllKnightsReady"

	// ReasonKnightsDegraded indicates some knights are not ready.
	ReasonKnightsDegraded = "KnightsDegraded"

	// ReasonRoundTableSuspended indicates the roundtable was manually suspended.
	ReasonRoundTableSuspended = "Suspended"

	// ReasonRoundTableProvisioning indicates the roundtable is being provisioned.
	ReasonRoundTableProvisioning = "Provisioning"

	// ReasonOverBudget indicates the roundtable exceeded its cost budget.
	ReasonOverBudget = "OverBudget"

	// ReasonStreamsReady indicates NATS streams are configured and healthy.
	ReasonStreamsReady = "StreamsReady"

	// ReasonStreamError indicates NATS stream creation or update failed.
	ReasonStreamError = "StreamError"

	// ===== Chain Condition Reasons =====

	// ReasonChainValid indicates the chain spec passed all validation checks.
	ReasonChainValid = "Valid"

	// ReasonMissingRoundTableRef indicates the chain is missing roundTableRef.
	ReasonMissingRoundTableRef = "MissingRoundTableRef"

	// ReasonInvalidKnightRef indicates a step references a non-existent knight.
	ReasonInvalidKnightRef = "InvalidKnightRef"

	// ReasonCyclicDependency indicates the chain has cyclic step dependencies.
	ReasonCyclicDependency = "CyclicDependency"

	// ReasonInvalidSchedule indicates the schedule or timeZone is invalid.
	ReasonInvalidSchedule = "InvalidSchedule"

	// ReasonInvalidTemplate indicates a step's Go template failed to parse.
	ReasonInvalidTemplate = "InvalidTemplate"

	// ReasonInvalidForEach indicates a step has an invalid forEach / maxParallel / failurePolicy configuration.
	ReasonInvalidForEach = "InvalidForEach"

	// ReasonInvalidCondition indicates a step's when or loop.until CEL expression does not compile.
	ReasonInvalidCondition = "InvalidCondition"

	// ReasonInvalidLoop indicates a step has an invalid loop configuration.
	ReasonInvalidLoop = "InvalidLoop"

	// ReasonChainSucceeded indicates all chain steps completed successfully.
	ReasonChainSucceeded = "Succeeded"

	// ReasonChainPartiallySucceeded indicates some steps failed but had continueOnFailure.
	ReasonChainPartiallySucceeded = "PartiallySucceeded"

	// ReasonChainFailed indicates one or more steps failed without continueOnFailure.
	ReasonChainFailed = "Failed"

	// ReasonChainTimeout indicates the chain exceeded its timeout duration.
	ReasonChainTimeout = "Timeout"

	// ===== Mission Condition Reasons =====

	// ReasonMissionSucceeded indicates all mission chains completed successfully.
	ReasonMissionSucceeded = "Succeeded"

	// ReasonMissionChainFailed indicates one or more mission chains failed.
	ReasonMissionChainFailed = "ChainFailed"

	// ReasonMissionFailed indicates the mission failed before its chains could
	// complete (e.g. assembly timeout or planning failure).
	ReasonMissionFailed = "Failed"

	// ReasonMissionTimeout indicates the mission exceeded its timeout.
	ReasonMissionTimeout = "Timeout"

	// ReasonMissionExpired indicates the mission exceeded its TTL.
	ReasonMissionExpired = "Expired"

	// ReasonBriefingPublished indicates briefing was published successfully.
	ReasonBriefingPublished = "Published"

	// ReasonBriefingPublishFailed indicates briefing publish to NATS failed.
	ReasonBriefingPublishFailed = "PublishFailed"

	// ReasonNoBriefing indicates no briefing text was configured.
	ReasonNoBriefing = "NoBriefing"

	// ReasonCleanupComplete indicates mission cleanup finished successfully.
	ReasonCleanupComplete = "CleanedUp"

	// ===== Notification Condition Reasons =====

	// ReasonNotifyDelivered indicates the completion webhook was delivered.
	ReasonNotifyDelivered = "Delivered"

	// ReasonNotifyRetrying indicates delivery failed and will be retried.
	ReasonNotifyRetrying = "DeliveryRetrying"

	// ReasonNotifyDeliveryFailed indicates delivery permanently failed after
	// the bounded retry window.
	ReasonNotifyDeliveryFailed = "DeliveryFailed"

	// ReasonNotifyURLNotAllowed indicates the webhook URL did not match the
	// operator's allowed URL prefixes (SSRF guard) and was rejected.
	ReasonNotifyURLNotAllowed = "URLNotAllowed"

	// ===== Trigger Condition Types and Reasons =====

	// ConditionTriggerReady is True while a Trigger is listening for events
	// (its consumer runs, or its schedule is registered).
	ConditionTriggerReady = "Ready"

	// ReasonTriggerListening indicates the Trigger is listening.
	ReasonTriggerListening = "Listening"

	// ReasonTriggerSuspended indicates spec.suspend is set.
	ReasonTriggerSuspended = "Suspended"

	// ReasonTriggerInvalidSpec indicates a template, subject or schedule
	// is invalid.
	ReasonTriggerInvalidSpec = "InvalidSpec"

	// ReasonTriggerTargetNotFound indicates the target Chain does not exist.
	ReasonTriggerTargetNotFound = "TargetNotFound"

	// ReasonTriggerRoundTableNotFound indicates roundTableRef does not
	// resolve.
	ReasonTriggerRoundTableNotFound = "RoundTableNotFound"

	// ReasonTriggerConsumerError indicates the JetStream consumer could not
	// be created or read.
	ReasonTriggerConsumerError = "ConsumerError"

	// ConditionTriggerGitHubReady is True while a github-source Trigger's
	// token resolves and its last poll succeeded.
	ConditionTriggerGitHubReady = "GitHubReady"

	// ReasonTriggerPolling indicates the last GitHub poll succeeded.
	ReasonTriggerPolling = "Polling"

	// ReasonTriggerTokenError indicates the GitHub token could not be read.
	ReasonTriggerTokenError = "TokenError"

	// ReasonTriggerPollError indicates the last GitHub poll failed.
	ReasonTriggerPollError = "PollError"

	// ReasonTriggerRateLimited indicates GitHub's rate limit is used up; the
	// poller waits for it to reset.
	ReasonTriggerRateLimited = "RateLimited"
)
