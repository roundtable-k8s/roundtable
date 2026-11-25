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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Label keys used across Round Table resources
const (
	// LabelEphemeral marks resources as ephemeral (mission-owned)
	LabelEphemeral = "ai.roundtable.io/ephemeral"

	// LabelMission links resources to their owning Mission
	LabelMission = "ai.roundtable.io/mission"

	// LabelRoundTable links resources to their RoundTable
	LabelRoundTable = "ai.roundtable.io/round-table"

	// LabelRole specifies a knight's role within a mission
	LabelRole = "ai.roundtable.io/role"

	// LabelCreatedBy records which component created a resource. Missions
	// created through the invoke gateway carry CreatedByGateway; the planner
	// then only accepts template-based ephemeral knights for them.
	LabelCreatedBy = "ai.roundtable.io/created-by"

	// CreatedByGateway is the LabelCreatedBy value of gateway-created resources.
	CreatedByGateway = "gateway"

	// AnnotationShutdownTime is an RFC 3339 timestamp after which a knight's
	// runtime may be shut down. The mission assembler sets it on mission
	// knights to the mission's expiry (start + ttlSeconds); the sandbox
	// runtime copies it into the Sandbox's spec.shutdownTime.
	AnnotationShutdownTime = "ai.roundtable.io/shutdown-time"
)

// KnightSpec defines the desired state of a Knight — an AI agent in the Round Table.
type KnightSpec struct {
	// runtime selects the backend for managing this knight's pod.
	// "deployment" uses a standard Kubernetes Deployment (default).
	// "sandbox" uses an Agent Sandbox (agents.x-k8s.io/v1beta1 Sandbox,
	// agent-sandbox v1.0.0 or later) for lifecycle management.
	// +kubebuilder:validation:Enum=deployment;sandbox
	// +kubebuilder:default="deployment"
	// +optional
	Runtime string `json:"runtime,omitempty"`

	// domain is the knight's area of expertise (e.g., "security", "infrastructure", "finance").
	// Used for NATS subject routing and skill filtering.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Domain string `json:"domain"`

	// model is the AI model to use (e.g., "openrouter/deepseek/deepseek-v3.2", "claude-sonnet-4-20250514").
	// When unset the knight's RoundTable spec.defaults.model applies, and when
	// that is unset too "openrouter/deepseek/deepseek-v3.2". The model the
	// knight runs is reported in status.model.
	// +optional
	Model string `json:"model,omitempty"`

	// effort is the knight's default reasoning effort. It is passed to the
	// pod as KNIGHT_EFFORT; the runtime maps it onto its own setting. A chain
	// step's effort overrides it for that task. Unset leaves the runtime's
	// own configuration alone.
	// +kubebuilder:validation:Enum=low;medium;high;xhigh;max
	// +optional
	Effort string `json:"effort,omitempty"`

	// image is the container image for the knight runtime.
	// If empty, the knight's RoundTable spec.defaults.image applies, and when
	// that is unset too the operator's DEFAULT_KNIGHT_IMAGE env var.
	// +optional
	Image string `json:"image,omitempty"`

	// skills defines which skill categories this knight has access to.
	// The operator will configure the skill-filter sidecar accordingly.
	// +kubebuilder:validation:MinItems=1
	Skills []string `json:"skills"`

	// tools defines additional system packages and tools the knight needs.
	// +optional
	Tools *KnightTools `json:"tools,omitempty"`

	// nats configures the knight's NATS JetStream consumer and subjects.
	// +kubebuilder:validation:Required
	NATS KnightNATS `json:"nats"`

	// serviceAccountName is the name of the ServiceAccount to use for the knight pod.
	// If empty, the namespace default ServiceAccount is used.
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`

	// vault configures the shared Obsidian vault mount. When unset the
	// knight's RoundTable spec.vault applies; a knight's own vault is never
	// merged with the table's.
	// +optional
	Vault *KnightVault `json:"vault,omitempty"`

	// prompt allows overriding the knight's system prompt components.
	// +optional
	Prompt *KnightPrompt `json:"prompt,omitempty"`

	// capabilities configures optional runtime capabilities for the knight pod.
	// +optional
	Capabilities *KnightCapabilities `json:"capabilities,omitempty"`

	// resources are the compute resource requests and limits of the knight's
	// app container, applied verbatim. When unset the operator requests 100m
	// CPU and 256Mi memory and sets no limits.
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// runtimeClassName is the name of the Kubernetes RuntimeClass the knight's
	// pod runs under (for example "gvisor"). It applies to both runtime:
	// deployment and runtime: sandbox. When unset the table default applies,
	// and when that is unset too the operator default (KNIGHT_DEFAULT_RUNTIME_CLASS,
	// "gvisor" unless the install turns it off). An empty string means the
	// cluster's default runtime. isolation.runtime: none clears it. If the
	// RuntimeClass does not exist the knight is parked with
	// RuntimeAvailable=False and Isolated=False, reason RuntimeClassNotFound.
	// +optional
	RuntimeClassName *string `json:"runtimeClassName,omitempty"`

	// egress restricts the knight's outbound traffic to DNS, its NATS server
	// and an allowlist of hostnames. The operator renders it as a
	// CiliumNetworkPolicy knight-<name>-egress with toFQDNs rules; without the
	// cilium.io/v2 CiliumNetworkPolicy API it falls back to a NetworkPolicy that
	// allows the allowed ports to any destination and reports
	// EgressRestricted=False (FQDNPolicyUnavailable). When unset the table
	// default applies, and when that is unset too an empty allowlist (unless
	// the operator's default egress mode is off). The baseline (Kubernetes
	// API, the operator's base hosts and the arsenal git hosts) is always
	// allowed. isolation.egress: unrestricted renders no policy.
	// +optional
	Egress *KnightEgress `json:"egress,omitempty"`

	// isolation opts the knight out of the operator's isolation defaults. By
	// default every knight runs under the operator's default RuntimeClass
	// (gvisor) and behind a default-deny egress policy (DNS, NATS, the
	// Kubernetes API, the operator's base hosts, its arsenal git hosts and its
	// own allowedHosts). Opting out needs a reason, and the knight reports
	// Isolated=False with reason OptedOut. There is deliberately no table-level
	// opt-out.
	// +optional
	Isolation *KnightIsolation `json:"isolation,omitempty"`

	// concurrency is the maximum number of concurrent tasks the knight can process.
	// When unset the knight's RoundTable spec.defaults.concurrency applies, and
	// when that is unset too 2.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10
	// +optional
	Concurrency int32 `json:"concurrency,omitempty"`

	// replicas is the number of worker pods for this knight. All replicas bind
	// to the same durable consumer, so JetStream hands each one different
	// tasks. When the knight is autoscaled (see maxReplicas and
	// lifecycle.suspendPolicy) this is KEDA's minReplicaCount instead, except
	// with suspendPolicy auto, which always scales down to 0.
	// More than one replica needs workspace.mode: ephemeral (or a
	// ReadWriteMany workspace.existingClaim). Defaults to 1.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=50
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// maxReplicas is the upper bound for autoscaling (KEDA maxReplicaCount).
	// A value above 1 makes the operator render a KEDA ScaledObject that
	// scales the knight on its consumer's pending and in-flight (ack-pending) tasks. concurrency stays
	// the number of tasks per pod. Requires KEDA; without it the knight runs
	// spec.replicas pods and reports Autoscaled=False.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=50
	// +optional
	MaxReplicas *int32 `json:"maxReplicas,omitempty"`

	// planBacked marks a knight whose tasks spend the shared Claude plan
	// quota (a setup token shared by several knights). The RoundTable's
	// policies.maxConcurrentPlanTasks caps the combined capacity
	// (replicas x concurrency) of its plan-backed knights. When unset, knights
	// running a claude-knight image count as plan-backed.
	// +optional
	PlanBacked *bool `json:"planBacked,omitempty"`

	// taskTimeoutSeconds is the default task timeout, in seconds, for tasks
	// that don't carry their own (chain steps send theirs). When unset the
	// knight's RoundTable spec.defaults.taskTimeoutSeconds applies, and when
	// that is unset too 120.
	// +kubebuilder:validation:Minimum=30
	// +kubebuilder:validation:Maximum=3600
	// +optional
	TaskTimeoutSeconds int32 `json:"taskTimeoutSeconds,omitempty"`

	// generatedSkills contains inline skill definitions created by the planner.
	// Each skill is a markdown document mounted at /skills/<name>.md
	// +optional
	GeneratedSkills []GeneratedSkill `json:"generatedSkills,omitempty"`

	// env defines additional environment variables for the knight container.
	// +optional
	Env []corev1.EnvVar `json:"env,omitempty"`

	// mcpServers declares remote MCP servers the knight uses. Rendered only
	// for claude-knight images (into MCP_SERVERS_JSON and
	// CLAUDE_DISALLOWED_TOOLS); on any other runtime nothing is rendered and
	// the Degraded condition is set with reason MCPServersUnsupported.
	// Prefer a CLI in the tools image; use MCP for services with no CLI.
	// +listType=map
	// +listMapKey=name
	// +optional
	MCPServers []KnightMCPServer `json:"mcpServers,omitempty"`

	// envFrom defines sources of environment variables (secrets, configmaps).
	// +optional
	EnvFrom []corev1.EnvFromSource `json:"envFrom,omitempty"`

	// arsenals is the single ordered list of skill sources merged into /skills.
	// The first entry is the base; later entries override it per skill:
	// /skills/<category>/<skill> links to the LAST arsenal that provides that
	// skill. Each git entry runs its own git-sync-<name> sidecar. When empty
	// (and no RoundTable default applies) it defaults to one entry
	// {name: platform, repo: https://github.com/roundtable-k8s/arsenal}.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=8
	Arsenals []KnightArsenal `json:"arsenals,omitempty"`

	// workspace configures the knight's persistent workspace.
	// +optional
	Workspace *KnightWorkspace `json:"workspace,omitempty"`

	// memory gives the knight a persistent memory volume, separate from the
	// /data workspace, that every replica mounts read-write at
	// /memory/<knight> (KNIGHT_MEMORY_DIR). Unset means no memory volume.
	// +optional
	Memory *KnightMemory `json:"memory,omitempty"`

	// lifecycle controls suspend/resume behavior.
	// +optional
	Lifecycle *KnightLifecycle `json:"lifecycle,omitempty"`

	// github configures GitHub credentials for the knight. When
	// github.tokenSecretRef is set it overrides the RoundTable's
	// spec.github.tokenSecretRef.
	// +optional
	GitHub *GitHubConfig `json:"github,omitempty"`

	// suspend, if true, scales the knight to 0 replicas.
	// +kubebuilder:default=false
	// +optional
	Suspend bool `json:"suspend,omitempty"`

	// card overrides fields of the knight's agent card, which the RoundTable
	// controller generates from this spec and keeps in the cards KV bucket
	// (key <table>.<knight>, see docs/agent-cards.md).
	// +optional
	Card *KnightCard `json:"card,omitempty"`
}

// KnightCard holds overrides for a knight's generated agent card.
type KnightCard struct {
	// description is the card description. When unset it is the first
	// paragraph of spec.prompt.identity, else "<Name>, <domain> knight".
	// +kubebuilder:validation:MaxLength=1024
	// +optional
	Description string `json:"description,omitempty"`

	// tags label the knight as a whole (card roundtable.tags).
	// +kubebuilder:validation:MaxItems=32
	// +optional
	Tags []string `json:"tags,omitempty"`

	// skills are merged over the generated skills by id: set fields replace
	// the generated ones, and an id that is not generated adds a skill.
	// +listType=map
	// +listMapKey=id
	// +kubebuilder:validation:MaxItems=64
	// +optional
	Skills []KnightCardSkill `json:"skills,omitempty"`
}

// KnightCardSkill overrides one skill on a knight's agent card.
type KnightCardSkill struct {
	// id is the skill id: the skill name a running knight reports, or a
	// spec.skills category before the knight has run.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	ID string `json:"id"`

	// description replaces the skill description.
	// +kubebuilder:validation:MaxLength=1024
	// +optional
	Description string `json:"description,omitempty"`

	// tags replace the skill tags.
	// +optional
	Tags []string `json:"tags,omitempty"`

	// examples replace the skill examples (sample prompts).
	// +optional
	Examples []string `json:"examples,omitempty"`
}

// KnightArsenal configures one skill arsenal: either a git-sync sidecar (the
// default) or, with oci set, a read-only OCI image volume.
// +kubebuilder:validation:XValidation:rule="!has(self.oci) || !has(self.repo)",message="oci and repo are mutually exclusive"
type KnightArsenal struct {
	// name identifies this arsenal: it becomes the sync path /arsenal/<name>
	// and the sidecar name git-sync-<name>. Defaults to the repo's basename;
	// a name already taken by an earlier arsenal gets a "-2", "-3"... suffix.
	// +kubebuilder:validation:MaxLength=40
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +optional
	Name string `json:"name,omitempty"`

	// repo is the git repository URL containing skills. Defaults to
	// https://github.com/roundtable-k8s/arsenal (applied by the operator, so
	// it stays unset for an oci arsenal); mutually exclusive with oci.
	// +optional
	Repo string `json:"repo,omitempty"`

	// ref is the git ref to sync.
	// +kubebuilder:default="main"
	// +optional
	Ref string `json:"ref,omitempty"`

	// periodSeconds is how often git-sync pulls the repo, in seconds.
	// +kubebuilder:default=300
	// +kubebuilder:validation:Minimum=1
	// +optional
	PeriodSeconds int32 `json:"periodSeconds,omitempty"`

	// image overrides the git-sync container image.
	// +kubebuilder:default="registry.k8s.io/git-sync/git-sync:v4.4.0"
	// +optional
	Image string `json:"image,omitempty"`

	// oci loads the arsenal from an OCI image mounted read-only as an image
	// volume at /arsenal/<name> instead of syncing a git repo. No git-sync
	// sidecar is added; repo, ref, periodSeconds and image are ignored.
	// +optional
	OCI *KnightArsenalOCI `json:"oci,omitempty"`
}

// KnightArsenalOCI references an arsenal published as an OCI image.
type KnightArsenalOCI struct {
	// reference is the image reference, pinned by digest
	// (e.g. registry.example.com/arsenal@sha256:...).
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:Pattern=`@sha256:[a-f0-9]{64}$`
	Reference string `json:"reference"`

	// pullPolicy is the image volume pull policy.
	// +kubebuilder:validation:Enum=Always;IfNotPresent;Never
	// +kubebuilder:default=IfNotPresent
	// +optional
	PullPolicy corev1.PullPolicy `json:"pullPolicy,omitempty"`
}

// Workspace modes for KnightWorkspace.Mode.
const (
	// WorkspaceModePVC mounts one ReadWriteOnce PVC at /data (default).
	WorkspaceModePVC = "pvc"
	// WorkspaceModeEphemeral mounts a per-pod emptyDir at /data, sized by
	// workspace.size. Its contents are lost when the pod goes away.
	WorkspaceModeEphemeral = "ephemeral"
)

// KnightWorkspace configures the knight's workspace storage at /data.
// +kubebuilder:validation:XValidation:rule="!has(self.fromSnapshot) || !has(self.existingClaim)",message="workspace.fromSnapshot cannot be combined with workspace.existingClaim"
// +kubebuilder:validation:XValidation:rule="!has(self.fromSnapshot) || !has(self.mode) || self.mode != 'ephemeral'",message="workspace.fromSnapshot cannot be combined with workspace.mode: ephemeral"
type KnightWorkspace struct {
	// mode selects the workspace storage.
	// "pvc" (default) uses one persistent ReadWriteOnce volume, so the knight
	// runs a single pod.
	// "ephemeral" gives every pod its own emptyDir (size limit: size), which
	// lets the knight run several replicas. Anything the runtime keeps under
	// /data, including its memory files, is lost when the pod is replaced.
	// +kubebuilder:validation:Enum=pvc;ephemeral
	// +kubebuilder:default="pvc"
	// +optional
	Mode string `json:"mode,omitempty"`

	// existingClaim references an existing PVC to use instead of creating a new one.
	// Useful for migrating existing knights to operator management.
	// With more than one replica the claim must be ReadWriteMany; the operator
	// cannot check access modes, so a ReadWriteOnce claim leaves a stuck pod.
	// +optional
	ExistingClaim string `json:"existingClaim,omitempty"`

	// size is the storage request for auto-created PVCs, or the emptyDir
	// size limit in ephemeral mode.
	// +kubebuilder:default="1Gi"
	// +optional
	Size *resource.Quantity `json:"size,omitempty"`

	// fromSnapshot is the name of a VolumeSnapshot in the knight's namespace to
	// create the workspace PVC from (restore or fork). It only applies when the
	// PVC is created: an existing PVC is left alone. The PVC is not created
	// until the snapshot exists and is readyToUse. The PVC size is the larger
	// of size and the snapshot's restoreSize. Not allowed with existingClaim
	// or mode: ephemeral (an empty existingClaim set alongside it is rejected
	// too). Not inherited from RoundTable defaults. Needs the
	// snapshot.storage.k8s.io/v1 API.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	FromSnapshot string `json:"fromSnapshot,omitempty"`

	// snapshots configures on-demand CSI VolumeSnapshots of the workspace PVC
	// (mode: pvc only). A snapshot is taken when the ai.roundtable.io/snapshot
	// annotation gets a new value. Needs the snapshot.storage.k8s.io/v1 API.
	// +optional
	Snapshots *WorkspaceSnapshots `json:"snapshots,omitempty"`
}

// AnnotationKnightSnapshot, set on a Knight to a value not seen before, takes
// a VolumeSnapshot of the knight's workspace PVC. The value is arbitrary.
const AnnotationKnightSnapshot = "ai.roundtable.io/snapshot"

// WorkspaceSnapshots configures workspace VolumeSnapshots.
type WorkspaceSnapshots struct {
	// className is the VolumeSnapshotClass. Empty uses the cluster default.
	// +optional
	ClassName string `json:"className,omitempty"`

	// keep is how many snapshots of this knight to retain; once the newest is
	// ready, the oldest beyond keep are deleted.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	// +optional
	Keep *int32 `json:"keep,omitempty"`
}

// KnightWorkspaceStatus reports the state of the workspace.
type KnightWorkspaceStatus struct {
	// lastSnapshotRequest is the last value of the ai.roundtable.io/snapshot
	// annotation that was handled.
	// +optional
	LastSnapshotRequest string `json:"lastSnapshotRequest,omitempty"`

	// snapshots lists the knight's workspace VolumeSnapshots, oldest first.
	// +listType=atomic
	// +optional
	Snapshots []WorkspaceSnapshotStatus `json:"snapshots,omitempty"`
}

// WorkspaceSnapshotStatus is one workspace VolumeSnapshot.
type WorkspaceSnapshotStatus struct {
	// name is the VolumeSnapshot name.
	Name string `json:"name"`

	// createdAt is when the VolumeSnapshot object was created.
	// +optional
	CreatedAt *metav1.Time `json:"createdAt,omitempty"`

	// readyToUse mirrors the VolumeSnapshot's status.readyToUse.
	// +optional
	ReadyToUse bool `json:"readyToUse,omitempty"`
}

// KnightMemory configures the knight's persistent memory volume.
//
// The layout is the same with or without existingClaim: the knight's own
// memory is read-write at /memory/<knight> (KNIGHT_MEMORY_DIR) and /memory
// (KNIGHT_MEMORY_ROOT) is its parent. Without existingClaim the operator
// creates the ReadWriteMany PVC <knight>-memory, owned by the Knight and
// deleted with it, and mounts it at /memory/<knight>. With existingClaim the
// whole claim is mounted read-only at /memory (every knight's memory) and the
// claim's <knight> sub-directory read-write at /memory/<knight>; the operator
// never creates or deletes that claim.
type KnightMemory struct {
	// size is the storage request of the PVC the operator creates.
	// Defaults to 1Gi. Ignored with existingClaim.
	// +optional
	Size *resource.Quantity `json:"size,omitempty"`

	// storageClassName is the StorageClass of the PVC the operator creates;
	// empty uses the cluster default. It must support ReadWriteMany when the
	// knight runs more than one pod. Ignored with existingClaim.
	// +optional
	StorageClassName string `json:"storageClassName,omitempty"`

	// existingClaim is a ReadWriteMany PVC shared by several knights (for
	// example a whole table) instead of a per-knight claim. Each knight writes
	// only its own <knight> sub-directory and reads everyone else's.
	// +optional
	ExistingClaim string `json:"existingClaim,omitempty"`
}

// KnightLifecycle controls suspend/resume behavior for the knight.
type KnightLifecycle struct {
	// suspendPolicy controls when the knight is suspended.
	// Values: auto, manual, never (default: never).
	// auto makes the operator render a KEDA ScaledObject that scales the
	// knight to 0 when its consumer has no pending tasks and back up when
	// tasks arrive (requires KEDA).
	// +kubebuilder:validation:Enum=auto;manual;never
	// +kubebuilder:default="never"
	// +optional
	SuspendPolicy string `json:"suspendPolicy,omitempty"`

	// idleTimeoutSeconds is how long, in seconds, the knight stays up after
	// its last pending task before it is scaled down. It becomes the KEDA
	// ScaledObject cooldownPeriod (default 300). Only used when the knight is
	// autoscaled (suspendPolicy auto, or maxReplicas above 1).
	// +kubebuilder:validation:Minimum=1
	// +optional
	IdleTimeoutSeconds int32 `json:"idleTimeoutSeconds,omitempty"`

	// scaleDownStabilizationSeconds is the HPA scale-down stabilization
	// window: the autoscaler only scales in to the highest replica count
	// recommended during this many seconds, so a brief dip in load does not
	// remove a pod that is still running tasks. It becomes the ScaledObject's
	// advanced.horizontalPodAutoscalerConfig.behavior.scaleDown.
	// stabilizationWindowSeconds. Defaults to 600; 0 disables stabilization.
	// Only used when the knight is autoscaled.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=3600
	// +optional
	ScaleDownStabilizationSeconds *int32 `json:"scaleDownStabilizationSeconds,omitempty"`
}

// GitHubConfig configures GitHub credentials for knights.
type GitHubConfig struct {
	// tokenSecretRef selects the key of a Secret (in the knight's namespace)
	// holding a GitHub token, for example a GitHub App installation token that
	// an external controller refreshes. The operator mounts it read-only into
	// the knight at /var/run/secrets/roundtable/github/token and sets
	// GITHUB_TOKEN_FILE to that path. Because it is a volume, not an env var,
	// the kubelet refreshes the file in place when the Secret changes. With
	// optional: true the pod starts even if the Secret or key is missing.
	// +optional
	TokenSecretRef *corev1.SecretKeySelector `json:"tokenSecretRef,omitempty"`
}

// KnightCapabilities defines optional runtime capabilities for the knight pod.
type KnightCapabilities struct {
	// browser enables a headless Chrome sidecar with agent-browser CLI for web automation.
	// When true, the operator injects a browser sidecar and sets BROWSER_ENABLED=true.
	// +optional
	Browser bool `json:"browser,omitempty"`
}

// Values of KnightIsolation.Runtime and KnightIsolation.Egress.
const (
	IsolationDefault      = "default"
	IsolationRuntimeNone  = "none"
	IsolationUnrestricted = "unrestricted"
)

// KnightIsolation is a knight's explicit opt-out of the isolation defaults.
// The reason rule is a Knight.spec rule (not on this type) so it costs nothing
// where KnightSpec is nested in Mission and RoundTable lists; a mission
// template without a reason is rejected when its Knight is created.
type KnightIsolation struct {
	// runtime is "default" (run under the RuntimeClass from spec.runtimeClassName,
	// the table default or the operator default) or "none" (run without a
	// sandboxing RuntimeClass, even if one is set elsewhere).
	// +kubebuilder:validation:Enum=default;none
	// +kubebuilder:validation:MaxLength=16
	// +optional
	Runtime string `json:"runtime,omitempty"`

	// egress is "default" (spec.egress, the table default or an empty
	// allowlist, on top of the baseline) or "unrestricted" (no egress policy).
	// +kubebuilder:validation:Enum=default;unrestricted
	// +kubebuilder:validation:MaxLength=16
	// +optional
	Egress string `json:"egress,omitempty"`

	// reason says why the knight opts out (for example "needs ptrace for the
	// debugger skill"). Required, at least 10 characters, when runtime or
	// egress is not "default". It is shown in the Isolated condition.
	// +kubebuilder:validation:MinLength=10
	// +kubebuilder:validation:MaxLength=512
	// +optional
	Reason string `json:"reason,omitempty"`
}

// RuntimeOptedOut reports whether the knight opts out of the sandboxing
// RuntimeClass (isolation.runtime: none).
func (i *KnightIsolation) RuntimeOptedOut() bool {
	return i != nil && i.Runtime == IsolationRuntimeNone
}

// EgressOptedOut reports whether the knight opts out of the egress policy
// (isolation.egress: unrestricted).
func (i *KnightIsolation) EgressOptedOut() bool {
	return i != nil && i.Egress == IsolationUnrestricted
}

// KnightEgress is a knight's egress allowlist.
type KnightEgress struct {
	// allowedHosts are the hostnames the knight may reach, each an exact host
	// ("api.anthropic.com") or a "*." wildcard prefix ("*.github.com", which
	// matches subdomains of github.com but not github.com itself). Empty
	// allows only DNS and NATS.
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:MaxLength=253
	// +kubebuilder:validation:items:Pattern=`^(\*\.)?([a-z0-9]([-a-z0-9]*[a-z0-9])?\.)*[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +listType=set
	// +optional
	AllowedHosts []string `json:"allowedHosts,omitempty"`

	// allowedPorts are the TCP ports allowed to the allowed hosts.
	// +kubebuilder:default={443}
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:Minimum=1
	// +kubebuilder:validation:items:Maximum=65535
	// +listType=set
	// +optional
	AllowedPorts []int32 `json:"allowedPorts,omitempty"`

	// allowedServices are in-cluster Services the knight may reach, each on
	// its own ports. A "*.svc.cluster.local" name under allowedHosts does not
	// open a Service: FQDN rules only match addresses outside the cluster.
	// The policy selects the Service's pods (its namespace and pod selector),
	// so a Service without a selector, or one that does not exist, is left out
	// and reported on the EgressRestricted condition.
	// +kubebuilder:validation:MaxItems=32
	// +listType=atomic
	// +optional
	AllowedServices []EgressService `json:"allowedServices,omitempty"`
}

// EgressService is an in-cluster Service a knight may reach on the listed ports.
type EgressService struct {
	// name is the Service's name.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z]([-a-z0-9]*[a-z0-9])?$`
	// +required
	Name string `json:"name"`

	// namespace is the Service's namespace. Empty means the knight's namespace.
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +optional
	Namespace string `json:"namespace,omitempty"`

	// ports are the Service ports the knight dials. The policy allows each
	// port's targetPort on the Service's pods (policies apply after the
	// Service address is translated to a pod); a port the Service does not
	// expose is allowed as a pod port as-is.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=8
	// +kubebuilder:validation:items:Minimum=1
	// +kubebuilder:validation:items:Maximum=65535
	// +listType=set
	// +required
	Ports []int32 `json:"ports"`
}

// KnightTools defines system-level tools the knight needs installed.
type KnightTools struct {
	// nix is a list of nixpkgs packages (e.g., "nmap", "whois", "dnsutils").
	// The operator builds each unique list once into an OCI tools image and
	// the knight mounts it read-only at /nix. Requires the operator to be
	// configured with a tools image repository.
	// +optional
	Nix []string `json:"nix,omitempty"`

	// flakes adds packages from Nix flakes outside nixpkgs to the same tools
	// image as nix. Each ref must be pinned to a commit, because the image is
	// rebuilt only when the spec changes; a branch name would never refresh.
	// git+ssh refs need matching SSH credentials on the tools builder Job.
	// +kubebuilder:validation:MaxItems=16
	// +listType=atomic
	// +optional
	Flakes []KnightToolsFlake `json:"flakes,omitempty"`

	// apt is a list of apt packages to install (fallback, requires root — prefer nix).
	// +optional
	Apt []string `json:"apt,omitempty"`

	// mise is a list of tools to install via mise (e.g., "shodan", "kubectl").
	// +optional
	Mise []string `json:"mise,omitempty"`
}

// KnightToolsFlake selects packages from one Nix flake for the tools image.
type KnightToolsFlake struct {
	// ref is a flake reference pinned to a commit, e.g.
	// github:roundtable-k8s/roundtable/<40-hex-rev> or
	// git+https://host/repo.git?rev=<40-hex-rev>. Branch names and tags are
	// rejected.
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:Pattern=`^((github|gitlab|sourcehut):[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[0-9a-f]{40}(\?dir=[A-Za-z0-9_./-]+)?|git\+(https|ssh)://[A-Za-z0-9_.@:/-]+\?([A-Za-z0-9_.=/-]+&)*rev=[0-9a-f]{40}(&[A-Za-z0-9_.=/-]+)*)$`
	Ref string `json:"ref"`

	// packages are output names under the flake's packages.<system> (x86_64-linux).
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:Pattern=`^[A-Za-z0-9_][A-Za-z0-9_.+-]*$`
	// +kubebuilder:validation:items:MaxLength=128
	// +listType=atomic
	Packages []string `json:"packages"`
}

// KnightNATS configures the knight's NATS JetStream connection and consumers.
type KnightNATS struct {
	// url is the NATS server URL.
	// +kubebuilder:default="nats://nats.database.svc:4222"
	// +optional
	URL string `json:"url,omitempty"`

	// subjects defines the JetStream filter subjects for task consumption.
	// e.g., ["fleet-a.tasks.security.>"]
	// +kubebuilder:validation:MinItems=1
	Subjects []string `json:"subjects"`

	// stream is the JetStream stream name to consume from.
	// +kubebuilder:default="fleet_a_tasks"
	// +optional
	Stream string `json:"stream,omitempty"`

	// resultsStream is the JetStream stream to publish results to.
	// +kubebuilder:default="fleet_a_results"
	// +optional
	ResultsStream string `json:"resultsStream,omitempty"`

	// consumerName overrides the auto-generated durable consumer name.
	// Defaults to "knight-{name}".
	// +optional
	ConsumerName string `json:"consumerName,omitempty"`

	// maxDeliver is the maximum number of delivery attempts per message.
	//
	// Deprecated: ignored. Knights always run a task at most once; lost and
	// failed tasks go to the dead-letter stream and are retried by hand.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxDeliver int32 `json:"maxDeliver,omitempty"`
}

// KnightVault configures the shared Obsidian vault mount.
type KnightVault struct {
	// claimName is the PVC name for the shared vault.
	// +kubebuilder:default="obsidian-vault"
	// +optional
	ClaimName string `json:"claimName,omitempty"`

	// readOnly mounts the base vault as read-only. Unset means true.
	// +optional
	ReadOnly *bool `json:"readOnly,omitempty"`

	// writablePaths are subpaths within the vault the knight can write to.
	// There is no default: an absent or empty list means no writable paths.
	// +optional
	WritablePaths []string `json:"writablePaths,omitempty"`
}

// KnightPrompt allows overriding system prompt components.
type KnightPrompt struct {
	// identity overrides the knight's identity/persona description.
	// +optional
	Identity string `json:"identity,omitempty"`

	// instructions provides additional instructions appended to the system prompt.
	// +optional
	Instructions string `json:"instructions,omitempty"`

	// configMapRef references a ConfigMap containing prompt overrides.
	// Keys: "AGENTS.md", "TOOLS.md", "SOUL.md"
	// +optional
	ConfigMapRef *corev1.LocalObjectReference `json:"configMapRef,omitempty"`
}

// KnightPhase represents the current lifecycle phase of the Knight.
// +kubebuilder:validation:Enum=Pending;Provisioning;Ready;Degraded;Suspended
type KnightPhase string

const (
	KnightPhasePending      KnightPhase = "Pending"
	KnightPhaseProvisioning KnightPhase = "Provisioning"
	KnightPhaseReady        KnightPhase = "Ready"
	KnightPhaseDegraded     KnightPhase = "Degraded"
	KnightPhaseSuspended    KnightPhase = "Suspended"
)

// KnightScalingStatus is the knight's replica bounds and scaling state.
type KnightScalingStatus struct {
	// autoscaled is true when a KEDA ScaledObject owns the replica count.
	Autoscaled bool `json:"autoscaled"`

	// minReplicas is the fixed replica count, or the KEDA minReplicaCount.
	MinReplicas int32 `json:"minReplicas"`

	// maxReplicas is the KEDA maxReplicaCount (after the plan-task cap); equal
	// to minReplicas when not autoscaled.
	MaxReplicas int32 `json:"maxReplicas"`

	// desiredReplicas is the replica count the autoscaler (or the Deployment
	// spec when not autoscaled) is asking for.
	// +optional
	DesiredReplicas *int32 `json:"desiredReplicas,omitempty"`

	// lastScaleTime is when the autoscaler last changed the replica count.
	// +optional
	LastScaleTime *metav1.Time `json:"lastScaleTime,omitempty"`
}

// KnightStatus defines the observed state of Knight.
type KnightStatus struct {
	// phase is the current lifecycle phase of the knight.
	// +optional
	Phase KnightPhase `json:"phase,omitempty"`

	// ready indicates whether the knight is ready to accept tasks.
	// +optional
	Ready bool `json:"ready,omitempty"`

	// tasksCompleted is the total number of tasks completed since creation.
	// +optional
	TasksCompleted int64 `json:"tasksCompleted,omitempty"`

	// tasksFailed is the total number of tasks that failed.
	// +optional
	TasksFailed int64 `json:"tasksFailed,omitempty"`

	// lastTaskAt is the timestamp of the last completed task.
	// +optional
	LastTaskAt *metav1.Time `json:"lastTaskAt,omitempty"`

	// totalCost is the cumulative cost in USD of all tasks processed.
	// +optional
	TotalCost string `json:"totalCost,omitempty"`

	// model is the effective model the knight runs: spec.model, else the
	// RoundTable's spec.defaults.model, else the operator default.
	// +optional
	Model string `json:"model,omitempty"`

	// natsConsumer is the name of the reconciled NATS durable consumer.
	// +optional
	NATSConsumer string `json:"natsConsumer,omitempty"`

	// toolsImage is the tools image reference that has been built and pushed
	// for this knight's current Nix tool list. Empty until the build Job
	// succeeds; used to avoid rebuilding unchanged tool lists and as the
	// image the knight's pod mounts.
	// +optional
	ToolsImage string `json:"toolsImage,omitempty"`

	// replicas is the number of knight pods the Deployment currently has.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// readyReplicas is the number of knight pods that are ready.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// scaling reports the knight's replica bounds and current scaling state,
	// so clients need not read KEDA objects.
	// +optional
	Scaling *KnightScalingStatus `json:"scaling,omitempty"`

	// workspace reports workspace snapshots.
	// +optional
	Workspace *KnightWorkspaceStatus `json:"workspace,omitempty"`

	// observedGeneration is the most recent generation observed by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// conditions represent the current state of the Knight resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=kn,categories=roundtable
// +kubebuilder:printcolumn:name="Domain",type=string,JSONPath=`.spec.domain`
// +kubebuilder:printcolumn:name="Model",type=string,JSONPath=`.status.model`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Ready",type=boolean,JSONPath=`.status.ready`
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.status.readyReplicas`,priority=1
// +kubebuilder:printcolumn:name="Isolated",type=string,JSONPath=`.status.conditions[?(@.type=="Isolated")].status`
// +kubebuilder:printcolumn:name="Tasks",type=integer,JSONPath=`.status.tasksCompleted`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Knight is the Schema for the knights API.
// A Knight represents a specialized AI agent in the Round Table,
// deployed as a Kubernetes workload with NATS JetStream connectivity,
// skill injection, and shared vault access.
type Knight struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Knight
	// +required
	// +kubebuilder:validation:XValidation:rule="!has(self.maxReplicas) || !has(self.replicas) || self.maxReplicas >= self.replicas",message="maxReplicas must be greater than or equal to replicas"
	// +kubebuilder:validation:XValidation:rule="((has(self.replicas) ? self.replicas : 1) <= 1 && (has(self.maxReplicas) ? self.maxReplicas : 1) <= 1) || (has(self.workspace) && ((has(self.workspace.mode) && self.workspace.mode == 'ephemeral') || (has(self.workspace.existingClaim) && size(self.workspace.existingClaim) > 0)))",message="replicas or maxReplicas above 1 need workspace.mode: ephemeral (or a ReadWriteMany workspace.existingClaim); the default workspace PVC is ReadWriteOnce"
	// +kubebuilder:validation:XValidation:rule="!has(self.workspace) || !has(self.workspace.mode) || self.workspace.mode != 'ephemeral' || !has(self.workspace.existingClaim) || size(self.workspace.existingClaim) == 0",message="workspace.existingClaim cannot be combined with workspace.mode: ephemeral"
	// +kubebuilder:validation:XValidation:rule="!has(self.isolation) || has(self.isolation.reason) || ((!has(self.isolation.runtime) || self.isolation.runtime == 'default') && (!has(self.isolation.egress) || self.isolation.egress == 'default'))",message="isolation.reason (at least 10 characters) is required when isolation.runtime or isolation.egress is not default"
	// +kubebuilder:validation:XValidation:rule="!has(self.runtime) || self.runtime != 'sandbox' || ((has(self.replicas) ? self.replicas : 1) <= 1 && (has(self.maxReplicas) ? self.maxReplicas : 1) <= 1)",message="runtime: sandbox runs a single pod; replicas and maxReplicas must be 1"
	Spec KnightSpec `json:"spec"`

	// status defines the observed state of Knight
	// +optional
	Status KnightStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// KnightList contains a list of Knight
type KnightList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Knight `json:"items"`
}

// GeneratedSkill is an inline skill definition created by the planner.
// Each skill is a markdown document that gets mounted into the knight's /skills/ directory.
type GeneratedSkill struct {
	// name is the skill filename (without .md extension).
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// content is the full markdown skill document.
	// +kubebuilder:validation:Required
	Content string `json:"content"`
}

func init() {
	SchemeBuilder.Register(&Knight{}, &KnightList{})
}

// KnightMCPServer is a remote MCP server wired into a claude-knight pod.
type KnightMCPServer struct {
	// name is the server name; tools are exposed as mcp__<name>__<tool>.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Name string `json:"name"`

	// url is the server endpoint.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	URL string `json:"url"`

	// transport is the MCP transport.
	// +kubebuilder:validation:Enum=streamable-http;sse
	// +kubebuilder:default="streamable-http"
	// +optional
	Transport string `json:"transport,omitempty"`

	// toolFilter restricts the tools the knight may call.
	// +optional
	ToolFilter *KnightMCPToolFilter `json:"toolFilter,omitempty"`

	// headersFrom adds HTTP headers (typically Authorization) whose values
	// come from a Secret key in the knight's namespace. Each becomes an env
	// var on the knight container (MCP_<SERVER>_<HEADER>, valueFrom copied
	// from here) and the rendered MCP config holds only a ${VAR}
	// placeholder, so the value is never written into a ConfigMap or status.
	// Header names are unique per server; a later entry that differs from an
	// earlier one only in case is dropped and reported in MCPHeadersReady.
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=16
	// +optional
	HeadersFrom []KnightMCPHeader `json:"headersFrom,omitempty"`
}

// KnightMCPHeader is an HTTP header sent to an MCP server, with its value
// taken from a Secret key.
type KnightMCPHeader struct {
	// name is the HTTP header name (RFC 9110 token characters), e.g.
	// Authorization or X-Api-Key.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:Pattern="^[A-Za-z0-9!#$%&*+.^_|~-]+$"
	Name string `json:"name"`

	// valueFrom is the source of the header value.
	// +kubebuilder:validation:Required
	ValueFrom KnightMCPHeaderSource `json:"valueFrom"`
}

// KnightMCPHeaderSource selects the value of an MCP header.
type KnightMCPHeaderSource struct {
	// secretKeyRef selects a key of a Secret in the knight's namespace
	// (there is no namespace field). The whole key value is the header
	// value, so include any scheme, e.g. "Bearer <token>".
	// +kubebuilder:validation:Required
	SecretKeyRef KnightSecretKeyRef `json:"secretKeyRef"`
}

// KnightSecretKeyRef names a key of a Secret in the knight's namespace.
type KnightSecretKeyRef struct {
	// name is the Secret name.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// key is the key within the Secret.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[-._a-zA-Z0-9]+$`
	Key string `json:"key"`
}

// KnightMCPToolFilter restricts a server's tools.
type KnightMCPToolFilter struct {
	// exclude lists tool names denied for this server (a deny list, because
	// the Claude Code harness exposes a bound server whole).
	// +optional
	Exclude []string `json:"exclude,omitempty"`
}
