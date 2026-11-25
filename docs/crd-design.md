# Round Table Operator — CRD Design Document

## 1. High-Level Architecture

```
┌─────────────────────────────────────────────────────┐
│                    RoundTable                        │
│  (fleet-level config, policies, NATS infrastructure) │
│                                                      │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐            │
│  │ Knight   │ │ Knight   │ │ Knight   │  ...        │
│  │ (gawain) │ │ (mordred)│ │ (lancelot│            │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘            │
│       │             │             │                   │
│  ┌────┴─────────────┴─────────────┴──────┐          │
│  │         NATS JetStream                 │          │
│  │  {prefix}.tasks.{domain}.{knight}      │          │
│  │  {prefix}.results.{domain}.{knight}    │          │
│  └───────────────────────────────────────┘          │
│                                                      │
│  ┌──────────────┐    ┌──────────────────┐           │
│  │    Chain      │    │     Mission       │           │
│  │  (pipelines)  │    │ (ephemeral teams) │           │
│  │  step→step    │    │ knights + chains  │           │
│  └──────────────┘    └──────────────────┘           │
└─────────────────────────────────────────────────────┘
```

### Resource Relationships

- **RoundTable** → owns many **Knights** (via label selector)
- **RoundTable** → provides NATS config and policies inherited by Knights
- **Chain** → references Knights by name, orchestrates task pipelines
- **Mission** → references Knights (existing or ephemeral) and Chains
- **Mission** → creates ephemeral Knights and temporary NATS subjects
- **Chain** can optionally reference a **RoundTable** for NATS config
- **Trigger** → starts copies of a **Chain** (owned by the Trigger) from a NATS event on the RoundTable's events stream, a webhook delivered through the gateway, a cron schedule, or a GitHub issue or pull request matching a label query

### Ownership Hierarchy

```
RoundTable (optional, top-level)
├── Knight (core unit, can exist independently)
├── Chain (references knights, publishes to NATS)
└── Mission (ephemeral grouping)
    ├── ephemeral Knights (owned, cleaned up)
    └── Chain executions (scoped to mission)
```

## 2. Go Type Definitions

All types are defined in `api/v1alpha1/` following the patterns established by `knight_types.go`:
- Apache 2.0 license header
- kubebuilder markers for validation, defaults, and print columns
- Phase enums with `+kubebuilder:validation:Enum`
- Standard status pattern: phase, observedGeneration, conditions
- `omitzero` on ObjectMeta and Status, `omitempty` on optional fields

### Files

| CRD | File | Spec Type | Status Type |
|-----|------|-----------|-------------|
| Knight | `knight_types.go` | `KnightSpec` | `KnightStatus` |
| Chain | `chain_types.go` | `ChainSpec` | `ChainStatus` |
| Mission | `mission_types.go` | `MissionSpec` | `MissionStatus` |
| RoundTable | `roundtable_types.go` | `RoundTableSpec` | `RoundTableStatus` |
| Trigger | `trigger_types.go` | `TriggerSpec` | `TriggerStatus` |

See the actual Go files for complete type definitions. Key design decisions:

**Chain:** Steps use `dependsOn` for DAG-style dependencies rather than simple ordering. This enables parallel fan-out (multiple steps with no dependencies run concurrently) and fan-in (step depends on multiple prior steps). Step outputs are accessible via Go templates in downstream step tasks.

A step with `forEach` fans out over a JSON list taken from `spec.input` or a prior step's output (`from: input | steps.<name>`, optional JSONPath `path`, `maxItems` default 50, optional `uniqueBy: <key>` keeping only the first item per value of that key). Each item is its own task (own taskId, runId `<runId>-i<index>`) rendered with `{{ .Item }}` / `{{ .ItemJSON }}` / `{{ .ItemIndex }}`; `maxParallel` caps items in flight (default: the knight's concurrency); a step `model` override applies to every item. When all items are terminal the step output becomes the join — a JSON array of `{index, item, phase, output, error}` — so a later step (or another `forEach` over `steps.<name>`) consumes per-item results. `failurePolicy: failFast` (default) stops dispatching after the first failed item and fails the step; `continue` runs every item and the chain ends `PartiallySucceeded` if any failed. Status is bounded: `status.stepStatuses[].fanOut` keeps one phase character per item plus counts, and detailed entries for in-flight items and at most 50 finished ones (the rest are read back from the `chain-outputs` KV bucket at join time). `ai.roundtable.io/retry-failed` re-runs only failed/skipped items. See `examples/chain-fan-out.yaml`.

A step's `when` is a CEL expression over prior steps (`steps.<name>.{phase,output,error,reason,outcome,iterations}`, `input`, plus `json()` and the extended string functions). It is evaluated when the step's dependencies are done. When it is false the step is `Skipped` (`reason: WhenFalse`) without dispatching anything, and its dependents still run. A step with `loop` is a bounded loop. Its 1–5 body steps (each with its own `knightRef`, `task`, `model`, `timeoutSeconds`, `retry`) run in order, round after round. The CEL `until` expression is checked against `last` (the `from` step on entry, then the last body step after each round). The loop ends `Converged`, `Exhausted` after `maxIterations` rounds (the step fails, or with `onExhausted: continue` succeeds so dependents can branch on `steps.<loop>.outcome`), or `Failed`. Body tasks reuse the chain `runId`, so a coder resumes its session. Status is bounded: `status.stepStatuses[].loop` holds the round counter, the outcome and one latest-round entry per body step. `retry-failed` resumes a failed loop at the failed body step and gives an exhausted one another `maxIterations` rounds. See `docs/conditional-steps.md` and `examples/chain-pr-pipeline.yaml`.

Any Chain's active run can be cancelled with the `ai.roundtable.io/cancel` annotation (the value is the reason):

```bash
kubectl annotate chain <name> -n <ns> ai.roundtable.io/cancel="wrong input" --overwrite
```

Every Pending/Running step becomes `Failed` with `error: "cancelled: <reason>"`, the chain ends `Failed` with condition `Cancelled=True` and a `RunCancelled` event, and no further steps, loop rounds or fan-out items start. The controller then removes the annotation. A chain with no active run ignores it. A task a knight already picked up is not recalled; its result is ignored.

**Mission:** Knights can be `ephemeral: true` with an inline `ephemeralSpec` (reusing `KnightSpec`), allowing missions to spin up purpose-built agents. The mission lifecycle (Assembling → Briefing → Active → Succeeded/Failed → CleaningUp) maps to real-world round table semantics.

**RoundTable:** Uses a label selector (`knightSelector`) rather than explicit knight lists, following the Kubernetes pattern (like Deployments select Pods). Provides fleet-wide defaults that individual knight specs can override, plus cost budgets and concurrency policies.

## 3. Controller Behavior

### Chain Controller

**Reconciliation Loop:**

1. **Validate** — Ensure all `knightRef` values resolve to existing Knights. Validate DAG has no cycles.
2. **Schedule Check** — If `schedule` is set and it's time, create a new run (reset step statuses, set phase=Running).
3. **Step Execution** — For each step in `Pending` phase:
   - Check if all `dependsOn` steps are `Succeeded` (or `Failed` with `continueOnFailure`)
   - If ready, publish task to NATS: `{prefix}.tasks.{knight-domain}.{knight-name}` with chain context
   - Set step phase to `Running`
4. **Monitor** — Watch for results on `{prefix}.results.chain.{chain-name}.{step-name}`
   - On success: set step `Succeeded`, store output
   - On failure: retry per policy, then set `Failed`
   - On timeout: set `Failed`
5. **Complete** — When all steps are terminal, set chain phase to `Succeeded` or `Failed`

**NATS Subjects:**
- Task publish: `{prefix}.tasks.{domain}.{knight}` (reuses existing knight subjects)
- Result subscribe: `{prefix}.results.chain.{chain-name}.{step-name}`
- Chain events: `{prefix}.chains.{chain-name}.events`

**Created Resources:**
- ConfigMap per run: stores step outputs for template rendering
- No Deployments (uses existing Knights)

### Mission Controller

**Reconciliation Loop:**

1. **Assembling** — Create ephemeral Knight CRs (owned by Mission) directly, always cold. Wait for all knights to reach Ready phase.
2. **Briefing** — Publish briefing message to `mission-{name}.briefing` NATS subject. Configure additional NATS consumers on participating knights for mission-scoped subjects.
3. **Active** — Execute setup chains, then active chains. Monitor for objective completion or timeout.
4. **Complete** — Set `Succeeded` or `Failed`. Execute teardown chains.
5. **CleaningUp** — Delete ephemeral Knights, remove mission NATS consumers, clean up ConfigMaps.
6. **TTL Expiry** — After TTL, delete the Mission CR itself (if `cleanupPolicy=Delete`).

**NATS Subjects:**
- Mission briefing: `mission-{name}.briefing`
- Mission tasks: `mission-{name}.tasks.{domain}.{knight}`
- Mission results: `mission-{name}.results.{domain}.{knight}`
- Mission events: `mission-{name}.events`

**Created Resources:**
- Ephemeral Knight CRs (ownerRef → Mission)
- NATS consumers for mission-scoped subjects
- ConfigMap with mission context

**Planner (#566):**

A meta-mission (`spec.metaMission: true`) is planned by one knight, picked in
this order:

1. The mission's own `spec.planner` (`knightRef`, `templateName` or
   `ephemeralSpec`).
2. The planner of the mission's RoundTable (`spec.roundTableRef`): its
   `spec.planner.knightRef`, else the Knight `<table>-planner` the table owns.
   Planning waits (up to the planning timeout) for a table planner that is not
   created yet.
3. Deprecated: a knight labelled `ai.roundtable.io/role=planner` that carries
   no other table's `ai.roundtable.io/round-table` label (the old install-wide
   planner, Helm `planner.enabled=true`). The mission gets a
   `DeprecatedPlanner` warning event.

A mission is never planned by another table's planner. The gateway's
`POST /v1/missions` uses the same order when `planner.knight` is omitted.

Every non-ephemeral RoundTable gets a planner unless `spec.planner.enabled` is
false:

```yaml
spec:
  planner:
    # enabled: true               # default; false deletes <table>-planner
    # knightRef: {name: my-planner}  # use an existing knight, create none
    model: opus                   # else spec.defaults.model, else Helm planner.model
    arsenals: [...]               # else spec.defaults.arsenals, else Helm planner.arsenals
    prompt: {instructions: "..."} # else Helm planner.prompt
    resources: {...}              # else spec.defaults.resources
    runtimeClassName: gvisor      # else spec.defaults.runtimeClassName, else the isolation default
    egress: {...}                 # else spec.defaults.egress, else the isolation default
```

The table-owned planner `<table>-planner` has an owner reference to the table
(deleted with it), the labels `ai.roundtable.io/round-table=<table>` and
`ai.roundtable.io/role=planner`, domain `planner` and the subject
`<prefix>.tasks.planner.>` on the table's tasks and results streams. Like any
knight labelled with the table it inherits the table's `spec.defaults`,
`spec.secrets`, `spec.github` and vault, runs under the isolation defaults and
has an agent card. It uses `lifecycle.suspendPolicy: auto`, so it scales to
zero when idle (with KEDA) and wakes when a planning task is queued; the
mission controller dispatches to a planner that is asleep this way instead of
waiting for it to be Ready. `skills`, `concurrency` and `taskTimeoutSeconds`
come from Helm `planner.*`. `status.plannerKnight` on the table names its
planner.

**Mission knights and the sandbox runtime (#483):**

Each mission knight is created as `<mission>-<knight>` with an owner
reference to the Mission, so deleting the mission deletes the knight. It
carries the annotation `ai.roundtable.io/shutdown-time`, set to the mission's
expiry (`status.expiresAt` = start + `spec.ttlSeconds`). When the knight's
template (or inline `ephemeralSpec`) says `runtime: sandbox`, the knight is one
bare agent-sandbox `Sandbox`, and the Sandbox backend copies the annotation into
`Sandbox.spec.shutdownTime`. agent-sandbox stops the pod at that time even if
the mission's cleanup never runs; the default `shutdownPolicy: Retain` keeps the
Sandbox object until the Knight is deleted. Deployment knights ignore the
annotation.

> **Breaking change (#483):** `RoundTable.spec.warmPool` and
> `RoundTable.status.warmPool` are removed, along with the
> `roundtable_warm_pool_knights` metric and its dashboard panel. The pool never
> reused a running pod: a claim created a new mission knight and deleted the
> warm one. Remove `warmPool` from RoundTable manifests before applying the new
> CRD. Warm starts will come back through upstream agent-sandbox pools
> (`SandboxTemplate` + `SandboxWarmPool`) if the cold-start spike in #368
> supports them.

**Mission results (`spec.retainResults`, default true):**

Before cleanup the controller writes one JSON document to the NATS KV bucket
`mission-results`, under the key `<mission name>`. No ConfigMap is written.
`status.resultsRef` (`bucket`, `key`) says where it is; the deprecated
`status.resultsConfigMap` holds the same location as `<bucket>.<key>` and is
removed in a later release.

The document holds `summary`, `timeline`, per-chain step `chains` outputs,
`knights`, `costs`, and, so planner work can be copied out and reused,
`generatedChains` (the full chain specs) and `generatedSkills`
(`name`, `knight`, `content`).

Entries expire after **30 days** by default. Set the Helm value
`mission.resultsTTL` (a Go duration such as `720h`) to change it; the operator
applies it when it creates the bucket and updates an existing bucket whose TTL
differs. Read a result before it expires:

```bash
# with the NATS CLI (-s <server URL>)
nats kv get mission-results <mission-name> --raw | jq '.generatedChains, .generatedSkills'
```

Find the key with `kubectl get mission <name> -o jsonpath='{.status.resultsRef}'`
(the gateway's mission endpoints and the dashboard report the mission's phase
and result, not the KV location). Archive anything you need beyond the TTL;
the operator does not commit results to git.

### RoundTable Controller

**Reconciliation Loop:**

1. **NATS Setup** — If `createStreams=true`, ensure JetStream streams exist with correct subjects and retention policy.
2. **Knight Discovery** — List Knights matching `knightSelector`. Update status with knight summaries.
3. **Defaults Propagation** — For Knights that don't specify certain fields, the controller does NOT mutate Knight specs. Instead, the Knight controller finds the knight's RoundTable (the `ai.roundtable.io/round-table` label, then the legacy `ai.roundtable.io/table` label) and inherits defaults at reconcile time (#372). Precedence, per field, is **knight > table > operator default**; values are never merged:

   | Knight field | Table field | Operator default |
   |---|---|---|
   | `spec.model` | `spec.defaults.model` | `openrouter/deepseek/deepseek-v3.2` |
   | `spec.image` | `spec.defaults.image` | `DEFAULT_KNIGHT_IMAGE` |
   | `spec.taskTimeoutSeconds` | `spec.defaults.taskTimeoutSeconds` | `120` |
   | `spec.concurrency` | `spec.defaults.concurrency` | `2` |
   | `spec.resources` | `spec.defaults.resources` | none |
   | `spec.workspace` | `spec.defaults.workspace` (never `existingClaim`) | PVC |
   | `spec.memory` | `spec.defaults.memory` (including `existingClaim`) | no memory volume |
   | `spec.arsenals` | `spec.defaults.arsenals` (when the knight sets none) | one `platform` arsenal (`roundtable-k8s/arsenal`) |
   | `spec.runtimeClassName` | `spec.defaults.runtimeClassName` | cluster default |
   | `spec.nodeSelector` | `spec.defaults.nodeSelector` | none |
   | `spec.affinity` | `spec.defaults.affinity` | none |
   | `spec.tolerations` | `spec.defaults.tolerations` | none |
   | `spec.topologySpreadConstraints` | `spec.defaults.topologySpreadConstraints` | none |
   | `spec.priorityClassName` | `spec.defaults.priorityClassName` | none |
   | `spec.vault` | `spec.vault` | no vault |

   The Knight CRD has no schema defaults for these fields, so "unset" means unset. `status.model` (the `Model` column of `kubectl get knight`) shows the effective model. A change to a table's defaults or vault re-reconciles the knights labelled with it, and their pods roll when the rendered pod template changes. Chains and the gateway read knights with the same defaults applied (effective image, concurrency and model).
4. **Policy Enforcement:**
   - `policies.costBudgetUSD`: aggregate costs. If exceeding it, the table goes `OverBudget`, `Available` is set to `False` and a `BudgetExceeded` event is recorded. Knights are not suspended or otherwise limited.
   - `policies.maxConcurrentPlanTasks`: caps plan-backed knights' replicas (see Knight `PlanCapLimited`).
   - `policies.maxMissions`: caps concurrently active missions; extra missions wait in `Pending`.
   - Not enforced (deprecated): `policies.maxConcurrentTasks`, `policies.maxKnights` and `policies.costResetSchedule`. They are still accepted; a standing table that sets them gets a `DeprecatedFieldsUsed=True` condition and a Warning event. The reported cost is cumulative; nothing resets it.
   - To cap the number of knights in a namespace, use a Kubernetes `ResourceQuota`:

     ```yaml
     apiVersion: v1
     kind: ResourceQuota
     metadata:
       name: knight-count
     spec:
       hard:
         count/knights.ai.roundtable.io: "15"
     ```
5. **Health Aggregation** — Count knights as ready, scaled to zero (KEDA, `ScaledToZero` or `Waking`), suspended (`spec.suspend`) or not ready (`status.knightsReady`, `knightsScaledToZero`, `knightsSuspended`, `knightsNotReady`; `kubectl get rt` shows the `Idle` column). Compute phase: Ready (no knight that should run is down; scaled-to-zero and suspended knights count as healthy), Degraded (some not ready), Suspended, OverBudget.
6. **Mission Counting** — Count active Missions referencing this table.

**NATS Subjects:**
- Manages streams: `{subjectPrefix}_tasks` and `{subjectPrefix}_results`
- Fleet events: `{subjectPrefix}.fleet.events`

**Created Resources:**
- JetStream streams (if `createStreams=true`)
- The table's planner Knight `<table>-planner` (ownerRef → RoundTable), unless
  `spec.planner` disables it or names a `knightRef` (#566; see the Mission
  Controller's Planner section)
- Other Knights are not owned (uses selector, like a Service)

### Issue pickup (#224)

A second controller on RoundTables (`issuepickup`) acts only when
`spec.issuePickup` is set. Every `intervalSeconds` it lists open issues
labelled `rt:ready` in `spec.issuePickup.repos` with the table's
`spec.github` token, hands issues that need a human back (`needs-human`),
and, within `maxActive`/`maxActivePerRepo`, copies the `chainRef` Chain to
`<chainRef>-<repo>-<issue>` with the issue as input, starts it and claims the
issue (`rt:in-progress` + comment). The copies are owned by the RoundTable;
finished ones are deleted after `ttlSecondsAfterFinished`. Progress is in
`status.issuePickup`. With `spec.issuePickup.engine: trigger` it instead owns
a generated github-source Trigger, `<table>-issue-pickup`, that starts the
runs, and keeps only the post-run checks and cleanup. See
[issue-pickup.md](issue-pickup.md).

### Trigger controller (#402)

A `Trigger` starts a copy of its target Chain per event, with `spec.input`
rendered from the event by a Go template. A `nats` source gets one durable
consumer (`trigger-<namespace>_<name>`) on the table's events stream
(`<subjectPrefix>.events.>`, created with `createStreams`); each message is
acked only after the run is created or deliberately skipped. A `schedule`
source fires on a cron with the same catch-up rule as `Chain.spec.schedule`.
A `github` source runs one poll loop per Trigger that publishes matching issues
or pull requests to the Trigger's own subject on the events stream, read like a
`nats` source; its `claim` labels an item only after its run was created.
`dedupeKey` names the run, so a repeated key starts nothing; `concurrency`
(`Allow` with `maxActive`, `Forbid`, `Replace`) bounds active runs. Firings are
recorded in `status.recentFirings`. See [triggers.md](triggers.md).

### Knight scaling (#215)

A Knight runs `spec.replicas` pods (default 1) bound to the same durable
consumer `<Knight>-consumer`, so JetStream hands each pod different tasks.
`concurrency` stays the number of tasks per pod.

- **Workspace.** `workspace.mode: pvc` (default) is one ReadWriteOnce PVC and
  allows a single pod (Recreate strategy). `workspace.mode: ephemeral` gives
  each pod an `emptyDir` limited to `workspace.size` (RollingUpdate strategy)
  and sets `KNIGHT_WORKSPACE_MODE=ephemeral`. CEL on `Knight.spec` rejects
  `replicas`/`maxReplicas` above 1 on the default PVC (a ReadWriteMany
  `existingClaim` is allowed) and on `runtime: sandbox`. See
  [memory.md](memory.md) for how workspace, vault and shared mounts
  relate to knight memory. An `existingClaim` used with more than one replica
  must be ReadWriteMany: the operator and CEL cannot read a claim's access
  modes, so a ReadWriteOnce claim with `replicas > 1` is not rejected and
  leaves a surge pod stuck instead.
- **Tools flakes.** `spec.tools.flakes` refs that use `git+ssh` need matching
  SSH credentials on the tools builder Job; without them the build fails.
- **Memory.** `spec.memory` adds a persistent memory volume separate from the
  workspace, so memory survives pod replacement, scaling and restarts. The
  knight's own memory is read-write at `/memory/<knight>`
  (`KNIGHT_MEMORY_DIR`), and `/memory` (`KNIGHT_MEMORY_ROOT`) is its parent.
  Without `existingClaim` the operator creates the ReadWriteMany PVC
  `<knight>-memory` (`size`, default 1Gi; `storageClassName`), owned by the
  Knight and deleted with it, and mounts it at `/memory/<knight>`. With
  `existingClaim` (for example one RWX claim for a whole table) the claim is
  mounted read-only at `/memory` and its `<knight>` sub-directory read-write
  at `/memory/<knight>`: each knight writes only its own directory and reads
  everyone else's. The operator never creates, changes or deletes an
  `existingClaim`. A knight that can run more than one pod whose
  operator-created memory PVC is bound without ReadWriteMany reports
  `MemoryShared=False` reason `MemoryNotRWX`.
- **Autoscaling.** With `maxReplicas > 1` or `lifecycle.suspendPolicy: auto`
  the operator renders a KEDA `ScaledObject` (same name as the Knight):
  `nats-jetstream` trigger on `spec.nats.stream` / `<Knight>-consumer`,
  `lagThreshold: "1"`, min `0` for `auto` (else `replicas`), max `maxReplicas`,
  `cooldownPeriod` from `lifecycle.idleTimeoutSeconds` (default 300). The
  `nats-jetstream` trigger only counts *pending* messages, so a second
  `metrics-api` trigger reads the consumer's `num_ack_pending` (in-flight
  tasks) from the monitoring endpoint's `/jsz` and targets `concurrency` per
  pod; the HPA uses the larger of the two, so a pod running a task is not
  scaled away when the queue empties. The ScaledObject also sets
  `advanced.horizontalPodAutoscalerConfig.behavior.scaleDown.stabilizationWindowSeconds`
  from `lifecycle.scaleDownStabilizationSeconds` (default 600, `0` disables).
  The NATS
  monitoring endpoint is `--nats-monitoring-endpoint` or `<nats host>:8222`.
  The Deployment is created with one pod (the runtime creates its consumer on
  start), after which KEDA owns `replicas`. A knight KEDA scaled to zero
  reports phase `Suspended`, `Available=False` reason `ScaledToZero`.
  `spec.suspend: true` deletes the ScaledObject before scaling to 0.
- **Knight image requirement.** Lag-based scaling (`maxReplicas > 1`) and
  `suspendPolicy: auto` need knight images built from main at or after
  pull-when-free (claude-knight#13, pi-knight#61). Those runtimes fetch tasks
  only into free slots and ack on completion, so consumer lag is the real
  backlog. Older images ack on receipt and queue tasks in memory: lag stays 0
  while work piles up, so KEDA never scales out, and `auto` can scale a pod
  holding queued tasks to zero and lose them.
- **KEDA is optional.** `--enable-keda=auto` probes discovery for
  `keda.sh/v1alpha1 ScaledObject` once at startup (like the sandbox backend).
  Without it the knight runs `spec.replicas` pods and reports
  `Autoscaled=False` reason `KEDAUnavailable`.
- **PodDisruptionBudget.** Knights that can run more than one pod get a PDB
  with `maxUnavailable: 1`.
- **Plan-task cap.** `RoundTable.spec.policies.maxConcurrentPlanTasks` caps the
  combined capacity (`pods × concurrency`) of the table's plan-backed knights
  (`spec.planBacked`, default: knights on a claude-knight image). Each knight
  keeps one pod; spare capacity is handed out one pod at a time in name order.
  A capped knight reports `PlanCapLimited=True`.

## 4. NATS Subject Mapping

### Current State (Implicit fleet-a)

```
fleet-a.tasks.{domain}.{knight}     — task delivery
fleet-a.results.{domain}.{knight}   — result delivery
```

### With RoundTable

```
{rt.nats.subjectPrefix}.tasks.{domain}.{knight}
{rt.nats.subjectPrefix}.results.{domain}.{knight}
{rt.nats.subjectPrefix}.fleet.events
```

### Chain Subjects

```
{prefix}.tasks.{domain}.{knight}          — reuses knight subjects
{prefix}.results.chain.{chain}.{step}     — step results
{prefix}.chains.{chain}.events            — chain lifecycle events
```

### Mission Subjects

```
mission-{name}.briefing                      — mission briefing
mission-{name}.tasks.{domain}.{knight}       — mission-scoped tasks
mission-{name}.results.{domain}.{knight}     — mission-scoped results
mission-{name}.events                        — mission lifecycle events
```

## 5. Example YAML

### RoundTable

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: RoundTable
metadata:
  name: fleet-a
  namespace: ai
spec:
  description: "Primary AI agent fleet for infrastructure and security operations"
  nats:
    url: "nats://nats.database.svc:4222"
    subjectPrefix: "fleet-a"
    tasksStream: "fleet_a_tasks"
    resultsStream: "fleet_a_results"
    createStreams: true
    streamRetention: "WorkQueue"
  defaults:
    model: "claude-sonnet-4-20250514"
    image: "ghcr.io/roundtable-k8s/pi-knight:latest"
    taskTimeoutSeconds: 120
    concurrency: 2
  policies:
    costBudgetUSD: "50.00"
    maxMissions: 5   # concurrent active missions; 0 = unlimited. Extra missions wait in Pending (Admitted=False, TableAtMissionLimit), oldest first
  knightSelector:
    matchLabels:
      roundtable.ai.roundtable.io/fleet: fleet-a
  secrets:
    - name: anthropic-api-key
  # Short-lived GitHub token, mounted as a refreshing file (GITHUB_TOKEN_FILE).
  github:
    tokenSecretRef:
      name: roundtable-github-token
      key: token
  vault:
    claimName: obsidian-vault
    readOnly: true
    # Writable paths are never defaulted: list every path the knight may write.
    # Omit the list (or leave it empty) for a strictly read-only vault.
    writablePaths:
      - "Briefings/"
      - "Roundtable/"
```

#### Which RoundTable fields apply to which knights

| Field | Standing knights (labelled `ai.roundtable.io/round-table`, or legacy `ai.roundtable.io/table`) | Mission knights |
|-------|------|------|
| `secrets` | Added to the pod's `envFrom` (deduplicated against the knight's own `envFrom`) | Injected by the mission assembler, followed by `mission.spec.secrets` |
| `github` | Inherited unless the knight sets its own `spec.github` | Inherited unless the template sets its own |
| `sharedWorkspace` | Mounted (legacy `table` label only) | Not applied |

Standing knights without a table label get none of these.

### Chain

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: Chain
metadata:
  name: security-audit
  namespace: ai
spec:
  description: "Run a security audit: scan, analyze, report"
  roundTableRef: {name: fleet-a}
  timeoutSeconds: 900
  retryPolicy:
    maxRetries: 1
    backoffSeconds: 30
  steps:
    - name: scan
      knightRef: {name: mordred}
      task: "Run a comprehensive network scan of the internal cluster network 10.43.0.0/16. Output open ports and services as JSON."
      timeoutSeconds: 300

    - name: vuln-check
      knightRef: {name: mordred}
      task: "Analyze these scan results for known vulnerabilities: {{ .Steps.scan.Output }}"
      dependsOn: ["scan"]
      timeoutSeconds: 180

    - name: compliance-check
      knightRef: {name: percival}
      task: "Check these services against our compliance policy: {{ .Steps.scan.Output }}"
      dependsOn: ["scan"]
      timeoutSeconds: 180

    - name: report
      knightRef: {name: gawain}
      task: |
        Compile a security audit report from:
        - Vulnerability findings: {{ .Steps.vuln-check.Output }}
        - Compliance findings: {{ .Steps.compliance-check.Output }}
        Write the report to the vault at Briefings/security-audit-latest.md
      dependsOn: ["vuln-check", "compliance-check"]
      timeoutSeconds: 120
```

A step can override the knight's model for that task only with `model:` — a model
ID or an alias from the RoundTable's `spec.modelAliases`, validated against the
knight's runtime. See [knight-task-contract.md](knight-task-contract.md#model) and
[examples/chain-model-override.yaml](../examples/chain-model-override.yaml).

### Chain with Schedule

`spec.schedule` is a standard cron expression evaluated in `spec.timeZone`
(an IANA name such as `America/New_York`; default `UTC`). The schedule follows
local wall-clock time across daylight-saving changes. An invalid zone sets the
`ChainValid` condition to false with reason `InvalidSchedule`. The next fire time
is reported in `status.nextScheduledAt` and shown by `kubectl get chains`.

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: Chain
metadata:
  name: daily-briefing
  namespace: ai
spec:
  description: "Generate daily briefing from all knight activity"
  roundTableRef: {name: fleet-a}
  schedule: "0 8 * * *"
  timeZone: America/New_York   # optional IANA zone; defaults to UTC
  timeoutSeconds: 300
  steps:
    - name: gather
      knightRef: {name: gawain}
      task: "Query the last 24h of results from all knights and summarize key events."
      timeoutSeconds: 120
    - name: publish
      knightRef: {name: gawain}
      task: "Format this into a daily briefing and write to Briefings/daily/{{ now | date \"2006-01-02\" }}.md: {{ .Steps.gather.Output }}"
      dependsOn: ["gather"]
      timeoutSeconds: 60
```

### Mission

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: Mission
metadata:
  name: incident-response-2026-02
  namespace: ai
spec:
  objective: "Investigate and remediate the suspicious outbound traffic detected on node talos-3"
  successCriteria: "Root cause identified, affected services contained, and remediation applied or documented"
  roundTableRef: {name: fleet-a}
  ttlSeconds: 7200
  timeoutSeconds: 3600
  briefing: |
    Alert: Anomalous outbound traffic detected from node talos-3 to IP 203.0.113.42.
    Traffic pattern suggests potential data exfiltration. Priority: HIGH.
    Each knight should focus on their specialty and report findings to the mission channel.
  knights:
    - name: mordred
      role: lead
    - name: percival
      role: compliance-reviewer
    - name: incident-analyst
      role: forensics
      ephemeral: true
      ephemeralSpec:
        domain: forensics
        model: "claude-sonnet-4-20250514"
        image: "ghcr.io/roundtable-k8s/pi-knight:latest"
        skills: ["forensics", "networking", "linux"]
        tools:
          nix: ["tcpdump", "wireshark-cli", "volatility3"]
        nats:
          subjects: ["mission-incident-response-2026-02.tasks.forensics.>"]
          stream: "fleet_a_tasks"
        concurrency: 3
        taskTimeoutSeconds: 300
  chains:
    - name: security-audit
      phase: Setup
      inputOverride: '{"target": "talos-3", "scope": "outbound-traffic"}'
  cleanupPolicy: Retain
```

## 6. Migration Path

### Current State
- 10 Knights deployed in `ai` namespace
- Implicit "fleet-a" NATS prefix hardcoded in knight configs
- Streams `fleet_a_tasks` / `fleet_a_results` exist
- Gawain handles orchestration via prompt engineering

### Migration Steps

#### Phase 1: Add Labels (Non-breaking)
```bash
# Label all existing knights for fleet-a selection
kubectl label knights -n ai --all roundtable.ai.roundtable.io/fleet=fleet-a
```

#### Phase 2: Create RoundTable CR (Non-breaking)
```bash
kubectl apply -f - <<EOF
apiVersion: ai.roundtable.io/v1alpha1
kind: RoundTable
metadata:
  name: fleet-a
  namespace: ai
spec:
  nats:
    subjectPrefix: "fleet-a"
    tasksStream: "fleet_a_tasks"
    resultsStream: "fleet_a_results"
    createStreams: false  # streams already exist
  knightSelector:
    matchLabels:
      roundtable.ai.roundtable.io/fleet: fleet-a
  defaults:
    model: "claude-sonnet-4-20250514"
    taskTimeoutSeconds: 120
    concurrency: 2
  policies:
    costBudgetUSD: "50.00"
EOF
```

The RoundTable controller will discover existing knights and begin aggregating status. No changes to knight behavior.

#### Phase 3: Convert Gawain Orchestrations to Chains (Incremental)
Identify Gawain's recurring orchestration patterns and express them as Chain CRDs. This can happen one chain at a time — Gawain continues handling anything not yet converted.

Example: If Gawain currently coordinates "scan → analyze → report" via prompt engineering, create the `security-audit` Chain above. Gawain's workload decreases as chains take over.

#### Phase 4: Enable Missions for Ad-Hoc Work
Start using Mission CRDs for incident response and exploratory tasks that currently require manual knight coordination.

#### Phase 5: Cleanup
- Set `createStreams: true` on RoundTable to let the operator manage stream lifecycle
- Remove hardcoded NATS config from knights that can inherit from RoundTable defaults
- Consider creating a second RoundTable for dev/test knights

### Backward Compatibility

- Knights continue to work without a RoundTable (standalone mode)
- Chains reference knights by name, no knight changes needed
- NATS subjects remain identical — same prefix, same structure
- All new CRDs are additive; no existing resources are modified

## 7. Knight `spec.mcpServers` (claude-knight only)

CLI-first (#179): prefer a CLI in the tools image; use MCP only for services
with no CLI. `spec.mcpServers` declares remote MCP servers (`name`, `url`,
`transport` = `streamable-http` (default) or `sse`). A server's tools are
all available to the knight; narrowing them is the server's or an MCP
gateway's job.
For a claude-knight image the operator renders them into `MCP_SERVERS_JSON`
(`{"mcpServers":{"<name>":{"type":"http"|"sse","url":...}}}`) and the
deprecated `toolFilter.exclude` entries (see [section 8](#8-deprecated-fields))
into `CLAUDE_DISALLOWED_TOOLS` as `mcp__<name>__<tool>`. If `spec.env` already
sets either variable, the user's entries are kept and the operator's are added (on a server-name clash the
user's server wins). On any other runtime nothing is rendered and the Knight
gets `Degraded=True` with reason `MCPServersUnsupported`.

Under default-deny egress the operator allows each server's URL itself (an
in-cluster Service URL as an `allowedServices` entry, any other host as an
`allowedHosts` entry on the URL's port), so it need not be listed again under
`spec.egress`; see [egress.md](egress.md#mcp-servers-listing-the-server-is-enough).

### Auth headers: `headersFrom`

A server that needs an auth header takes it from a Secret key in the knight's
namespace (credentials stay in the pod, as narrow as possible; #359):

```yaml
mcpServers:
  - name: vault
    url: https://vault-mcp.example.com/mcp   # the egress allowance is derived from the URL
    headersFrom:
      - name: Authorization            # HTTP header name, unique per server
        valueFrom:
          secretKeyRef:                # no namespace: always the knight's
            name: vault-reader-mcp
            key: authorization         # whole header value, e.g. "Bearer <token>"
```

Each entry becomes an env var on the knight container named
`MCP_<SERVER>_<HEADER>` (upper-cased, every character other than `A-Z0-9`
turned into `_`, e.g. `MCP_VAULT_AUTHORIZATION`) with `valueFrom.secretKeyRef`
copied from the spec. `MCP_SERVERS_JSON` holds only the placeholder
(`"headers":{"Authorization":"${MCP_VAULT_AUTHORIZATION}"}`), which Claude
Code expands from its environment when it connects (verified with Claude Code
2.1.329, for both an `--mcp-config` file and inline JSON). The operator never
reads the value into a ConfigMap, annotation, status field or log line. If two
headers map to the same variable name (server `a-b` + header `c` and server
`a` + header `b-c`), or the name is already set in `spec.env`, later ones get a
`_2`, `_3`, ... suffix in spec order.

The API server rejects a header name listed twice on one server. Header names
are case-insensitive in HTTP, so an entry that repeats an earlier one in a
different case is dropped and reported as `MCPHeadersReady=False` with reason
`MCPHeaderDuplicate`.

The operator checks that every referenced Secret and key exists and sets the
`MCPHeadersReady` condition: `True`, or `False` with reason
`MCPHeaderSecretMissing` and a message naming the missing Secret or key
(otherwise the pod would fail to start with an opaque
`CreateContainerConfigError`). Secrets are not watched; while a reference is
missing the knight is re-checked every 30 seconds. The condition is absent when
no headers are rendered.

- **One token per MCP server.** Don't reuse a token across servers or knights;
  scope each to what that knight needs.
- **Egress.** The server's host must be on the knight's `spec.egress`
  allowlist (see [egress](egress.md)).
- **Rotation** works like any other knight credential: update the referenced
  Secret, then restart the knight (`kubectl rollout restart deployment/<knight>`);
  env vars are read at pod start.
- The value is visible to anything running in the knight container (as every
  pod credential is), including the agent's shell.

Example: [`examples/knight-mcp-headers.yaml`](../examples/knight-mcp-headers.yaml).

## 8. Deprecated fields

These fields still work in this release but are reported on the Knight as
`DeprecatedFieldsUsed=True` (with one Warning event per field per generation)
and will be removed one release later:

| Field | Replacement | Why |
|---|---|---|
| `spec.tools.apt` | `spec.tools.nix`, `spec.tools.flakes` | Knight containers run as non-root, so apt installs cannot succeed. |
| `spec.tools.mise` | `spec.tools.nix`, `spec.tools.flakes` | Tools are written as `latest`, never pinned, and downloaded at every cold start. |
| `spec.mcpServers[].toolFilter` | none (use the MCP server's own filtering or a gateway) | Round Table does not manage which tools a knight has; excluded tools are still denied until removal. |
| `spec.nats.maxDeliver` | none | Ignored; tasks run at most once. |
