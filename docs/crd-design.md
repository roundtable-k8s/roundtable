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

See the actual Go files for complete type definitions. Key design decisions:

**Chain:** Steps use `dependsOn` for DAG-style dependencies rather than simple ordering. This enables parallel fan-out (multiple steps with no dependencies run concurrently) and fan-in (step depends on multiple prior steps). Step outputs are accessible via Go templates in downstream step tasks.

A step with `forEach` fans out over a JSON list taken from `spec.input` or a prior step's output (`from: input | steps.<name>`, optional JSONPath `path`, `maxItems` default 50). Each item is its own task (own taskId, runId `<runId>-i<index>`) rendered with `{{ .Item }}` / `{{ .ItemJSON }}` / `{{ .ItemIndex }}`; `maxParallel` caps items in flight (default: the knight's concurrency); a step `model` override applies to every item. When all items are terminal the step output becomes the join — a JSON array of `{index, item, phase, output, error}` — so a later step (or another `forEach` over `steps.<name>`) consumes per-item results. `failurePolicy: failFast` (default) stops dispatching after the first failed item and fails the step; `continue` runs every item and the chain ends `PartiallySucceeded` if any failed. Status is bounded: `status.stepStatuses[].fanOut` keeps one phase character per item plus counts, and detailed entries for in-flight items and at most 50 finished ones (the rest are read back from the `chain-outputs` KV bucket at join time). `ai.roundtable.io/retry-failed` re-runs only failed/skipped items. See `examples/chain-fan-out.yaml`.

A step's `when` is a CEL expression over prior steps (`steps.<name>.{phase,output,error,reason,outcome,iterations}`, `input`, plus `json()` and the extended string functions). It is evaluated when the step's dependencies are done. When it is false the step is `Skipped` (`reason: WhenFalse`) without dispatching anything, and its dependents still run. A step with `loop` is a bounded loop. Its 1–5 body steps (each with its own `knightRef`, `task`, `model`, `timeoutSeconds`, `retry`) run in order, round after round. The CEL `until` expression is checked against `last` (the `from` step on entry, then the last body step after each round). The loop ends `Converged`, `Exhausted` after `maxIterations` rounds (the step fails, or with `onExhausted: continue` succeeds so dependents can branch on `steps.<loop>.outcome`), or `Failed`. Body tasks reuse the chain `runId`, so a coder resumes its session. Status is bounded: `status.stepStatuses[].loop` holds the round counter, the outcome and one latest-round entry per body step. `retry-failed` resumes a failed loop at the failed body step and gives an exhausted one another `maxIterations` rounds. See `docs/conditional-steps.md` and `examples/chain-pr-pipeline.yaml`.

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

1. **Assembling** — Create ephemeral Knight CRs (owned by Mission). Wait for all knights to reach Ready phase.
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

### RoundTable Controller

**Reconciliation Loop:**

1. **NATS Setup** — If `createStreams=true`, ensure JetStream streams exist with correct subjects and retention policy.
2. **Knight Discovery** — List Knights matching `knightSelector`. Update status with knight summaries.
3. **Defaults Propagation** — For Knights that don't specify certain fields, the controller does NOT mutate Knight specs. Instead, the Knight controller checks for a parent RoundTable and inherits defaults at reconcile time.
4. **Policy Enforcement:**
   - Count total concurrent tasks across knights. If exceeding `maxConcurrentTasks`, pause NATS consumers on lowest-priority knights.
   - Aggregate costs. If exceeding `costBudgetUSD`, suspend all knights (set annotation `roundtable.ai.roundtable.io/budget-suspended=true`).
   - Check cost reset schedule, reset counter when due.
5. **Health Aggregation** — Compute phase: Ready (all knights ready), Degraded (some not ready), Suspended, OverBudget.
6. **Mission Counting** — Count active Missions referencing this table.

**NATS Subjects:**
- Manages streams: `{subjectPrefix}_tasks` and `{subjectPrefix}_results`
- Fleet events: `{subjectPrefix}.fleet.events`

**Created Resources:**
- JetStream streams (if `createStreams=true`)
- No direct Knight ownership (uses selector, like a Service)

### Issue pickup (#224)

A second controller on RoundTables (`issuepickup`) acts only when
`spec.issuePickup` is set. Every `intervalSeconds` it lists open issues
labelled `rt:ready` in `spec.issuePickup.repos` with the table's
`spec.github` token, hands issues that need a human back (`needs-human`),
and, within `maxActive`/`maxActivePerRepo`, copies the `chainRef` Chain to
`<chainRef>-<repo>-<issue>` with the issue as input, starts it and claims the
issue (`rt:in-progress` + comment). The copies are owned by the RoundTable;
finished ones are deleted after `ttlSecondsAfterFinished`. Progress is in
`status.issuePickup`. See [issue-pickup.md](issue-pickup.md).

### Knight scaling (#215)

A Knight runs `spec.replicas` pods (default 1) bound to the same durable
consumer `<Knight>-consumer`, so JetStream hands each pod different tasks.
`concurrency` stays the number of tasks per pod.

- **Workspace.** `workspace.mode: pvc` (default) is one ReadWriteOnce PVC and
  allows a single pod (Recreate strategy). `workspace.mode: ephemeral` gives
  each pod an `emptyDir` limited to `workspace.size` (RollingUpdate strategy)
  and sets `KNIGHT_WORKSPACE_MODE=ephemeral`. CEL on `Knight.spec` rejects
  `replicas`/`maxReplicas` above 1 on the default PVC (a ReadWriteMany
  `existingClaim` is allowed) and on `runtime: sandbox`.
- **Autoscaling.** With `maxReplicas > 1` or `lifecycle.suspendPolicy: auto`
  the operator renders a KEDA `ScaledObject` (same name as the Knight):
  `nats-jetstream` trigger on `spec.nats.stream` / `<Knight>-consumer`,
  `lagThreshold: "1"`, min `0` for `auto` (else `replicas`), max `maxReplicas`,
  `cooldownPeriod` from `lifecycle.idleTimeoutSeconds` (default 300). The NATS
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
    maxConcurrentTasks: 20
    costBudgetUSD: "50.00"
    costResetSchedule: "0 0 1 * *"
    maxKnights: 15
    maxMissions: 5
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
    costResetSchedule: "0 0 1 * *"
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
