# Architecture

## Overview

**Knights of the Round Table** is a Kubernetes operator that orchestrates fleets of AI agents as first-class cluster resources. It provides declarative management, workflow orchestration, and cost control through four Custom Resource Definitions.

### Core Metaphor

| Concept | Kubernetes Resource | Purpose |
|---------|-------------------|---------|
| **Knight** | Pod (Deployment/Sandbox) | A single AI agent with a domain, model, and skills |
| **RoundTable** | Fleet config | A group of knights sharing NATS infrastructure and policies |
| **Chain** | DAG workflow | Multi-step task pipeline with output chaining between knights |
| **Mission** | Ephemeral team | Auto-assembles knights for a time-bounded objective |

## System Architecture

```
┌────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                       │
│                                                            │
│  ┌─────────────────────────────────────────────────────┐  │
│  │              Operator Pod                            │  │
│  │  ┌───────────────┐  ┌───────────────────────────┐  │  │
│  │  │  Controllers   │  │  Runtime Backends          │  │  │
│  │  │  • Knight      │  │  • DeploymentBackend      │  │  │
│  │  │  • RoundTable  │  │  • SandboxBackend         │  │  │
│  │  │  • Chain       │  │    (scale-to-zero)        │  │  │
│  │  │  • Mission     │  └───────────────────────────┘  │  │
│  │  └───────────────┘                                   │  │
│  └─────────────────────────────────────────────────────┘  │
│           │ watches/reconciles                             │
│           ▼                                                │
│  ┌─────────────────────────────────────────────────────┐  │
│  │              CRDs (etcd)                             │  │
│  │  RoundTable ──owns──► Knight[]                      │  │
│  │  Mission ──creates──► ephemeral RoundTable + Chain[]│  │
│  │  Chain ──references──► Knight (by name/domain)      │  │
│  └─────────────────────────────────────────────────────┘  │
│           │ creates pods                                   │
│           ▼                                                │
│  ┌─────────────────────────────────────────────────────┐  │
│  │         Knight Pods (pi-knight runtime)              │  │
│  │  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐      │  │
│  │  │Galahad │ │Tristan │ │msn-a-  │ │msn-a-  │      │  │
│  │  │security│ │ infra  │ │ scout  │ │ coder  │      │  │
│  │  └───┬────┘ └───┬────┘ └────────┘ └────────┘      │  │
│  │      │          │                                    │  │
│  │      ▼          ▼                                    │  │
│  │  ┌─────────────────────────────────────────────┐    │  │
│  │  │           NATS JetStream                     │    │  │
│  │  │  Tasks:   {prefix}.tasks.{domain}.>          │    │  │
│  │  │  Results: {prefix}.results.{task_id}         │    │  │
│  │  │  KV:     chain-outputs (step result cache)   │    │  │
│  │  └─────────────────────────────────────────────┘    │  │
│  └─────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────┘
```

## Controller Reconciliation

Each CRD has a dedicated reconciler following the standard Kubernetes controller pattern:

**KnightReconciler**: Task consumer → ConfigMap → PVC → Runtime (Deployment or Sandbox) → Status
**RoundTableReconciler**: Knight discovery → Health aggregation → NATS streams → Agent cards → Cost check
**ChainReconciler**: Step ordering → Task dispatch → Output collection → Template rendering → Next step
**MissionReconciler**: State machine (Pending → Provisioning → Planning → Assembling → Briefing → Active → Cleanup)

## Runtime Backends

The operator uses a pluggable `RuntimeBackend` interface:

```go
type RuntimeBackend interface {
    Reconcile(ctx context.Context, knight *Knight) error
    Cleanup(ctx context.Context, knight *Knight) error
    IsReady(ctx context.Context, knight *Knight) (bool, error)
    Suspend(ctx context.Context, knight *Knight) error
    Resume(ctx context.Context, knight *Knight) error
}
```

- **DeploymentBackend** — Standard Kubernetes Deployment. Always running. Good for persistent knights.
- **SandboxBackend** — Uses the `agents.x-k8s.io/v1beta1` Sandbox CRD (agent-sandbox v1.0.0 or later). Suspends and resumes through `spec.operatingMode`. Good for cost optimization.

Selected per-knight via `spec.runtime: deployment|sandbox`. Why two backends, how gVisor, KEDA and egress fit each, and what was rejected: [design/runtime-backends-adr.md](design/runtime-backends-adr.md).

## NATS Communication

### Subject Routing

```
{prefix}.tasks.{domain}.>        — All tasks for a domain
{prefix}.tasks.{domain}.{knight} — Tasks for a specific knight
{prefix}.results.{task_id}       — Result for a specific task
```

### Consumer Model

Each knight reads its tasks from a **durable JetStream pull consumer**
`<Knight>-consumer` (the Knight name capitalised) on `spec.nats.stream`:

- Filter subjects: `spec.nats.subjects`
- Explicit ack, deliver all, `max_deliver: 1` (at most once; no automatic redelivery), 30 s `ack_wait` (a heartbeat window; running tasks send in-progress acks)
- Lost and failed tasks go to the dead-letter stream and are retried by hand (see [knight-task-contract.md](knight-task-contract.md#delivery-and-failure))

KEDA scales the knight on this consumer's lag, so its name and config do not
change. The values live in one place, `natspkg.KnightTaskConsumerConfig`
(`pkg/nats/consumer.go`), copied from the runtimes' `subscribe()` in
roundtable-k8s/knights `packages/core/src/nats/index.ts`.

### JetStream topology: who owns what

| Object | Owner | Lifecycle |
|--------|-------|-----------|
| Tasks, results, dead-letter and advisories streams | RoundTable controller (when `spec.nats.createStreams` is true) | Created with the table |
| Knight task consumer `<Knight>-consumer` | **Knight controller** | Created when the Knight is reconciled, so it exists while the knight is scaled to zero or suspended; deleted by the Knight's finalizer |
| Dead-letter consumer `roundtable-dead-letter` | RoundTable controller (dead-letter manager) | With the dead-letter stream |
| Result poll consumers (`chain-poll-*`, mission planners) | Chain and Mission controllers | Ephemeral: deleted after the result is read |
| Agent cards KV bucket | RoundTable controller (entries also dropped by the Knight finalizer) | Per knight entry |

The Knight controller reconciles the task consumer on every reconcile:

- **Missing**: created.
- **Matches** (for example one a runtime created first): adopted as it is, never
  rewritten, so tasks in flight and the delivery state are kept.
- **Filters, `ack_wait` or `max_deliver` differ**: updated in place.
- **A field NATS cannot update differs** (deliver, ack or replay policy, start
  sequence or time, push instead of pull): deleted and recreated, with a
  `ConsumerRecreated` Warning event. Only then is the consumer's state lost.

The `ConsumerReady` condition on the Knight and the gauge
`roundtable_knight_consumer_ready{table,knight}` (1 or 0) report the result;
`ConsumerReady=False` carries `NATSUnavailable`, `StreamNotFound` or
`ConsumerFailed` and is retried every 30 s. It does not fail the rest of the
reconcile. Without a NATS connection configured the operator leaves consumers
alone.

When a Knight is deleted, its finalizer deletes the consumer. If NATS is
unreachable it retries every 10 s for up to 5 minutes, then gives up with a
`ConsumerDeleteAbandoned` Warning event naming the consumer to delete by hand
(`nats consumer rm <stream> <Knight>-consumer`), so an outage never blocks the
deletion.

The runtimes still create the consumer at start-up with the same config until
they only bind to it (roundtable-k8s/knights follow-up). Because both sides
produce the same config, neither rewrites the other's consumer.

### Message Flow

1. Tim/Chain/Mission publishes task → `fleet-a.tasks.security.galahad`
2. Galahad's consumer pulls the message
3. pi-knight runtime processes it (LLM call + tool use)
4. Result published → `fleet-a.results.{task_id}`
5. Chain controller or caller picks up the result

## Mission Lifecycle

```
Pending → Provisioning → Planning → Assembling → Briefing → Active → Succeeded/Failed → CleaningUp
```

| Phase | What Happens |
|-------|-------------|
| **Pending** | Validate spec, set initial status |
| **Provisioning** | Create ephemeral RoundTable + NATS streams |
| **Planning** | (meta-missions) Dispatch to planner knight, receive Chain definitions |
| **Assembling** | Create mission knights, wait for Ready |
| **Briefing** | Publish mission context to all knights via NATS |
| **Active** | Execute chains, track costs, monitor timeout |
| **CleaningUp** | Delete ephemeral resources, preserve results if configured |

## Mission Knights

The assembler creates each ephemeral mission knight directly as `<mission>-<knight>`, owned by the Mission, so deleting the mission deletes the knight. Every mission knight carries the annotation `ai.roundtable.io/shutdown-time`: the mission's expiry (`status.expiresAt`, start + `ttlSeconds`). A knight whose template says `runtime: sandbox` is one bare agent-sandbox `Sandbox`, and the Sandbox backend copies that annotation into the Sandbox's `spec.shutdownTime`, so its pod stops at the mission's expiry even if cleanup never runs. Mission knights always start cold.

There is no warm pool: `RoundTable.spec.warmPool` was removed in #483 because it never reused a running pod. Warm starts will come back through upstream agent-sandbox pools (`SandboxTemplate` + `SandboxWarmPool`) if the cold-start spike in #368 supports them.

## Chain Execution

Chains are multi-step DAG workflows with Go template output chaining:

```yaml
steps:
  - name: scan
    knightRef: {name: galahad}
    prompt: "Scan for CVEs"
  - name: report
    knightRef: {name: kay}
    prompt: "Summarize: {{ .Steps.scan.Output }}"
```

Steps execute in dependency order. Independent steps run in parallel. Each step's output is available to subsequent steps via `{{ .Steps.<name>.Output }}`.

## Cost Tracking

Costs tracked at three levels:
- **Per-knight**: `status.totalCost` (lifetime)
- **Per-mission**: `status.totalCost` (mission duration)
- **Per-RoundTable**: aggregated across all knights

Budget enforcement: `spec.policies.costBudgetUSD` triggers OverBudget phase when exceeded.

## Operator Metrics

All operator metrics follow the `roundtable_*` scheme and, except the per-knight wake histogram, carry the `table` label. The gauges are recomputed from the Knight and Mission lists on each RoundTable reconcile.

| Metric | Type | Labels |
|---|---|---|
| `roundtable_knights` | gauge | `table`, `phase` |
| `roundtable_missions` | gauge | `table`, `phase` |
| `roundtable_table_cost_usd` | gauge | `table` |
| `roundtable_knight_isolated` | gauge | `table`, `knight`, `reason` (1 for the current `Isolated` condition reason) |
| `roundtable_knight_consumer_ready` | gauge | `table`, `knight` (1 when the knight's task consumer exists with the expected config, else 0) |
| `roundtable_chain_runs_total` | counter | `table`, `chain`, `status` (`succeeded`, `partially_succeeded`, `failed`) |
| `roundtable_chain_noop_runs_total` | counter | `table`, `chain` |
| `roundtable_chain_step_duration_seconds` | histogram | `table`, `chain`, `step`, `status` |
| `roundtable_knight_wake_seconds` | histogram | `knight`, `namespace` |
| `roundtable_tasks_dead_lettered_total` | counter | `table`, `knight`, `reason` |
| `roundtable_dead_letters_open` | gauge | `table`, `knight` |

Knight pods carry `ROUNDTABLE_TABLE=<table>` (empty without a table) and the `ai.roundtable.io/round-table` pod label.

## Directory Structure

```
api/v1alpha1/           — CRD type definitions
internal/controller/    — Reconcilers (one per CRD)
internal/mission/       — KnightAssembler, mission lifecycle helpers
pkg/runtime/            — RuntimeBackend interface + implementations
pkg/nats/               — JetStream client wrapper
charts/roundtable-operator/ — Helm chart
examples/               — Real-world manifest examples
docs/                   — Documentation
```
