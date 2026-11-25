# Principles

## What Round Table is

Round Table is an opinionated, open-source reference platform for running fleets of AI agents on Kubernetes. It is a showcase people can copy, not a company product and not a neutral, CNCF-style tool. The maintainer's own fleets are the showcase. The install has to work, but it does not need to be generic. Standards are adopted only where they serve one of the opinions below.

Anything described here as **target** is not on `main` yet. The linked issue tracks it.

## The principles

### 1. Agents are infrastructure

*An agent is a declared, reconciled cluster resource, not a process someone started.*

**What it means in practice.** Knights, chains, missions and tables are CRDs. They live in git, are applied by GitOps and are reviewed like any other manifest. The operator reconciles them into pods and NATS state. The platform does not offer a way to create an agent that exists only in someone's shell or a database.

**See:** [`api/v1alpha1`](../api/v1alpha1), [CRD design](crd-design.md), [Helm chart](../charts/roundtable-operator), [Helm values](helm-values.md). A Flux install guide is a **target**: [#337](https://github.com/roundtable-k8s/roundtable/issues/337).

### 2. Specialists, not generalists

*A knight is a persona. A chain is a job.*

**What it means in practice.** Each knight has a `domain` and a list of `skills`, and receives tasks on its own subject, `<prefix>.tasks.<domain>.<knight>`. Work is routed to the specialist whose domain matches. The platform does not provide a single do-everything agent. A RoundTable also carries a human-readable `description`.

**See:** [`knight_types.go`](../api/v1alpha1/knight_types.go) (`domain`, `skills`), [knight task contract](knight-task-contract.md), [`roundtable_types.go`](../api/v1alpha1/roundtable_types.go) (`description`).

### 3. Messages, not calls

*Agents talk through durable messages. They do not call each other.*

**What it means in practice.** Tasks and results travel over NATS JetStream, in per-table tasks and results streams. A task can wait for a busy knight, be replayed, and be observed by anyone who subscribes. The invoke gateway gives callers a synchronous HTTP front door, but it only publishes onto JetStream and reads the result back. Knights see no difference.

**See:** [knight task contract](knight-task-contract.md), [architecture](ARCHITECTURE.md), [task activity events](design/task-activity-events.md), [invoke gateway](gateway.md).

### 4. Any model, chosen by cost

*The model is a per-step decision, made on cost and difficulty.*

**What it means in practice.** A RoundTable maps intent names to model IDs with `modelAliases`. A chain step can override the model for that task only. There are two knight runtimes: pi-knight for any provider and claude-knight for Claude. When a coder is out of quota, the PR pipeline can retry the step on a fallback coder. Missions and tables carry cost budgets. The platform does not tie a fleet to one provider.

**See:** [`roundtable_types.go`](../api/v1alpha1/roundtable_types.go) (`modelAliases`, `costBudgetUSD`), [`mission_types.go`](../api/v1alpha1/mission_types.go) (`costBudgetUSD`), [task contract: model](knight-task-contract.md#model), [PR pipeline](pr-pipeline.md).

### 5. Humans set goals, agents do the rest

*A person states the goal. The agents plan and execute.*

**What it means in practice.** There are no approval gates (see [Autonomy and safety](#autonomy-and-safety)). A person asks a knight, runs a chain or launches a mission, and a mission's planner composes the team and the steps. The platform does not ask a human to approve each step.

**See:** [dynamic missions](phase-3-dynamic-missions.md), [`examples/mission-meta.yaml`](../examples/mission-meta.yaml), [three rungs of work](#three-rungs-of-work).

### 6. Isolated by default

*Every knight gets its own pod and its own workspace.*

**What it means in practice.** Each knight runs in its own pod with its own workspace volume. The runtime backends build those pods, and a knight can request a RuntimeClass such as gVisor. The platform refuses to run knights in a shared process. Today, the platform creates a dedicated ServiceAccount and NetworkPolicy only for mission knights ([`internal/mission/assembler.go`](../internal/mission/assembler.go), [`internal/mission/netpol.go`](../internal/mission/netpol.go)). A standing knight uses the ServiceAccount named in `spec.serviceAccountName`, or the namespace default if that is empty, and gets no generated NetworkPolicy. A per-knight ServiceAccount and NetworkPolicy by default are a **target** with no tracking issue yet.

**Target.** gVisor on every knight and default-deny egress with a hostname allowlist are not the default today: [#212](https://github.com/roundtable-k8s/roundtable/issues/212) (gVisor RuntimeClass on the reference cluster), [#348](https://github.com/roundtable-k8s/roundtable/issues/348) (`spec.egress`), [#349](https://github.com/roundtable-k8s/roundtable/issues/349) (`defaults.egress` and starter allowlists). RuntimeClass support itself shipped in [#164](https://github.com/roundtable-k8s/roundtable/issues/164).

**See:** [`internal/knight`](../internal/knight), [`pkg/runtime`](../pkg/runtime), [runtime isolation](runtime-isolation.md).

### 7. Reproducible capabilities

*What a knight can do is pinned and versioned.*

**What it means in practice.** Skills live in git and reach knights through the arsenal sidecar. Tools come from Nix tools images pinned to an immutable `nixpkgs` ref. Two knights built from the same inputs have the same capabilities. The platform does not let a knight install tools by hand and call that its configuration.

**Target.** Skills as Agent Skills spec packages delivered as OCI image volumes: [#178](https://github.com/roundtable-k8s/roundtable/issues/178).

**See:** [`knight_types.go`](../api/v1alpha1/knight_types.go) (`arsenals`), [Helm values](helm-values.md), [`charts/roundtable-operator/values.yaml`](../charts/roundtable-operator/values.yaml) (`nixpkgs`, `toolsImage`).

### 8. You can watch everything

*Nothing an agent does is hidden from the operator of the fleet.*

**What it means in practice.** Task activity is published as events, the operator exports Prometheus metrics, and the dashboard shows the fleet, chains and missions. Safety rests on seeing and revoking after the fact (see [Autonomy and safety](#autonomy-and-safety)).

**Target.** OpenTelemetry across the whole stack: [#153](https://github.com/roundtable-k8s/roundtable/issues/153).

**See:** [roundtable-ui](https://github.com/roundtable-k8s/roundtable-ui), [task activity events](design/task-activity-events.md), [telemetry conventions](design/telemetry-conventions.md), [`pkg/metrics`](../pkg/metrics).

## Three rungs of work

All three are built from the same primitives.

1. **Ask a knight.** One call, one answer, through the invoke gateway. See [gateway](gateway.md).
2. **Run a chain.** A declared, repeatable job made of steps. See [CRD design](crd-design.md) and the [README](../README.md#chain--workflow-dag).
3. **Launch a mission.** A planner composes standing knights and chains toward a large goal. See [dynamic missions](phase-3-dynamic-missions.md).

## Autonomy and safety

Round Table has no approval gates. Autonomy is the point. Safety comes from constraint:

- least-privilege, narrowly scoped credentials;
- sandboxing;
- network limits;
- budgets;
- after-the-fact observability and revocation.

If an action is too dangerous to run unattended, the knight does not get the capability. Human checkpoints are a recipe concern, for anyone who wants them.

## Platform and recipes

| Platform | Recipes |
|---|---|
| The CRDs and the operator | The dev factory: issue pickup, PR pipeline, PR feedback |
| The gateway API | The personal fleet: vault, briefings |
| The dashboard | Homelab watch |
| The knight runtimes and the task contract | Human checkpoints |
| | [Fleet self-improvement](recipe-fleet-self-improvement.md) |
| Triggers (planned) | |
| Observability | |

The dev factory still lives in the core today: `RoundTable.spec.issuePickup`, `spec.prFeedback` and the chart's `prPipeline`. Extracting it into recipes is tracked in [#399](https://github.com/roundtable-k8s/roundtable/issues/399) (generic triggers and recipes). See also [issue pickup](issue-pickup.md), [PR pipeline](pr-pipeline.md) and [PR feedback](pr-feedback.md).
