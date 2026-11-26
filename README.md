# 🏰 Knights of the Round Table

**Kubernetes-Native Multi-Agent AI Orchestration**

[![Go](https://img.shields.io/badge/Go-1.23-00ADD8?logo=go)](https://go.dev)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![CI](https://github.com/roundtable-k8s/roundtable/actions/workflows/ci.yml/badge.svg)](https://github.com/roundtable-k8s/roundtable/actions)

> *"We are the knights who say... `kubectl apply`!"*

Round Table is a Kubernetes operator that manages fleets of AI agents as native cluster resources. Define agents, orchestrate workflows, and launch ephemeral missions — all through CRDs and GitOps.

```bash
kubectl apply -f knight.yaml   # A fully wired AI agent appears in your cluster
kubectl apply -f mission.yaml  # An ephemeral team spins up, executes, and cleans up
```

**Not a framework. Not a chatbot. An orchestration layer that treats AI agents like infrastructure.**

---

## Why Round Table?

Most multi-agent systems are either app-layer task managers or single-agent wrappers. Round Table takes a different approach: **agents ARE infrastructure**.

| | Round Table | Agent Frameworks | Workflow Engines | Task Managers |
|---|---|---|---|---|
| **Agent lifecycle** | K8s pods — ephemeral, scalable, isolated | Library objects in a process | Static containers | External processes |
| **Communication** | NATS JetStream — durable, replay, fan-out | In-memory function calls | HTTP/gRPC | Polling / webhooks |
| **State** | CRDs + NATS KV (etcd-backed, declarative) | In-memory / DB | ConfigMaps / DB | Postgres |
| **Orchestration** | Chain DAGs with templated output chaining | Code-defined graphs | YAML DAGs | Org chart delegation |
| **Isolation** | Pod-level plus gVisor sandbox and default-deny egress | None (shared process) | Container-level | App-level scoping |
| **Ephemerality** | Missions auto-provision and teardown teams | Manual | Manual | N/A |
| **GitOps** | Native (CRDs + Flux/ArgoCD) | N/A | Partial | N/A |

**Principles:** agents are infrastructure · specialists, not generalists · messages, not calls · any model, chosen by cost · humans set goals, agents do the rest · isolated by default · reproducible capabilities · you can watch everything · don't rebuild the wheel. See [docs/principles.md](docs/principles.md), the [decision register](docs/decisions.md) and [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). More in [docs/](docs/): [memory](docs/memory.md), [workspace snapshots](docs/workspace-snapshots.md), [runtime backends](docs/design/runtime-backends-adr.md), [scheduled work](docs/scheduled-work.md), [migrating from kagent](docs/migrating-from-kagent.md), [cutting a release](docs/releasing.md).

## Features

🗡️ **5 Custom Resources** — Knight, RoundTable, Chain, Mission, Trigger — declare your entire agent fleet in YAML

⚔️ **Ephemeral Missions** — spin up a purpose-built team of agents, execute an objective, tear everything down

🔗 **Chain DAGs** — multi-step workflows with parallel execution, templated output injection between steps

📡 **NATS JetStream** — durable task routing with WorkQueue retention, fan-out, and KV store for results

🛡️ **Pod-Level Isolation** — each knight gets its own pod, ServiceAccount, workspace PVC, gVisor sandbox and default-deny egress NetworkPolicy

📦 **Arsenal** — git-synced skill repository, auto-distributed to knights via sidecar

🖥️ **Dashboard** — real-time fleet monitoring, chain visualization, mission tracking ([roundtable-ui](https://github.com/roundtable-k8s/roundtable-ui))

🔄 **GitOps-Native** — CRDs reconcile through Flux or ArgoCD, same as any other cluster workload

---

## Custom Resources

### Knight — An AI Agent

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: Knight
metadata:
  name: galahad
spec:
  domain: security
  model: claude-sonnet-4-20250514
  skills: [security, opencti, shared]
  tools:
    nix: [nmap, whois, bind]
  nats:
    subjects: ["fleet-a.tasks.security.>"]
  concurrency: 2
  workspace:
    size: 2Gi
```

The operator reconciles this into a Deployment, PVC, ConfigMaps, NATS consumer, and optional ServiceAccount. One YAML, fully wired agent.

#### Nix tools and flakes

`spec.tools.nix` lists nixpkgs packages; the operator builds each unique list once into a tools image the knight mounts at `/nix`. To add packages from a flake outside nixpkgs, use `spec.tools.flakes`. Those packages go into the same image:

```yaml
spec:
  tools:
    nix: [nmap, whois]
    flakes:
      - ref: github:roundtable-k8s/roundtable/0123456789abcdef0123456789abcdef01234567
        packages: [my-tool]   # output names under packages.x86_64-linux
```

`ref` must be pinned to a full 40-character commit: `github:`, `gitlab:` or `sourcehut:` `<owner>/<repo>/<rev>`, or `git+https://…?rev=<rev>` (also `git+ssh`). Branch names and tags are rejected at admission, because the image is rebuilt only when the spec changes, so a floating ref would never refresh. Changing a ref or its packages changes the image tag and triggers a rebuild.

### RoundTable — Fleet Configuration

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: RoundTable
metadata:
  name: personal
spec:
  nats:
    url: nats://nats.database.svc:4222
    subjectPrefix: fleet-a
    tasksStream: fleet-a-tasks
    resultsStream: fleet-a-results
    createStreams: true
  knightSelector:
    matchLabels:
      ai.roundtable.io/roundtable: personal
```

Manages NATS streams and selects knights by label into logical fleets. Multiple RoundTables can coexist for multi-tenant isolation.

### Chain — Workflow DAG

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: Chain
metadata:
  name: security-scan
spec:
  steps:
    - name: scan
      knightRef: {name: galahad}
      task: "Scan example.com for CVEs. Report findings."
      timeoutSeconds: 300

    - name: verify
      knightRef: {name: tristan}
      dependsOn: [scan]
      task: |
        Galahad found: {{ .Steps.scan.Output }}
        Check if any of these CVEs affect our cluster.
      timeoutSeconds: 300

    - name: report
      knightRef: {name: kay}
      dependsOn: [verify]
      task: "Write a threat report from {{ .Steps.scan.Output }} and {{ .Steps.verify.Output }}"
      timeoutSeconds: 600
```

Steps run in dependency order. Parallel steps execute concurrently. Outputs are injected into downstream tasks via Go templates.

### Mission — Ephemeral Objective

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: Mission
metadata:
  name: pentest-sprint
spec:
  objective: "Penetration test the staging environment and produce a findings report."
  ttlSeconds: 3600
  timeoutSeconds: 1800
  cleanupPolicy: Delete
  knights:
    - name: lead
      role: coordinator
      ephemeral: true
      ephemeralSpec:
        domain: security
        model: claude-sonnet-4-20250514
        skills: [security, shared]
        nats:
          subjects: ["mission-pentest-sprint.tasks.security.>"]
        concurrency: 1
        workspace:
          size: 2Gi
    - name: galahad
      role: scanner
```

Missions auto-provision ephemeral knights, create isolated NATS streams, execute the objective, and clean up all resources on completion. Mix ephemeral and existing knights in the same mission.

**Lifecycle:** `Pending` → `Provisioning` → `Assembling` → `Briefing` → `Active` → `Succeeded/Failed` → `CleaningUp`

### Trigger — Start Work from an Event

A Trigger starts a Chain run, or sends one task to a knight, when something happens: a cron time, a NATS message, an authenticated webhook, or a GitHub issue or pull request matching a label query. This example runs a Chain every weekday at 07:00 New York time:

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: Trigger
metadata:
  name: weekday-security-report
spec:
  source:
    schedule:
      cron: "0 7 * * 1-5"
      timeZone: America/New_York
  target:
    chainRef: {name: security-report}
  input: |
    Generate the security report for {{ .Event.Time.Format "Monday 2006-01-02" }}.
```

The full file (with concurrency and deadline settings) is [`examples/trigger-schedule.yaml`](examples/trigger-schedule.yaml). Sources, dedupe keys and the status fields are in [docs/triggers.md](docs/triggers.md); the schedule behaviour is in [docs/scheduled-work.md](docs/scheduled-work.md).

---

## Architecture

The operator runs a Knight, Mission, Chain and Trigger controller. They route work over NATS JetStream (tasks, results, KV) to knight pods: the runtime plus skill-filter and git-sync sidecars. The dashboard reads through the Kubernetes API. JetStream topology and request flow: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Observability

Every component (operator, invoke gateway, dashboard, knights) emits
OpenTelemetry traces through a collector to the backend you choose, with one
trace per Chain run or Mission and the table on every signal. The chart also
ships Grafana dashboards and Prometheus alerts. Prompts and responses are
captured by default and redacted; turn that off per install or per table.

- [docs/observability.md](docs/observability.md): architecture, the
  `telemetry` values, exporter examples (Tempo, Jaeger, OTLP SaaS), TraceQL
  queries, dashboards, alerts and privacy
- [examples/observability/](examples/observability/): try it with Jaeger
- [Telemetry conventions](docs/design/telemetry-conventions.md): span,
  attribute and metric names

## Ecosystem

| Component | Description | Repo |
|-----------|-------------|------|
| **roundtable** | Operator + CRDs + controllers | [roundtable-k8s/roundtable](https://github.com/roundtable-k8s/roundtable) |
| **pi-knight** | Knight runtime (Node.js agent that consumes NATS tasks) | [roundtable-k8s/pi-knight](https://github.com/roundtable-k8s/pi-knight) |
| **roundtable-ui** | Dashboard for fleet monitoring and chain visualization | [roundtable-k8s/roundtable-ui](https://github.com/roundtable-k8s/roundtable-ui) |
| **arsenal** | Skill repository (git-synced to knights via sidecar) | [roundtable-k8s/arsenal](https://github.com/roundtable-k8s/arsenal) |

## Quick Start

The fastest path is the [10-minute kind quickstart](docs/getting-started.md): one
script creates a local cluster with NATS and the operator, then you apply a
RoundTable, a Knight and a two-step Chain from [`examples/quickstart/`](examples/quickstart/).

```bash
hack/quickstart-kind.sh
```

On your own cluster, see [Pointing Round Table at your NATS](docs/install.md#pointing-round-table-at-your-nats).

For an existing cluster (Kubernetes 1.28+, NATS with JetStream, a model API key),
install the chart (it installs the CRDs, the operator and the [dashboard](#ecosystem)):

```bash
helm install roundtable \
  oci://ghcr.io/roundtable-k8s/charts/roundtable-operator \
  --namespace roundtable --create-namespace
```

For Flux (`OCIRepository` + `HelmRelease`) and CRD upgrade notes, see [installing Round Table](docs/install.md).

### Install from source

```bash
git clone https://github.com/roundtable-k8s/roundtable.git
cd roundtable

make install   # CRDs
make deploy IMG=ghcr.io/roundtable-k8s/roundtable:latest
```

### What Happens

1. The operator sees the `Knight` CR and creates a Deployment with the pi-knight runtime. Knights run under gVisor (`runsc`) and default-deny egress by default; opting out is explicit on the Knight ([runtime isolation](docs/runtime-isolation.md), [egress allowlists](docs/egress.md))
2. A git-sync sidecar per arsenal pulls the latest skill definitions (`spec.arsenals`; empty = the default platform arsenal)
   (an arsenal with `oci` set is instead mounted read-only as an OCI image volume at `/arsenal/<name>`, pinned by digest, with no sidecar — see `examples/knight-arsenal-oci.yaml`)
3. A skill-filter sidecar merges them into `/skills`, per skill, for the categories in `spec.skills` — later arsenals override earlier ones on a `<category>/<skill>` clash. An arsenal may also use the flat Agent Skills layout (`<skill>/SKILL.md` at the arsenal root, no category folder): a category folder is optional, and `spec.skills: [<skill>]` links that directory as `/skills/<skill>/<skill>`
4. A NATS JetStream consumer is configured with the knight's filter subjects
5. The knight starts consuming tasks from the stream

### Ways In

- **Invoke gateway** — the synchronous front door. Send a task to a knight and get the answer in the same HTTP request, or a task ID to poll if it takes longer. It also runs chains and launches missions. The gateway is in the operator's chart; see [docs/gateway.md](docs/gateway.md).

  ```bash
  curl -s -X POST "$GATEWAY/v1/knights/galahad/invoke" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d '{"task": "Scan example.com for open ports", "timeoutSeconds": 60}'
  ```

- **Dashboard** — monitor the fleet, chain runs and missions from the browser ([roundtable-ui](https://github.com/roundtable-k8s/roundtable-ui)).
- **kubectl** — apply CRs and read their status, as in the examples above.
- **NATS** — the low-level path: publish a task straight to a knight's subject, e.g. `nats pub fleet-a.tasks.security.scan '{"task": "..."}'` ([task contract](docs/knight-task-contract.md)).

### Recipes

[`examples/recipes/`](examples/recipes/) has complete fleets you can copy, each with its own README on how to install and test it:

- [`homelab-watch/`](examples/recipes/homelab-watch/): an Alertmanager webhook starts a read-only triage chain
- [`personal-fleet/`](examples/recipes/personal-fleet/): three persona knights on a shared notes volume, a scheduled briefing and an on-demand chain
- [`dev-factory/`](examples/recipes/dev-factory/): issue to merged PR, with GitHub triggers, the PR pipeline and feedback chains
- [`backlog-grooming/`](examples/recipes/backlog-grooming/): a scheduled lead knight that files capped `groomed` sub-issues
- [`nightly-digest/`](examples/recipes/nightly-digest/): a scheduled read-only knight that writes a namespace digest

## Development

```bash
# Generate CRD manifests + deep copy
make manifests generate

# Run locally against your cluster
make run

# Run tests
make test

# Pinned toolchain via Nix: `nix develop`, or the installable
# `nix build .#knight-tools` (tools in bin/, envtest assets in share/envtest)
# Build the operator image
make docker-build docker-push IMG=ghcr.io/roundtable-k8s/roundtable:latest
```

## Roadmap

Shipped: the operator, Chains, Missions (existing and ephemeral knights), Triggers, the invoke gateway and isolation by default (gVisor, default-deny egress). Open work follows the GitHub milestones:

- [ ] **M6** — Expose the API outside the cluster (auth, Cloudflare, NATS users)
- [ ] **M7** — Isolated by default + API front door (finishing the sandbox opt-out and gateway work)
- [ ] **M8** — Restore the fleet + retire kagent
- [ ] **M8.5** — Codebase health & DevEx (shared runtime core, typed task contract, operator decomposition)
- [ ] **M9** — Fleet platform (agent cards and discovery, never-lose-a-task, pluggable memory, triggers and recipes)
- [ ] **M10** — Observability standard (OTel everywhere, `roundtable_*` metrics; the backend is still being chosen, [docs/observability.md](docs/observability.md))

## Contributing

Contributions welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

Areas where help is needed (see the milestones above):
- Exposing the API outside the cluster (M6)
- Codebase health and DevEx (M8.5)
- Observability stack and dashboards (M10)
- Additional knight runtime implementations
- Documentation

## License

Apache 2.0 — See [LICENSE](LICENSE)

---

*"On second thought, let's not go to Lambda. 'Tis a silly place."* 🏰
