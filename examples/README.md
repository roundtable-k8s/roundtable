# Examples

Real-world examples of Round Table resources.

| File | Description |
|------|-------------|
| [`knight-basic.yaml`](knight-basic.yaml) | A single knight — security domain with NATS config |
| [`knight-scaled.yaml`](knight-scaled.yaml) | A horizontally scaled knight — ephemeral workspace, KEDA 0..N on pending tasks |
| [`knight-mcp-headers.yaml`](knight-mcp-headers.yaml) | A claude-knight using a remote MCP server with an `Authorization` header from a Secret (`spec.mcpServers[].headersFrom`, [docs](../docs/crd-design.md#7-knight-specmcpservers-claude-knight-only)) |
| [`knight-persona-jobs.yaml`](knight-persona-jobs.yaml) | A tool-less persona Knight and two job Chains on it, run through the gateway ([migrating from kagent](../docs/migrating-from-kagent.md)) |
| [`roundtable-fleet.yaml`](roundtable-fleet.yaml) | Full fleet config with warm pool, policies, and NATS streams |
| [`chain-sequential.yaml`](chain-sequential.yaml) | Multi-step chain with output templating between steps |
| [`chain-model-override.yaml`](chain-model-override.yaml) | Per-step model override with RoundTable `modelAliases` |
| [`chain-fan-out.yaml`](chain-fan-out.yaml) | `forEach` fan-out/fan-in over a planner's JSON list |
| [`chain-scheduled-briefing.yaml`](chain-scheduled-briefing.yaml) | Scheduled briefing: a cron Chain fans out to domain knights in parallel, then `kay` writes the synthesis to `Briefings/` in the vault (see [Scheduled briefing](#scheduled-briefing)) |
| [`chain-pr-pipeline.yaml`](chain-pr-pipeline.yaml) | PR pipeline: plan → implement (or `forEach` over sub-issues) → review → bounded fix `loop` → merge, or escalate via `when`. Rendered from the chart's `prPipeline` values ([docs](../docs/pr-pipeline.md)) |
| [`roundtable-issue-pickup.yaml`](roundtable-issue-pickup.yaml) | Issue pickup: poll `rt:ready` issues and start one PR pipeline Chain per issue ([docs](../docs/issue-pickup.md)) |
| [`trigger-nats.yaml`](trigger-nats.yaml) | Trigger: start a Chain run per message on `fleet-a.events.deploys.>`, input rendered from the event ([docs](../docs/triggers.md)) |
| [`trigger-schedule.yaml`](trigger-schedule.yaml) | Trigger: start a Chain on a cron schedule with input computed from the schedule time ([docs](../docs/triggers.md)) |
| [`observability/`](observability/) | Try-it-out tracing: Jaeger all-in-one plus the chart's OTel collector (`values-telemetry.yaml`), see [README](observability/README.md) and [docs](../docs/helm-values.md) |
| [`chain-pr-feedback.yaml`](chain-pr-feedback.yaml) | PR pipeline feedback: the operator feeds a red CI run or merge-queue rejection to the PR's author knight (`resumeRunId` resumes its session), then the lead re-queues, or escalates. Rendered from the chart's `prPipeline.feedback` values ([docs](../docs/pr-feedback.md)) |
| [`egress/`](egress/) | Starter egress allowlists (GitHub read, package docs, model providers, research, feeds); see [docs](../docs/egress.md) |
| [`mission-ephemeral.yaml`](mission-ephemeral.yaml) | Mission using existing fleet knights |
| [`mission-meta.yaml`](mission-meta.yaml) | Meta-mission — planner auto-generates chains and knights |

## Scheduled briefing

A non-development fleet pattern: a cron-triggered Chain runs domain knights in
parallel, then one knight combines their output and writes a dated note.

- [`chain-scheduled-briefing.yaml`](chain-scheduled-briefing.yaml) — `schedule` and
  `timeZone`; steps `security` (galahad) and `infrastructure` (tristan) have no
  `dependsOn`, so they run in parallel; `synthesis` (kay) depends on both and sets
  `outputPath: "Briefings/{{ .Date }}-{{ .Chain }}.md"`. `outputKnightRef: kay` is
  the knight that writes that file.
- [`kay.yaml`](kay.yaml) — the writer knight. It may write only to `Briefings/`.

Apply it with the fleet and its knights:

```bash
kubectl apply -f roundtable-fleet.yaml -f galahad.yaml -f tristan.yaml -f kay.yaml
kubectl apply -f chain-scheduled-briefing.yaml
kubectl get chain daily-briefing -n roundtable   # Schedule, TimeZone and Next columns
```

Trigger a run by hand, without waiting for the schedule. Any new annotation value
starts a fresh run:

```bash
kubectl annotate chain daily-briefing -n roundtable \
  ai.roundtable.io/run="$(date +%s)" --overwrite
```

## Quick Start

```bash
# 1. Install the operator
helm install roundtable oci://ghcr.io/roundtable-k8s/charts/roundtable-operator \
  --namespace roundtable --create-namespace

# 2. Create a fleet
kubectl apply -f roundtable-fleet.yaml

# 3. Deploy a knight
kubectl apply -f knight-basic.yaml

# 4. Watch it come online
kubectl get knights -n roundtable -w

# 5. Run a chain
kubectl apply -f chain-sequential.yaml

# 6. Launch a mission
kubectl apply -f mission-ephemeral.yaml
kubectl get missions -n roundtable -w
```
