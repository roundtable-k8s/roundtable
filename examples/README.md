# Examples

Real-world examples of Round Table resources.

| File | Description |
|------|-------------|
| [`quickstart/`](quickstart/) | Minimal RoundTable, Knight and two-step Chain for the [kind quickstart](../docs/getting-started.md) |
| [`dev/`](dev/) | One RoundTable and one Knight for the [Tilt inner loop](../docs/local-development.md) |
| [`knight-basic.yaml`](knight-basic.yaml) | A single knight — security domain with NATS config |
| [`knight-ollama.yaml`](knight-ollama.yaml) | A knight on a local model: `ollama/<tag>` served in-cluster, `KNIGHT_MODEL_BASE_URL`, reached through `egress.allowedServices`, no hosted key |
| [`knight-scaled.yaml`](knight-scaled.yaml) | A horizontally scaled knight — ephemeral workspace, KEDA 0..N on pending tasks |
| [`knight-scheduling.yaml`](knight-scheduling.yaml) | A coder knight pinned to large nodes with `nodeSelector` and a toleration |
| [`knight-mcp-headers.yaml`](knight-mcp-headers.yaml) | A claude-knight using a remote MCP server with an `Authorization` header from a Secret (`spec.mcpServers[].headersFrom`, [docs](../docs/crd-design.md#7-knight-specmcpservers-claude-knight-only)) |
| [`knight-persona-jobs.yaml`](knight-persona-jobs.yaml) | A tool-less persona Knight and two job Chains on it, run through the gateway ([migrating from kagent](../docs/migrating-from-kagent.md)) |
| [`knight-arsenal-oci.yaml`](knight-arsenal-oci.yaml) | A knight whose skills come from an OCI arsenal artifact |
| [`knight-fork-from-snapshot.yaml`](knight-fork-from-snapshot.yaml) | A knight whose workspace is forked from a snapshot ([docs](../docs/workspace-snapshots.md)) |
| [`knight-templates/`](knight-templates/) | RoundTable `knightTemplates` and a Mission that uses them, see [README](knight-templates/README.md) |
| [`flux/`](flux/) | Install the operator with Flux: Namespace, OCIRepository and HelmRelease |
| [`roundtable-fleet.yaml`](roundtable-fleet.yaml) | Full fleet config with a sandbox mission-knight template, policies, and NATS streams |
| [`chain-sequential.yaml`](chain-sequential.yaml) | Multi-step chain with output templating between steps |
| [`chain-model-override.yaml`](chain-model-override.yaml) | Per-step model override with RoundTable `modelAliases` |
| [`chain-fan-out.yaml`](chain-fan-out.yaml) | `forEach` fan-out/fan-in over a planner's JSON list |
| [`chain-scheduled-briefing.yaml`](chain-scheduled-briefing.yaml) | Scheduled briefing: a cron Chain fans out to domain knights in parallel, then `kay` writes the synthesis to `Briefings/` in the vault (see [Scheduled briefing](#scheduled-briefing)) |
| [`chain-fleet-self-improvement.yaml`](chain-fleet-self-improvement.yaml) | Fleet self-improvement recipe: a weekly Chain where the lead lists the table's knights, collects a self-assessment from each via `forEach`, and writes one report ([docs](../docs/recipe-fleet-self-improvement.md)) |
| [`chain-pr-pipeline.yaml`](chain-pr-pipeline.yaml) | PR pipeline: plan → implement (or `forEach` over sub-issues) → review → bounded fix `loop` → merge, or escalate via `when`. Rendered from the chart's `prPipeline` values ([docs](../docs/pr-pipeline.md)) |
| [`chain-pr-pipeline-postrun.yaml`](chain-pr-pipeline-postrun.yaml) | The PR pipeline with `prPipeline.postRun: chain`: `verify`, `close_parent` and `handback` steps check the run on GitHub instead of the operator ([docs](../docs/pr-pipeline.md#post-run-checks-builtin-or-chain)) |
| [`roundtable-issue-pickup.yaml`](roundtable-issue-pickup.yaml) | Issue pickup: poll `rt:ready` issues and start one PR pipeline Chain per issue ([docs](../docs/issue-pickup.md)) |
| [`trigger-nats.yaml`](trigger-nats.yaml) | Trigger: start a Chain run per message on `fleet-a.events.deploys.>`, input rendered from the event ([docs](../docs/triggers.md)) |
| [`trigger-webhook.yaml`](trigger-webhook.yaml) | Trigger: start a Chain run per Alertmanager webhook delivered through the gateway, authenticated with the Trigger's own token ([docs](../docs/triggers.md#webhook-source)) |
| [`gateway-serviceaccount-cronjob.yaml`](gateway-serviceaccount-cronjob.yaml) | A CronJob that runs a chain through the gateway and waits, authenticated with its own projected ServiceAccount token instead of a stored one ([docs](../docs/gateway.md#calling-from-a-workload)) |
| [`trigger-github.yaml`](trigger-github.yaml) | Trigger: start a Chain run per open issue labelled `security-review` in two repositories, then relabel it ([docs](../docs/triggers.md#github-source)) |
| [`trigger-schedule.yaml`](trigger-schedule.yaml) | Trigger: start a Chain on a cron schedule with input computed from the schedule time ([docs](../docs/triggers.md)) |
| [`trigger-knight-task.yaml`](trigger-knight-task.yaml) | Trigger: send one task straight to a knight, nightly and per alert, without a Chain ([docs](../docs/triggers.md#knight-target)) |
| [Scheduled work](../docs/scheduled-work.md) | The three schedule targets (task, Chain, Mission), the one-task-naming-a-skill pattern, and what happens to missed, overlapping, lost and deleted runs |
| [`trigger-mission.yaml`](trigger-mission.yaml) | Trigger: launch a read-only backlog-review Mission every Monday, planned by the table's planner knight ([docs](../docs/triggers.md#mission-target)) |
| [`recipes/`](recipes/) | Copyable fleets, see [README](recipes/README.md): [`homelab-watch/`](recipes/homelab-watch/) starts a read-only triage chain from an Alertmanager webhook; [`nightly-digest/`](recipes/nightly-digest/) sends a scheduled one-task digest to a knight through a skill with scripts; [`research-digest/`](recipes/research-digest/) watches a few feeds weekly from an egress allowlist and remembers what it reported; [`backlog-grooming/`](recipes/backlog-grooming/), [`dev-factory/`](recipes/dev-factory/) and [`personal-fleet/`](recipes/personal-fleet/) |
| [`observability/`](observability/) | Try-it-out tracing: Jaeger all-in-one plus the chart's OTel collector (`values-telemetry.yaml`), see [README](observability/README.md) and [docs](../docs/helm-values.md) |
| [`chain-pr-feedback.yaml`](chain-pr-feedback.yaml) | PR pipeline feedback: the operator feeds a red CI run or merge-queue rejection to the PR's author knight (`resumeRunId` resumes its session), then the lead re-queues, or escalates. Rendered from the chart's `prPipeline.feedback` values ([docs](../docs/pr-feedback.md)) |
| [`egress/`](egress/) | Starter egress allowlists (GitHub read, package docs, model providers, research, feeds); see [docs](../docs/egress.md) |
| [`mission-ephemeral.yaml`](mission-ephemeral.yaml) | Mission using existing fleet knights |
| [`mission-meta.yaml`](mission-meta.yaml) | Meta-mission — planner auto-generates chains and knights |
| [`mission-backlog-review.yaml`](mission-backlog-review.yaml) | Meta-mission showcase: read-only backlog review on `roundtable-dev` with standing knights (recorded run: 5 min, $1.58) |
| [`mission-archive-results.yaml`](mission-archive-results.yaml) | Mission whose final chain step has kay write a dated summary to `Briefings/` through `outputPath` (see [Keeping mission output](../docs/keeping-mission-output.md)) |

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

## Adapting the examples

The examples come from one author's cluster. Before applying one, change what
is theirs:

| What | Where | Change to |
|------|-------|-----------|
| NATS URL | `spec.nats.url` on RoundTables and Knights | Your NATS server. Examples use `nats://nats.database.svc:4222` (the author's namespace `database`); [`quickstart/`](quickstart/), [`dev/`](dev/) and [`egress/model-providers.yaml`](egress/model-providers.yaml) use `nats://nats.roundtable.svc:4222`, where the [quickstart](../docs/getting-started.md) installs NATS. These are the only two URLs a test accepts in examples. **A Knight does not inherit the RoundTable's URL**: one without `spec.nats.url` connects to the CRD default, `nats://nats.database.svc:4222`, so add `spec.nats.url` to every Knight (the `sed` below does not add it) |
| Namespace | `metadata.namespace`, `roundTableRef`, egress `allowedServices` | The namespace you installed into (`roundtable` in the quickstart) |
| Subject prefix and stream names | `spec.nats.subjectPrefix`, `stream`, `resultsStream` on the RoundTable and on every Knight (a knight does not inherit them) | Your table's values (`fleet-a` and its streams in most examples); change them together |
| Model or alias | `spec.model`, step `model` | A model your key can use. `deep`, `fast` and similar aliases resolve only if the RoundTable defines `spec.modelAliases` |
| Secrets | `llm-api-keys` and token Secrets (`secretKeyRef`, `headersFrom`) | Create them in your namespace, or point at yours; none are in the examples |
| `storageClassName` | PVCs, where present | A class that exists in your cluster (RWX where the example says so) |

To rewrite the NATS URLs that are written out in the files you copied (works
with GNU and BSD sed; run it in the copied directory, it recurses):

```bash
find . -name '*.yaml' -exec sed -i.bak 's#nats://nats\.\(database\|roundtable\)\.svc:4222#nats://YOUR-NATS.YOUR-NS.svc:4222#g' {} + \
  && find . -name '*.yaml.bak' -delete
```

It only changes URLs that are present. A Knight that sets no `spec.nats.url`
still uses the CRD default (the author's server), so add
`nats: {url: nats://YOUR-NATS.YOUR-NS.svc:4222}` to each such Knight by hand.

## Getting started

To install the operator and NATS and run a first Chain, follow
[`docs/getting-started.md`](../docs/getting-started.md), which applies
[`quickstart/`](quickstart/). Then try the files above, adapted as described in
[Adapting the examples](#adapting-the-examples), or copy a fleet from
[`recipes/`](recipes/).
