# Observability

How Round Table emits traces, metrics and logs, and how to wire them to a
backend. Names (services, spans, attributes, metrics) come from the
[telemetry conventions ADR](design/telemetry-conventions.md) (#154); this page
explains how to use them.

**Backend status.** The standard stack (trace store, access control,
retention) is still being chosen in #385. Until then Round Table is
collector-first: every component sends OTLP to an OpenTelemetry Collector, and
you choose where the collector exports. The examples below cover Tempo, Jaeger
and an OTLP SaaS endpoint.

## Architecture

```
 roundtable-operator ─┐
 roundtable-gateway  ─┤
 roundtable-ui       ─┼── OTLP ──► OTel Collector ──► backend (Tempo, Jaeger, SaaS, ...)
 knights             ─┘            memory_limiter →
 (pi-knight,                       redaction → batch
  claude-knight)
```

| Component | `service.name` | What it emits |
|---|---|---|
| Operator | `roundtable-operator` | Chain and Mission root spans, step/item/iteration spans, NATS publish spans |
| Invoke gateway | `roundtable-gateway` | Root span for a single "ask a knight" call |
| Dashboard API | `roundtable-ui` | Its own HTTP spans; links to traces via `telemetry.traceUrlTemplate` |
| pi-knight / claude-knight | `pi-knight` / `claude-knight` | `invoke_agent`, `chat` and `execute_tool` spans (pi-knight#43, claude-knight#2) |

**One trace per Chain run or Mission** (decision 14). The operator creates the
root span once, stores it on the resource as `status.traceContext` (W3C trace
context) and `status.traceID`, and parents later spans from it. Tasks travel to
knights over NATS with `traceparent`/`tracestate` headers (#155), so the
knight's spans join the same trace. A single "ask a knight" call through the
gateway is its own trace. A `retry-failed` resume starts a new trace.

```
chain <chain>                          (operator, root)
└─ step <step>                         (forEach/loop steps add item <step>[<i>] /
   │                                    iteration <step> <n> children)
   └─ publish <subject>                (producer, messaging.*)
      └─ invoke_agent <knight>         (knight)
         ├─ chat <model>               (one per LLM turn)
         └─ execute_tool <tool>        (one per tool call)
   └─ (result consume span back in the operator)

mission <name>
└─ plan
└─ chain <chain> ...                   (same trace)
└─ invoke_agent <knight> ...
```

Find a run's trace ID with:

```bash
kubectl get chain <name> -o jsonpath='{.status.traceID}'
kubectl get mission <name> -o jsonpath='{.status.traceID}'
```

**The table on every signal** (decision 16). Spans and resources carry
`roundtable.table` (`none` for a knight without a table); metrics carry the
`table` label; logs carry a `table` field. Knights learn their table from the
`ROUNDTABLE_TABLE` env var, which the operator injects from the Knight's
`ai.roundtable.io/round-table` label. Install-wide signals (dashboard HTTP
metrics, reconcile errors) have no table.

Other span attributes you will query on: `roundtable.knight`,
`roundtable.knight.runtime`, `roundtable.chain`, `roundtable.chain.step`,
`roundtable.chain.step.type`, `roundtable.chain.item.index`,
`roundtable.chain.loop.iteration`, `roundtable.mission`, `roundtable.run.id`,
`roundtable.task.id`, `roundtable.attempt`, `roundtable.cost.usd`, and the
GenAI conventions `gen_ai.operation.name`, `gen_ai.provider.name`,
`gen_ai.request.model`, `gen_ai.response.model`, `gen_ai.usage.input_tokens`,
`gen_ai.usage.output_tokens`, `gen_ai.agent.name`, `gen_ai.tool.name`,
`gen_ai.tool.call.id`. See the ADR for the full list.

## Turning it on: the `telemetry` values

The chart's `telemetry` block (from
[`charts/roundtable-operator/values.yaml`](../charts/roundtable-operator/values.yaml)):

<!-- telemetry-values:begin (checked by scripts/check-observability-doc.sh) -->
```yaml
telemetry:
  enabled: false
  otlp:
    endpoint: ""          # e.g. http://otel-collector.observability:4317
    protocol: grpc        # grpc | http/protobuf
  sampling:
    ratio: 1.0            # parentbased_traceidratio
  captureContent: true    # prompt/response text on gen_ai spans (redacted); only matters when enabled
  resourceAttributes: {}  # extra key=value pairs (cluster name, env...)
  traceUrlTemplate: ""    # UI deep link, e.g. https://grafana/explore?...{traceId}
  # Optional OpenTelemetry Collector (#386): OTLP in, secrets redacted, exporters
  # plugged in through values. When enabled and otlp.endpoint is empty, the
  # operator/gateway/dashboard export to this collector's Service.
  collector:
    enabled: false
    # Pod scheduling for the collector (empty = use the global values).
    nodeSelector: {}
    affinity: {}
    tolerations: []
    image:
      repository: otel/opentelemetry-collector-contrib
      tag: "0.120.0"
    resources: {}
    redaction:
      enabled: true
      # Go (RE2) regexes. The default list is ported from the knights' shared
      # secret pattern list (pi-knight src/hooks.ts SECRET_PATTERNS, used by
      # redactSecrets). Setting this replaces the list.
      blockedValues:
        - '(?i)(?:api[_-]?key|apikey|secret[_-]?key|access[_-]?token|auth[_-]?token|bearer)\s*[:=]\s*[''"]?[A-Za-z0-9_\-/.+=]{20,}'
        - '(?i)-----BEGIN (?:RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----'
        - 'ghp_[A-Za-z0-9_]{36,}'
        - 'ghs_[A-Za-z0-9_]{36,}'
        - 'sk-[A-Za-z0-9]{40,}'
        - 'xoxb-[A-Za-z0-9\-]+'
        - 'AKIA[A-Z0-9]{16}'
    exporters:            # passed through verbatim into the collector config
      debug: {}
    pipelines:            # exporters each signal uses
      traces: [debug]
      metrics: [debug]
      logs: [debug]
    extraConfig: {}       # merged last, for processors/connectors we don't model
```
<!-- telemetry-values:end -->

- `telemetry.enabled` sets the standard `OTEL_*` variables on the operator,
  gateway and dashboard Deployments. With it off they get only
  `OTEL_SDK_DISABLED=true`.
- The operator passes the same settings on to every knight pod, so knights are
  configured in one place.
- `telemetry.collector.enabled` deploys the chart's collector
  (`<release>-roundtable-operator-otel-collector`, OTLP gRPC 4317 and HTTP 4318).
  When `telemetry.otlp.endpoint` is empty, everything exports to it. Every
  pipeline runs `memory_limiter`, `redaction`, `batch`.
- `telemetry.traceUrlTemplate` makes the dashboard link a run to its trace; it
  must contain `{traceId}`.

Every key is described in [helm-values.md](helm-values.md#telemetry).

### Per-table override: RoundTable `spec.telemetry`

A RoundTable can override the chart values for its own knights (#345), for
example to send a tenant's traces to its own collector or to turn content
capture off for one table:

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: RoundTable
metadata:
  name: fleet-a
spec:
  telemetry:
    otlpEndpoint: http://otel-collector.tenant-a:4317   # OTEL_EXPORTER_OTLP_ENDPOINT
    otlpProtocol: grpc                                   # grpc | http/protobuf
    samplingRatio: "0.25"                                # parentbased_traceidratio
    captureContent: false                                # no prompt/response text
    # disabled: true                                     # OTEL_SDK_DISABLED=true
```

Precedence: knight `spec.env` > table `spec.telemetry` > chart values. Unset
fields inherit the chart values. OTLP auth headers are not modelled.

## Exporter examples

All examples enable the chart collector and replace its default `debug`
exporter. Helm merges maps, so remove `debug` with `null`.

### Jaeger (try it out)

A ready-made, tested example lives in
[`examples/observability/`](../examples/observability/README.md): an in-memory
Jaeger all-in-one plus `values-telemetry.yaml`. The exporter part is:

```yaml
telemetry:
  enabled: true
  collector:
    enabled: true
    exporters:
      debug: null
      otlp/jaeger:
        endpoint: jaeger.roundtable.svc.cluster.local:4317
        tls: {insecure: true}
    pipelines:
      traces: [otlp/jaeger]
      metrics: []
      logs: []
```

Jaeger there keeps traces in memory with no authentication; it is not for
production.

### Grafana Tempo

```yaml
telemetry:
  enabled: true
  # Dashboard deep link: your Grafana trace view URL with {traceId} in it.
  traceUrlTemplate: "https://grafana.example.com/explore?...{traceId}"
  collector:
    enabled: true
    exporters:
      debug: null
      otlp/tempo:
        endpoint: tempo-distributor.observability.svc.cluster.local:4317
        tls: {insecure: true}          # in-cluster plaintext; use TLS across clusters
    pipelines:
      traces: [otlp/tempo]
      metrics: []
      logs: []
```

Adjust the Tempo Service name to your install.

### An OTLP SaaS endpoint

Most hosted backends accept OTLP over HTTP with an API-key header:

```yaml
telemetry:
  enabled: true
  collector:
    enabled: true
    exporters:
      debug: null
      otlphttp/saas:
        endpoint: https://otlp.vendor.example.com
        headers:
          x-api-key: "<api key>"
    pipelines:
      traces: [otlphttp/saas]
      metrics: []
      logs: []
```

The collector config is rendered into a ConfigMap, and the chart does not
mount Secrets into the collector, so a key written here is stored in plain
text. If that is not acceptable, run your own collector with the key in a
Secret and point `telemetry.otlp.endpoint` (and, per table,
`spec.telemetry.otlpEndpoint`) at it. Keep redaction on in that collector:
see [Privacy](#privacy).

Metrics can also go through the collector (add an exporter to
`pipelines.metrics`), but the shipped dashboards and alerts read the
Prometheus metrics scraped from the pods (see below).

## Example TraceQL queries

For Tempo. Unscoped `.roundtable.table` matches both the span and the
resource attribute.

Which step was slow, in a chain run:

```
{ resource.service.name = "roundtable-operator" && span.roundtable.run.id = "<run id>" && name =~ "step .*" }
  | select(span.roundtable.chain.step, span.roundtable.attempt, duration)
```

Steps of a chain that took longer than ten minutes, across runs:

```
{ span.roundtable.chain = "pr-pipeline" && name =~ "step .*" && duration > 10m }
```

Which tool failed:

```
{ span.gen_ai.operation.name = "execute_tool" && status = error }
  | select(span.gen_ai.tool.name, span.roundtable.knight, .roundtable.table)
```

Slow LLM turns, by model:

```
{ span.gen_ai.operation.name = "chat" && duration > 60s }
  | select(span.gen_ai.request.model, span.gen_ai.usage.input_tokens, span.gen_ai.usage.output_tokens)
```

Expensive knight tasks on one table:

```
{ .roundtable.table = "fleet-a" && span.gen_ai.operation.name = "invoke_agent" && span.roundtable.cost.usd > 1 }
```

Tasks that were redelivered:

```
{ span.roundtable.attempt > 1 }
```

In Jaeger, search by service and tags instead, for example
`roundtable.run.id=<run id>`, or `gen_ai.operation.name=execute_tool error=true`.

## Dashboards and alerts

These read Prometheus metrics (the `roundtable_*` scheme in the ADR), not
traces. Enable scraping with `metrics.enabled`, `metrics.serviceMonitor.enabled`
and `metrics.podMonitor.enabled`.

**Dashboards** (`dashboards.enabled`, #331): rendered as Grafana sidecar
ConfigMaps or `GrafanaDashboard` CRs.

- *Round Table / Fleet overview*: knights by phase, tasks per minute, task error rate, active tasks, NATS connected, KEDA desired vs
  actual replicas, queue depth by knight (pending tasks from KEDA's `keda_scaler_metrics_value` for the `nats-jetstream` scaler; needs KEDA's metrics scraped and covers only knights with a ScaledObject, i.e. `maxReplicas > 1` or scale-to-zero).
- *Round Table / Chains and platform*: chain runs by status, p95 step
  duration, provider retries, missions by phase, dead letters open, ready
  knight consumers, isolation state and wake latency.
- *Round Table / Cost and tokens*: 24h spend, spend rate per model and per
  knight, spend per table, tokens per model and per knight, cost per job
  (chain, step, knight) and cost per mission.
- *Round Table / Triggers and gateway*: trigger firings per minute by outcome,
  time since each Trigger last fired, trigger error and skipped share, gateway
  requests by route and code, gateway p95 latency and 5xx rate, dead letters
  by reason and open count.

Every dashboard has a `table` variable applied to every `roundtable_*` query
(the gateway p95 latency panel excepted: its histogram has no `table` label).

**Alerts** (`alerts.enabled`, #332): a `PrometheusRule` with

| Alert | Fires when |
|---|---|
| `KnightTaskFailures` | a knight fails more than `alerts.taskFailuresPerHour` tasks in an hour |
| `KnightNatsDisconnected` | a knight lost its NATS connection |
| `KnightNatsFlapping` | a knight reconnected to NATS more than `alerts.natsReconnectsPer30m` times in 30m |
| `KnightMetricsAbsent` | no knight metrics are scraped |
| `KnightTaskLost` | tasks were delivered to a knight that stopped before finishing |
| `KnightLLMCostSpike` | a table's 24h LLM spend exceeds `alerts.costSpikeUSDPer24h` |
| `RoundtableChainStepsSlow` | a chain's p95 step duration exceeds `alerts.chainStepSlowSeconds` |
| `RoundtableDeadLettersOpen` | dead letters stay open for `alerts.deadLettersOpenMinutes` |
| `KnightNotReady` | a knight stays `Pending`, `Provisioning` or `Degraded` for `alerts.knightNotReadyMinutes` |
| `RoundtableToolsImageBuildFailed` | a knight's tools image build Job (`<knight>-toolsimg-<tag>`) failed; its pod rollout is held |
| `KnightConsumerNotReady` | a knight's task consumer is missing or misconfigured for 10m |
| `KnightNotIsolated` | a knight's `Isolated` reason is a failure (not `Isolated`, `OptedOut` or `IsolationDisabled`) for 10m |
| `RoundtableChainRunsFailing` | the failed share of chain runs in 1h exceeds `alerts.chainFailureRatio` |
| `RoundtableGatewayErrors` | a gateway route's 5xx share over 15m exceeds `alerts.gatewayErrorRatio` (routes with fewer than 20 requests are ignored) |
| `RoundtableReconcileErrors` | an operator controller's `controller_runtime_reconcile_errors_total` rate stays above 0 for 15m |
| `RoundtableScheduleMissed` | a scheduled Trigger's CronJob (`trigger-*`, not suspended) is more than `alerts.scheduleMissedSeconds` past its next scheduled time (kube-state-metrics) |
| `RoundtableScheduledRunFailed` | a scheduled knight or mission run finished `failed` or `lost` in the last hour (`roundtable_trigger_runs_finished_total`) |
| `RoundtableScheduleJobFailed` | a scheduled Trigger's run Job failed (kube-state-metrics) |
| `RoundtableTriggerFiringErrors` | a Trigger's firings reported `outcome="error"` in the last hour (10m) |
| `RoundtablePodRestarting`, `RoundtablePodOOMKilled`, `RoundtablePodEvicted` | pod health |
| `RoundtablePVCAlmostFull` | a PVC is nearly full |
| `RoundtableKedaScalerErrors`, `RoundtableHPAScalingInactive` | scaling problems (`alerts.scaling.enabled`) |
| `RoundtableQueueBacklog` | a knight's pending tasks stay above `alerts.queueBacklog.pending` for `alerts.queueBacklog.forMinutes` (opt-in: `alerts.queueBacklog.enabled`) |

When an alert names a chain or knight, use the TraceQL queries above with the
same `table`, `chain` or `knight` to find the slow or failing span. Details and
thresholds: [helm-values.md](helm-values.md#grafana-dashboards).

## Operator and gateway metrics

Every `roundtable_*` metric the operator and the invoke gateway register, with
its type, labels and meaning. `scripts/check-metrics-doc.sh` (run by
`make helm-check`) fails when this table and the code disagree on a name or its
labels. The operator's metrics are in `pkg/metrics/metrics.go`; the gateway
uses the same registry. The trigger and gateway metrics are also described
below with their label values.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `roundtable_knights` | gauge | `table`, `phase` | Knights per table and phase; knights no RoundTable counts (none selects them, and none owns them as planner) are counted under `table="none"` |
| `roundtable_knight_wake_seconds` | histogram | `table`, `knight`, `namespace` | Time from a scaled-to-zero knight entering Waking (tasks queued) until it is Ready. Buckets 1s to ~8.5m; condition timestamps have 1s resolution, so values are whole seconds |
| `roundtable_knight_isolated` | gauge | `table`, `knight`, `reason` | 1 for the knight's current Isolated condition reason (`Isolated` when isolated) |
| `roundtable_knight_consumer_ready` | gauge | `table`, `knight` | 1 when the knight's durable task consumer has the expected config, 0 otherwise |
| `roundtable_tasks_dead_lettered_total` | counter | `table`, `knight`, `reason` | Tasks written to the dead-letter stream; `reason` is `lost`, `failed`, `timeout`, `usage_limited` or `refused` |
| `roundtable_dead_letters_open` | gauge | `table`, `knight` | Dead-letter entries still open (neither resolved by a later success nor retried) |
| `roundtable_chain_runs_total` | counter | `table`, `chain`, `status` | Chain runs that reached a terminal phase; `status` is `succeeded`, `partially_succeeded` or `failed` |
| `roundtable_chain_noop_runs_total` | counter | `table`, `chain` | Chain runs that completed without executing any step (all outputs restored from cache) |
| `roundtable_chain_provider_retries_total` | counter | `table`, `chain`, `step`, `reason` | Chain step retries after a transient model provider failure; `reason` is `overloaded`, `provider_5xx`, `network` or `unknown` |
| `roundtable_chain_step_duration_seconds` | histogram | `table`, `chain`, `step`, `status` | Duration of finished chain steps; `status` is `succeeded`, `failed` or `skipped` |
| `roundtable_chain_step_cost_usd_total` | counter | `table`, `chain`, `step`, `knight`, `billing` | Cost in USD of finished chain step attempts |
| `roundtable_chain_step_tokens_total` | counter | `table`, `chain`, `step`, `knight`, `type` | Tokens of finished chain step attempts; `type` is `input`, `output`, `cacheRead` or `cacheWrite` |
| `roundtable_missions` | gauge | `table`, `phase` | Missions per table and phase; missions with no `roundTableRef`, or one naming a table that does not exist, are counted under `table="none"` |
| `roundtable_mission_cost_usd_total` | counter | `table`, `mission`, `billing` | Cost in USD of a mission's finished step attempts |
| `roundtable_table_cost_usd` | gauge | `table` | Cumulative cost in USD of a table, set from `RoundTable.status.totalCost` |
| `roundtable_trigger_firings_total` | counter | `table`, `trigger`, `source`, `outcome` | Trigger events by source (`schedule`, `nats`, `webhook`, `github`) and outcome (`started`, `deduped`, `skipped`, `error`) |
| `roundtable_trigger_runs_finished_total` | counter | `table`, `trigger`, `source`, `outcome` | Knight and mission Trigger runs that finished, by `outcome` (`succeeded`, `failed`, `lost`); chain targets are in `roundtable_chain_runs_total` |
| `roundtable_trigger_last_fired_timestamp_seconds` | gauge | `table`, `trigger` | Unix time a Trigger last started a run |
| `roundtable_gateway_requests_total` | counter | `route`, `code`, `table`, `caller_kind` | Invoke gateway HTTP requests by route template, status code, table and caller kind |
| `roundtable_gateway_request_duration_seconds` | histogram | `route` | Invoke gateway HTTP request duration |

## Trigger and gateway metrics

Both carry the `table` label (`none` when unknown). A Trigger's table is its
target chain's `roundTableRef`.

| Metric | Type | Labels |
|---|---|---|
| `roundtable_trigger_firings_total` | counter | `table`, `trigger`, `source` (`schedule`, `nats`, `webhook`, `github`), `outcome` |
| `roundtable_trigger_runs_finished_total` | counter | `table`, `trigger`, `source`, `outcome` (`succeeded`, `failed`, `lost`) |
| `roundtable_trigger_last_fired_timestamp_seconds` | gauge | `table`, `trigger` |
| `roundtable_gateway_requests_total` | counter | `table`, `route`, `code`, `caller_kind` (`serviceaccount`, `token`, `none`) |
| `roundtable_gateway_request_duration_seconds` | histogram | `route` |

`outcome` is `started` (a run started), `deduped` (its run already exists),
`skipped` (concurrency, a missed `startingDeadlineSeconds`, a filter) or `error`
(a template that cannot render, or a failed attempt, which is retried and
counted again). A repeated `Nats-Msg-Id` is dropped by the JetStream stream
before the Trigger sees it, so stream-level duplicates are not counted; only a
repeated `dedupeKey` counts as `deduped`. A deleted Trigger's last-fired series
is removed. `route` is the registered route template, such as
`POST /v1/knights/{name}/invoke`, never the raw path; requests that match no
route or fail authentication count under `unmatched`. The gateway serves
`/metrics` without authentication on its own port (`--metrics-listen`, set to
`:8081` when `metrics.enabled`), separate from the API port.

The shipped rules (`alerts.enabled`) cover schedules: `RoundtableScheduleMissed`
and `RoundtableScheduleJobFailed` read kube-state-metrics for the Trigger
CronJobs, and `RoundtableTriggerFiringErrors` reads the counter above. Use
`time() - roundtable_trigger_last_fired_timestamp_seconds` for an ad-hoc check
of a Trigger's last firing: a fixed threshold is wrong for schedules with
different periods. Example: `sum by (route) (rate(roundtable_gateway_requests_total{code=~"5.."}[5m]))`
finds failing gateway routes.

## Cost and token totals per run, step and mission

Each finished task attempt reports `cost` (USD), `tokens` (`input`, `output`
and optionally `cacheRead`, `cacheWrite`; `input` excludes cached input, see
[knight-task-contract.md](knight-task-contract.md#usage-cost-billing-and-tokens))
and `billing` in its result. The operator sums them:

- **Status.** `Chain.status.costUSD` and `status.tokens` are the totals of the
  current (or most recent) run; every step has the same `costUSD` and `tokens`,
  summed over its retries, fan-out items and loop rounds.
  `Mission.status.totalCost` and `status.tokens` sum the mission's chains
  (`status.chainStatuses[]` carries each chain's numbers); `totalCost` is never
  below what `costBudgetUSD` is checked against.
- **Metrics**, incremented once per finished attempt (a retried step counts
  every attempt):

| Metric | Labels |
|---|---|
| `roundtable_chain_step_cost_usd_total` | `table`, `chain`, `step`, `knight`, `billing` |
| `roundtable_chain_step_tokens_total` | `table`, `chain`, `step`, `knight`, `type` (`input`, `output`, `cacheRead`, `cacheWrite`) |
| `roundtable_mission_cost_usd_total` | `table`, `mission`, `billing` |

The `chain` label of a run copy started by issue pickup, a Trigger or the
gateway is the **template or target chain's name**, not the per-run copy's
(read from the `ai.roundtable.io/run-template` annotation), so cardinality
stays bounded by the number of templates. Look up a single run's cost in its
`status`.

`billing` is what the result reports (`api`, `subscription`, ...), or
`unknown` when it reports none. Subscription-billed models report a notional
cost; filter on `billing` to separate real spend.

`roundtable_table_cost_usd{table}` is a gauge set from `RoundTable.status.totalCost`:
the table's Missions' `totalCost` plus its Chains' current (or most recent) run
`costUSD`. It drops when a chain or mission is deleted or a chain starts a new
run, so trust the `*_cost_usd_total` counters above for spend over time.

```promql
# cost per job (chain) per day
sum by (chain) (increase(roundtable_chain_step_cost_usd_total[1d]))

# real spend only
sum by (table) (increase(roundtable_chain_step_cost_usd_total{billing!="subscription"}[1d]))

# cost per mission over 24h, real spend only
sum by (mission) (increase(roundtable_mission_cost_usd_total{billing!="subscription"}[24h]))

# output tokens per step
sum by (chain, step) (increase(roundtable_chain_step_tokens_total{type="output"}[1d]))
```

## Privacy

**Captured by default** (decision 15). Prompts, system instructions and
responses go on `chat` spans as `gen_ai.input.messages`,
`gen_ai.output.messages` and `gen_ai.system_instructions`; tool arguments and
results go on `execute_tool` spans. Each content attribute is capped (16 KiB by
default); cut values get `roundtable.content.truncated=true`. Activity previews
on the dashboard follow the same switch.

**Redaction happens twice.**

1. In-process: the knights pass every value through the shared `redactSecrets`
   pattern list before setting it.
2. In the collector: the chart collector's `redaction` processor
   (`telemetry.collector.redaction`) masks the same patterns for every source,
   including Claude Code's native spans, which cannot be redacted in-process.
   Add your own patterns to `blockedValues` (setting it replaces the default
   list, so copy the defaults in). If you run your own collector instead, add
   an equivalent `redaction` processor to it.

Redaction is pattern-based: it catches known credential formats, not every
secret or personal detail a prompt may contain.

**Turning content off.** Any one of these turns capture off; spans keep their
names, durations, token counts, model and cost:

- whole install: `telemetry.captureContent: false`
- one table: RoundTable `spec.telemetry.captureContent: false` (also sets
  `ACTIVITY_CONTENT=none` for that table's knights)
- one knight: `ROUNDTABLE_TELEMETRY_CAPTURE_CONTENT=false` in its `spec.env`

**Access and retention** are not decided yet: they come with the standard
stack in #385. Until then they are whatever your backend enforces. Traces can
contain (redacted) prompt text, so restrict who can read the trace store and
set a retention period that matches your data policy.
