# Telemetry conventions

Status: accepted (whiteboard decisions 14-17, #377). Issue: #154.

This is the one telemetry contract for the operator, the invoke gateway, the
dashboard API, pi-knight and claude-knight. It is the source of names for #57,
#155, #156, #157, #159, roundtable-ui#151, pi-knight#43 and claude-knight#2 (and
#343-#346, #386). Instrumentation code, the metric renames in the knight
runtimes, and the choice of backend, access and retention (#385) are out of
scope here.

The doc assumes **no backend**. Components emit OTLP to a collector (#386); the
collector decides where signals go.

## Decisions

| # | Decision |
|---|---|
| 14 | One trace per chain run or mission. Tasks are child spans. A single "ask a knight" call is its own trace. |
| 15 | Prompts and responses are captured in traces, on by default, redacted before export and size-bounded. |
| 16 | Metrics use one `roundtable_*` scheme. Every signal carries the table. |
| 17 | Vendor-neutral: OTel GenAI semantic conventions, collector in front, no backend assumed. |

## Services

| Component | `service.name` |
|---|---|
| Operator | `roundtable-operator` |
| Invoke gateway | `roundtable-gateway` |
| Dashboard API | `roundtable-ui` |
| pi-knight | `pi-knight` |
| claude-knight | `claude-knight` |

Any future runtime sets its own `service.name` and honours the rest of this doc.

### Environment contract

Every component honours the standard OTel SDK variables. A runtime that does so
gets telemetry with no further wiring.

| Variable | Meaning |
|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Collector endpoint. Unset means no export. |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `grpc` or `http/protobuf`. |
| `OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG` | Sampling. Sampling is parent-based so a trace is kept or dropped as a whole. |
| `OTEL_RESOURCE_ATTRIBUTES` | Extra resource attributes. |
| `OTEL_SDK_DISABLED` | `true` turns the SDK off. |
| `ROUNDTABLE_TELEMETRY_CAPTURE_CONTENT` | `true` (default) or `false`. See [Content capture](#content-capture). |

## The table on every signal

An install has many tables, so every signal that is about one table says which.

| Signal | Where the table goes |
|---|---|
| Resource and span attributes | `roundtable.table` |
| Metrics | label `table` |
| Logs | field `table` |
| Activity events | field `table` (optional, additive, stays `v: 1`) |
| Agent cards | in the key `<table>.<knight>` and as a field |

Knight pods learn their table from the `ROUNDTABLE_TABLE` env var, injected by
the operator from the Knight's `ai.roundtable.io/round-table` label (legacy
`ai.roundtable.io/table` is read as a fallback). Empty means the knight has no
table: report it as `none`.

**Install-wide signals omit `table`**: dashboard HTTP metrics
(`roundtable_ui_*`), gateway process metrics, reconcile errors and anything else
not about one table. Do not invent a placeholder value for them.

The earlier draft attribute `roundtable.name` (it meant the table) is retired in
favour of `roundtable.table`.

## Attributes

Resource attributes describe the process; span attributes describe the work.
The same key is used for both where the value is stable for the process.

| Attribute | Notes | Activity event field |
|---|---|---|
| `roundtable.table` | Table name, or `none`. | `table` |
| `roundtable.knight` | Knight name. | `knight` |
| `roundtable.knight.runtime` | `pi` or `claude`. | (metric label `runtime`) |
| `roundtable.chain` | Chain name. | `chainName` |
| `roundtable.chain.step` | Step name. | `stepName` |
| `roundtable.mission` | Mission name. | n/a |
| `roundtable.run.id` | Chain or mission run id. | `runId` |
| `roundtable.task.id` | Task id. | `taskId` |
| `roundtable.attempt` | 1-based delivery attempt; grows with dead-letter retries (decision 12). | n/a |
| `roundtable.cost.usd` | Cost of the span's work in USD. | `costUsd` |
| `k8s.namespace.name`, `k8s.pod.name`, `k8s.node.name` | From the downward API. | `pod`, `node` |

The activity event JSON names are the camelCase form of these; the mapping above
is normative.

## Span tree

### Chain run (trace root)

```
chain <chain>                          (operator, root)
└─ step <step>
   └─ publish <subject>                (producer, messaging.*)
      └─ invoke_agent <knight>         (knight; gen_ai.operation.name=invoke_agent, gen_ai.agent.name)
         ├─ chat <model>               (one per LLM turn)
         └─ execute_tool <tool>        (one per tool call)
   └─ (result consume span back in the operator)
```

### Mission (trace root)

```
mission <name>
└─ plan
└─ chain <chain> ...                   (chains the mission starts: same trace, not a new one)
└─ invoke_agent <knight> ...           (knight tasks the mission starts)
```

### Ask a knight through the gateway

Its own trace. The root span is in the gateway, then `publish <subject>` and
`invoke_agent <knight>` as above.

### Async reconciles

Reconciles are async and re-entrant, so the run's root span context is created
**once** and stored on the resource:

- `status.traceContext`: the W3C trace context of the root span (additive field
  on Chain and Mission).
- `status.traceID`: the trace id alone, for the dashboard to link to.

Later reconciles parent their spans from `status.traceContext`. Where a
parent/child relationship does not fit (for example a redelivered result), use a
span link instead of a parent.

The trace context travels to the knight in the NATS message headers (#155) as
`traceparent`/`tracestate`.

### Claude Code native spans

When claude-knight passes `TRACEPARENT` to Claude Code, its native spans appear
under `invoke_agent`. They map to the GenAI names as follows:

| Claude Code span | GenAI equivalent |
|---|---|
| `claude_code.interaction` | the `invoke_agent <knight>` span's body; kept as a child, not renamed |
| `claude_code.llm_request` | `chat <model>` |
| `claude_code.tool` | `execute_tool <tool>` |

claude-knight#2 owns the exact attribute mapping. Native spans cannot be
redacted in-process, which is why the collector runs a second redaction pass.

## GenAI semantic conventions

Used as defined by OpenTelemetry:

`gen_ai.operation.name`, `gen_ai.provider.name`, `gen_ai.request.model`,
`gen_ai.response.model`, `gen_ai.usage.input_tokens`,
`gen_ai.usage.output_tokens`, `gen_ai.agent.name`, `gen_ai.tool.name`,
`gen_ai.tool.call.id`.

**Pinned version.** Written against the OpenTelemetry GenAI semantic conventions
as of 2026-11 (status: *Development*). They moved to
`open-telemetry/semantic-conventions-genai` in 2026. Implementations record the
semconv version they follow in code, and a component upgrades by following the
upstream changelog. Renames are handled as they have been before, for example
`gen_ai.system` became `gen_ai.provider.name`: the new name is adopted
everywhere in one change, and the old name is not dual-emitted. Because the
conventions are unstable, the OTel opt-in variable
`OTEL_SEMCONV_STABILITY_OPT_IN` is honoured where an SDK offers it.

Roundtable-specific values live under `roundtable.*`, never under `gen_ai.*`.

## Content capture

On by default (decision 15).

- Prompts, system instructions and responses go on `chat` spans as the GenAI
  content attributes `gen_ai.input.messages`, `gen_ai.output.messages` and
  `gen_ai.system_instructions`.
- Tool arguments and results go on `execute_tool` spans.
- **Redaction.** Every value goes through the shared `redactSecrets` pattern list
  before it is set (pi-knight `src/hooks.ts`; claude-knight `src/activity.ts`;
  same patterns as activity events). The collector (#386) runs a second
  redaction pass for sources that cannot be redacted in-process (Claude Code
  native export).
- **Bounds.** Each content attribute is capped at 16 KiB by default
  (configurable). Cut values are suffixed and the span gets
  `roundtable.content.truncated=true`.
- **Off switches** (any one turns capture off): chart value
  `telemetry.captureContent: false`; per table `spec.telemetry.captureContent`
  (#345); per knight `ROUNDTABLE_TELEMETRY_CAPTURE_CONTENT=false`. With capture
  off, spans keep names, durations, token counts, model and cost.
- Activity previews (`activityContent`) follow the same switch.
- Who may view traces and how long they are kept: see #385.

## Metrics

Scheme: `roundtable_<component>_<thing>_<unit>`.

- Counters end in `_total`; gauges never do.
- Cost is in `_usd`; durations in `_seconds`.
- Labels: `table` on everything table-scoped, plus `knight`, `runtime`, `model`,
  `chain`, `step` where they apply. **Never** task ids or run ids as labels.
- When metrics move to the OTel meter, the OTel instrument name uses dots
  (`roundtable.knight.tasks`) and exports to Prometheus as
  `roundtable_knight_tasks_total`.
- Knights also emit the semconv histograms `gen_ai.client.token.usage` and
  `gen_ai.client.operation.duration`.

### Operator (landed in #441)

The operator renames are done; see "Operator Metrics" in
[ARCHITECTURE.md](../ARCHITECTURE.md). For the record:

| Before | Now |
|---|---|
| `roundtable_knights_total{phase,table}` | `roundtable_knights{table,phase}` (gauge, recomputed per table) |
| `roundtable_tasks_completed_total` | dropped (never incremented; knight-side `roundtable_knight_tasks_total` covers it) |
| `roundtable_task_duration_seconds` | dropped (never observed; knight-side metric covers it) |
| `roundtable_chain_runs_total{chain,status}` | `{table,chain,status}`, now incremented |
| `roundtable_chain_noop_runs_total{chain}` | `{table,chain}` |
| (new) | `roundtable_chain_step_duration_seconds{table,chain,step,status}` |
| `roundtable_missions_total{phase}` | `roundtable_missions{table,phase}` |
| `roundtable_cost_total_usd{table}` | `roundtable_table_cost_usd{table}` |
| `roundtable_warm_pool_size{state,table}` | `roundtable_warm_pool_knights{table,state}` |
| `roundtable_reconcile_errors_total{controller}` | dropped (controller-runtime exports `controller_runtime_reconcile_errors_total`) |

### Knight runtimes (target)

All get `table` and `runtime` labels.

| pi-knight today | claude-knight today | Target |
|---|---|---|
| `pi_knight_tasks_total{knight,status}` | `claude_knight_tasks_total{knight,status}` | `roundtable_knight_tasks_total{table,knight,runtime,status}` |
| `pi_knight_task_duration_seconds{knight}` | `claude_knight_task_duration_seconds{knight}` | `roundtable_knight_task_duration_seconds{table,knight,runtime}` |
| `pi_knight_llm_cost_dollars_total{knight,model}` | `claude_knight_llm_cost_dollars_total{knight,model,billing}` | `roundtable_knight_llm_cost_usd_total{table,knight,runtime,model,billing}` (pi reports its auth mode as `billing`) |
| `pi_knight_tokens_total{knight,direction}` (`input`/`output`/`cached`) | `claude_knight_tokens_total{knight,direction}` | `roundtable_knight_llm_tokens_total{table,knight,runtime,model,type}` (`input`/`output`/`cache_read`/`cache_write`; adds `model`) |
| `pi_knight_nats_connected{knight}` | `claude_knight_nats_connected{knight}` | `roundtable_knight_nats_connected{table,knight,runtime}` |
| `pi_knight_active_tasks{knight}` | `claude_knight_active_tasks{knight}` | `roundtable_knight_active_tasks{table,knight,runtime}` |
| `pi_knight_tool_calls_total{tool,status}` | `claude_knight_tool_calls_total{tool,status}` | `roundtable_knight_tool_calls_total{table,knight,runtime,tool,status}` (adds `knight`) |
| `pi_knight_tool_call_duration_seconds{tool}` | (none) | `roundtable_knight_tool_call_duration_seconds{table,knight,runtime,tool}` (claude-knight adds it) |
| `pi_knight_tool_call_errors_total{tool}` | (none) | dropped: use `roundtable_knight_tool_calls_total{status="error"}` |
| `pi_knight_tool_calls_blocked_total{tool,reason}` | `claude_knight_tool_calls_blocked_total{tool,reason}` | `roundtable_knight_tool_calls_blocked_total{table,knight,runtime,tool,reason}` |
| (none) | `claude_knight_usage_limit_hits_total{knight}` | `roundtable_knight_usage_limit_hits_total{table,knight,runtime}` |
| (none) | `claude_knight_plan_utilization{knight,window}` | `roundtable_knight_plan_utilization{table,knight,runtime,window}` |
| `process_*`, `nodejs_*` | same | kept (standard names) |

### Dashboard API and gateway

`roundtable_ui_http_requests_total{route,method,code}`,
`roundtable_ui_http_request_duration_seconds{route,method}`,
`roundtable_ui_ws_dropped_messages_total` and
`roundtable_ui_ws_dispatch_rate_limited_total` already fit the scheme. They stay
and carry no `table` (install-wide). The gateway has no metrics today; new ones
follow the scheme.

### Migration

Rename in one step, **no dual emission**: the chart ships no dashboards or
alerts yet, and only dapper-cluster's hand-written copies use the old names. The
owner updates dapper-cluster's hand-written `PrometheusRule roundtable` and its
dashboards in the same rollout as the runtime renames.

## Logs

JSON logs carry `trace_id`, `span_id`, `table`, `knight` (knights), and
`roundtable.*` keys with the same names as the attributes above.
