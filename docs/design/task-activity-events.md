# Task activity events

Status: contract v1 (roundtable-k8s/roundtable#249). Publishers: pi-knight#64,
claude-knight#18. Consumers: roundtable-ui (imports `pkg/nats`).

Live, fire-and-forget telemetry of what a knight is doing while a task runs.
It is for live viewing only; it does not replace traces (#154, #155) or results.

## Transport

- **Subject:** `<prefix>.activity.<Knight>.<taskId>` (`natspkg.ActivitySubject`).
  `<Knight>` is the capitalised `KNIGHT_NAME`.
- **Core NATS only.** Never published to or captured by JetStream. The task and
  results streams use `<prefix>.tasks.>` and `<prefix>.results.>`; keep stream
  subjects that narrow. `pkg/nats` has a test that `<prefix>.activity.>` matches
  no stream subject.
- **Consumers** subscribe to `<prefix>.activity.>` (or `...<Knight>.>`) and must
  tolerate loss and reordering.
- **Payload:** one JSON object per message, UTF-8, max **8 KiB**.

## Pod identity env (operator-provided)

The operator injects via the downward API; knights read these instead of
`os.hostname()`:

| Env | Source | Event field |
|---|---|---|
| `POD_NAME` | `metadata.name` | `pod` |
| `POD_NAMESPACE` | `metadata.namespace` | (not in events) |
| `NODE_NAME` | `spec.nodeName` | `node` |

User-defined `spec.env` is applied after these and can override them.

## Common fields

| Field | Type | Notes |
|---|---|---|
| `v` | int | Contract version, `1`. Consumers ignore unknown fields and reject a higher major they do not know. |
| `type` | string | See types below. |
| `ts` | string | RFC 3339 UTC with milliseconds. |
| `knight` | string | `KNIGHT_NAME`. |
| `pod` | string | `POD_NAME`. |
| `node` | string | `NODE_NAME`; omitted if unset. |
| `taskId` | string | Task id (`roundtable.task_id`). |
| `runId` | string | Optional; chain/mission run id from the task. |
| `chainName`, `stepName` | string | Optional; from the task when it comes from a Chain. |
| `model` | string | Optional; resolved model. |
| `seq` | int | Per-task, starts at 1, +1 per event published (including heartbeats). A gap means loss. |
| `truncated` | bool | `true` if any text field was cut to its limit (omitted otherwise). |

Names follow the OTel attribute names from #154. When trace context lands
(#155) `traceId` is added as an optional field (additive).

## Types

| `type` | Extra fields |
|---|---|
| `task.started` | `slot` (concurrency slot, 0-based), `concurrency`, `queuedFor` (ms from publish to pickup, when known) |
| `task.heartbeat` | `turns`, `inputTokens`, `outputTokens`, `costUsd` (so far), `currentTool` (if any). Sent with each `working()` call. |
| `tool.started` | `tool`, `toolCallId`, `argsSummary` |
| `tool.finished` | `tool`, `toolCallId`, `durationMs`, `isError`, `outputPreview` |
| `assistant.message` | `preview` |
| `task.finished` | `success`, `errorClass` (on failure), `durationMs`, `costUsd`, `turns`, `inputTokens`, `outputTokens` |

`errorClass` values: `timeout`, `cancelled`, `model_error`, `tool_error`,
`internal`, `other`.

## Bounds

- Event <= 8 KiB serialized. If still too big after truncation, drop preview
  fields entirely and set `truncated: true`.
- `preview` and `outputPreview` <= 500 characters; `argsSummary` <= 200.
  Cut text sets `truncated: true`.
- Rate limit per task: 20 events/s. Above it, `tool.*` and `assistant.message`
  events may be sampled (dropped, and `seq` is still consumed only by published
  events, so sampling is not visible as a gap; heartbeats show progress).
  `task.started`, `task.heartbeat` and `task.finished` are **never** dropped.

## Redaction (mandatory, done by the knight before publishing)

Never include env vars, headers, credentials, or contents of `.env` files or
secret mounts. Run every `argsSummary`, `outputPreview` and `preview` through the
shared secret patterns (pi-knight `redactSecrets`; claude-knight shares the same
pattern list). Previews can be switched off per knight with
`activityContent: none|preview` (default `preview`; follows the content-capture
policy in #154). With `none`, previews and `argsSummary` are omitted; tool names,
durations and counters are still sent. The CRD field (`spec.telemetry`) is a
follow-up; until it exists knights default to `preview`.

## NATS authz (#170)

Per-knight NATS permissions must allow **publish** on
`<prefix>.activity.<Knight>.>` (`ActivityPublishSubject`) and
`<prefix>.introspect.<Knight>.>` (`IntrospectPublishSubject`). Dashboards need
subscribe on `<prefix>.activity.>`.

## Compatibility

Additive: new subjects and new env vars only. No existing field, subject or CRD
changes. Adding optional fields stays `v: 1`; removing or changing a field's
meaning bumps `v`.

## JSON schema

`task-activity-events.schema.json` (for TypeScript knights and the UI).
