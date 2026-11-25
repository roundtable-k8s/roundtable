# Knight task contract

How the operator hands a task to a knight runtime (claude-knight, pi-knight) over
NATS, and what it expects back. Go types: `pkg/nats/payload.go`.

## Task payload

Published by the chain controller and the [invoke gateway](gateway.md) to
`<prefix>.tasks.<domain>.<knight>`:

```json
{
  "taskId": "chain-review-pr-review.4b1c…-1763914401000",
  "chainName": "review-pr",
  "stepName": "review",
  "runId": "4b1c…",
  "task": "Review the diff …",
  "model": "openrouter/z-ai/glm-5.3"
}
```

| Field | Required | Meaning |
|-------|----------|---------|
| `taskId` | yes | Unique task ID. The result is published to `<prefix>.results.<taskId>`. |
| `task` | yes | The rendered prompt. |
| `chainName`, `stepName` | no | Set for chain steps and mission briefings. |
| `runId` | no | Chain run ID, or for gateway tasks the conversation ID. Key agent sessions on it so context never bleeds between runs; a gateway caller sends the same `runId` again to continue a conversation. A chain step with `resumeRunId` sends an earlier run's ID instead (see below). |
| `model` | no | Per-task model override (see below). Absent means "use your configured model" (`KNIGHT_MODEL`, from `Knight.spec.model`). |
| `attempt`, `retryOf`, `previousFailure` | no | Set by a manual retry (#397): the 1-based attempt number, the task ID being retried and why that attempt was dead-lettered (at most 2 KB). Absent means attempt 1. A retried task may have partly run, so check for partial work before starting over. Used to de-duplicate dead-letter entries (see [Delivery and failure](#delivery-and-failure)). |

### `model`

When present, the knight must run **this task only** on that model and then go
back to its configured model. The operator has already:

1. resolved RoundTable aliases (`RoundTable.spec.modelAliases`), so the value is a
   concrete model, never an alias name;
2. checked the value suits the knight's runtime (below). A task the runtime can't
   run is never published: the chain step fails with
   `invalid model for knight "<name>": …` and a `InvalidStepModel` warning event.

| Runtime | Accepted `model` values | Knight behaviour |
|---------|-------------------------|------------------|
| claude-knight | Claude CLI aliases (`opus`, `sonnet`, `haiku`, `fable`, `opusplan`) or `claude-*` model IDs, optionally with a context suffix such as `[1m]`. No `provider/` prefix. | Pass as `claude --model <model>` for this task. |
| pi-knight | `provider/model`, e.g. `anthropic/claude-sonnet-4-5`, `openrouter/z-ai/glm-5.3`. | Set the session model for this task. |

The runtime is inferred from the knight's image repository name
(`spec.image`, else the operator's `DEFAULT_KNIGHT_IMAGE`, else
`ghcr.io/roundtable-k8s/pi-knight`): a name starting with `claude-knight` is
claude-knight, `pi-knight` is pi-knight. Any other image is treated as an unknown
runtime and receives the model unvalidated. (`Knight.spec.runtime` is unrelated:
it picks the pod backend, deployment or sandbox.)

A knight that does not understand `model` ignores it and uses its configured
model — the field is additive and old knights keep working. A knight that does
understand it but cannot honour it (unknown model, provider not configured) must
fail the task with an `error` rather than silently fall back.

## Result payload

Published to `<prefix>.results.<taskId>`. Both spellings are accepted:

```json
{ "taskId": "…", "output": "…", "error": "", "model": "claude-opus-4-1" }
{ "task_id": "…", "result": "…", "success": true, "model": "openrouter/z-ai/glm-5.3" }
```

| Field | Meaning |
|-------|---------|
| `taskId` / `task_id` | Echo of the task ID. |
| `output` / `result` | Task output. Empty output counts as a failure. |
| `error` / `success: false` | Task failed. |
| `model` | Optional: the model actually used. The chain controller records it in `status.stepStatuses[].model`. Report it whether or not the task carried an override. |

### Structured results: `prs` and `verdict`

Optional, additive fields so consumers never have to parse prose (a result that
says `#245` instead of a URL used to stall the review loop):

```json
{
  "taskId": "…", "output": "Reviewed.",
  "prs": [{"repo": "roundtable-k8s/roundtable", "number": 245,
           "url": "https://github.com/roundtable-k8s/roundtable/pull/245",
           "action": "opened"}],
  "verdict": "needs_changes"
}
```

| Field | Meaning |
|-------|---------|
| `prs` | Pull requests the task opened or updated: `{repo, number, url, action}` with `action` one of `opened` or `updated`. |
| `verdict` | A review outcome: `ship_it`, `needs_changes`, or `null`/absent when the task is not a review. |

### Refusals: `usage_limited` and `refused`

Optional fields a knight sets when it declines a task before running it:

| Field | Meaning |
|-------|---------|
| `usage_limited` | Boolean, `true` on a plan-quota refusal (claude-knight: a plan window is at the knight reserve). Not a failure: the caller should defer the task or use the fallback coder. The operator does not escalate it (#321). |
| `refused` | Optional string naming the refusal reason. Today only `"credits"` (pi-knight): the provider answered 402 or the account is out of credits, which does not reset on its own. |

The operator parses `refused` (`TaskResult.Refused`) but does not act on it
yet: a result carrying it is handled as a plain failure and escalated as
usual. Dead-letter entries use reason `refused` when it is set. Pipeline
behaviour for `refused: "credits"` (for example routing to another knight) is
not defined.

Readers use the fields when present and otherwise fall back to the prose
`output` (`pkg/nats.ParseStructured`): a `Verdict: SHIP IT` / `Verdict: needs
changes` line (the last one wins), and PR references as GitHub PR URLs,
`owner/repo#N`, or, only when nothing better is found, a bare `#N` (number
only, no repo or URL). Fallback `prs` entries have no `action`. An explicitly
reported field always wins over prose. Knights that don't know the fields keep
working unchanged; an unrecognised `verdict` value reads as no verdict.

Consumers:

- **Chains**: `when` and `loop.until` see `steps.<name>.prs` (list of
  `{repo, number, url, action}`) and `steps.<name>.verdict` (string, `""` when
  none); `until` also sees them on `last` and `body.<name>`. As with `output`,
  they are populated for succeeded steps the expression mentions, e.g.
  `until: 'last.verdict == "ship_it"'` or `when: 'steps.build.prs.size() > 0'`.
  Fields are kept next to the full output in the `chain-outputs` KV entry
  (the CRD status is unchanged); fan-out items and loop wrapper steps use the
  prose fallback.
- **Task templates** (`task`) see `.Input` (the raw `spec.input`) and, when the
  input is a JSON object, `.Fields` (its top-level fields, e.g.
  `{{ .Fields.url }}`) so a step can render single trusted fields instead of
  all of the input.
- **PR feedback watcher**: finds the knight that opened a PR from step
  outputs with the same `prs` extraction ([pr-feedback.md](pr-feedback.md)).
- **Gateway**: task records (`GET /v1/tasks/<id>`, invoke responses) include
  `prs` and `verdict`.

## Chain and RoundTable fields

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: RoundTable
metadata:
  name: fleet-a
spec:
  modelAliases:
    deep: opus
    fast: haiku
    review: openrouter/z-ai/glm-5.3
---
apiVersion: ai.roundtable.io/v1alpha1
kind: Chain
spec:
  roundTableRef: {name: fleet-a}
  steps:
    - name: design
      knightRef: {name: rt-operator}   # claude-knight
      model: deep              # → opus
      task: "…"
    - name: review
      knightRef: {name: reviewer}      # pi-knight
      model: review            # → openrouter/z-ai/glm-5.3
      dependsOn: [design]
      task: "…"
```

- `steps[].model` is a model ID or an alias name; alias names win over literal
  models (an alias `opus: claude-opus-4-1` pins what `opus` means for the table).
  Aliases resolve one level deep.
- Aliases come from the chain's `roundTableRef` (for mission chains: the
  mission's `roundTableRef`, else `default`).
- `status.stepStatuses[].model` shows the resolved model sent with the task, then
  the model the knight reports having used.
- A model mismatch is a spec error: the step fails immediately and is **not**
  retried, regardless of `retry` / `retryPolicy`.

## Delivery and failure

**At most once.** A task is delivered to one knight once and never re-run
automatically (whiteboard decision 12, #377). The runtimes create their tasks
consumer with `max_deliver: 1` and ack only after the result is published, so a
knight that dies mid-task does not get the task again. Retry is a manual action
(#397). Chains keep their own step retries (`retry` / `retryPolicy`): those
publish a new task and are unaffected by what follows.

**Dead-letter stream.** So nothing vanishes, the operator records every lost or
failed task in a per-table dead-letter stream. It is set up with the other
streams when `RoundTable.spec.nats.createStreams` is true:

| Stream | Subjects | Retention |
|--------|----------|-----------|
| `<tasksStream>_ADVISORIES` | `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.<tasksStream>.*` | Limits, 7 days |
| `<tasksStream>_DLQ` | `<prefix>.dlq.>` | Limits, `spec.nats.deadLetterMaxAge` (default `720h`, 30 days); 10 min duplicate window |

What lands there, on `<prefix>.dlq.<knight>.<taskId>`:

- **Lost tasks** (`reason: lost`): a task that was delivered and never acked
  (crash, OOM, hang) hits max deliveries and JetStream emits an advisory. The
  advisories stream stores it even when no pod of that knight is running
  (scale to zero, crash loop, deleted knight). The operator's durable consumer
  `roundtable-dead-letter` on that stream loads the original task by
  `stream_seq`, writes the entry with the original payload and then deletes the
  never-acked message from the tasks stream, but only once the knight's own
  `lost: true` result has arrived or about 45 s have passed since the advisory
  (the knights load the same message to publish that result, which the chain
  controller, planner and gateway wait for). If the original is already gone,
  the entry carries what the advisory has (knight from the consumer name, the
  sequence) and `taskId: seq-<n>`.
- **Failed tasks**: every result with `success: false` (or an `error` and no
  `success`) on `<prefix>.results.>`. `reason` is `lost` for a result with
  `lost: true`, `usage_limited` for `usage_limited: true`, `refused` when
  `refused` is set, `timeout` when the error mentions a timeout, else `failed`.
  Include the original task payload as `task` in failure results so the entry
  can be retried. The operator reads results with a core subscription, so
  failures published while the operator is down are not captured (the knight's
  task metric and the dispatcher still see them).

Entry body:

```json
{
  "taskId": "…", "table": "fleet-a", "knight": "galahad",
  "reason": "lost", "error": "…", "attempt": 1, "retryOf": "",
  "chainName": "…", "stepName": "…", "runId": "…",
  "task": { "taskId": "…", "task": "…" },
  "streamSeq": 42, "deadLetteredAt": "2026-11-24T12:00:00Z"
}
```

`task` is the original task payload or `null`; `streamSeq` is the tasks-stream
sequence for lost tasks (0 for failed results). Each entry is published with
`Nats-Msg-Id: dlq-<taskId>-<attempt>` (`attempt` from the task payload, default
1), and the operator skips a task attempt it has already recorded, so the
advisory and the knight's own `lost: true` result give one entry.

**State** lives in the KV bucket `roundtable-dead-letters`, key
`<table>.<taskId>`, as `{state, reason, knight, attempt, subject, updatedAt, retriedAs}`:
`open` on capture; `resolved` when a `success: true` result for the same task
id arrives later (a spurious "lost" after an event-loop stall, pi-knight#84);
`retried` is set by the gateway's retry API (`retriedAs` is the new task ID). Keys expire after 30 days.

**Metrics**: `roundtable_tasks_dead_lettered_total{table,knight,reason}` and
`roundtable_dead_letters_open{table,knight}`.

## Draining on rollout

Knight Deployments are rolled while tasks may be in flight, so the operator and
the knight runtime share a drain contract (#246).

**Operator side**

- `terminationGracePeriodSeconds` is `spec.taskTimeoutSeconds + 60`.
- Strategy is `RollingUpdate` with `maxSurge: 1, maxUnavailable: 0` when more
  than one pod can mount the workspace: `workspace.mode: ephemeral`, or a
  `workspace.existingClaim` with `replicas`/`maxReplicas` above 1 (which the
  Knight CEL rules require to be ReadWriteMany). The new pod starts pulling
  while the old one drains.
- A single-replica knight on the default PVC (or a single-replica
  `existingClaim`, assumed ReadWriteOnce) keeps `Recreate`: two pods cannot
  mount the volume, so the old pod must be gone first. It still gets the longer
  grace period, so it drains before it is killed, but there is a gap with no
  pod pulling. Use `workspace.mode: ephemeral` or an RWX claim for gap-free
  rollouts.

**Runtime side** (claude-knight / pi-knight; not implemented by the operator)

On SIGTERM a knight must stop fetching new tasks, keep heartbeating in-flight
tasks, publish their results, then exit, waiting up to the grace period rather
than a fixed 60s. `/ready` must report `draining` (not ready) so the pod leaves
Service endpoints.

### `resumeRunId`

A chain step can hand its task the `runId` of an earlier run, so the knight
resumes that agent session instead of starting a fresh one:

```yaml
steps:
  - name: fix
    knightRef: {name: rt-operator}
    resumeRunId: 'json(input).runId'
    task: "…"
```

- It is a CEL expression with the same variables as `when`
  ([conditional-steps.md](conditional-steps.md#expression-reference)) that
  must yield a string. An empty string keeps the chain run's `runId`; an
  evaluation error fails the step without dispatching it.
- Only the payload's `runId` changes. The chain's own `status.runId`, the
  `taskId` and the `chain-outputs` KV keys keep the chain run's identity, so
  outputs of the two runs never mix.
- Not valid on `forEach` (items get per-item runIds) or `loop` steps.
- The PR feedback Chain uses it to send a CI failure to the session that
  opened the PR ([pr-feedback.md](pr-feedback.md)).

## Trace context

Task and result messages may carry W3C Trace Context as NATS message headers:
`traceparent` and, optionally, `tracestate` (and `baggage`). The operator and
the invoke gateway inject the caller's context into every task they publish,
and extract it from the results they read, so a trace does not break at the
NATS hop. JetStream preserves headers, so no payload field is needed.

Knights should extract the headers from the task message, run the task as a
child of that context, and inject the resulting context into the headers of the
result message. Headers are optional: a message without them is handled exactly
as before, and the operator adds none when it has no tracer provider.

Producer spans are named `publish <subject>` and carry `messaging.system=nats`,
`messaging.destination.name`, `messaging.operation.type=publish`,
`roundtable.table` and `roundtable.task.id` when known. On consume the operator
records `messaging.nats.delivery_count`; a redelivered result links to the
original context instead of becoming a second child of it.
