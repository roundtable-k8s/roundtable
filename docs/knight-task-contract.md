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
