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
| `runId` | no | Chain run ID, or for gateway tasks the conversation ID. Key agent sessions on it so context never bleeds between runs; a gateway caller sends the same `runId` again to continue a conversation. |
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
  roundTableRef: fleet-a
  steps:
    - name: design
      knightRef: rt-operator   # claude-knight
      model: deep              # → opus
      task: "…"
    - name: review
      knightRef: reviewer      # pi-knight
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
