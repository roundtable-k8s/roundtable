# Conditional steps and bounded loops

A Chain is still a DAG of steps. Two step fields add control flow, and both
are evaluated by the operator, so no knight task is dispatched to decide
anything:

- `when` is a condition on a step. When it is false the step is skipped.
- `loop` makes a step a bounded loop over a short body of steps, e.g. a
  review → fix → re-review cycle.

Conditions use [CEL](https://cel.dev), the same language as Kubernetes CRD
validation, not Go templates. CEL has no side effects, always terminates and
runs under a cost limit. A syntax or type error makes the Chain invalid
(`ChainValid=False`, reason `InvalidCondition`) before anything runs.

Full example: [`examples/chain-pr-pipeline.yaml`](../examples/chain-pr-pipeline.yaml),
the PR pipeline described in [pr-pipeline.md](pr-pipeline.md).

## `when`

```yaml
steps:
  - name: recon
    knightRef: {name: tristan}
    task: "Scan ports 1-1000 of {{ .Input }}"
  - name: ssh_audit
    knightRef: {name: tristan}
    dependsOn: [recon]
    when: 'steps.recon.output.contains("22/tcp open")'
    task: "Audit SSH on {{ .Input }}"
  - name: report
    knightRef: {name: gawain}
    dependsOn: [ssh_audit]
    task: "Report. SSH audit: {{ .Steps.ssh_audit.Phase }} {{ .Steps.ssh_audit.Output }}"
```

- `when` is evaluated once the step's `dependsOn` are done, right before it
  would be dispatched.
- **false**: the step becomes `Skipped` with `reason: WhenFalse`, and nothing
  is published.
- **error** (e.g. `json()` on prose, or an unknown step name at runtime): the
  step `Failed` with `error: "when: ..."`. Use `continueOnFailure` if that
  should not fail the chain.
- **A step whose output is read as JSON** (`json(steps.<name>.output)` in a
  later step's `when`, `until` or `resumeRunId`, or `forEach.from:
  steps.<name>`) must reply with a JSON document. If its reply has none, it
  is re-run once (`reason: JSONRetry`) with a reminder to reply with only the
  JSON block; a second reply without one fails that step (#314), so the
  error names the step that broke instead of every dependent.
- **A skipped step satisfies `dependsOn`.** Dependents still run. Gate them
  with their own `when` (e.g. `steps.x.phase == "Succeeded"`) if they should
  not. Steps skipped because the chain already failed get
  `reason: ChainFailed`.
- On a `forEach` step, `when` gates the whole fan-out (it is evaluated once,
  not per item).

## `loop`

```yaml
- name: fixloop
  dependsOn: [review]
  loop:
    from: review                                   # optional
    until: 'last.output.contains("Verdict: ship it")'
    maxIterations: 3                               # 1-20, default 3
    onExhausted: continue                          # fail (default) | continue
    steps:                                         # 1-5 body steps, run in order
      - name: fix
        knightRef: {name: rt-operator}
        model: deep                                # per-body-step model (#219)
        timeoutSeconds: 3600
        retry: {maxAttempts: 1, backoffSeconds: 60}
        task: "Address this review: {{ .Loop.Last.Output }}"
      - name: review
        knightRef: {name: rt-lead}
        timeoutSeconds: 1800
        task: "Re-review after: {{ .Loop.Last.Output }}"
```

A loop step is one node in the DAG. It has no `knightRef`, `task`, `forEach`,
`model`, `retry` or `outputPath` of its own; those go on its body steps.

The loop tracks **`last`**, the most recent step in its flow:
`from → body[0] → body[1] → … → body[n-1] → body[0] → …`

1. **Entry.** If `from` is set, `until` is evaluated with `last` = the `from`
   step. If it is already true, the loop ends `Converged` after zero rounds
   and its output is the `from` step's output.
2. **Round k.** Each body step runs in order, one task at a time. In a body
   task template, `{{ .Loop.Last.Output }}` is the previous step in the flow.
   For the first body step that is the `from` step in round 1, and the last
   body step of the previous round after that. A failed body task is retried
   per its `retry` (or the chain's `retryPolicy`). If it still fails, the loop
   ends `Failed` and the step fails.
3. **End of round.** `until` is evaluated with `last` = the last body step.
   - true: `Converged`, and the step succeeds.
   - false with `k == maxIterations`: `Exhausted`.
   - false otherwise: round k+1 starts.

`until` is only checked at entry and after a full round, never in the middle
of a round.

**Exhausted.** With `onExhausted: fail` (default) the step fails, which fails
the chain unless the step has `continueOnFailure`. With
`onExhausted: continue` the step succeeds with outcome `Exhausted`, so
dependents can branch on it with `when`. The chain then ends
`PartiallySucceeded` at best.

The **output** of the loop step is the output of `last` when it ended (the
final review in the example). Downstream steps read it as
`{{ .Steps.fixloop.Output }}`, and `{{ .Steps.fixloop.Outcome }}` /
`{{ .Steps.fixloop.Iterations }}` are available too.

## Structured results in task templates

Besides `.Output`, `.Error`, `.Phase`, `.Reason`, `.Outcome` and `.Iterations`,
every `.Steps.<name>` entry exposes the step's structured result (the `prs`
and `verdict` a knight reports, or the `PRs:` / `Verdict:` lines parsed from its
output when it reports none):

| Field | Type | Empty value |
|---|---|---|
| `.Steps.<name>.PRs` | list of `.URL`, `.Repo`, `.Number`, `.State` | empty list |
| `.Steps.<name>.Verdict` | `ship_it`, `needs_changes` | `""` |

`.State` is what the task did with the PR (`opened`, `updated`, ...) and is
empty when the knight did not say. A step that did not succeed, has no
structured result renders the empty values, never an error. A step that does
not exist is not in the template data at all, so `{{ .Steps.<missing>.Verdict }}`
renders `<no value>` (also never an error); check the step name.
The fields work in plain steps, `forEach` item tasks and loop bodies, and in
`.Loop.Steps.<body>` for the other body steps of the loop.

The helper `prURLs` joins PR URLs with spaces; it takes the list or the step:

```yaml
task: |
  Verify {{ prURLs .Steps.implement.PRs }}
  Verdict of the review: {{ .Steps.review.Verdict }}
  {{ range .Steps.implement.PRs }}- {{ .Repo }}#{{ .Number }} {{ .URL }}
  {{ end }}
```

**Sessions.** Body tasks are published with the chain's `runId`, the same one
every ordinary step of the run uses. Knights key agent sessions on `runId`, so
the coder in `fix` resumes the session it opened in `implement` and keeps its
PR context. Each task still has its own `taskId`
(`chain-<chain>-<loop>-<body>-r<round>.<runId>-<ms>`), and its `stepName` is
`<loop>.<body>`.

**Branching on the outcome**

```yaml
- name: merge
  dependsOn: [fixloop]
  when: 'steps.fixloop.outcome == "Converged"'
  ...
- name: escalate
  dependsOn: [fixloop]
  when: 'steps.fixloop.outcome == "Exhausted"'
  ...
```

### retry-failed

`ai.roundtable.io/retry-failed` resumes a loop instead of restarting it:

- **Loop `Failed`** (a body step failed): only that body step re-runs, in the
  same round. Finished body steps are not redone.
- **Loop `Exhausted`** with `onExhausted: fail`: the loop gets another
  `maxIterations` rounds (`status.loop.maxIterations` grows) and continues
  from the last review.

`ai.roundtable.io/run` starts a fresh run with a fresh loop.

### Status (bounded)

```yaml
stepStatuses:
  - name: fixloop
    phase: Succeeded
    reason: Converged
    output: "...Verdict: ship it"          # truncated at 4000 chars
    loop:
      iteration: 2                          # current/last round
      maxIterations: 3
      outcome: Converged                    # Converged | Exhausted | Failed
      steps:                                # one entry per body step, latest round only
        - {name: fix,    iteration: 2, phase: Succeeded, taskId: ..., model: opus, output: ...}
        - {name: review, iteration: 2, phase: Succeeded, taskId: ..., output: ...}
      history:                              # earlier rounds too, oldest first
        - {round: 1, step: fix,    phase: Succeeded, taskId: ..., startedAt: ..., finishedAt: ..., outputSummary: ...}
        - {round: 1, step: review, phase: Failed,    taskId: ..., error: ...}
        - {round: 2, step: fix,    phase: Succeeded, taskId: ..., outputSummary: ...}
```

`loop.history` has one entry each time a body task finishes (a retried attempt
adds one per attempt), so the dashboard can show every round. It keeps only the
last `min(maxIterations, 20)` rounds and truncates `outputSummary` to 1 KiB.
`until` and templates still read the latest round only (`steps`).

The status stays bounded as rounds grow. Outputs in status are
truncated at 4000 characters. The full outputs are in the `chain-outputs` KV
bucket under `<chain>.<loop>.<body>` (latest round) and `<chain>.<loop>` (loop
output).

## Expression reference

| Variable | In | Type / fields |
|---|---|---|
| `input` | `when`, `until` | `spec.input` (string) |
| `steps` | `when`, `until` | map step name → `{name, phase, output, error, reason, outcome, iterations}` |
| `last` | `until` | `{step, phase, output, error}` (see above) |
| `iteration`, `maxIterations` | `until` | the round just finished (`0` on entry) and the round cap (int) |
| `body` | `until` | map body step name → `{phase, output, error, iteration}` (latest round each ran in) |

- `phase`: `Pending | Running | Succeeded | Failed | Skipped`.
- `outcome` and `iterations` are set on loop steps only (`""` and `0`
  otherwise).
- Outputs are the **full** outputs, read back from the `chain-outputs` KV
  bucket for the steps the expression mentions, not the 4000-character status
  copies. A verdict at the end of a long review is still seen. Each output is
  capped at 256 KiB.
- Use `steps["my-step"]` for names that aren't CEL identifiers (e.g. names
  with `-`, or reserved words such as `loop`).

Available functions:

- Standard CEL: `contains`, `startsWith`, `endsWith`, `matches` (RE2),
  `size`, `in`, `exists`, `all`, `filter`, `map`, `has`, `&&`, `||`, `!`,
  `?:`, comparisons, `int()` / `string()` conversions.
- [Extended strings](https://pkg.go.dev/github.com/google/cel-go/ext#Strings):
  `lowerAscii`, `upperAscii`, `trim`, `split`, `indexOf`, `replace`,
  `substring`, `join`, `format`, ...
- `json(string)`: parses a JSON document into maps and lists. Prose around a
  fenced ```` ```json ```` block is tolerated, as in `forEach`. Integers come
  back as `int`, other numbers as `double`.

Examples:

```cel
steps.recon.output.contains("22/tcp open") && !steps.recon.output.contains("patched")
steps.review.output.matches("(?m)^Verdict: ship it$")
json(steps.scan.output).vulnerable == true
json(steps.plan.output).issues.size() > 1
steps.impl.phase == "Succeeded" || steps.impl.reason == "WhenFalse"
steps["vuln-scan"].output.lowerAscii().contains("critical")
iteration >= 2 && last.output.contains("only nits")
```

## Limits and non-goals

- **Loops do not fan out.** A step can't have both `loop` and `forEach`, and
  body steps cannot be `forEach` steps. To review/fix many PRs, chain them:
  fan out the implementation, then run a loop over the joined output, or run
  one PR-pipeline Chain per item. Per-item loops are a follow-up.
- Body steps run strictly in sequence. They have no `dependsOn`, `when`,
  `outputPath` or `continueOnFailure` of their own.
- A loop is still bounded by the chain's `timeoutSeconds`. Size it for
  `maxIterations` × the body's step timeouts.
- Round Table is not a durable-execution engine: no unbounded loops,
  arbitrary jumps, or loops spanning several DAG nodes.
