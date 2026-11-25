# PR pipeline Chain

The PR pipeline is the standard Chain for a Ready GitHub issue. It replaces
dispatching a coder by hand:

```
plan (lead) ─┬─ implement_<coder>_<alias>     one PR, body ends "Closes #n"
             └─ split_<coder>   (forEach)      one PR per sub-issue
→ review (lead)
→ fixloop_<coder>: fix (coder) → review (lead), until "Verdict: ship it",
  at most maxIterations rounds, onExhausted: continue
→ merge    (loop Converged: lead runs gh pr merge --auto --squash and waits)
| escalate (loop Exhausted: lead labels needs-human and comments)
```

It is built only from existing Chain features: `when` and `loop`
([conditional-steps.md](conditional-steps.md)), `forEach`, and per-step
`model` with RoundTable `modelAliases`
([knight-task-contract.md](knight-task-contract.md)).

- Chart template: `charts/roundtable-operator/templates/pr-pipeline.yaml`
  (values `prPipeline.*`, off by default).
- Rendered example: [`examples/chain-pr-pipeline.yaml`](../examples/chain-pr-pipeline.yaml)
  (coders `rt-operator` and `rt-ui`). It is generated from the chart; change
  the template and re-render instead of editing it.
  [`examples/chain-pr-pipeline-postrun.yaml`](../examples/chain-pr-pipeline-postrun.yaml)
  is the same with `prPipeline.postRun: chain`
  ([post-run checks](#post-run-checks-builtin-or-chain)).

## Input

`spec.input` is a JSON object:

```json
{"repo": "roundtable-k8s/roundtable", "issue": 225}
```

## Steps

| Step | Knight | Model | Runs when |
|---|---|---|---|
| `plan` | lead | lead default (or `leadModel`) | always |
| `implement_<coder>_<alias>` | coder | the planner's alias | the plan picked this coder and alias, no split |
| `split_<coder>` (forEach over `.issues`) | coder | `deep` | the plan picked this coder and split |
| `review` | lead | lead default (or `leadModel`) | an implement or split step succeeded |
| `fixloop_<coder>` (loop: `fix` → `review`) | coder, lead | `fixModel` (default `deep`), lead default | the review ran and the plan picked this coder |
| `merge` | lead | lead default (or `leadModel`) | a fix loop `Converged` |
| `escalate` | lead | lead default (or `leadModel`) | a fix loop is `Exhausted`, or no implementation ran |

**Quota refusals (#321).** A claude-knight coder refuses a task before
running it once a Claude plan window passes the knight reserve:

```json
{"success": false, "usage_limited": true,
 "result": "Task refused: Claude plan five_hour window 91% used — at the knight reserve of 80% (resets 2026-11-24T18:00:00.000Z)"}
```

That is not a failure, it is "not yet". With `prPipeline.fallbackCoder` set
(e.g. `rt-coder`, a pi-knight on GLM via OpenRouter), the chart renders a
`fallbackRef` on every implement, split and fixloop step: the operator re-runs
the refused step on the fallback knight with **no** model override (the
`cheap`/`fast`/`deep` aliases are Claude models, so the fallback uses its own
default model). Without a fallback — or when the fallback refuses the same
way — the step turns `Deferred` with `deferUntil` = the window's reset time
plus jitter, and the chain requeues then: no further step runs meanwhile, so
nothing fails and nothing escalates, and the issue keeps `rt:in-progress`
instead of `needs-human`. Issue pickup counts a quota-deferred chain as
active, so it holds its slot and no new chain starts for it until after the
reset.

One refusal shape still escalates: a refusal message that carries no reset
time (e.g. `"Task refused: Claude plan five_hour limit reached"` with no
`(resets ...)` suffix — the SDK normally supplies the reset time, so this is
rare) is not recognized as a quota refusal and the step fails as before.

Lead steps set no `model`, so the lead runs its own default model whatever its
runtime. Model aliases are single-valued per RoundTable and validated against
the runtime of the knight running the step, so one alias cannot suit both a
claude-knight coder (`opus`) and a pi-knight lead (`provider/model`). Set
`prPipeline.leadModel` to pin the lead's model (an alias or a model ID valid on
the lead's runtime).

`<coder>` is the knight name with `-` replaced by `_` (`rt-operator` →
`implement_rt_operator_deep`), so templates can use `.Steps.<name>`.

**Plan.** The lead reads the issue and replies with a fenced JSON block:

```json
{"coder": "rt-operator", "model": "deep", "split": false, "breaking": false,
 "base": "", "issues": [{"number": 225, "title": "PR pipeline chain"}], "reason": "..."}
```

**Model tiers.** `implementModels` may include `cheap` (a RoundTable alias,
e.g. `cheap: haiku`) next to `fast` and `deep`. The planner picks `cheap` only
for small, well-specified issues (single file or area, clear acceptance
criteria, no API/CRD/contract change, no `breaking` label), `fast` by default
and `deep` for design-heavy or cross-cutting work, and records the reason in
the plan JSON. Every fix round runs `fixModel` (default `deep`), so a Haiku
draft is fixed by Sonnet (`fast`) or Opus. The chart default stays
`[deep, fast]`; enable the cheap tier with `implementModels: [cheap, fast, deep]`.

`coder` must be one of `prPipeline.coders` and `model` one of
`prPipeline.implementModels` (`none` blocks the issue, see
[Breaking changes](#breaking-changes)). If the issue is too large for one PR, the lead
creates sub-issues (`Part of #n`), lists them in `issues` and sets `split` to
true.

**One PR per sub-issue (#313).** After a split, `issues` lists each sub-issue
once and never the parent. The split step's `forEach.uniqueBy: number` drops a
repeated entry, so every sub-issue gets exactly one implement task. That task
works on a branch named after the sub-issue (`rt/<sub-issue>-<slug>`) and first
looks for an open PR for it (body `Closes #<sub-issue>` or a branch starting
`rt/<sub-issue>-`); if one exists it pushes to that branch and updates the PR
instead of opening another. The single-PR implement step does the same for the
input issue.

**Why one step per choice.** A step's `knightRef` and `model` are static, so
the planner cannot set them. The chart renders one implement step per coder
× alias, one split step and one fix loop per coder, each gated by a `when`
on the plan's JSON. Exactly one implement (or split) step and one fix loop
run; the rest are `Skipped` with reason `WhenFalse`. Skipped steps satisfy
`dependsOn`, so `review`, `merge` and `escalate` just depend on all of them.

**Review contract.** Every review (the first one and each re-review in the
loop) starts with `PRs: <url> [<url> ...]` and ends with exactly one line,
`Verdict: ship it` or `Verdict: needs changes`. The loop's `until` is

```cel
last.output.matches("(?mi)^\\W*verdict:\\s*ship it\\W*$")
```

so it only matches a verdict on a line of its own (markdown bold is fine,
`"Verdict: ship it" or "Verdict: needs changes"` quoted back is not).

**Fix loop.** `from: review`: a first review that already says ship it runs
zero rounds. Each round sends the latest review to the coder, who pushes to
the same branch, then the lead re-reviews. Loop body tasks carry the chain's
`runId`, which is also the `runId` of the single-PR implement step, so the
coder resumes the agent session it opened while implementing. forEach items
get per-item runIds, so after a split the fix rounds start a new session (the
review lists the PRs to work on).

**Merge.** The lead queues every PR from the final review with
`gh pr merge --auto --squash` and waits up to 50 minutes for each to be merged,
closed or blocked by a failing required check. Its output has one line per PR
and the number of fix rounds. A merge-queue rejection or a red CI run after
the pipeline ended goes back to the PR's author through the feedback Chain
([pr-feedback.md](pr-feedback.md)). Like issue pickup, PR feedback runs on one
of two engines, chosen per table with `spec.prFeedback.engine`: `builtin` (the
default, the operator's own poller) or `trigger` (generated Triggers
`<table>-pr-feedback-<author>` with a [github source](triggers.md#pr-feedback-on-a-trigger)
that selects pull requests by checks and merge state; same input, attempt cap
and escalation, no flaky-test issues). See
[pr-feedback.md](pr-feedback.md#engines).

**Escalate.** With `onExhausted: continue` an exhausted loop succeeds with
outcome `Exhausted`, `escalate` labels the issue and PRs `needs-human` and
comments the last review, and the chain ends `PartiallySucceeded`. It also
runs (labelling the issue) when the plan names a coder or alias the chain
does not have.

## Breaking changes

A PR that changes an API type, a CRD, the task contract or another interface
other repos and live resources depend on is *breaking* (#260; #241 ran
alongside three other PRs and cost three rebase-and-re-review rounds and an
abandoned PR). Breaking PRs are serialised:

1. **Label.** The `breaking` label marks the PR. The planner sets `"breaking":
   true` in its plan, the implementing coder labels the PR and adds a
   "Migration" section to its body, and rt-lead or a human can add the label
   later. The `protected-paths` label from the merge guard
   (`.github/merge-guard-paths`) is separate and only shows that a PR touches
   sensitive paths; use it as a hint when deciding to add `breaking`.
2. **Migration note.** rt-lead requires the migration note on `breaking` PRs
   and holds them for a human-sequenced rollout instead of queueing the merge.
3. **Pickup holds new work.** While a repo has an open PR labelled `breaking`
   (`spec.issuePickup.breakingLabel`, default `breaking`), [issue
   pickup](issue-pickup.md#breaking-changes) starts no new Chain for that repo.
   Ready issues stay ready and are picked up on the first poll after the
   breaking PR is merged or closed. This is per repo, not per package: before
   the plan there is no diff to compare with, so pickup errs on the safe side.
4. **The plan checks packages.** For work started by hand (or a Chain already
   running), the planner lists open `breaking` PRs with their files. If one
   touches the packages the issue touches, the plan names coder `none` with the
   reason `blocked by #<pr>`, so the chain escalates and the issue waits for a
   human. If the issue builds on that PR's change, the plan sets `"base"` to the
   PR's branch and the coder branches from it and opens its PR against it
   instead of `origin/main`.

Non-breaking work that does not touch the same packages is not affected.

## Output

| What | Where |
|---|---|
| PR URLs | `PRs:` line of `status.stepStatuses[fixloop_<coder>].output`, and the `merge` output |
| Fix rounds | `status.stepStatuses[fixloop_<coder>].loop.iteration`; `{{ .Steps.fixloop_<coder>.Iterations }}` |
| Merge state | `merge` output |
| Resolved model per step | `status.stepStatuses[].model` (loop body steps: `.loop.steps[].model`) |

The `merge` output ends with a `Tiering:` line (implement alias, fix model,
review rounds), so the tiering can be compared across runs. Cost per step is
not recorded on Chains yet (only Missions and RoundTables track cost); it is a
follow-up.

## Install

The RoundTable must define the aliases the coders use (`deep`, plus every
entry of `implementModels`); they only need to resolve on the coders' runtime:

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: RoundTable
metadata:
  name: roundtable-dev
spec:
  modelAliases:
    deep: opus
    fast: sonnet
    cheap: haiku
```

Every knight in the chain (`prPipeline.lead` and all `prPipeline.coders`) must
exist in the release namespace, or the Chain is `ChainValid=False`.

```yaml
# values.yaml
prPipeline:
  enabled: true
  roundTableRef:
    name: roundtable-dev
  lead: rt-lead
  coders: [rt-operator, rt-ui]
  implementModels: [cheap, fast, deep]   # chart default: [deep, fast]
  fixModel: deep
  maxIterations: 3
```

Example with a pi-knight lead (rt-lead on GLM via OpenRouter) and claude-knight
coders: the aliases map to Claude models for the coders, and the lead steps
need no alias. Optionally pin the lead's model in its own form:

```yaml
prPipeline:
  enabled: true
  lead: rt-lead                 # image pi-knight; uses its default model
  # leadModel: openrouter/z-ai/glm-5.3
  coders: [rt-operator, rt-ui]  # image claude-knight
  implementModels: [deep, fast]
```

Re-render the example after changing the template:

```bash
cat > /tmp/pr-pipeline-values.yaml <<'EOV'
prPipeline:
  enabled: true
  coders: [rt-operator, rt-ui]
  input: '{"repo": "roundtable-k8s/roundtable", "issue": 225}'
EOV
helm template rt charts/roundtable-operator -n roundtable \
  -f /tmp/pr-pipeline-values.yaml --show-only templates/pr-pipeline.yaml
```

then drop the `# Source` line, `---` and the Helm labels, and keep the
example's header comment. Render `examples/chain-pr-pipeline-postrun.yaml`
the same way with `--set prPipeline.postRun=chain`.

`make helm-check` lints the chart and fails if the example no longer matches
the rendered template. CI doesn't run it yet, so run it after any change to
the template, the `prPipeline` values or the Chain API.

## Run it for an issue

[Issue pickup](issue-pickup.md) does this automatically for issues labelled
`rt:ready` (RoundTable `spec.issuePickup`). It runs on one of two engines,
chosen per table with `spec.issuePickup.engine`:

- `builtin` (the default): the issue pickup controller polls GitHub itself.
- `trigger`: the controller generates a Trigger `<table>-issue-pickup` with a
  [github source](triggers.md#issue-pickup-on-a-trigger) and the Trigger
  starts the runs. Same run names, input (`{"repo", "issue"}`), labels,
  comments, caps, breaking-PR hold and needs-human hand-back; the post-run
  checks (merge verification, failed-run hand-back, split parents) stay with
  the issue pickup controller for both engines.

Switch with `kubectl patch roundtable <table> --type merge -p
'{"spec":{"issuePickup":{"engine":"trigger"}}}'`, and back with
`"engine":"builtin"`: that deletes the generated Trigger, running Chains keep
going and are not started twice. See [issue-pickup.md](issue-pickup.md#engines).

## Post-run checks: builtin or chain

After a pickup run finishes, someone has to check GitHub: are the PRs merged
or queued, does each carry the review verdict, is a failed run's issue handed
back, is a split run's parent issue closed? Each table picks who does it with
`spec.issuePickup.postRun` on the RoundTable, and the chart's pipeline must
match it with `prPipeline.postRun`.

| | `builtin` (default) | `chain` |
|---|---|---|
| Merge verification | operator: parses the `PRs:` and `Verdict:` lines, posts a missing review, queues an open ship-it PR, hands problems back | `verify` step: `rt-verify` |
| Failed run | operator: needs-human, comment naming the failed steps, in-progress removed | `handback` step: `rt-handback` |
| Split parent | operator: closes it once every sub-issue is completed (polls) | `close_parent` step: `rt-close-parent` |
| Operator after a run | all of the above | only releases the issue (below) |
| Chart | `prPipeline.postRun: builtin` | `prPipeline.postRun: chain` ([`examples/chain-pr-pipeline-postrun.yaml`](../examples/chain-pr-pipeline-postrun.yaml)) |

### The chain mode

`prPipeline.postRun: chain` renders three more steps, all on the lead:

```
... → merge → verify        (merge Succeeded)
            → close_parent  (verify Succeeded and the plan split)
handback                    (any step Failed; runs last)
```

- Every work step (plan to escalate) gets `continueOnFailure: true`, so a
  failure reaches `handback` instead of skipping it. The implement and split
  steps also need a succeeded plan, and escalate needs no failed work step, so
  after a failure only `handback` runs.
- `verify`, `close_parent` and `handback` are one-round `loop`s whose `until`
  checks the script's `ok` line (`rt-verify: ok`, `rt-close-parent: ok`,
  `rt-handback: ok`). A failed check, a knight that skipped the script, or a
  missing script (the task tells the knight to reply
  `<script>: missing script <path>` and not to do the work by hand) exhausts
  the loop and fails the step.
- `handback` has no `continueOnFailure`: when it fails, the run fails.
- The GitHub work lives in arsenal scripts, not in prompts. Their directory is
  `prPipeline.postRunScripts` (default `/skills/project/pr-postrun/scripts`):

| Step | Script | Interface |
|---|---|---|
| `verify` | `rt-verify <repo> <issue> <ship_it\|needs_changes> <review-file> <pr-url>...` | For each PR: merged or queued is fine; an open PR whose newest review verdict differs gets the review file posted with `rt-review` (`project/pr-review`), which also queues a ship-it PR; closed, draft, `needs-human`/`hold` or not queueable is a problem. Problems: issue gets `needs-human`, a comment listing them, in-progress removed; last line `rt-verify: needs-human ...`, exit 1. Otherwise last line `rt-verify: ok ...`. |
| `close_parent` | `rt-close-parent <repo> <parent> <sub-issue>...` | Waits (default 50 min, `--wait-minutes`) for the sub-issues to close. All completed: closes the parent with a comment, removes in-progress. One closed as not planned, labelled `needs-human` or still open at the timeout: hands the parent back (`needs-human`, comment, in-progress removed). Both end `rt-close-parent: ok closed\|handed-back ...`. |
| `handback` | `rt-handback <repo> <issue> <summary-file>` | When the issue is open and in progress: adds `needs-human`, comments the summary, removes in-progress (last, so a retry repeats nothing visible). A closed issue or one no longer in progress is left alone. Ends `rt-handback: ok ...`. |

All three accept `--in-progress-label` and `--needs-human-label` (defaults
`rt:in-progress`, `needs-human`) and exit 2 on bad usage. The scripts are
tracked in [roundtable-k8s/arsenal#16](https://github.com/roundtable-k8s/arsenal/issues/16);
until they ship, a chain-mode run fails its post-run steps and the operator
hands the issue back.

Task templates do not expose the structured `prs` and `verdict` fields yet,
so `verify` gets the final review's text (whose `PRs:` and `Verdict:` lines are
what the structured fields are parsed from) and the knight passes the PR URLs
and the verdict to `rt-verify` as arguments.

### What the operator still does with `postRun: chain`

The operator parses no step output for the table. It counts unfinished runs
against `maxActive` / `maxActivePerRepo` (a finished run frees its slot) and
garbage-collects finished Chains after `ttlSecondsAfterFinished`. Once per
finished run it also releases the issue if the run left it open with
`rt:in-progress`:

- the issue already has `needs-human` (escalate, verify or handback labelled
  it): `rt:in-progress` is removed;
- the run did not succeed (Failed, e.g. `handback` itself failed, or
  PartiallySucceeded): `needs-human`, a comment naming the failed steps, and
  `rt:in-progress` removed;
- the run succeeded: nothing. The queued PRs close the issue when they merge,
  and `close_parent` settled a split parent.

So no issue stays `rt:in-progress` because a `verify` or `handback` step
failed.

### Switching a table

Switch both settings together while the table has no unfinished pickup runs
and no split parent still waiting to close (a builtin run's parent is only
closed by the builtin mode):

1. Set `prPipeline.postRun: chain` in the chart values and upgrade the
   release (the `pr-pipeline` Chain template gets the new steps; runs already
   started keep their copy).
2. `kubectl patch roundtable <table> --type merge -p '{"spec":{"issuePickup":{"postRun":"chain"}}}'`.

Back: `"postRun":"builtin"` on the table and `prPipeline.postRun: builtin`.
Never run a chain-mode pipeline on a builtin table (both would verify and hand
back), or a builtin pipeline on a chain table (nobody would verify the merge).

By hand:

A spec change resets the Chain to `Idle`; the run annotation starts a run:

```bash
kubectl -n roundtable patch chain pr-pipeline --type merge \
  -p '{"spec":{"input":"{\"repo\": \"roundtable-k8s/roundtable\", \"issue\": 231}"}}'
kubectl -n roundtable annotate chain pr-pipeline ai.roundtable.io/run="$(date +%s)" --overwrite
```

A Chain runs one issue at a time. For several issues at once, create one copy
of the Chain per issue (e.g. `pr-pipeline-231`) with its own input.
`ai.roundtable.io/retry-failed` resumes a failed run (see
[conditional-steps.md](conditional-steps.md#retry-failed)).
