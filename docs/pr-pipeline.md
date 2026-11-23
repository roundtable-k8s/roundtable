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
([pr-feedback.md](pr-feedback.md)).

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
example's header comment.

`make helm-check` lints the chart and fails if the example no longer matches
the rendered template. CI doesn't run it yet, so run it after any change to
the template, the `prPipeline` values or the Chain API.

## Run it for an issue

[Issue pickup](issue-pickup.md) does this automatically for issues labelled
`rt:ready` (RoundTable `spec.issuePickup`). By hand:

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
