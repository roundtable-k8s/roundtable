# Dev factory

An issue-to-merged-PR software factory built only from generic pieces: GitHub
Triggers pick up issues and watch PRs, the `pr-pipeline` and
`pr-pipeline-feedback` Chains do the work, the post-run checks are Chain
steps, and the arsenal's `project` skills hold the GitHub scripts. Nothing
here is special to the platform: the same objects run any other workflow.

The chart still carries the built-in pipeline; this recipe is the generic route
next to it and removes nothing ([switching over](#from-the-built-in-pipeline-to-the-recipe)).

| File | What |
|---|---|
| [`values.yaml`](values.yaml) | Chart values: the `pr-pipeline` Chain in chain post-run mode, the `pr-pipeline-feedback` Chain |
| [`roundtable.yaml`](roundtable.yaml) | The table: `issuePickup.engine: trigger`, `issuePickup.postRun: chain`, `prFeedback.engine: trigger`, model aliases |
| [`knights.yaml`](knights.yaml) | The lead `rt-lead` and one coder `rt-coder`, isolated with an allowlist |

The repo `acme/api`, the table name `roundtable-dev` and the subject prefix
`roundtable-dev` are placeholders; change them together.

## The life of an issue

```
issue labelled rt:ready
  → Trigger roundtable-dev-issue-pickup          claims it (rt:in-progress, comment)
  → Chain pr-pipeline-acme-api-<n>               one copy per issue
      plan (lead) → implement (coder) → review (lead) ⇄ fix (coder)
      → merge (lead: gh pr merge --auto --squash)
      → verify (lead: rt-verify) → close_parent (split runs only)
      | escalate (lead: needs-human)
      | handback (lead: rt-handback, when any step failed)
  → merge queue → merged, issue closed by "Closes #<n>"
  red CI or merge-queue rejection on the PR
  → Trigger roundtable-dev-pr-feedback-<author>  fires per PR
  → Chain pr-pipeline-feedback: fix (author coder, same session) → requeue (lead)
```

| Object | What it does |
|---|---|
| RoundTable `roundtable-dev` | Holds the token, model aliases, caps and the three switches; generates the Triggers |
| Trigger `<table>-issue-pickup` (generated) | Polls the repos for `rt:ready` issues and starts one Chain copy per issue ([triggers docs](../../../docs/triggers.md#issue-pickup-on-a-trigger)) |
| Chain `pr-pipeline` (chart) | The pipeline above ([pr-pipeline docs](../../../docs/pr-pipeline.md)) |
| Chain steps `verify`, `close_parent`, `handback` | Post-run checks, calling the arsenal `project/pr-postrun` scripts and `project/pr-review` |
| Triggers `<table>-pr-feedback-<author>` (generated) | Select the author's PRs by checks and merge state ([triggers docs](../../../docs/triggers.md#pr-feedback-on-a-trigger)) |
| Chain `pr-pipeline-feedback` (chart) | Sends the failure back to the PR's author, then the lead re-queues ([pr-feedback docs](../../../docs/pr-feedback.md)) |
| Knights `rt-lead`, `rt-coder` | Lead plans, reviews, merges and hands back; the coder writes code and opens PRs |

The operator keeps a small role in chain post-run mode: it counts unfinished
runs against `maxActive` / `maxActivePerRepo`, deletes finished Chains after
`ttlSecondsAfterFinished`, and releases an issue a failed run left
`rt:in-progress`.

## Labels

| Label | Set by | Meaning |
|---|---|---|
| `rt:ready` | a human | Take this issue. Whoever can apply it controls the pipeline, see [who may trigger pickup](../../../docs/issue-pickup.md#who-may-trigger-pickup) |
| `rt:in-progress` | pickup | A run owns the issue; removed on hand-back |
| `needs-human` | `escalate`, `verify`, `handback`, feedback after `maxAttempts` | The pipeline stopped; a human decides. Removing it on a PR resets feedback |
| `breaking` | planner, coder or a human | While a `breaking` PR is open, pickup starts no new run in that repo ([breaking changes](../../../docs/pr-pipeline.md#breaking-changes)) |
| `protected-paths` | the merge-guard check | Visibility only: the PR touches sensitive paths |

## Model tiers and the fallback coder

The planner picks a tier per issue from `implementModels`: `cheap` (a small,
well-specified issue, one area, no API change), `fast` (the default), `deep`
(design-heavy or cross-cutting). `modelAliases` on the table map each tier to
a model; the aliases must resolve on the coder's runtime. Every fix round runs
`fixModel` (`deep`), so a cheap draft is repaired by a stronger model. Lead
steps set no model and use the lead knight's own; pin one with `leadModel`.

With `fallbackCoder` set (a knight on a runtime with its own quota), a coder
refusing a task for plan quota gets that step re-run on the fallback, with no
model override. Without one the step is deferred until the quota window resets,
nothing fails and the issue stays `rt:in-progress`
([quota refusals](../../../docs/pr-pipeline.md#steps)).

## When a run fails

1. The issue. A failed run ends with `needs-human` on the issue and a
   comment from `handback` naming the failed steps; `rt:in-progress` is gone.
   That comment is the first thing to read.
2. The Chain. `kubectl -n roundtable get chain` lists the runs;
   `kubectl -n roundtable describe chain pr-pipeline-acme-api-<n>` shows each
   step's status and output (`status.stepStatuses`). The `merge` output has the
   PR URLs and fix rounds. Resume a failed run with the
   `ai.roundtable.io/retry-failed` annotation
   ([conditional steps](../../../docs/conditional-steps.md#retry-failed)).
3. The dead-letter view. A task a knight lost or could not finish lands in the
   table's dead-letter stream: `GET /v1/dead-letters` on the gateway lists
   them, and `POST /v1/dead-letters/{taskId}/retry` republishes one
   ([gateway docs](../../../docs/gateway.md#get-v1dead-lettersknightreasonstatelimit)).
   A task that belongs to a chain is retried through the chain instead.
4. The Triggers. `kubectl -n roundtable describe trigger
   roundtable-dev-issue-pickup`: the `GitHubReady` condition, events and
   `status.recentFirings` show polling problems, held issues and hand-backs.
   Also `kubectl get roundtable roundtable-dev -o yaml` for
   `status.issuePickup` and `status.prFeedback`.

## Costs to expect

- Every issue is at least three lead tasks (plan, review, merge) plus verify
  and a coder task; each fix round adds a coder task and a re-review. Fix
  rounds are capped by `maxIterations` (3); feedback rounds by `maxAttempts`.
- The coder dominates: long tasks on a build-and-test loop. `cheap` for small
  issues is the main lever; `maxActive` bounds how many run at once.
- Chains do not record cost per step yet. The RoundTable's
  `status.totalCost` and the `roundtable_knight_*` metrics track spend
  per table and knight, and the `KnightLLMCostSpike` alert
  ([observability](../../../docs/observability.md)) catches a runaway.
- A failed or escalated run still cost its steps. Start with `maxActive: 1`
  and a few small issues.

## What this needs from you

Outside the cluster; none of it is automated here.

- **A GitHub token** for the knights and the operator, as the Secret
  `roundtable-github-token`: a GitHub App installation token or a bot token
  with contents, pull requests and issues write on the listed repos
  (checks and Actions read on private repos). No workflows or administration
  permission. Set `authors` in `roundtable.yaml` to that bot's login.
  ```bash
  kubectl -n roundtable create secret generic roundtable-github-token \
    --from-literal=token="$GITHUB_TOKEN"
  ```
- **A merge queue** on the target branch: the lead queues PRs with
  `gh pr merge --auto --squash`, and PR feedback reads merge-queue rejections.
- **The `merge-guard` required check** with its `merge-guard-paths` file in
  the target repo, so a PR touching sensitive paths is labelled
  `protected-paths` and not merged unchecked ([breaking
  changes](../../../docs/pr-pipeline.md#breaking-changes) describes the file).
  `merge-guard` is in `ignoreChecks` for feedback, so a policy failure is
  not a fix round.
- **The arsenal** with `project/pr-postrun` and `project/pr-review`
  ([arsenal#16](https://github.com/roundtable-k8s/arsenal/issues/16)): without
  the scripts `verify` fails and the run is handed back.
- **NATS streams from the table** (`nats.createStreams: true`): github-source
  Triggers need the events stream.
- **A repo you trust:** the issue text is the knights' task, and anyone who can
  label `rt:ready` starts a run. `humanPatterns` hand workflow, secret and auth
  issues back to a human ([issue pickup](../../../docs/issue-pickup.md#fields)).
- **Review of what ships:** the lead's review is a model's. Keep branch
  protection and required CI as the real gate.

## Install

```bash
kubectl -n roundtable create secret generic roundtable-github-token --from-literal=token="$GITHUB_TOKEN"
kubectl apply -f knights.yaml
helm upgrade --install roundtable charts/roundtable-operator -n roundtable \
  -f <your values> -f values.yaml
kubectl apply -f roundtable.yaml
```

The Chains' `roundTableRef` and the knights' subjects must name your table;
every knight in `prPipeline` must exist or the Chain is `ChainValid=False`.
Then label a small issue `rt:ready` and watch
`kubectl -n roundtable get chain,trigger -w`.

## From the built-in pipeline to the recipe

Three independent switches, in this order. Do them while the table has no
unfinished pickup runs and no split parent waiting to close.

1. **Issue pickup on a Trigger.**
   `kubectl -n roundtable patch roundtable roundtable-dev --type merge -p '{"spec":{"issuePickup":{"engine":"trigger"}}}'`.
   The table needs `nats.createStreams: true`; check the Trigger's `Ready`
   and `GitHubReady` conditions. Running Chains keep going and are not
   started twice.
2. **Post-run checks as chain steps.** Set `prPipeline.postRun: chain` in the
   chart values and upgrade, then
   `kubectl -n roundtable patch roundtable roundtable-dev --type merge -p '{"spec":{"issuePickup":{"postRun":"chain"}}}'`.
   Never run a chain-mode pipeline on a builtin table (both verify) or the
   reverse (nobody does).
3. **PR feedback on Triggers.** Enable `prPipeline.feedback` in the values,
   then
   `kubectl -n roundtable patch roundtable roundtable-dev --type merge -p '{"spec":{"prFeedback":{"engine":"trigger"}}}'`.
   Flaky-test issues are not filed on this engine.

To go back, undo in the reverse order: `prFeedback.engine: builtin`,
`issuePickup.postRun: builtin` together with `prPipeline.postRun: builtin`,
then `issuePickup.engine: builtin`. Switching an engine back deletes the
generated Triggers; runs they started keep going.
