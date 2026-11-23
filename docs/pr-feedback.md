# PR feedback: red CI and merge-queue rejections go back to the author

When a pull request the [PR pipeline](pr-pipeline.md) opened goes red in CI or
is dropped from the merge queue, the operator notices and hands the failure
back to the knight that wrote the PR, in the same agent session. Nobody has to
watch the queue.

```
operator (RoundTable spec.prFeedback, polls GitHub every 5 min)
  detect: failed checks on the PR head | failed merge_group run
  trace:  PR URL → the chain step that opened it → knight + runId
  ├─ flaky (failed and passed on one commit) → flake issue, no fix round
  ├─ fix rounds used up → label needs-human + comment
  └─ otherwise → feedback Chain: spec.input = failure + trimmed log, run
        fix_<coder>  (resumeRunId = pipeline runId)  "Result: pushed | no change"
        → requeue (lead: re-review if code beyond tests changed, gh pr merge --auto)
        | escalate (no fix ran: needs-human)
```

Two pieces, both off by default:

- **The watcher** is a controller inside the operator (no extra deployment).
  It is switched on per RoundTable with `spec.prFeedback`.
- **The feedback Chain** is rendered by the chart (`prPipeline.feedback`,
  template `templates/pr-feedback.yaml`, example
  [`examples/chain-pr-feedback.yaml`](../examples/chain-pr-feedback.yaml)). It
  reuses `prPipeline.lead`, `prPipeline.coders` and `prPipeline.roundTableRef`.

## Install

```yaml
# values.yaml
prPipeline:
  enabled: true
  coders: [rt-operator, rt-ui]
  feedback:
    enabled: true
```

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: RoundTable
metadata:
  name: roundtable-dev
spec:
  github:
    tokenSecretRef: {name: roundtable-github-token, key: token}
  prFeedback:
    enabled: true
    repos: [roundtable-k8s/roundtable, roundtable-k8s/roundtable-ui]
    # defaults:
    # authors: ["dunce-bot[bot]"]
    # chainRef: {name: pr-pipeline-feedback}
    # maxAttempts: 3
    # pollIntervalSeconds: 300
    # escalationLabel: needs-human
    # flakeLabel: flaky-test
    # fallbackKnightRef: {name: rt-operator}   # PRs no chain run opened
    # tokenSecretRef: {name: ..., key: ...}    # else spec.github.tokenSecretRef
```

**Token.** The operator reads the token Secret (by default the table's
`spec.github.tokenSecretRef`, the knights' dunce-bot token) on every poll, so
a rotating GitHub App installation token keeps working. It needs:

| Call | Permission |
|---|---|
| list PRs, read a PR | Pull requests: read |
| check runs on the PR head, merge_group workflow runs, job logs | Checks / Actions: read. **Public repositories need none.** |
| label and comment on PRs, open flake issues | Issues / Pull requests: write |

No `workflows` or Actions write access: the watcher never re-runs jobs or
touches workflows; the lead re-queues with `gh pr merge --auto` as it does in
the pipeline's merge step. For a private repository add Checks: read and
Actions: read to the GitHub App.

## Detection

Each poll, per repository in `repos`:

1. `GET /pulls?state=open`, keeping PRs by one of `authors` without the
   `escalationLabel`.
2. `GET /actions/runs?event=merge_group` (one call per repository). Merge-group
   runs are matched to PRs by their branch, `gh-readonly-queue/<base>/pr-<n>-…`;
   the newest merge commit per PR is that PR's latest queue attempt.
3. `GET /commits/<head>/check-runs?filter=all` per PR.

A PR has a failure when the newest attempt of a check on its head commit
failed (`failure` or `timed_out`), or else when a workflow run of its latest
merge group failed. Every failure has a key (`ci:<head>:<checks>` or
`mq:<merge commit>`) that is handled once; a re-run of the same failure does
not start another round.

**Tracing the author.** The watcher looks through the Chains in the
RoundTable's namespace for a step whose output contains the PR URL: the
pipeline's implement step replies with it on its first line, a `forEach`
split item carries its own runId, and loop body outputs are searched too. The
first such step names the knight; the chain's `status.runId` (or the item's)
is the session to resume, and the chain input's `issue` is recorded. While
that chain is still `Running` (its own fix loop is still pushing) the PR is
left alone. A PR no chain run opened goes to `fallbackKnightRef` in a fresh
session, or is ignored when that is unset. The mapping is stored in the
RoundTable status as soon as it is found, so a later pipeline run that
overwrites the chain status does not lose it.

## Routing

The feedback Chain runs one failure at a time. When it is free (no run the
watcher started is pending, and it is not `Running` or suspended), the watcher
takes the oldest `Queued` PR, fetches the failed jobs' logs, and in one patch
sets the Chain's `spec.input` and its `ai.roundtable.io/run` annotation:

```json
{"repo": "roundtable-k8s/roundtable", "pr": 221, "url": "https://github.com/roundtable-k8s/roundtable/pull/221",
 "issue": 214, "knight": "rt-operator", "runId": "4b1c…", "attempt": 1, "maxAttempts": 3,
 "kind": "merge-queue", "headSha": "342bb3f…", "failedChecks": ["CI"],
 "failureUrl": "https://github.com/roundtable-k8s/roundtable/actions/runs/45230627457",
 "log": "=== job build ===\n# github.com/…/internal/controller\n##[error]internal/controller/knight_scaling.go:98:9: undefined: agentRuntimeForImage\n…"}
```

The log is trimmed per job (at most three jobs): GitHub's timestamps are
stripped and only the 15 lines before each of the first five `##[error]`
lines are kept (the last 40 lines when there is none), capped at 3000 bytes.

The PR's code controls its CI output, so the log is treated as untrusted:
ANSI escapes and control characters are stripped, and the excerpt is wrapped
in a `<<<UNTRUSTED-CI-LOG-<nonce>` ... `UNTRUSTED-CI-LOG-<nonce>>>>` fence with a
fresh random nonce per dispatch, preceded by a note that it is data, not
instructions, to be used only to locate the failure.

The Chain then runs:

| Step | Knight | Runs when |
|---|---|---|
| `fix_<coder>` | the PR's author | `json(input).knight` is that coder |
| `requeue` | lead | a fix step succeeded |
| `escalate` | lead | no fix step succeeded (unknown knight, or the fix failed) |

`fix_<coder>` and `requeue` set `resumeRunId: 'json(input).runId'`, so their
tasks carry the pipeline run's `runId` and both knights resume the sessions
in which they wrote and reviewed the PR
([knight-task-contract.md](knight-task-contract.md#resumerunid)). The coder
fixes the branch (rebasing when the base moved, the common merge-queue cause)
or, for a failure unrelated to the change, pushes nothing. The lead compares
the head with `headSha`, re-reviews if anything beyond tests or docs changed,
and re-queues with `gh pr merge --auto --squash` (or labels `needs-human`
instead). It does not wait for the merge: the next poll sees the outcome.

## Retry cap and flakes

- **Cap.** Every feedback run is a fix round (`attempts`). A new failure after
  `maxAttempts` rounds labels the PR `escalationLabel` and comments the
  failure; from then on the PR is skipped. Removing the label resets the
  rounds.
- **Flaky now.** If every failed check also passed on the same head commit
  (for example a re-run went red after a green run), the watcher opens a
  `Flaky check: <name>` issue labelled `flakeLabel` (or comments on the open
  one), comments the link on the PR, and starts no fix round.
- **Flaky afterwards.** If a failure was fed back, the coder pushed nothing,
  and the same commit then passes (a newer merge group passes, the checks go
  green, or the PR merges with that head), the failed checks are reported as
  flaky the same way.

The watcher has no Actions write access, so it does not re-run a flaky job
itself; a re-queue by the lead or a manual re-run does that.

## Status

```bash
kubectl -n roundtable get roundtable roundtable-dev -o jsonpath='{.status.prFeedback}' | jq
```

```yaml
status:
  prFeedback:
    lastPollTime: "2026-11-23T17:50:00Z"
    lastError: ""                       # e.g. a missing token or a GitHub error
    activeRun: roundtable-k8s/roundtable#221@prfeedback-221-1795455000
    pullRequests:
      - repo: roundtable-k8s/roundtable
        number: 221
        phase: Fixing                   # Watching | Queued | Fixing | Escalated
        knight: rt-operator
        runId: 4b1c…
        issue: 214
        attempts: 1
        lastFailure: mq:9a0dd02e…
        failureKind: merge-queue
        failureHeadSHA: 342bb3f…
        failedChecks: [CI]
        message: fix round 1/3 started on pr-pipeline-feedback
```

Only PRs with a failure history are recorded (at most 200); merged and closed
PRs are dropped. Events `PRFeedbackDetected`, `PRFeedbackDispatched` and
`PRFeedbackEscalated` are recorded on the RoundTable.

## Limits

- Polling, not webhooks: a failure is noticed within `pollIntervalSeconds`
  (default 5 minutes). The GitHub cost is 2 + (watched open PRs) requests per
  repository per poll, plus a few to fetch logs per feedback run.
- All failed checks count, not only required ones (reading branch protection
  needs admin access).
- A PR is traced only while the chain run that opened it is still the
  chain's latest run, or after the watcher recorded it. Run one Chain copy per
  issue (as [pr-pipeline.md](pr-pipeline.md#run-it-for-an-issue) suggests) to
  keep that window open.
- One feedback run at a time per RoundTable; further failures wait as
  `Queued`. If the feedback Chain does not start a run within 30 minutes
  (invalid or suspended), the failure is queued again.
- Merge conflicts that remove a PR from the queue without a failed run are
  not detected.
