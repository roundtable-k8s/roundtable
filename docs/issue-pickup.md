# Issue pickup

Issue pickup closes the front of the loop (#224, epic #240): the operator
polls GitHub for issues labelled `rt:ready`, claims each one and starts the
[PR pipeline Chain](pr-pipeline.md) with `{"repo": ..., "issue": ...}` as
input. A human only has to label an issue ready.

It is part of the operator (an `issuepickup` controller on RoundTables), not a
separate deployment, and it is **off** unless a RoundTable sets
`spec.issuePickup`.

## How it works

Every `intervalSeconds` (default 300) the controller, for each RoundTable with
`spec.issuePickup`:

1. Lists the Chains it created for this table (label
   `ai.roundtable.io/pickup-roundtable=<table>`), counts the unfinished ones
   per repo, and deletes finished ones older than `ttlSecondsAfterFinished`
   (default 7 days; `0` keeps them).
2. Reads the GitHub token from the table's `spec.github.tokenSecretRef`
   Secret (the same token the table's knights get; re-read on every poll, so
   rotated installation tokens work).
3. For each repo in `repos`, lists open issues (not PRs) with `readyLabel`,
   oldest first, and for each issue:
   - **Needs a human** (label `needsHumanLabel`, or the title/body matches one
     of `humanPatterns`): adds `needsHumanLabel`, comments why, removes
     `readyLabel`. No Chain is started.
   - **Over capacity** (`maxActive` unfinished Chains for the table, or
     `maxActivePerRepo` for the repo): left alone; it is picked up by a later
     poll.
   - Otherwise it **starts a Chain** and **claims** the issue (below).

### Starting the Chain

The Chain named by `chainRef` (normally `pr-pipeline`, rendered by the chart's
`prPipeline` values) is a template. Pickup copies its spec to a new Chain named
`<chainRef>-<repo name>-<issue>` (e.g. `pr-pipeline-roundtable-224`), sets
`spec.input` to `{"repo": "<owner/name>", "issue": <n>}`, drops `schedule`, and
starts it with the `ai.roundtable.io/run` annotation. The copy is owned by the
RoundTable and annotated with `ai.roundtable.io/pickup-repo` and
`ai.roundtable.io/pickup-issue`. The template itself is never run by pickup.

This is the "one copy of the Chain per issue" pattern from
[pr-pipeline.md](pr-pipeline.md#run-it-for-an-issue), so several issues run
at once without sharing a run.

### Claiming

The claim adds `inProgressLabel` (`rt:in-progress`), assigns `assignees`
(optional), removes `readyLabel` and comments with the Chain name. Once all of
that succeeded, `ai.roundtable.io/pickup-claimed` on the Chain is set to the
run trigger.

Claiming is idempotent:

- The Chain name is derived from repo and issue, so the same issue never gets
  a second Chain; a poll that finds the Chain running only finishes a claim
  that a previous poll left half-done (a GitHub call failed) or drops a ready
  label someone re-added.
- The comment is the last GitHub call, so a retried claim does not post it
  twice.
- Only the operator's leader runs the controller.

### Running an issue again

When the Chain has finished (merged, escalated or failed), add `rt:ready` to
the issue again: the next poll re-runs the same Chain with the template's
current spec and a new run trigger, and claims the issue again. To start from
scratch, delete the Chain first.

## Enable it

Prerequisites:

- The PR pipeline Chain exists in the RoundTable's namespace
  (`prPipeline.enabled: true` in the chart values, see
  [pr-pipeline.md](pr-pipeline.md#install)).
- The RoundTable has `spec.github.tokenSecretRef`. The token needs **Issues:
  read & write** on every listed repo (the knights' narrowed bot token,
  contents/pull_requests/issues, is enough). It does not need workflows,
  projects or administration permissions.

## Who may trigger pickup

Anyone who can apply the ready label (`rt:ready` by default) on a listed repo
can start a pipeline run: that is triage/write access on the repo. Treat label
rights on a listed repo as pipeline access, because the issue text becomes the
knights' task. List only repos whose label rights you trust, and never list a
repo where untrusted users can label issues.

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: RoundTable
metadata:
  name: roundtable-dev
  namespace: roundtable
spec:
  # ...
  github:
    tokenSecretRef:
      name: roundtable-github-token
      key: token
  issuePickup:
    repos:
      - roundtable-k8s/roundtable
      - roundtable-k8s/roundtable-ui
    chainRef:
      name: pr-pipeline
    maxActive: 2          # unfinished pickup Chains for the table
    maxActivePerRepo: 1   # ... and per repo
    intervalSeconds: 300
```

Full example: [`examples/roundtable-issue-pickup.yaml`](../examples/roundtable-issue-pickup.yaml).

Pause it with `spec.issuePickup.suspend: true` (running Chains continue) or
remove `spec.issuePickup`. A suspended RoundTable (`spec.suspended`) does not
poll either.

## Fields

| Field | Default | Description |
|---|---|---|
| `repos` | (required) | `owner/name` repos to poll (1–50) |
| `chainRef.name` | (required) | Chain copied per issue (the PR pipeline) |
| `readyLabel` | `rt:ready` | Label that marks issues to pick up |
| `inProgressLabel` | `rt:in-progress` | Label set when claimed |
| `needsHumanLabel` | `needs-human` | Label that blocks pickup; set when handing an issue back |
| `humanPatterns` | `\.github/workflows`, `(?i)\bsecrets?\b`, `(?i)\b(auth\|oauth\|authn\|authz\|authentication\|authori[sz]ation\|credentials?)\b` | RE2 patterns on title + body that hand the issue back to a human |
| `assignees` | `[]` | Logins assigned on claim (GitHub ignores logins it cannot assign, including App bots) |
| `intervalSeconds` | `300` | Seconds between polls (60–86400) |
| `maxActive` | `2` | Unfinished pickup Chains for the table |
| `maxActivePerRepo` | `1` | Unfinished pickup Chains per repo |
| `ttlSecondsAfterFinished` | `604800` | Delete finished pickup Chains after this long; `0` keeps them |
| `suspend` | `false` | Stop polling |

The operator flag `--github-api-url` (default `https://api.github.com`, set
through the chart's `extraArgs`) points it at another GitHub API, e.g. GitHub
Enterprise Server.

## Status

```bash
kubectl -n roundtable get roundtable roundtable-dev -o jsonpath='{.status.issuePickup}'
kubectl -n roundtable get chains -l ai.roundtable.io/pickup-roundtable=roundtable-dev
```

`status.issuePickup` has `lastPollTime`, `active` (unfinished pickup Chains),
`pickedTotal`, `skippedTotal` and `lastError` (empty when the last poll
succeeded). The RoundTable also gets `IssuePickedUp`, `IssueNeedsHuman` and
`IssuePickupFailed` events.

## Limits

- **Labels, not the project board.** The bot token has no
  `organization_projects` permission, so pickup reads the `rt:ready` label,
  not the org project's **Ready** column, and does not move cards. Use a
  project workflow (or a human) to keep the board in sync with the labels.
- **Bots cannot be assignees.** GitHub does not let a GitHub App's bot user be
  assigned, so the claim is the `rt:in-progress` label and the comment;
  `assignees` is for human owners.
- **The human check is text-only.** Before the plan there is no diff, so
  `humanPatterns` match words in the issue. It errs on the safe side (an
  issue that merely mentions "auth" is handed back); the PR pipeline's
  `escalate` step still catches the rest. Tune `humanPatterns` per table.
- **First page only.** Each poll reads up to 100 ready issues per repo.
- **Polling.** Event-driven pickup via a GitHub webhook into JetStream is
  #176.
