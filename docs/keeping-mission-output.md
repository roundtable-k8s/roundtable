# Keeping mission output

A Mission's status holds a result and a cost breakdown, and with
`retainResults: true` the operator copies the results to a ConfigMap before
cleanup. That copy is for post-mortems, not a record you curate. To keep what a
mission produced, a knight writes it somewhere durable that the owner chose.
There are three ways to do that, and they can be combined.

| Way | Who writes | Where it ends up | Use it when |
|-----|-----------|------------------|-------------|
| `outputPath` + `outputKnightRef` | The output knight, from a step's result | A file in the knight's vault mount (for example `Briefings/`) | You want a dated note you read in Obsidian or grep later |
| Final step opens a pull request | A knight with a repository token, through `gh` | A commit and a PR on a repo | The output is code, docs or a report that belongs in version control and review |
| `notify.webhook` | The operator, after completion | A `roundtable.notify/v1` JSON payload POSTed to your endpoint | Another system needs to know the mission finished, and the full output lives elsewhere |

## 1. `outputPath` with `outputKnightRef`

Set `outputPath` on a step. After the step succeeds, the controller dispatches
a write task to the knight named by the chain's `outputKnightRef` (default
`gawain`), and that knight writes the step's output to the path. The path may
use `{{ .Date }}`, `{{ .Chain }}` and `{{ .Step }}`.

The writer needs a vault mount, and the path must sit under one of the
knight's `vault.writablePaths`:

```yaml
spec:
  vault:
    claimName: roundtable-vault
    writablePaths:
      - "Briefings/"
```

The example is [`examples/mission-archive-results.yaml`](../examples/mission-archive-results.yaml):
a research step, then a final `archive` step with
`outputPath: "Briefings/{{ .Date }}-{{ .Chain }}.md"`, run by a Mission. The
scheduled briefing in [`examples/chain-scheduled-briefing.yaml`](../examples/chain-scheduled-briefing.yaml)
uses the same mechanism on a cron Chain.

Choose this when the output is prose for people, and the vault is where they
read it.

## 2. A final step that opens a pull request

When the output is a change to a repository, do not write a file. Make the last
step a task for a knight that has a repository token. It commits the files on a
branch and runs `gh pr create`. Put `Closes #<n>` in the body only when the
mission is working an issue.

This is the same shape the PR pipeline uses; see [PR pipeline](pr-pipeline.md).
The pipeline's review and merge steps are optional for a plain archive, so a
mission can stop at "PR opened".

Choose this when the output needs review, history, or a merge. A PR is also the
only one of the three that a human can edit before it lands.

## 3. `notify.webhook`

`spec.notify.webhook` is set on a Mission or a Chain. On completion the operator
POSTs a `roundtable.notify/v1` JSON payload to the URL. The payload carries the
name, namespace, phase, start and finish times, and the output. The output is
truncated to a size limit, and `truncated` says so. For the full result, pair
the webhook with one of the two ways above.

The URL must match one of the operator's `notify.allowedURLPrefixes` (a Helm
value). Other URLs are rejected, with a `NotificationSent=False` condition and a
warning event.

```yaml
spec:
  notify:
    webhook:
      url: "https://hooks.example.com/roundtable"   # must match an allowed prefix
```

Choose this to trigger something downstream, such as a chat message or another
pipeline, when the mission ends.

## Combining them

The three are independent. A typical setup is `outputPath` for the archive, plus
`notify.webhook` so a dashboard or chat channel hears about it:

1. The final step writes `Briefings/<date>-<chain>.md` through `outputKnightRef`.
2. The chain or mission posts the webhook when it completes.

## Checklist

- The writer knight has a vault claim, and `writablePaths` covers the `outputPath`.
- The `outputKnightRef` on the Chain names that knight (it defaults to `gawain`).
- The PR author knight has a repository token before you rely on option 2.
- The webhook URL matches `notify.allowedURLPrefixes`.
