# Backlog grooming

On a schedule, a lead knight reads a repository's open epics and files
loop-ready sub-issues for work that has no issue yet. Issues carry the
`groomed` label and never `rt:ready`; a human promotes the ones worth doing.
Nothing here is specific to one repository or to the table's own: the
repository and the labels come from the Trigger's input.

```
Trigger backlog-grooming (cron, Forbid) --> Chain backlog-grooming
   survey (groomer)  reads epics and existing issues, picks candidates
   file   (groomer)  runs only when survey found work; files the issues
```

| File | What |
|---|---|
| [`knight.yaml`](knight.yaml) | The `groomer` Knight: GitHub token, egress to `api.github.com` only, no cluster access |
| [`chain.yaml`](chain.yaml) | The `backlog-grooming` Chain: `survey`, then `file` gated with `when` |
| [`trigger.yaml`](trigger.yaml) | The schedule Trigger and its `spec.input` settings |

## Before you start

Needs the [shared "Before you start"](../README.md#before-you-start): operator and NATS, a RoundTable `fleet-a`, `spec.nats.url` on the `groomer` Knight, model credentials (the `sonnet` alias runs on the claude-knight image the manifest sets); plus:

- A RoundTable named `fleet-a` exists (see
  [`roundtable-fleet.yaml`](../../roundtable-fleet.yaml)). Rename `fleet-a`
  (and the `fleet-a.tasks.` subject) in the manifests if yours differs.
- The `shared` skill is in your arsenal; change `spec.skills` if not.
- The knight image provides the `gh` CLI. This recipe assumes it does: the
  `gh` shim in the claude-knight image passes through to the next `gh` on
  `PATH`, and the image's Dockerfile is not in this repository, so that is
  unverified here. Check with
  `kubectl -n roundtable exec <groomer pod> -- gh --version`; if it is
  missing, add `gh` to `spec.tools.nix` on the Knight.

## The token

Create the token Secret the Knight refers to (`groomer-github`, key `token`):

```bash
kubectl -n roundtable create secret generic groomer-github --from-literal=token=<token>
```

Use a fine-grained token or GitHub App installation token for the target
repository with only **Issues: read and write** (and **Metadata: read**,
which is implied). Do not give it Contents or Pull requests write. The
prompts forbid the knight from doing anything but filing issues, but a prompt
is not a limit: the token's scope is. With this scope the worst the knight
can do is file or edit issues and labels.

## Install

Set `repo` in [`trigger.yaml`](trigger.yaml) (the placeholder `OWNER/REPO`),
then:

```bash
kubectl apply -f knight.yaml -f chain.yaml -f trigger.yaml
kubectl -n roundtable get trigger backlog-grooming   # Ready, Last Fired
```

## Change the repository and labels

Everything is in the Trigger's `spec.input`, a JSON object the Chain's prompts
read:

| Field | Default | Meaning |
|---|---|---|
| `repo` | `OWNER/REPO` | The repository to groom |
| `epicLabel` | `epic` | Open issues with this label are the epics to read |
| `issueLabel` | `groomed` | The label filed issues carry (created if missing) |
| `humanLabel` | `needs-human` | Issues (epics or sub-issues) with this label are left alone |
| `maxIssuesPerRun` | `3` | At most this many issues filed per run |
| `maxOpenGroomed` | `10` | No issues filed while this many open issues already carry `issueLabel` |

To groom a second repository, copy `trigger.yaml` under another name with its
own `repo`; the Chain and Knight are shared. The schedule is `cron` and
`timeZone` in the same file. `concurrency.policy: Forbid` stops a slow run
being doubled by the next fire.

## Run it once

Put your repository into the `input` of `chain.yaml` (its default is the same
placeholder as the Trigger's), apply it, and start a run:

```bash
kubectl -n roundtable annotate chain backlog-grooming ai.roundtable.io/run="$(date +%s)" --overwrite
kubectl -n roundtable get chains,tasks -w
```

Or set the Trigger's `cron` to the next minute and put it back afterwards.
The `file` step shows as `Skipped` (`WhenFalse`) when the survey found nothing
to file. Its output is one `#<number> <title>` line per issue filed.

## What the knight may and may not do

May:

- Read the repository's issues, and its files through the GitHub API, to name
  real files in the issues it files.
- Create issues labelled `issueLabel`, and create that label if it is missing.
- Reach `api.github.com` (and the always-allowed baseline: DNS, NATS, model
  providers).

May not (prompt policy, bounded by the token's scope):

- Add the `rt:ready` label, so nothing it files is picked up by
  [issue pickup](../../../docs/issue-pickup.md) on its own.
- Touch an issue labelled `humanLabel`.
- File more than `maxIssuesPerRun` per run, or any while `maxOpenGroomed`
  groomed issues are open.
- Edit, comment on or close an existing issue.
- Change anything in the cluster or push code: no cluster permissions, and a
  token without contents access.

Issue titles and bodies are outside data, and the knight's prompt says not to
follow instructions found in them.

## Promote a groomed issue

Read the issue. If it is right, add `rt:ready`
(`gh issue edit <n> --repo OWNER/REPO --add-label rt:ready`); the pickup
engine then consumes it. If it needs a person's judgement, add `needs-human`
and the groomer ignores it from then on.
