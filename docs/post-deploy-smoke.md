# Post-deploy smoke: verify every rollout, hold on failure

After an operator or knight image rollout, the operator runs a cheap smoke
Chain against every knight. A pass records the rollout as known-good. A failure
opens a `needs-human` issue with the step outputs and holds image automation
(#228; the automation is the Round Table Autodeploy workflow in
`dapperdivers/dapper-cluster`).

```
operator (RoundTable spec.smoke, polls every 60 s, no GitHub call to detect)
  version = hash(OPERATOR_IMAGE + every persistent knight's image)
  new version, all knights Ready, seen on two polls in a row,
  smoke chain free → set the Chain's input, start a run
  ├─ pass → known-good: status.smoke + ConfigMap <roundtable>-smoke
  └─ fail → issue (label needs-human) with step outputs
            + label `hold` on the autodeploy PR (spec.smoke.hold)
```

Two pieces, both off by default:

- **The watcher** is a controller inside the operator. It is switched on per
  RoundTable with `spec.smoke`.
- **The smoke Chain** is rendered by the chart (`smoke`, template
  `templates/smoke.yaml`, example
  [`examples/chain-smoke.yaml`](../examples/chain-smoke.yaml)). It checks
  `prPipeline.lead` and `prPipeline.coders` unless `smoke.knights` is set.
  The example is generated from the chart: `make helm-check` fails when it
  drifts; re-render it like the [PR pipeline](pr-pipeline.md#install) example.

## Install

```yaml
# values.yaml
smoke:
  enabled: true
  roundTableRef: {name: roundtable-dev}
  # model: haiku   # optional per-task model override check
```

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: RoundTable
metadata:
  name: roundtable-dev
spec:
  github:
    tokenSecretRef: {name: roundtable-github-token, key: token}
  smoke:
    enabled: true
    issueRepo: roundtable-k8s/roundtable
    hold:
      repo: dapperdivers/dapper-cluster
      # defaults: branch autodeploy/roundtable, label hold
      # tokenSecretRef: {name: cluster-hold-token, key: token}
    # defaults:
    # chainRef: {name: rollout-smoke}
    # needsHumanLabel: needs-human
    # pollIntervalSeconds: 60
    # tokenSecretRef: {name: ..., key: ...}   # else spec.github.tokenSecretRef
```

The chart sets `OPERATOR_IMAGE` on the operator pod to its own image; without
it only knight images are part of the version.

## What is tested

Per knight, in parallel across knights:

| Step | Checks |
|---|---|
| `smoke_<knight>` | identity; `git ls-remote` and `gh auth status`; skills listed; the guardrail blocks a recursive delete of `/` (run with `--preserve-root`, so even a missing guardrail cannot delete anything); the `Model:` line equals the resolved model when `smoke.model` is set |
| `ctx_set_<knight>` → `ctx_probe_<knight>` | the first step is told a codeword in one agent session; the second runs in a different session (`resumeRunId` `smoke-a-<version>` vs `smoke-b-<version>`) and must not know it |

The NATS round trip is the task reaching the knight and the reply reaching
the operator; a knight that does not answer fails its steps.

Deviation from the issue: chain tasks always carry a `runId`, so "two run-less
tasks share no context" is tested as "two tasks with different `runId`s share
no context", the property the [task contract](knight-task-contract.md)
guarantees.

Every step ends with exactly one line `Result: pass` or `Result: fail
(<reason>)`. The watcher judges the run from the step statuses: a step must
have succeeded and carry a `Result: pass` line of its own (quoting it in prose
does not count), and when the step ran with a resolved model, the `Model:` line
must name that model. A step that failed, timed out, or said nothing fails the
smoke, as does a run in which no step ran. All steps use `continueOnFailure`, so
one broken knight does not hide another.

## Rollout detection

Each poll the watcher lists the table's Knights (ephemeral and warm-pool
knights are ignored) and hashes `operator=<OPERATOR_IMAGE>` with
`knight/<name>=<effective image>`. A version is tested when

1. it is neither the last tested nor the known-good version,
2. every non-suspended knight is `Ready` (a rollout still in progress is not),
3. the previous poll saw the same version, and
4. the smoke Chain is not running or suspended.

A failed version is not retried until the next image change; to test it again,
start the Chain by hand (`ai.roundtable.io/run` annotation) or roll a new
image. Image tags that do not change (`:latest`) are not detected.

## Known-good record

On a pass the version and its images go to `status.smoke`
(`knownGoodVersion`, `knownGoodComponents`) and to the ConfigMap
`<roundtable>-smoke` in the table's namespace:

| Key | Value |
|---|---|
| `known-good-version`, `known-good-components` | last passing version and its images (one per line) |
| `last-tested-version`, `last-result` | last smoke run and `Passed` or `Failed` |
| `held` | `true` while a failure holds image automation |
| `updated-at` | RFC 3339 |

## Failure: issue and hold

On a failed smoke the watcher

1. opens an issue in `spec.smoke.issueRepo` labelled `needsHumanLabel`
   (default `needs-human`): the deployed images, the last known-good images and
   each failed step with its (sanitized, fenced, truncated) output. Step output
   is knight text that may echo untrusted content: treat it as data. A failed
   issue call is retried on the next poll.
2. sets `status.smoke.held` and the ConfigMap's `held: "true"`, and
3. if `spec.smoke.hold` is set, labels the open pull request from
   `hold.branch` in `hold.repo` with `hold.label`.

dapper-cluster's *Round Table Autodeploy* workflow skips while its open PR
carries the `hold` label, so labelling `autodeploy/roundtable` stops further
rollouts. If no such PR is open yet, or the call fails, the watcher retries
every poll while the hold stands (`status.smoke.lastError`).

A later passing smoke clears `status.smoke.held` and the ConfigMap flag but
leaves the label: removing the `hold` label is the human's decision, after
reading the issue.

### What a human must wire up

The knights' GitHub token cannot reach `dapperdivers/dapper-cluster`, so the
label write needs a credential the operator can use:

- Create a Secret in the release namespace with a token that has
  *Pull requests: write* on `dapperdivers/dapper-cluster` (a fine-grained PAT
  or a GitHub App installation token) and reference it as
  `spec.smoke.hold.tokenSecretRef`.
- Make sure the `hold` label exists in dapper-cluster (the labels API creates
  it on first use, but pre-creating it keeps the colour and description yours).
- Without the token, leave `hold` unset: the failure still opens the issue and
  sets `status.smoke.held` and the ConfigMap, and someone (or a job in
  dapper-cluster reading the ConfigMap's `held` key) applies the label.

The hook is the ConfigMap `held` key and `status.smoke.held`; the label itself
is the contract with Autodeploy.

## Breaking changes

This change is additive: a new optional `spec.smoke` and `status.smoke` on
RoundTable (CRD field additions only), a new optional chart value block, a new
`OPERATOR_IMAGE` env on the operator pod and a new controller that is idle
unless `spec.smoke.enabled`. No existing field, subject or behaviour changes,
so it does not carry the `breaking` label
([protocol](pr-pipeline.md#breaking-changes); no open PR was labelled
`breaking` when it was written). Apply the updated CRDs before upgrading the
operator, as for every release (Helm does not update CRDs in `crds/`).

## Cost

One checks task per knight and two tiny ones for the isolation test, per
rollout (not per poll). Set `smoke.model` to a cheap alias to bound the checks
step's cost.
