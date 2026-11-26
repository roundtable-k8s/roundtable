# Triggers

A `Trigger` starts a run of a Chain when something happens. A run is a copy of
the target Chain whose `spec.input` is rendered from the event (the same kind
of copy issue pickup and the gateway make, through `internal/chainrun`). The
design decisions are in #400.

Four sources exist today:

| Source | Fires on | Default dedupe key |
|---|---|---|
| `nats` | each message on a subject under the table's `<subjectPrefix>.events.` | `Nats-Msg-Id` header, else `<stream creation time>/<stream sequence>` |
| `webhook` | each authenticated `POST` to the gateway's `/v1/hooks/<namespace>/<name>` | the sender's delivery id (see [Webhook source](#webhook-source)), else `<stream creation time>/<stream sequence>` |
| `schedule` | each cron time, run by an operator-owned CronJob (see [Schedule source](#schedule-source)) | the schedule time (RFC 3339, UTC) |
| `github` | each issue or pull request matching a label query, polled | `<owner/name>#<number>` |

Exactly one source is set.

A firing either starts a run of a Chain (`target.chainRef`) or sends one task
to a knight (`target.knight`, see [Knight target](#knight-target)). Exactly
one target is set.

## Example: NATS

[`examples/trigger-nats.yaml`](../examples/trigger-nats.yaml):

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: Trigger
metadata:
  name: on-deploy
  namespace: roundtable
spec:
  roundTableRef: {name: fleet-a}
  source:
    nats:
      subject: fleet-a.events.deploys.>
  target:
    chainRef: {name: security-report}
  input: |
    Review the security impact of deploying {{ .Event.Data.service }} {{ .Event.Data.version }}.
  dedupeKey: "{{ .Event.Data.service }}@{{ .Event.Data.version }}"
```

Publish an event and watch the run start:

```bash
nats pub fleet-a.events.deploys.prod '{"service":"api","version":"1.2.3"}' \
  -H Nats-Msg-Id:deploy-api-1.2.3
kubectl -n roundtable get chains -l ai.roundtable.io/trigger=on-deploy
kubectl -n roundtable get trigger on-deploy -o yaml   # status.recentFirings
```

## Webhook source

For systems that can only send webhooks (Alertmanager, Flux
notification-controller, Home Assistant, a feed reader). The sender POSTs to
the [invoke gateway](gateway.md#webhooks); the gateway checks the sender's
credential **for that Trigger** and publishes the request body to
`<subjectPrefix>.events.webhook.<namespace>.<name>` on the table's events
stream. The Trigger reads that subject like a NATS source, so the bus stays the
system of record and everything under [Delivery](#delivery-nats-webhook-and-github-sources)
applies.

[`examples/trigger-webhook.yaml`](../examples/trigger-webhook.yaml):

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: alerts-hook
  namespace: roundtable
  labels:
    ai.roundtable.io/webhook-secret: "true"   # required
stringData:
  token: <at least 16 characters, e.g. openssl rand -hex 32>
---
apiVersion: ai.roundtable.io/v1alpha1
kind: Trigger
metadata:
  name: alerts
  namespace: roundtable
spec:
  roundTableRef: {name: fleet-a}
  source:
    webhook:
      auth:
        bearerTokenSecretRef: {name: alerts-hook, key: token}
  target:
    chainRef: {name: security-report}
  input: |
    Alertmanager reports {{ .Event.Data.status }} alerts with labels
    {{ toJson .Event.Data.commonLabels }}. Investigate and summarise.
  dedupeKey: "{{ .Event.Data.groupKey }}/{{ .Event.Data.status }}"
```

```bash
curl -X POST http://<fullname>-gateway.roundtable.svc/v1/hooks/roundtable/alerts \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"status":"firing","groupKey":"g1","commonLabels":{"alertname":"DiskFull"}}'
# {"subject":"fleet-a.events.webhook.roundtable.alerts","stream":"fleet_a_tasks_EVENTS","seq":42}
```

The same request with the gateway's own API token gets **401**: a hook URL
accepts only its Trigger's credential.

A complete, copyable version (read-only knight, triage chain, Alertmanager receiver, test steps) is the [homelab watch recipe](../examples/recipes/homelab-watch/README.md).

### Credentials

`source.webhook.auth` sets exactly one of:

| Field | The sender sends |
|---|---|
| `bearerTokenSecretRef` | `Authorization: Bearer <token>` |
| `hmacSecretRef` | `X-Hub-Signature-256: sha256=<hex>` (GitHub style) or `X-Signature: sha256=<hex>` (Flux `generic-hmac`): HMAC-SHA256 of the raw body with the key |

- The Secret is in the Trigger's namespace and must be labelled
  `ai.roundtable.io/webhook-secret: "true"`. The gateway can `get` Secrets in
  the namespaces it serves (RBAC cannot select by label) and refuses any
  Secret without the label, so a Trigger cannot point the gateway at an
  unrelated Secret. Secrets are never inline in the Trigger.
- The value must be at least 16 characters; a trailing newline is ignored.
  It is read on every delivery, so rotating the Secret takes effect at once.
- A leaked credential fires that one Trigger and nothing else. It does not
  open any other gateway endpoint.

### What gets published

- **Body:** the raw request body, unchanged (at most `gateway.hooks.maxBodyBytes`, 1 MiB by default). `.Event.Data` is it parsed as JSON, else the raw string.
- **Headers:** `Content-Type`, every `Ce-*` header (CloudEvents binary mode),
  `X-GitHub-Event`, `X-GitHub-Delivery` and `Idempotency-Key` become NATS
  headers (`.Event.Headers`, header names as Go canonicalises them, e.g.
  `X-Github-Event`). Credentials and signatures are never forwarded.
- **Dedupe:** the first of `X-GitHub-Delivery`, `Ce-Id` or `Idempotency-Key`
  becomes `Nats-Msg-Id` as `<namespace>/<name>/<id>`, so a sender's retry
  within the stream's duplicate window (2 minutes) is stored once (the
  response says `"duplicate": true` with the original sequence) and the
  default dedupe key keeps it to one run after that. Senders without an id
  (Alertmanager, Home Assistant) should get a `dedupeKey` template.

### eventType

`source.webhook.eventType` fires only on events whose `Ce-Type` header (else
`X-GitHub-Event`) equals it. Other deliveries are still stored on the stream
and acknowledged, but start nothing and are not recorded in
`status.recentFirings`.

### Senders

The gateway is reachable in the cluster only; webhooks from the internet
(GitHub) are out of scope until a public route exists (M6). Each sender's
namespace (or LAN range) must be allowed by `gateway.networkPolicy.from`, see
[gateway.md](gateway.md#webhooks).

**Alertmanager** (`webhook_configs`, with the token mounted from a Secret in
Alertmanager's namespace):

```yaml
receivers:
  - name: roundtable
    webhook_configs:
      - url: http://<fullname>-gateway.roundtable.svc/v1/hooks/roundtable/alerts
        send_resolved: true
        http_config:
          authorization:
            type: Bearer
            credentials_file: /etc/alertmanager/secrets/roundtable-hook/token
```

**Flux notification-controller** (`generic-hmac` provider; the Provider's
Secret key `token` is the HMAC key and must hold the same value as the
Trigger's `hmacSecretRef`):

```yaml
apiVersion: notification.toolkit.fluxcd.io/v1beta3
kind: Provider
metadata:
  name: roundtable
  namespace: flux-system
spec:
  type: generic-hmac
  address: http://<fullname>-gateway.roundtable.svc/v1/hooks/roundtable/flux-failures
  secretRef:
    name: roundtable-hook-hmac      # key "token"
---
apiVersion: notification.toolkit.fluxcd.io/v1beta3
kind: Alert
metadata:
  name: roundtable
  namespace: flux-system
spec:
  providerRef: {name: roundtable}
  eventSeverity: error
  eventSources:
    - kind: Kustomization
      name: '*'
---
apiVersion: ai.roundtable.io/v1alpha1
kind: Trigger
metadata:
  name: flux-failures
  namespace: roundtable
spec:
  roundTableRef: {name: fleet-a}
  source:
    webhook:
      auth:
        hmacSecretRef: {name: flux-hook, key: token}   # labelled webhook-secret
  target:
    chainRef: {name: diagnose-reconcile}
  input: |
    Flux reports {{ .Event.Data.reason }} on {{ .Event.Data.involvedObject.kind }}
    {{ .Event.Data.involvedObject.namespace }}/{{ .Event.Data.involvedObject.name }}:
    {{ .Event.Data.message }}
  dedupeKey: "{{ .Event.Data.involvedObject.name }}/{{ .Event.Data.metadata.revision }}"
```

(Flux puts the revision in `metadata.revision` on most events; drop it from
the key if yours don't carry it, since a missing key fails the template.)

**Home Assistant** (`rest_command`; Home Assistant usually runs on the LAN, so
allow its address with an `ipBlock` peer in `gateway.networkPolicy.from` and
expose the gateway Service to it):

```yaml
# configuration.yaml
rest_command:
  roundtable_doorbell:
    url: http://gateway.example.lan/v1/hooks/roundtable/home-events
    method: POST
    headers:
      authorization: !secret roundtable_hook_authorization   # "Bearer <token>"
      content-type: application/json
    payload: '{"event":"doorbell","at":"{{ now().isoformat() }}"}'
```

### Responses

| Code | When |
|---|---|
| 202 | Stored: `{"subject", "stream", "seq", "duplicate"}` |
| 401 | Missing or wrong credential, or the Trigger's Secret is missing, unlabelled or too short (the gateway logs which) |
| 404 | No such Trigger, not a webhook Trigger, or a namespace the gateway does not serve (all the same response) |
| 413 | Body over `gateway.hooks.maxBodyBytes` |
| 429 | The Trigger's rate limit (`gateway.hooks.rate` per second, burst `gateway.hooks.burst`) is used up; see `Retry-After` |
| 503 | The Trigger's RoundTable is missing or the publish failed (no events stream: set `createStreams`) |

## GitHub source

"When an issue or pull request in these repositories carries these labels,
start the chain with it as input." The operator polls GitHub's REST API with
the table's token (`RoundTable.spec.github.tokenSecretRef` of `roundTableRef`)
and publishes each item that needs handling to
`<subjectPrefix>.events.github.<namespace>.<name>` on the table's events
stream. The Trigger reads that subject like a NATS source, so everything under
[Delivery](#delivery-nats-webhook-and-github-sources) applies and the bus stays
the system of record. Issue pickup and PR feedback can run on a github-source
Trigger too, see [Issue pickup on a Trigger](#issue-pickup-on-a-trigger) and
[PR feedback on a Trigger](#pr-feedback-on-a-trigger). Pull requests can also be
selected by their checks and merge state instead of labels, see
[Pull request state](#pull-request-state).

[`examples/trigger-github.yaml`](../examples/trigger-github.yaml):

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: Trigger
metadata:
  name: security-issues
  namespace: roundtable
spec:
  roundTableRef: {name: fleet-a}
  source:
    github:
      repositories: [acme/api, acme/web]
      kind: issue                       # or pullRequest
      query:
        labels: [security-review]       # an item must carry all of them
        state: open                     # open (default), closed or all
      pollInterval: 2m                  # default 60s, at least 30s
      claim:
        removeLabels: [security-review]
        addLabels: [security-review:in-progress]
  target:
    chainRef: {name: security-report}
  input: |
    Review {{ .Event.Data.repo }}#{{ .Event.Data.number }} ({{ .Event.Data.url }}),
    "{{ .Event.Data.title }}" opened by {{ .Event.Data.author }}, labels
    {{ toJson .Event.Data.labels }}. Report the security impact.
  concurrency:
    maxActive: 2
```

### The event

`.Event.Data` of each item:

| Key | Value |
|---|---|
| `repo` | `owner/name` |
| `number` | the issue or pull request number |
| `title` | its title |
| `labels` | its label names (a list) |
| `url` | its web URL |
| `author` | the login that opened it |
| `kind` | `issue` or `pullRequest` |
| `state` | `open` or `closed` |

A source with a [state selector](#pull-request-state) adds `headSha`,
`mergeState`, `failureKind`, `failureUrl`, `failedChecks` and, for a merge
queue rejection, `mergeGroupSha`.

The message also carries a `Roundtable-Event-Key: <owner/name>#<number>`
header (`<owner/name>#<number>@<kind>:<head sha>` with a state selector), the
default dedupe key. `.Event.Subject` is the Trigger's subject and
`.Event.Time` when the item was published.

### Polling

- One poll loop per Trigger, started by the Trigger controller in the elected
  leader and stopped when the Trigger is suspended, deleted or switched to
  another source. A spec change restarts it, so it polls at once.
- Each poll lists every repository through the issues endpoint (`labels`,
  `state`; pull requests are told apart by GitHub's `pull_request` link;
  `author`, if set, keeps the items that login opened) and publishes:
  - an item **without a run** (by its dedupe key), unless the concurrency policy
    would hold it back anyway (`maxActive` reached, `Forbid` with a run active;
    `Replace` publishes at most one new item per poll). Held items are counted
    in `GitHubReady` instead of filling `status.recentFirings` every poll, and
    start on a later poll once a slot is free.
  - an item **whose run exists but whose claim is still pending** (see below).
- Everything else is left alone: an item whose run still exists does not fire
  again. Without `claim`, an item that keeps its labels fires again only after
  its run was deleted (`ttlSecondsAfterFinished`). With `claim`, an item whose
  run **finished** and that is selected again with its claim pending (someone
  put the selecting label back) starts that run again, under the same name.
- An item whose `input` or `dedupeKey` template fails to render is not
  published; it is named in `GitHubReady` until it is fixed.

### Claim

`claim.addLabels` / `claim.removeLabels` are applied to an item **after** its
run was created, never before. If labelling fails, the firing is recorded with
the run and a `skipReason` (`claim of acme/api#12 failed, retried on the next
poll: …`), and the next poll publishes the item again: its run is found by name,
so nothing starts twice, and only the claim is retried. Remove the selecting
label in the claim (as above) so an item is not picked again once its run is
deleted.

The order is: `addLabels`, `assignees`, `comment`, `removeLabels`.
`claim.comment` is a template posted once per run; besides `.Event` and
`.Trigger` it sees `.Run.Name` (the run Chain) and `.Run.ID` (its
`ai.roundtable.io/run` value). The run records the claim in its
`ai.roundtable.io/trigger-claimed` annotation, so a retried claim does not
comment again. Every github-source run carries the item's repository in
`ai.roundtable.io/trigger-github-repo`.

### Per-repository cap, hold and hand-back

Three optional rules decide whether an item without a run (or a finished one
it would start again) starts now:

- `skipIfBodyMatches`: an item whose title or body matches one of `patterns`
  (RE2), or that already carries one of `handBack.addLabels`, is handed back
  instead: `handBack.addLabels` are added, `handBack.comment` (a template that
  also sees `.Reason`) is posted, then `handBack.removeLabels` are removed.
  Remove the selecting label there so it is handed back once. Counted as
  "handed back" in `GitHubReady`, with a `HandedBack` event.
- `maxActivePerRepository`: at most this many active runs per repository
  (under `Allow`); more items wait for a later poll.
- `holdWhilePullRequestLabeled`: while a repository has an open pull request
  with this label, its items start no new run (counted as held in
  `GitHubReady`, with a `Held` event). If the pull requests cannot be listed,
  the repository is held too.

Checked in that order. Items whose run is active are never handed back or
held; only a pending claim is retried for them.

### Issue pickup on a Trigger

`RoundTable.spec.issuePickup.engine: trigger` makes issue pickup run on a
generated github-source Trigger instead of its builtin poller (the default
`builtin` is unchanged). The issue pickup controller owns
`<table>-issue-pickup` and rebuilds its spec from the `issuePickup` fields;
edit those, not the Trigger:

| issuePickup | Trigger |
|---|---|
| `repos` | `source.github.repositories` |
| `readyLabel` | `query.labels`, `claim.removeLabels`, `handBack.removeLabels` |
| `inProgressLabel` | `claim.addLabels` |
| `needsHumanLabel`, `humanPatterns` | `skipIfBodyMatches` (`handBack.addLabels`, `patterns`) |
| `breakingLabel` | `holdWhilePullRequestLabeled` |
| `assignees` | `claim.assignees` (with the pickup claim comment) |
| `intervalSeconds` | `pollInterval` |
| `maxActive` / `maxActivePerRepo` | `concurrency.maxActive` / `maxActivePerRepository` |
| `chainRef` | `target.chainRef`, with `input` rendering `{"repo": ..., "issue": ...}` |
| `suspend` (or the table's) | `suspend` |

A Trigger labelled `ai.roundtable.io/pickup-roundtable: <table>` and
controlled by that RoundTable differs from other Triggers in its runs only:
they are named like the builtin poller's (`<chain>-<repo>-<issue>`, not by
dedupe hash), carry the pickup label and `pickup-repo` / `pickup-issue`
annotations, are owned by the RoundTable (deleting the Trigger leaves them
running), count every pickup Chain of the table for the caps, and are deleted
by issue pickup after its post-run checks (`ttlSecondsAfterFinished` of the
Trigger is 0). So switching a table between engines in either direction never
runs an issue twice. Switching back to `builtin` deletes the Trigger. See
[issue-pickup.md](issue-pickup.md#engines).

### Pull request state

With `kind: pullRequest`, three state selectors pick pull requests with a
failure instead of (or next to) labels. `query.labels` becomes optional and
`query.state` must stay `open`:

| Field | Selects |
|---|---|
| `checks: failing` | a check on the head commit whose latest attempt failed or timed out |
| `mergeable: conflicting` | merge conflicts with the base branch (GitHub `mergeable_state: dirty`) |
| `mergeQueue: rejected` | a failed workflow run in the newest merge group (`merge_group` runs on `gh-readonly-queue/<base>/pr-<n>-<sha>`) |

`ignoreChecks` lists check (and merge queue workflow) names that never count
as failed, `author` keeps the pull requests one login opened. The poll lists
open pull requests through the pulls endpoint (labels filtered by the
operator), reads each one for its `mergeable_state`, its head commit's check
runs (`checks`) and, once per repository, the merge group runs (`mergeQueue`).
A pull request has at most one failure per poll, in this order: conflict
(nothing merges before a rebase, which re-runs CI anyway), failed checks,
merge queue rejection. `.Event.Data` then also has:

| Key | Value |
|---|---|
| `headSha` | the head commit |
| `mergeState` | GitHub's `mergeable_state` (empty if the pull request could not be read) |
| `failureKind` | `conflict`, `ci` or `merge-queue` |
| `failureUrl` | the first failed check or run, or the pull request for a conflict |
| `failedChecks` | `[{name, url, summary}]`: the failed checks (`ci`, `summary` is the check's output summary, sanitized and capped at 1000 bytes; the pull request's code controls it) or merge queue runs (`merge-queue`); empty for a conflict |
| `mergeGroupSha` | the rejected merge group's commit (`merge-queue` only) |

The event key is `<owner/name>#<number>@<failureKind>:<headSha>` (plus
`:<mergeGroupSha>` for `merge-queue`), so one failure fires once and a new
push (or merge queue attempt) can fire again. A pull request has **one run**,
`<chain>-<trigger>-<hash of owner/name#number>`, started again for each new
failure once it finished; while it is active the pull request is left alone
(counted "with an active run" in `GitHubReady`). The run counts its runs in
`ai.roundtable.io/trigger-item-runs`; templates see it as `.Run.Attempt` (in
`input`, the run being started: 1, 2, ...).

`maxRunsPerItem` caps those runs, and needs `onMaxRuns` with `addLabels`: the
next failure after the last run is handed back instead (`addLabels`, then
`comment`, which also sees `.Run.Attempt`, the runs started, then
`removeLabels`), with a `HandedBack` event. Pull requests carrying an
`onMaxRuns.addLabels` label are not selected; when a human removes it, the
count starts over and the pull request's current failure fires again. The
count lives on the run, so it also starts over once the finished run is
deleted (`ttlSecondsAfterFinished`).

```yaml
source:
  github:
    repositories: [acme/api]
    kind: pullRequest
    query: {}
    author: dunce-bot[bot]
    checks: failing
    ignoreChecks: [merge-guard]
    mergeable: conflicting
    mergeQueue: rejected
    maxRunsPerItem: 3
    onMaxRuns:
      addLabels: [needs-human]
      comment: "Still failing after {{ .Run.Attempt }} round(s); over to a human."
input: |
  {"repo": {{ toJson .Event.Data.repo }}, "pr": {{ toJson .Event.Data.number }},
   "attempt": {{ .Run.Attempt }}, "kind": {{ toJson .Event.Data.failureKind }},
   "failedChecks": {{ toJson (pluck "name" .Event.Data.failedChecks) }}}
```

### PR feedback on a Trigger

`RoundTable.spec.prFeedback.engine: trigger` makes [PR feedback](pr-feedback.md)
run on generated github-source Triggers instead of its builtin poller (the
default `builtin` is unchanged). The PR feedback controller owns one Trigger per
watched author, `<table>-pr-feedback-<author>` (the login lowercased, other
characters turned into `-`, e.g. `roundtable-dev-pr-feedback-dunce-bot-bot`),
and rebuilds it from the `prFeedback` fields; edit those, not the Triggers:

| prFeedback | Trigger |
|---|---|
| `repos` | `source.github.repositories` |
| `authors` | one Trigger per author, `source.github.author` |
| `ignoreChecks` | `ignoreChecks` (with `checks: failing`, `mergeable: conflicting`, `mergeQueue: rejected`) |
| `maxAttempts` | `maxRunsPerItem`, and `maxAttempts` in the input |
| `escalationLabel` | `onMaxRuns.addLabels` (with the builtin escalation comment) |
| `pollIntervalSeconds` | `pollInterval` |
| `chainRef` | `target.chainRef`, with `input` rendering the builtin input |

`concurrency.maxActive` is 1 (one fix round at a time, as the builtin engine's
single feedback Chain) and `ttlSecondsAfterFinished` 7 days, so a pull request's
attempt count lasts a week after its last fix round.

What only PR feedback knows stays in the operator, for Triggers labelled
`ai.roundtable.io/prfeedback-roundtable: <table>` and controlled by that
RoundTable: before a pull request is published, and again when it fires, the
operator finds its author knight and run in the chain run that opened it (the
step whose output names the pull request) or uses `fallbackKnightRef`. While
that chain run is still running, or when nothing opened the pull request and
there is no fallback, it is not published (counted "without a known author
knight" in `GitHubReady`). The fired event gets `knight`, `runId`, `issue` and,
for a CI or merge queue failure, the trimmed and fenced job `log`, so the
input is the builtin engine's: `repo`, `pr`, `url`, `knight`, `runId`,
`attempt`, `maxAttempts`, `kind`, `headSha`, `failedChecks` (names),
`failureUrl`, `log`, `issue`.

Flaky-test issues are filed by the builtin engine only: on the trigger engine
a check that failed and passed on the same commit is not reported (a failed
latest attempt starts a fix round), and `status.prFeedback.conditions` carries
`FlakeReportingSkipped`. Switching back to `builtin` deletes the Triggers and
their runs. See [pr-feedback.md](pr-feedback.md#engines).

### Rate limits and GitHubReady

`GitHubReady` reports the last poll:

| Status | Reason | When |
|---|---|---|
| True | `Polling` | The last poll succeeded. The message counts matching, published and held items and the rate limit remaining. |
| False | `TokenError` | The RoundTable has no `spec.github.tokenSecretRef`, or the Secret or key is missing. |
| False | `RateLimited` | GitHub's primary or secondary rate limit was hit. The loop waits until the limit resets (at least `pollInterval`, at most an hour; 5 minutes for a secondary limit without `Retry-After`) and lists no further repositories meanwhile. |
| False | `PollError` | Listing a repository or publishing failed, or an item's template failed to render. |

A poll that leaves the rate limit at 0 also waits for its reset.
`status.lastPolledAt` is the last successful poll.

## Schedule source

For a task, Chain or Mission on a cadence end to end, including missed and
overlapping runs, see [Scheduled work](scheduled-work.md).

[`examples/trigger-schedule.yaml`](../examples/trigger-schedule.yaml) runs a
Chain every weekday at 07:00 New York time with input computed from the
schedule time. For a fixed input, `Chain.spec.schedule` stays the short form.

A schedule runs on the cluster's own CronJob controller. For each Trigger with
`source.schedule` the operator creates, in the Trigger's namespace and owned
by it (deleting the Trigger deletes them):

- a ServiceAccount `trigger-<name>` with no API permissions and no
  automounted token;
- a `batch/v1` CronJob `trigger-<name>` (a name longer than 52 characters is
  shortened with a hash suffix): `schedule` = `cron`, `timeZone` (default
  `Etc/UTC`), `startingDeadlineSeconds`, `concurrencyPolicy` from
  `concurrency.policy`, `suspend` while the Trigger is suspended
  (`spec.suspend` or the `ai.roundtable.io/suspended` annotation), and keeps
  1 succeeded and 2 failed Jobs.

Each run starts one pod from the operator's own image that runs
`/manager fire-trigger --gateway <url> --trigger <namespace>/<name> --job-name $(ROUNDTABLE_JOB_NAME)`
(the env var is the Job's `batch.kubernetes.io/job-name` label). It sends a
projected ServiceAccount token for the gateway's audience
(`gateway.serviceAccountAuth.audience`) to the gateway's
`POST /v1/triggers/<namespace>/<name>/fire`, which accepts the call only from
that Trigger's own ServiceAccount (or a static gateway token) and publishes
the firing to `<subjectPrefix>.events.schedule.<namespace>.<name>` keyed by
its schedule time, read from the Job's name (`<cronjob>-<unix minutes of the
scheduled time>`), so a late start or a retry keeps its slot's key. The
Trigger reads that subject through its consumer like any other source, so
dedupe, concurrency and `status.recentFirings` work the same: a second firing
with the same key starts nothing. The pod is restricted: non-root,
no privilege escalation, all capabilities dropped, read-only root
filesystem, `restartPolicy: Never`, 120 seconds at most (2 retries).

The events stream is the one of `roundTableRef`, else of the target Chain's
`roundTableRef`, else of the target knight's RoundTable, else of the
RoundTable named `default`.

Requirements:

- The gateway is enabled with ServiceAccount auth
  (`gateway.serviceAccountAuth.enabled`, the default) and allows the Trigger's
  namespace (`gateway.serviceAccountAuth.allowedNamespaces`). With the gateway
  disabled, set `triggers.gatewayURL`, or scheduled Triggers stay not Ready
  (`CronJobError`).
- Pods in the Trigger's namespace reach the gateway: add the namespace to
  `gateway.networkPolicy.from` when it is not the release namespace.

See and control a schedule with kubectl:

```sh
kubectl -n roundtable get cronjobs -l ai.roundtable.io/trigger   # schedules, last run, suspended
kubectl -n roundtable get jobs -l ai.roundtable.io/trigger       # recent firing pods
kubectl -n roundtable create job --from=cronjob/trigger-weekday-security-report now   # fire once now
kubectl -n roundtable patch trigger weekday-security-report --type merge -p '{"spec":{"suspend":true}}'
```

Suspend the Trigger, not the CronJob: the operator keeps the CronJob in line
with the Trigger, so a CronJob edited by hand is reset at the next reconcile.
`status.lastScheduledAt` is the CronJob's `status.lastScheduleTime`;
`status.nextScheduledAt` is computed for display only.

`Chain.spec.schedule` runs on this mechanism only when the operator is started
in `trigger` mode; see [Chain schedules](#chain-schedules).

## Chain schedules

How `Chain.spec.schedule` runs is an operator setting:
`--chain-schedule-mode` (chart value `chains.scheduleMode`).

| Mode | What fires | What runs | Where the status is |
|------|------------|-----------|---------------------|
| `inprocess` (default) | the operator's in-process cron | the Chain object itself (phase, step statuses and run counters on the Chain) | the Chain |
| `trigger` | the CronJob of a schedule Trigger `<chain>-schedule` | a copy of the Chain per firing, named by the Trigger (`<chain>-<trigger>-<hash>`), like any Trigger with `target.chainRef` | the copy; the Trigger's `status.recentFirings` |

In `trigger` mode the chain controller creates and owns (controller owner
reference) the Trigger `<chain>-schedule`, labelled
`app.kubernetes.io/managed-by: roundtable-operator`:

- `source.schedule` from `spec.schedule`, `spec.timeZone` and
  `spec.startingDeadlineSeconds`;
- `target.chainRef` = the Chain, `roundTableRef` = the Chain's;
- `spec.suspend` = the Chain's `spec.suspend`;
- `concurrency.policy: Forbid` (a fire while the previous copy still runs is
  skipped, as the in-process scheduler does);
- no `spec.input`, so each copy gets the Chain's `spec.input` verbatim (it is
  never rendered as a Trigger template, so a literal `{{` stays literal).

The Trigger is updated when those fields change (a hand edit is reset) and
deleted when `spec.schedule` is cleared or the Chain is deleted; its CronJob,
ServiceAccount and run copies go with it. The Trigger's
`status.lastScheduledAt` and `status.nextScheduledAt` are copied to the
Chain's status fields of the same names, so `kubectl get chains` still shows
the next fire. A missed fire is caught up by the CronJob
(`startingDeadlineSeconds`), not by the operator. The schedules API lists the
Trigger as `managedBy: other` and refuses `PUT` and `DELETE` on it (409):
change the Chain instead. Suspend, resume and run-now work as for any
schedule.

If a Trigger named `<chain>-schedule` already exists and is not controlled by
the Chain, the chain controller leaves it alone, does not run the schedule,
and records a `ScheduleTriggerConflict` Warning event on the Chain. Rename or
delete that Trigger.

Switching modes never fires a schedule twice: in `trigger` mode the Chain has
no in-process cron entry, and in `inprocess` mode the chain controller deletes
the Trigger it owns before scheduling in process. The last fire time carries
over in `status.lastScheduledAt`, so the in-process catch-up does not re-run a
fire the CronJob already made.

Migrating to `trigger` mode:

1. Make sure scheduled Triggers work: the gateway is enabled (or
   `triggers.gatewayURL` is set) and the namespaces of scheduled Chains can
   reach it (`gateway.networkPolicy.from`) and are allowed by
   `gateway.serviceAccountAuth.allowedNamespaces`.
2. Check that nothing reads run results from the scheduled Chain object
   itself (its phase, step outputs or `runsSucceeded`): in `trigger` mode
   those are on the copies (`kubectl get chains -l ai.roundtable.io/trigger=<chain>-schedule`).
3. Set `chains.scheduleMode: trigger` and upgrade. Each scheduled Chain gets
   its Trigger and CronJob at the next reconcile:
   `kubectl get triggers,cronjobs -l app.kubernetes.io/managed-by=roundtable-operator`.
4. To go back, set `chains.scheduleMode: inprocess`; the owned Triggers are
   deleted and the in-process cron takes over.

## Knight target

`target.knight` sends one task straight to a knight per firing, so "ask kay
this every night" or "send tristan this when an alert fires" needs no
one-step Chain.
[`examples/trigger-knight-task.yaml`](../examples/trigger-knight-task.yaml)
has both.

```yaml
spec:
  source:
    schedule: {cron: "0 2 * * *"}
  target:
    knight:
      name: kay                 # Knight in the Trigger's namespace
      task: |                   # Go template, same data as spec.input
        Write the nightly briefing for {{ .Event.Time.Format "2006-01-02" }}.
      model: ""                 # optional per-task model (ID or RoundTable alias)
      timeoutSeconds: 3600      # default; no result after this long: lost
```

The task goes through the gateway's single-task path (the one behind
`POST /v1/knights/<name>/invoke`): the table's subject prefix and the knight's
domain are resolved as the gateway resolves them, the task is published to
`<subjectPrefix>.tasks.<domain>.<knight>`, and its record is kept in the
gateway's task KV bucket (`gateway.taskBucket`, kept `gateway.taskTTL`), so
`GET /v1/tasks/<task ID>`, the dashboard and the dead-letter view show it like
any gateway task. The operator writes to that bucket itself, so a knight target
works with the gateway disabled too.

- `status.recentFirings[].run` is the task ID; `outcome` is filled in once it
  is known: `succeeded`, `failed` (the knight reported an error or returned
  nothing) or `lost` (no result within `timeoutSeconds`, or the record is
  gone). The operator looks for results every 30 seconds while tasks are in
  flight.
- `status.activeRuns` lists the task IDs without a result: they are the active
  runs `concurrency` counts. `Forbid` skips while one is in flight, `Allow`
  caps them at `maxActive`. `Replace` is rejected (the Trigger is not Ready): a sent task cannot be
  withdrawn.
- Dedupe: the dedupe key becomes the task's gateway `Idempotency-Key`, so a
  key that already sent a task sends nothing (also on redelivery) while the
  gateway keeps its task records (`gateway.taskTTL`, 7 days by default), not
  `ttlSecondsAfterFinished`.
- The knight must exist, and with `roundTableRef` set it must sit at that
  RoundTable (its `ai.roundtable.io/round-table` label, as the gateway
  resolves it). Otherwise the Trigger is not Ready (`TargetNotFound` or
  `TargetWrongTable`) and fires nothing; NATS events wait in the consumer.
- A task that renders empty, or an invalid `model`, is recorded in
  `status.recentFirings` and the event is not retried. A suspended knight
  makes the event wait.
- `spec.input` is not used (put the text in `target.knight.task`), and a
  github source needs a Chain target.

## Mission target

`target.mission` creates a [Mission](../docs/phase-3-dynamic-missions.md) per
firing, so a goal can run on a schedule or on an event ("every Monday, review
the backlog"; "when this alert fires, investigate").
[`examples/trigger-mission.yaml`](../examples/trigger-mission.yaml) is a weekly
read-only review.

```yaml
spec:
  roundTableRef: {name: fleet-a}   # required: the Mission runs on this table
  target:
    mission:
      objective: "Investigate {{ .Event.Data.alert }}"   # Go template, same data as spec.input
      successCriteria: "A root cause and a proposed fix"  # also a template
      metaMission: true             # default; false needs chains
      recruitExisting: false
      planner: {knight: "", context: "", maxChains: 4, maxKnights: 2}
      costBudgetUSD: "2.50"         # required, > 0
      timeoutSeconds: 3600
      ttlSeconds: 86400
      knights: [kay]                # pre-seed existing knights
      chains: [{name: triage, input: "..."}]
```

Exactly one of `target.chainRef`, `target.knight` and `target.mission` may be
set. The Mission is built by the gateway's mission builder
(`POST /v1/missions`), so both paths validate the same fields and caps
(budget, ttl, timeout, references); a request it rejects is recorded in
`status.recentFirings` and not retried. There is no way to give an image,
Secret, pod or knight spec.

- A firing creates a Mission named `<trigger>-<8 hex of the dedupe key>` on
  `roundTableRef`, labelled `ai.roundtable.io/trigger=<trigger>` and owned by the
  Trigger. The Mission runs, then cleans itself up after `ttlSeconds`.
- `status.recentFirings[].run` is the Mission name; once it finishes `phase` is
  its final phase and `outcome` is `succeeded`, `failed` or `lost` (expired or
  already cleaned up).
- `status.activeRuns` lists the unfinished Missions: `Forbid` skips while one
  exists, `Allow` caps them at `maxActive`. `Replace` is rejected (the Trigger is not Ready).
- Dedupe: a Mission already named for the key means it fired; this holds until
  the Mission is cleaned up.
- A firing while the RoundTable is at `policies.maxMissions` is recorded as
  skipped with the reason, not left queued in `Pending`. The `maxMissions`
  check counts every Mission in the namespace that holds or waits for a slot,
  including queued (`Pending`) ones.
- `spec.input` is not used (put the text in `target.mission.objective`), and a
  github source needs a Chain target.

## Suspend and run now without editing the spec

Two annotations let you pause a Trigger, or fire it once, without changing
its spec. That matters for a Trigger applied from Git: Flux would revert a
spec edit, but it leaves annotations it does not own alone. The gateway's
[schedule routes](gateway.md#schedules) set them; `kubectl annotate` works
too.

| Annotation | Effect |
|---|---|
| `ai.roundtable.io/suspended` | Suspends the Trigger like `spec.suspend`. It is suspended while **either** is set; removing the annotation does not clear `spec.suspend`. Any value but `"false"` counts. The value records who suspended it (the gateway writes its caller). |
| `ai.roundtable.io/run-now` | Fires the Trigger once now, through the normal firing path: the same input rendering (`.Event.Time` is now, `.Event.Data` empty), concurrency policy, labels and `ttlSecondsAfterFinished`. It fires even while the Trigger is suspended. The value is a request ID: the firing's dedupe key is `manual:<value>` (`spec.dedupeKey` is not used), so the run is `<chain>-<trigger>-<hash of manual:<value>>` (a knight target sends one task, a mission target creates the Mission `<trigger>-<8 hex of the key hash>`). The operator removes the annotation once the firing is handled; the firing shows in `status.recentFirings` (with a `skipReason` if, for example, `Forbid` held it back). |
| `ai.roundtable.io/run-now-caller` | Who asked for the run-now firing; copied to the run (the Chain run, the knight task or the Mission) as `ai.roundtable.io/caller`. |

```bash
kubectl annotate trigger morning-digest ai.roundtable.io/suspended=me   # suspend
kubectl annotate trigger morning-digest ai.roundtable.io/suspended-     # resume
kubectl annotate trigger morning-digest ai.roundtable.io/run-now=$(date +%s)
```

`status.suspendedBy` says what suspends a Trigger (`spec`, `annotation` or
`spec,annotation`); it is the `Suspended` column of `kubectl get triggers`,
and the `Suspended` condition's message names it too.

## Spec

| Field | Meaning |
|---|---|
| `source.nats.subject` | Subject filter (wildcards allowed). Must sit under `<subjectPrefix>.events.` of `roundTableRef`. |
| `source.webhook.auth` | `bearerTokenSecretRef` or `hmacSecretRef`: a key of a labelled Secret in the Trigger's namespace. |
| `source.webhook.eventType` | Fire only on deliveries with this `Ce-Type` (else `X-GitHub-Event`). |
| `source.schedule.cron` / `timeZone` / `startingDeadlineSeconds` | Standard cron, IANA zone (default UTC), and how late a missed time may still fire: the CronJob's fields (see [Schedule source](#schedule-source)). |
| `source.github.repositories` | `owner/name` of each repository polled (1 to 50). |
| `source.github.kind` | `issue` (default) or `pullRequest`. |
| `source.github.query.labels` / `state` | Labels an item must all carry (1 to 10), and `open` (default), `closed` or `all`. |
| `source.github.pollInterval` | How often the repositories are listed (default `60s`, at least `30s`). |
| `source.github.claim.addLabels` / `removeLabels` | Labels added to and removed from an item once its run was created. |
| `source.github.claim.assignees` / `comment` | Logins assigned to the item, and a comment template posted once per run (`.Run.Name`, `.Run.ID`). |
| `source.github.maxActivePerRepository` | Active runs per repository under `Allow` (0: no cap). |
| `source.github.holdWhilePullRequestLabeled` | Hold a repository while it has an open pull request with this label. |
| `source.github.skipIfBodyMatches` | `patterns` on title and body, and `handBack` (`addLabels`, `removeLabels`, `comment` with `.Reason`) for items handed back instead of run. |
| `target.chainRef` | The Chain (same namespace) each firing copies. |
| `target.knight` | Instead of `chainRef`: `name`, `task` (template), `model` and `timeoutSeconds` of one task sent to a knight per firing (see [Knight target](#knight-target)). |
| `input` | Go template rendered into the run's `spec.input`. Empty keeps the target's own input. |
| `dedupeKey` | Go template whose result identifies the event. Defaults: see the table above. |
| `concurrency.policy` | `Allow` (default), `Forbid` or `Replace`. |
| `concurrency.maxActive` | Under `Allow`, the most active runs; a firing at the cap is skipped. 0 means no cap. |
| `suspend` | Stop firing (the `ai.roundtable.io/suspended` annotation does the same, see [above](#suspend-and-run-now-without-editing-the-spec)). The consumer is deleted, so events published while suspended don't fire on resume (the gateway still accepts and stores them; a github source stops polling and picks up matching items again on resume; a schedule source's CronJob is suspended). |
| `roundTableRef` | The RoundTable whose events stream the source is read from (and whose GitHub token a github source uses). Required for `nats`, `webhook` and `github`; a `schedule` source without it uses its target's table. |
| `ttlSecondsAfterFinished` | Delete a finished run this long after it completed (default 86400; 0 keeps runs). |

### Templates

`input`, `target.knight.task` and `dedupeKey` see:

- `.Event.Subject`: the NATS subject
- `.Event.Data`: the payload parsed as JSON, or the raw string when it is not JSON (github: see [The event](#the-event))
- `.Event.Headers`: header name to value (multiple values joined with `, `)
- `.Event.Time`: when the message was stored in the stream, or the schedule time (in the schedule's zone)
- `.Trigger.Name`, `.Trigger.Namespace`

A github source's `input` also sees `.Run.Name` and `.Run.Attempt` (state
selectors, see [Pull request state](#pull-request-state)).

A missing map key is an error (`missingkey=error`), so `{{ .Event.Data.service }}`
fails on an event without `service` instead of rendering `<no value>` (use
`index .Event.Data "service"` for an optional key). `toJson` encodes a value
as JSON, for embedding strings in a JSON input safely. `pluck "name" list`
returns the `name` of every object in a list.

A template that does not parse makes the Trigger not Ready (`InvalidSpec`) and
nothing is read until it is fixed. An event the template cannot render is
recorded in `status.recentFirings` with the reason and terminated: it is never
redelivered.

### Dedupe

The run's name is `<chain>-<trigger>-<hash of dedupe key>` (a Trigger
generated by issue pickup names runs `<chain>-<repo>-<issue>`, see
[Issue pickup on a Trigger](#issue-pickup-on-a-trigger); a github source with a
state selector has one run per pull request, see
[Pull request state](#pull-request-state)). An event whose key
matches a run that still exists starts nothing, so a key is remembered until
its run is deleted (`ttlSecondsAfterFinished`). The same name makes redelivery
safe: if the operator restarts after creating a run but before acking the
message, the redelivered message finds the run and is skipped.

JetStream also drops a repeated `Nats-Msg-Id` within the stream's duplicate
window (2 minutes) before the Trigger sees it.

## Delivery

`RoundTable.spec.nats.createStreams: true` creates an events stream next to the
tasks and results streams:

- name: `spec.nats.eventsStream`, default `<tasksStream>_EVENTS`
- subjects: `<subjectPrefix>.events.>`
- Limits retention, max age `spec.nats.eventsMaxAge` (default 168h)

Each Trigger gets one durable pull consumer on
it, `trigger-<namespace>_<name>`, filtered to `source.nats.subject` (webhook:
`<subjectPrefix>.events.webhook.<namespace>.<name>`, github:
`<subjectPrefix>.events.github.<namespace>.<name>`, schedule:
`<subjectPrefix>.events.schedule.<namespace>.<name>`). A message's default
dedupe key is its `Roundtable-Event-Key` header, else `Nats-Msg-Id`, else
`<stream creation unix nanoseconds>/<stream sequence>`: the creation time keeps a
recreated events stream, whose sequence restarts at 1, from matching the runs
of the old one. A message delivered before an upgrade under the former
`seq-<n>` key still counts as a duplicate when a chain or mission run exists
under that key and was created after the stream (a knight target is not
checked: it dedupes through the gateway). A new consumer
starts at new messages (not the stream's backlog). From then on, events
published while the operator is down fire when it comes back.

For each message the operator renders the templates, applies the dedupe key and
the concurrency policy, starts the run, records the firing, and only then acks.
Delivery is at least once and the dedupe key keeps it to one run. A message that
could not be handled for a transient reason (API error, target Chain missing)
is redelivered after 15 seconds.

Consumers run only in the elected leader. The consumer is deleted when the
Trigger is deleted (a finalizer) or suspended.

## Status

```yaml
status:
  lastFiredAt: "2026-11-24T07:00:03Z"
  suspendedBy: ""           # spec, annotation or spec,annotation while suspended
  activeRuns: [security-report-on-deploy-1f3a9c0b2d4e]
  recentFirings:            # newest first, at most 10
    - time: "2026-11-24T07:00:03Z"
      key: api@1.2.3
      run: security-report-on-deploy-1f3a9c0b2d4e
    - time: "2026-11-24T06:58:41Z"
      key: api@1.2.2
      skipReason: "concurrency Allow: maxActive 3 reached"
  stream: fleet_a_tasks_EVENTS
  consumer: trigger-roundtable_on-deploy
  conditions:
    - type: Ready
      status: "True"
      reason: Listening
```

`Ready` reasons: `Listening`, `Suspended`, `InvalidSpec` (template, subject, cron, or `spec.concurrency.policy: Replace` with a
knight or mission target), `TargetNotFound`, `TargetWrongTable` (knight target),
`RoundTableNotFound`, `ConsumerError` (for example no
events stream: set `createStreams` or create the stream yourself),
`CronJobError` (schedule source: no gateway URL, or a CronJob or
ServiceAccount of that name not owned by the Trigger). A github
source also has `GitHubReady` (see [Rate limits and GitHubReady](#rate-limits-and-githubready))
and `lastPolledAt`.

### Schedule health

`kubectl get trigger` shows Schedule (cron) and Next for a schedule source;
`-o wide` adds Time Zone and Last Scheduled. Besides `lastScheduledAt` and
`nextScheduledAt`, a schedule source has:

- `status.lastSuccessfulScheduledAt`: the CronJob's `status.lastSuccessfulTime`.
- Condition `ScheduleHealthy`: `False` with reason `FireJobFailing` and the
  failed Job's name in the message while the most recent Job of the CronJob
  failed (gateway unreachable, 403 for the ServiceAccount, image pull error);
  `True` otherwise. It does not change `Ready`.

Events: a Warning `FireJobFailed` once per failed Job, and a Normal
`FiringSkipped` (reason and schedule time) when a schedule firing is skipped,
for example by `concurrency: Forbid`. A retried slot (dedupe) emits none.
The operator needs `get;list;watch` on `jobs` (batch) for this.
