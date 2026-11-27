# Invoke gateway

The invoke gateway is the synchronous front door to the knights. Callers send a
task and get the answer back in the same HTTP request. If the answer takes too
long, they get a task ID to poll. The same operations are also served as an MCP
server (streamable HTTP), so OpenClaw, Claude and other MCP clients can register
it the way they register kagent's endpoint.

The gateway speaks the [knight task contract](knight-task-contract.md). It
publishes a `TaskPayload` on `<prefix>.tasks.<domain>.<knight>` and reads the
result from `<prefix>.results.<taskId>`. Knights need no changes.

## Reaching the gateway

The chart names the gateway Service `<fullname>-gateway`. It listens on port 80
(`gateway.service.port`); 8080 is the container port, not the Service port. So
callers in the cluster use `http://<fullname>-gateway.roundtable.svc` with no port.

`<fullname>` is the chart's `roundtable-operator.fullname`: `fullnameOverride` if
set; otherwise, with `nameOverride` taking the place of `roundtable-operator` below,
the release name when it already contains `roundtable-operator`
(release `roundtable-operator` gives `roundtable-operator`); otherwise
`<release>-roundtable-operator` (release `roundtable` gives
`roundtable-roundtable-operator`). To find the Service:

```bash
kubectl -n roundtable get svc -l app.kubernetes.io/component=gateway
```

The shell examples on this page set the name once and use `$FULLNAME` (a bare
`<fullname>` is a redirect in a shell, so it cannot be pasted):

```bash
FULLNAME=roundtable-roundtable-operator   # your <fullname>, see above
```

## Design

- **A separate Deployment, built from the operator image.** The gateway is
  `cmd/gateway`, shipped in the same image as `/gateway` and deployed by the
  same chart (`gateway.enabled`). It does not run inside the operator process,
  for these reasons:
  - **Smaller blast radius.** The operator holds broad RBAC (Deployments,
    Secrets, Jobs, cluster-wide). The gateway is the only network-facing,
    caller-driven component, so it runs under its own ServiceAccount with
    read-only `get`/`list` on Knights, RoundTables and Chains. The only
    objects it writes are Missions, run copies of opted-in Chains and
    schedule Triggers (created, replaced and deleted, and their suspend and
    run-now annotations), and only in the namespaces callers may target (see
    [Missions](#missions), [Chain runs](#chain-runs) and
    [Schedules](#schedules)). For [webhooks](#webhooks) it can `get`
    Triggers and `get` (not list) Secrets in those namespaces, and uses only
    Secrets labelled `ai.roundtable.io/webhook-secret: "true"`. The same
    Trigger access serves [schedule firings](#firing-a-schedule-trigger).
  - **Separate failure and scaling.** Invoke requests stay open for up to
    `maxTimeout`. Load spikes and restarts of the gateway don't touch the
    reconcilers. The operator's leader election would also either leave
    standby replicas serving HTTP without being the leader, or force the API
    to follow the leader. The gateway avoids both: it is stateless, so you
    can scale it freely.
  - **The cost is small.** It is one more Deployment, but it needs no new
    image or release pipeline.
- **Results are read by subject.** Before publishing, the gateway takes a core
  NATS subscription on the exact result subject. Then it publishes the task
  through JetStream, so the publish fails if no tasks stream captures it. A
  core subscription creates no JetStream consumer, so it can't clash with the
  one-consumer-per-subject rule of WorkQueue streams. It never acknowledges or
  deletes the result. `GET /v1/tasks/{id}` (and the wait, if it times out) reads
  the newest message on the subject straight from the RoundTable's results
  stream (`GetLastMsg`). The result can therefore be fetched any number of
  times.
- **Task records are kept in NATS KV.** They hold the taskId, runId, knight,
  status, output and where the result lives. They are stored in the
  `gateway.taskBucket` bucket with a TTL (`gateway.taskTTL`). Every replica sees
  the same records, and they survive restarts. `GET /v1/tasks/{id}` only serves
  tasks started through the gateway. It returns 404 for anything else, such as
  chain step results.
- **Prefix and streams come from the RoundTable.** The gateway looks for the
  knight's RoundTable in this order:
  1. the `ai.roundtable.io/round-table` label;
  2. the legacy `ai.roundtable.io/table` label;
  3. a RoundTable named `default` in the knight's namespace.

  The `subjectPrefix`, `resultsStream` and `modelAliases` come from that
  RoundTable. A knight without any of these falls back to its own
  `spec.nats.subjects` (for the prefix) and `spec.nats.resultsStream`.

## Auth

Every endpoint except `/healthz`, `/readyz` and the webhook endpoint
`POST /v1/hooks/{namespace}/{trigger}` (which takes each Trigger's own
credential instead, see [Webhooks](#webhooks)) requires
`Authorization: Bearer <token>`. The token comes from a Secret
(`gateway.auth.existingSecret`, or one the chart generates when unset; key `gateway.auth.secretKey`):

- The gateway refuses to start without a token, or if any token is shorter
  than 16 characters.
- The file may hold one token per line. To rotate, put the old and new token in
  the Secret, switch the callers, then remove the old one. Changes take effect
  within a minute without a restart.
- Tokens are compared as SHA-256 digests in constant time.
- A line may name its token: `<name> <token>` (letters, digits, `.`, `_`,
  `-`). The name is the caller recorded for requests with that token; a line
  without a name records `token`. Keep the chart-generated token unnamed: the
  dashboard sends the whole line.

In-cluster workloads can instead send their own Kubernetes ServiceAccount
token (`gateway.serviceAccountAuth`, on by default), see
[Calling from a workload](#calling-from-a-workload).

### Who called

The gateway records the caller of every authenticated request:
`<namespace>/<name>` for a ServiceAccount, the token's name for a static
token. It is in:

- the request log (one `Request` line per request with `caller`,
  `callerKind`, `route`, `code` and `table`; `/healthz` is not logged),
- the task record (`caller` in `GET /v1/tasks/{taskId}` and the MCP tools),
- the `ai.roundtable.io/caller` annotation on chain runs and missions the
  gateway starts (and `caller` in their records),
- the `caller_kind` label (`serviceaccount`, `token` or `none`) of
  `roundtable_gateway_requests_total`. The caller's name is not a label: one
  series per ServiceAccount would grow without bound.

The chart also creates a NetworkPolicy (`gateway.networkPolicy`). By default it
allows ingress only from pods in the release namespace. Add peers for your
callers, for example OpenClaw's namespace:

```yaml
gateway:
  enabled: true
  auth:
    existingSecret: roundtable-gateway-token
  networkPolicy:
    from:
      - podSelector: {}
      - namespaceSelector:
          matchLabels:
            kubernetes.io/metadata.name: ai
```

The gateway is **on by default**. If you set no `gateway.auth.existingSecret`,
the chart generates a random token into `<fullname>-gateway-token` (kept on
uninstall, reused on upgrade) and the dashboard reads it from a mounted file
(`GATEWAY_TOKEN_FILE`). Read it with:

```bash
kubectl -n roundtable get secret <fullname>-gateway-token \
  -o jsonpath='{.data.token}' | base64 -d
```

**The dashboard** uses the static token by default. Set
`dashboard.gateway.serviceAccountToken=true` to give it a projected
ServiceAccount token (audience `gateway.serviceAccountAuth.audience`, 1h,
rotated by the kubelet) at the same `GATEWAY_TOKEN_FILE` path instead; this
requires `gateway.serviceAccountAuth.enabled=true` (the chart refuses to render
otherwise). The static token stays for other callers.

To use your own token, create a Secret and set `gateway.auth.existingSecret`;
to rotate, put old and new tokens on separate lines, switch callers, then drop
the old one (mounted files update without restarts). Note `helm template`
cannot `lookup` the cluster, so it renders a new token every run. Disable the
gateway with `gateway.enabled=false`.

```bash
kubectl -n roundtable create secret generic roundtable-gateway-token \
  --from-literal=token="$(openssl rand -hex 32)"
```

## Calling from a workload

A pod in the cluster can call the gateway with its own ServiceAccount token,
so a CronJob or sync job needs no stored credential. Mount a projected
ServiceAccount token with the gateway's audience (`roundtable-gateway`,
`gateway.serviceAccountAuth.audience`) and send it as the bearer token. The
kubelet refreshes the file before the token expires; read it on every
request.

When a bearer token is not one of the static tokens, the gateway sends it to
a `TokenReview` with its audience. It accepts the request when the API server
authenticates the token for that audience as
`system:serviceaccount:<namespace>:<name>` and the namespace is in
`gateway.serviceAccountAuth.allowedNamespaces` (default `["*"]`, any). The
caller is recorded as `<namespace>/<name>`.

| Token | Response |
|-------|----------|
| Unknown, expired, or issued for another audience | `401` |
| Authenticated, but not a ServiceAccount, or its namespace is not allowed | `403` |
| The `TokenReview` itself failed (API server unreachable) | `503`, retry |
| Too many uncached tokens at once (see below) | `429` with `Retry-After` (seconds), retry |

A success is cached in memory until 30 seconds before the token expires and
at most 5 minutes, so a deleted pod's or ServiceAccount's token stops working
within that time. A refusal is cached for 5 seconds. The cache is bounded
(4096 tokens).

TokenReviews are rate limited so a flood of random tokens cannot overload the
API server. Cache misses share one bucket per gateway replica, by default 50
per second with a burst of 100 (`gateway.serviceAccountAuth.reviewRate` and
`reviewBurst`; 0 means the default, the limit cannot be turned off). Past it
the gateway answers `429` with `Retry-After`. Cached tokens and static tokens
are never limited.

> **This first version grants the whole gateway API to any allowed
> ServiceAccount**: it can invoke any knight, run any invokable chain and
> start missions in the namespaces the gateway serves. Limit who can reach
> the gateway with `gateway.networkPolicy` and which namespaces' ServiceAccounts
> are accepted with `gateway.serviceAccountAuth.allowedNamespaces`. Per-table
> and per-chain grants come later.

The gateway needs `create` on `tokenreviews.authentication.k8s.io`; the chart
grants it (ClusterRole `<fullname>-gateway-sa-auth`) only when
`gateway.serviceAccountAuth.enabled` is true. With it off (`--service-account-auth=false`),
only static tokens are accepted. The calling pod's namespace must also be let
through `gateway.networkPolicy.from`.

### Example: a CronJob that runs a chain by name and waits

The ServiceAccount needs no RBAC of its own; the projected token only proves
who it is. The same manifest is in
[`examples/gateway-serviceaccount-cronjob.yaml`](../examples/gateway-serviceaccount-cronjob.yaml).

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: nightly-digest
  namespace: roundtable
---
apiVersion: batch/v1
kind: CronJob
metadata:
  name: nightly-digest
  namespace: roundtable
spec:
  schedule: "0 6 * * *"
  concurrencyPolicy: Forbid
  jobTemplate:
    spec:
      backoffLimit: 0
      template:
        spec:
          serviceAccountName: nightly-digest
          # Only the projected token below is mounted, not the default one.
          automountServiceAccountToken: false
          restartPolicy: Never
          containers:
            - name: run
              image: curlimages/curl:8.11.0
              env:
                - name: GW
                  value: http://<fullname>-gateway.roundtable.svc
              command: ["/bin/sh", "-c"]
              args:
                - |
                  set -eu
                  TOKEN=$(cat /var/run/secrets/roundtable-gateway/token)
                  # Run the invokable chain "digest" and wait up to 25 minutes.
                  code=$(curl -sS --max-time 1560 -o /tmp/run.json -w '%{http_code}' \
                    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
                    "$GW/v1/chains/digest/runs" \
                    -d '{"input": {"date": "'"$(date +%F)"'"}, "waitSeconds": 1500}')
                  cat /tmp/run.json
                  # 200 = finished; 202 = still running after 1500 s.
                  [ "$code" = 200 ] && grep -q '"phase":"Succeeded"' /tmp/run.json
              volumeMounts:
                - name: gateway-token
                  mountPath: /var/run/secrets/roundtable-gateway
                  readOnly: true
          volumes:
            - name: gateway-token
              projected:
                sources:
                  - serviceAccountToken:
                      # Must equal gateway.serviceAccountAuth.audience.
                      audience: roundtable-gateway
                      expirationSeconds: 3600
                      path: token
```

The run copy carries `ai.roundtable.io/caller: roundtable/nightly-digest`, and
`GET /v1/runs/{namespace}/{run}` returns `"caller": "roundtable/nightly-digest"`.

## HTTP API

The API is described as an OpenAPI 3.1 document at `GET /openapi.json`. It
needs the same bearer token as `/v1`, reports the chart `appVersion` as
`info.version` and lists only the route groups that are enabled (missions,
chain runs, webhooks and schedule firings). It covers the REST routes, not the `/mcp` endpoint. A
test keeps it in sync with the routes the gateway serves.

```bash
curl -H "Authorization: Bearer $TOKEN" http://roundtable-gateway/openapi.json
```

### `POST /v1/knights/{name}/invoke`

Request body fields:

```json
{
  "task": "Summarise open PRs",
  "runId": "",
  "model": "",
  "timeoutSeconds": 60,
  "namespace": ""
}
```

| Field | Meaning |
|-------|---------|
| `task` | Required. The prompt (max 256 KiB). |
| `runId` | Optional. Pass the `runId` of an earlier invocation to continue that conversation. The knight keys its agent session on `runId` (pi-knight #53). If omitted, the gateway generates a fresh one. |
| `model` | Optional per-task model: a model ID or a RoundTable alias. It is resolved and checked against the knight's runtime exactly like `Chain.steps[].model` ([contract](knight-task-contract.md#model)). An unsuitable model returns 400 and nothing is published. |
| `timeoutSeconds` | How long to wait. Omit for `gateway.defaultTimeout`. `0` returns at once. Capped at `gateway.maxTimeout`. |
| `namespace` | The Knight's namespace. It defaults to `gateway.namespace` and must be that namespace or one listed in `gateway.allowedNamespaces`. |

Responses:

- **200**: the task finished. `status` is `succeeded` or `failed`. A failed task
  is still a 200; read `status` and `error`. When the knight refused because
  its plan quota is used up, the record also has `"usageLimited": true`
  (absent on other failures): back off instead of retrying. The gateway does
  not retry or defer the task itself.
- **202**: still running when the wait ended. The `Location` header is
  `/v1/tasks/{taskId}`.
- **400**: bad request, bad model, a bad `Idempotency-Key`, or a namespace that isn't allowed.
- **404**: unknown knight.
- **422**: an `Idempotency-Key` repeated with a different `task`, `model` or `runId`.
- **429**: `maxInFlight` waits are already open.
- **503**: the knight is suspended, or NATS is unavailable (also when the idempotency key cannot be looked up, so a replay can fail with 503 and be retried). A replay also gets 503 while the first call is still being started (its key exists but the task is not stored yet).

**Idempotency.** Send an `Idempotency-Key` header (1 to 255 printable ASCII
characters, else **400**) to make the call safe to retry after a dropped
connection. The first call publishes the task and stores the key (scoped to
namespace and knight) next to the task record; a repeat publishes nothing and
returns the original task with the response header `Idempotent-Replay: true`.
Like the first call, a repeat of a task that is still running waits up to
`timeoutSeconds` for the result (**200**), else answers **202**. That wait counts against
`maxInFlight`, so a repeat answers **429** when no slot is free. A repeat with a different
`task`, `model` or `runId` is a **422**. The key lives as long as the task
record (`gateway.taskTTL`); a call that fails before publishing releases it.
If the first call crashed after storing the key but before storing the task, a
repeat within 30 seconds gets **503** (retry shortly); after that the orphaned
key is removed and the repeat runs as a new call.

Both 200 and 202 return the task record:

```json
{
  "taskId": "invoke-operator-1763914401000-1a2b3c4d",
  "runId": "8c0e…",
  "knight": "operator",
  "namespace": "roundtable",
  "roundTable": "dev",
  "status": "succeeded",
  "output": "…",
  "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
  "requestedModel": "opus",
  "model": "claude-opus-4-1",
  "createdAt": "2026-11-23T10:00:00Z",
  "completedAt": "2026-11-23T10:00:41Z",
  "resultSubject": "rt-dev.results.invoke-operator-1763914401000-1a2b3c4d",
  "resultsStream": "rt_dev_results"
}
```

A quota refusal is `"status": "failed"` with `"usageLimited": true` and the
knight's message in `error`.

`traceId` is the OpenTelemetry trace of the call. It is present when tracing is
on (`OTEL_EXPORTER_OTLP_ENDPOINT` set; the chart does this when telemetry is
enabled) and is also echoed from an incoming `traceparent` header when the
gateway has no tracing. The gateway starts a server span per request (named `<METHOD> <route
template>`, none for `/healthz`), honours an incoming `traceparent` header
(the span becomes its child), and publishes the task with that span's context, so
the knight's `invoke_agent` span is in the same trace. The `Request` log line
carries the same `trace_id`.

Empty output counts as a failure, as it does in the task contract.

### `GET /v1/tasks/{taskId}`

Returns the task record. If the task is still `pending`, the gateway first
checks the results stream for the result and records it if found.

Once the task finished, the record also carries the usage the knight
reported, each omitted when the knight did not send it: `cost` (USD; notional
for subscription billing), `billing`, `durationMs`, `tokens`
(`input`, `output`, `cacheRead`, `cacheWrite`), `turns` and `toolCalls`.
`toolCalls` is absent, not `0`, when the knight does not report tool calls.

### `GET /v1/tasks/{taskId}/events`

Follows a task live as Server-Sent Events (`text/event-stream`). Connect to a
running task and the stream carries what the knight is doing, then the result,
then closes:

```bash
curl -N -H "Authorization: Bearer $TOKEN" \
  "$GATEWAY/v1/tasks/$TASK_ID/events"
```

```
event: activity
data: {"v":1,"type":"tool.started","knight":"operator","taskId":"invoke-operator-1760000000000-ab12cd34","seq":2,"tool":"Bash","argsSummary":"go test ./..."}

: heartbeat

event: result
id: result
data: {"taskId":"invoke-operator-1760000000000-ab12cd34","status":"succeeded","output":"..."}
```

| Event | Data |
|-------|------|
| `activity` | A knight's [task activity event](design/task-activity-events.md) as published: `task.started`, `task.heartbeat`, `tool.started`, `tool.finished`, `assistant.message`, `task.finished` |
| `result` | The task record, once the result arrived (same as `GET /v1/tasks/{taskId}`). The stream closes after it. |
| `error` | `{"error": "..."}`: the stream failed after it started |

A comment line (`: heartbeat`) is sent every 15 s when the stream is idle, so
proxies don't cut it.

- A task that already finished returns its `result` at once. An unknown task is a **404**.
- **Activity is not replayed.** Knights publish it on core NATS and it isn't
  stored, so a stream only shows events from the moment it connected. The
  `result` is always delivered, and activity is sampled above 20 events/s per
  task (see the design doc).
- The `result` event has `id: result`. A client that reconnects with
  `Last-Event-ID: result` gets **204** (no content, and `EventSource` stops
  reconnecting) instead of the result again.
- Streams count against `gateway.maxStreams` (default 64 per replica); beyond
  it callers get **429**. Streams are closed when the gateway shuts down.
- Only tasks started through the gateway (`invoke`) can be followed.

### `GET /v1/tasks?knight=&runId=&status=&limit=`

Lists recent gateway tasks, newest first. The default limit is 20 and the
maximum is 200.

### `GET /v1/dead-letters?knight=&reason=&state=&limit=`

Lists dead-lettered tasks (see the task contract's dead-letter section), newest
first, each with its `state` (`open`, `retried` or `resolved`). The limit
defaults to and is capped at 100. The list omits the original task payload.
An entry carries `traceId` (32 hex) when the original task had a valid
`traceparent` header, and omits it otherwise. The entry's `traceId` is
the same ID the dashboard's "View trace" link uses.

### `GET /v1/dead-letters/{taskId}`

Returns one entry, including `task`, the original payload. Add `?table=` if the
same task ID is dead-lettered in more than one RoundTable.

### `POST /v1/dead-letters/{taskId}/retry`

A deliberate retry (nothing retries automatically). It republishes the original
task to the same knight as a new task `<taskId>-r<attempt>` with `attempt` set
to the original attempt + 1, `retryOf` set to the original task ID and
`previousFailure` set to the dead-letter reason (2 KB at most), so the knight
can look for partial work. The new task is recorded like an invoke, so
`GET /v1/tasks/{id}` works, and the entry becomes `retried` with `retriedAs`
set. The state change is a compare-and-set, so two concurrent retries publish
one task.

Optional body: `{"force": true, "namespace": "...", "table": "..."}`.

| Status | When |
|--------|------|
| `202` | Published. Body: `{taskId, retryOf, attempt, knight}`. |
| `400` | The task ID is dead-lettered in several RoundTables and `table` is missing, the body is invalid, or the resolved knight belongs to another RoundTable than the entry. |
| `404` | No such entry. |
| `409` | The entry isn't `open` (body has the current `state`), or it belongs to a chain: rerun the chain or let its `retryPolicy` handle it. `force` retries the chain step on its own; the chain doesn't continue. |
| `422` | The entry has no original task payload. |

### `GET /v1/knights?namespace=&load=`

Lists knights with their name, domain, model, agent runtime, phase, readiness
and whether they are suspended.

With `load=true` each knight also carries `queue: {pending, inFlight}`: tasks
queued for it and tasks it has taken but not yet answered, read from its
durable NATS consumer (so work started by chains counts too). A knight whose
consumer does not exist yet reports zeros. If NATS cannot be read for a knight,
its `queue` is left out and the error is logged. Without `load` NATS is not
called and the response is unchanged.

### Knight snapshots

Workspace snapshots of a knight (see [workspace-snapshots.md](workspace-snapshots.md))
can be listed and requested without `kubectl`. Taking one only sets the Knight
annotation `ai.roundtable.io/snapshot` to a fresh value (unix nanoseconds) and
records the caller in `ai.roundtable.io/snapshot-caller`; the operator does the
rest. Taking one needs `gateway.knightSnapshots.enabled=true` (default `false`,
gateway flag `--knight-snapshots`), which lets the gateway `get` and `patch`
Knights. While it is off, `POST /v1/knights/{name}/snapshot` answers `403` with
an `error` saying the gateway was installed without
`gateway.knightSnapshots.enabled=true`, and the `snapshot_knight` MCP tool is not
listed. Listing snapshots is always served.

#### `GET /v1/knights/{name}/snapshots?namespace=`

`{knight, namespace, lastSnapshotRequest, snapshots: [{name, createdAt, readyToUse}], condition}`:
`status.workspace` (oldest snapshot first) and the `WorkspaceSnapshots`
condition (`status`, `reason`, `message`), which says why a request failed.
`404` for an unknown knight.

#### `POST /v1/knights/{name}/snapshot?namespace=&waitSeconds=`

Answers `202` with `{knight, namespace, request, expectedName, caller}`.
`expectedName` is `<knight>-ws-<unix seconds>`; the operator names the
snapshot with its own clock, so confirm with the list.

| Status | Meaning |
|---|---|
| `202` | Requested. A suspended knight answers with a `note`: the snapshot is taken when it resumes. |
| `403` | Snapshot requests are off: the gateway was installed without `gateway.knightSnapshots.enabled=true`. |
| `404` | Unknown knight. |
| `409` | Another request came in during the same second (the snapshot name has second resolution); retry shortly. |
| `422` | `workspace.mode` is `ephemeral`: only `pvc` workspaces can be snapshotted. |

With `waitSeconds` (capped at the gateway's maximum wait) the call polls until
the snapshot is `readyToUse` and then adds `ready: true` and `snapshot`; on
timeout it still answers `202`, with the current `condition`. Not available on
a suspended knight (it answers at once).

```bash
curl -sX POST -H "Authorization: Bearer $TOKEN" \
  "$GATEWAY/v1/knights/galahad/snapshot?waitSeconds=120"
curl -s -H "Authorization: Bearer $TOKEN" "$GATEWAY/v1/knights/galahad/snapshots"
```

## Missions

A mission is a goal that a planner knight breaks into chains over standing
knights and the RoundTable's knight templates (or a set of existing chains
run together). The gateway can launch one and follow it to its result. Turn
the endpoints off with `gateway.missions.enabled: false`.

### Why the request is restricted

A Mission written through the Kubernetes API can choose images, mount Secrets
and define pods: `knights[].ephemeralSpec`, `knightTemplates`, `secrets`,
`roundTableTemplate`, `planner.ephemeralSpec`, `knights[].specOverrides.env`.
A gateway token must not grant any of that. So `POST /v1/missions` takes its
own narrow body, not a Mission spec, and rejects unknown fields with 400:

- Callers name things that already exist: a RoundTable, Knights, Chains, a
  planner Knight. The gateway checks each one before creating the Mission.
- Ephemeral knights can only come from the RoundTable's `knightTemplates`,
  picked by the planner. The gateway labels its Missions
  `ai.roundtable.io/created-by: gateway`, and for those the operator's
  planner rejects any plan with an `ephemeralSpec`, `specOverrides.env` or
  `specOverrides.tools`. A caller therefore can't reach an image or a Secret
  by talking the planner into it through the objective.
- A budget is required and capped (`gateway.missions.maxBudgetUSD`), as are
  `ttlSeconds` and `timeoutSeconds`. `planner.allowSkillGeneration: true` is
  rejected unless `gateway.missions.allowSkillGeneration` is set.
- The gateway can only delete Missions it created.

The gateway gets `get`/`list`/`watch`/`create`/`delete` on Missions through a
Role in `gateway.namespace` and in each of `gateway.allowedNamespaces` (a
ClusterRole if that list holds `"*"`). Each Role and RoleBinding is a chart
resource in that namespace, so `helm install` fails if a listed namespace does
not exist yet: create it first.

### `POST /v1/missions`

```json
{
  "objective": "Compare NATS KV and Redis for per-key TTLs; recommend one",
  "successCriteria": "a one-page recommendation with sources",
  "roundTable": "dev",
  "costBudgetUSD": 3,
  "ttlSeconds": 7200,
  "timeoutSeconds": 3600,
  "planner": {"knight": "planner", "context": "prefer primary sources", "maxChains": 3, "maxKnights": 2},
  "recruitExisting": true,
  "knights": ["researcher"],
  "chains": [{"name": "deep-research", "input": "topic: NATS KV TTL"}],
  "waitSeconds": 0,
  "namespace": ""
}
```

| Field | Meaning |
|-------|---------|
| `objective` | Required. The goal (max 16 KiB). |
| `successCriteria` | Optional. How to judge success (max 16 KiB). |
| `roundTable` | Required. The RoundTable the mission runs under. Its `knightTemplates`, defaults and policies apply. |
| `costBudgetUSD` | Required JSON number, greater than 0 and at most `gateway.missions.maxBudgetUSD` (default 10). |
| `ttlSeconds` | Optional. Seconds until cleanup: 60 up to `gateway.missions.maxTTLSeconds`. Defaults to 3600 (or the cap if lower). |
| `timeoutSeconds` | Optional. Seconds the mission may run: 60 up to `gateway.missions.maxTimeoutSeconds`. Defaults to 1800. |
| `planner` | Optional. Makes this a meta-mission. `knight` is an existing Knight; omit it to use the RoundTable's planner (`spec.planner`, by default the table-owned `<table>-planner`), else (deprecated) the knight labelled `ai.roundtable.io/role=planner`. `context` (max 64 KiB), `maxChains` (1-20), `maxKnights` (1-50), `allowSkillGeneration` (see above). |
| `recruitExisting` | Optional. Lets the planner use standing knights. |
| `knights` | Optional. Names of existing Knights taking part (at most 50). |
| `chains` | Optional. Existing Chains to run, each with an optional `input` (max 64 KiB, at most 20 chains). Without a `planner`, at least one chain is required. |
| `waitSeconds` | Optional. Wait this long for the mission to finish (default 0, capped at `gateway.maxTimeout`). Waits count against `maxInFlight`; a busy gateway answers 429 before it creates anything. |
| `namespace` | Optional. As for invoke. |

Anything else (`ephemeralSpec`, `secrets`, `knightTemplates`,
`roundTableTemplate`, `image`, `notify`, `planner.templateName`, …) is a 400.
Responses: **201** with the mission and `Location: /v1/missions/{namespace}/{name}`;
**202** if `waitSeconds` ended before the mission did; **400** for a bad or
disallowed request. Mission names are `gw-<8 hex>`.

### `GET /v1/missions/{namespace}/{name}?waitSeconds=`

Returns the mission. With `waitSeconds`, the gateway waits up to that long
(capped at `gateway.maxTimeout`) for the mission to reach a terminal phase
(`Succeeded`, `Failed`, `Expired` or `CleaningUp`) and then returns it
either way. The response is a fixed projection of the status. It has no spec,
env, Secret or log fields:

```json
{
  "name": "gw-1a2b3c4d",
  "namespace": "roundtable",
  "phase": "Active",
  "done": false,
  "createdByGateway": true,
  "conditions": [{"type": "Ready", "status": "True", "reason": "Assembled", "message": "…", "lastTransitionTime": "…"}],
  "planning": {"chainsGenerated": 2, "knightsGenerated": 1, "skillsGenerated": 0, "rawOutput": "…", "rawOutputTruncated": true},
  "chains": [{"name": "gather", "chain": "gw-1a2b3c4d-gather", "phase": "Running",
              "steps": [{"name": "search", "phase": "Succeeded"}, {"name": "summarise", "phase": "Running"}]}],
  "knights": [{"name": "researcher", "ready": true, "tasksCompleted": 3, "ephemeral": false}],
  "result": "",
  "totalCost": "0.84",
  "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
  "createdAt": "…", "startedAt": "…", "completedAt": null, "expiresAt": "…"
}
```

`planning.rawOutput` holds only the first 4 KiB of the planner's output.

`traceId` is the mission's trace (its `status.traceID`). It is omitted when the
mission has no trace or the value is not a valid trace ID (32 hex). It is the
same ID the dashboard's "View trace" link uses.

### `GET /v1/missions?namespace=&phase=&limit=`

Lists missions in a namespace, newest first, in the same projection but
without chain steps. `phase` matches case-insensitively. The default limit
is 20 and the maximum is 200. The list is not limited to missions the gateway
created (`createdByGateway` tells them apart): it also returns missions made by
kubectl or GitOps, including their `result`.

### `DELETE /v1/missions/{namespace}/{name}`

Deletes a mission the gateway created (its chains and ephemeral knights go
with it). Any other mission is a **403**.

### Example: launch a research mission and poll it

```bash
GW=http://$FULLNAME-gateway.roundtable.svc
AUTH="Authorization: Bearer $TOKEN"

NAME=$(curl -s -H "$AUTH" -H 'Content-Type: application/json' "$GW/v1/missions" -d '{
  "objective": "Compare NATS KV and Redis for per-key TTLs and recommend one",
  "roundTable": "dev",
  "costBudgetUSD": 3,
  "planner": {"maxChains": 2},
  "recruitExisting": true
}' | jq -r .name)

# Long-poll in 5-minute slices until the mission is done.
until curl -s -H "$AUTH" "$GW/v1/missions/roundtable/$NAME?waitSeconds=300" \
    | tee /tmp/mission.json | jq -e .done >/dev/null; do
  jq -c '{phase, chains: [.chains[]? | {name, phase}]}' /tmp/mission.json
done
jq -r '.phase, .result, .totalCost' /tmp/mission.json
```

## Chain runs

A caller can run a Chain by name with its own input and either wait for the
final output or get a run to poll. These endpoints are served when
`gateway.runs.enabled` is true (the default).

- **Only opted-in Chains.** A Chain must carry the label
  `ai.roundtable.io/invokable: "true"`. Anything else (the dev factory's
  pipeline, mission-scoped chains, run copies made by issue pickup or by the
  gateway itself) is a **403**. Opt a Chain in with
  `kubectl label chain <name> ai.roundtable.io/invokable=true`.
- **A run is a copy.** A Chain has one run at a time, so every gateway run is
  a new Chain copied from the named one (the same `internal/chainrun` helper
  issue pickup uses). The copy has the template's steps, no schedule, and only
  `spec.input` set from the request: callers can never send steps. Copies are
  named `<chain>-gw-<8 hex>` and carry the labels
  `ai.roundtable.io/run-source: gateway` and
  `ai.roundtable.io/created-by: gateway`, and the annotation
  `ai.roundtable.io/run-of: <chain>`.
- **Copies are cleaned up.** The gateway deletes its own copies
  `gateway.runTTL` (default 24h) after they finish. That is why it has
  `create`, `patch` (to cancel a run) and `delete` on Chains, granted per namespace like Missions (a
  ClusterRole if `allowedNamespaces` holds `"*"`).

### `GET /v1/chains?namespace=`

`{"chains": [{name, namespace, description, steps, timeoutSeconds}]}`: the
opted-in Chains a caller can run.

### `POST /v1/chains/{name}/runs`

```json
{"input": {"topic": "NATS KV"}, "namespace": "", "waitSeconds": 1500}
```

| Field | Meaning |
|-------|---------|
| `input` | Optional. The run's `spec.input`: a JSON string is used as is, any other JSON value as its (compact) JSON text. At most 256 KiB. Omitted or `null` keeps the Chain's own input. |
| `namespace` | Optional. The Chain's namespace: the gateway's own or one of `allowedNamespaces`, else **400**. |
| `waitSeconds` | Optional. Wait this long for the run to finish (default 0, capped at `gateway.maxTimeout`). Waits count against `maxInFlight`; a busy gateway answers 429 before it creates anything. |

Unknown fields are a **400**, an unknown Chain a **404**, a Chain without the
label a **403** and a suspended Chain a **503**. The answer is the run record:
**200** when the run has finished, otherwise **202** with
`Location: /v1/runs/{namespace}/{run}`.

**Idempotency.** An `Idempotency-Key` header (1 to 255 printable ASCII
characters, else **400**) names the run copy `<chain>-gw-<first 8 hex of
sha256(namespace/chain/key)>` instead of a random suffix, so a repeat hits
Kubernetes `AlreadyExists` and returns the existing run with
`Idempotent-Replay: true` instead of starting another. The key lasts as long
as the run copy: once the janitor deletes it (`gateway.runs.ttl` after it
finished), the key can start a new run.
A repeat of the same key with different input is not an error here: it returns
the existing run, whatever the new input says, and starts nothing.

```json
{
  "run": "blog-drafter-gw-1a2b3c4d",
  "chain": "blog-drafter",
  "namespace": "roundtable",
  "runId": "2b9c…",
  "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
  "phase": "Succeeded",
  "done": true,
  "createdAt": "2026-11-24T07:00:00Z",
  "startedAt": "2026-11-24T07:00:01Z",
  "completedAt": "2026-11-24T07:24:12Z",
  "steps": [
    {"name": "research", "phase": "Succeeded", "output": "…"},
    {"name": "draft", "phase": "Succeeded", "output": "…"}
  ],
  "output": "…"
}
```

`phase` is the Chain's phase (`Pending` until the chain controller starts the
run, then `Running`, `Succeeded`, `PartiallySucceeded` or `Failed`). Each
step's `output` is cut at 64 KiB (`outputTruncated: true`). `output` is the
output of the final step: the one step no other step depends on. If several
steps have no dependents, `output` is omitted; read `steps`. `traceId` is the
Chain's `status.traceID`; a mission's view carries the Mission's. Both are
omitted when empty or not a valid 32-hex trace ID.

### `POST /v1/runs/{namespace}/{run}/cancel`

Cancels an unfinished run the gateway started. The optional body
`{"reason": "..."}` is recorded on the run. The gateway sets the
`ai.roundtable.io/cancel` annotation on the run copy; the chain controller then
marks every Pending or Running step `Failed` with `error: "cancelled: <reason>"`,
ends the run `Failed` with the condition `Cancelled=True`, emits a
`RunCancelled` event and starts no further steps (loop rounds and fan-out items
included). The call waits up to 10 seconds for that and returns the run record
(**200** once the run is `Failed`, else **202** with a `Location` to poll).

- A finished run is a **409** (`{"error": "...", "state": "<phase>"}`).
- A run started by a Trigger or by issue pickup is a **403**: only gateway runs can be cancelled.
- A Chain that is not a run copy is a **404**.
- A task a knight already picked up is not recalled: the knight finishes or
  times out and its result is ignored.

This needs `patch` on Chains, which the chart grants only when
`gateway.runs.enabled` is true.

### `POST /v1/runs/{namespace}/{run}/rerun`

Re-runs a finished run the gateway started, in one call. The optional body is
`{"input": ..., "waitSeconds": N}`: the new run gets the finished run's stored
`spec.input` unless `input` overrides it (same encoding as `run_chain`).
`Idempotency-Key` works as on `POST /v1/chains/{name}/runs`.

The run's template Chain is started through the same path as `run_chain`, so all
its checks apply: the template must still exist (**404** otherwise), carry the
label `ai.roundtable.io/invokable=true` (**403**) and not be suspended (**503**),
and a waiting call needs a `maxInFlight` slot (**429**). A run that is not
finished is a **409** (`{"error": "...", "state": "<phase>"}`). Failed steps are
not resumed: the whole chain runs again.

The response is the new run's record (**200** if finished, else **202** with a
`Location`), with `rerunOf: "<run>"` naming the original (also set as the
`ai.roundtable.io/rerun-of` annotation on the new copy).

### `GET /v1/runs/{namespace}/{run}?waitSeconds=`

Returns a run the gateway started (any other Chain is a **404**), with the
same 200/202 convention. `waitSeconds` long-polls (capped at
`gateway.maxTimeout`), so a caller whose connection dropped resumes waiting
where it left off.

The record's `traceId` is the run's trace, taken only from the Chain's
`status.traceID`. It is omitted when the trace is unknown, is not a valid trace
ID (32 hex) or the run is still `Pending` (any trace on the Chain is from before
the run). It is the same ID the dashboard's "View trace" link uses.

### `GET /v1/runs/{namespace}/{run}/events`

Follows a gateway run as Server-Sent Events, with the same framing, heartbeat,
`Last-Event-ID` and `maxStreams` rules as the task stream:

```bash
curl -N -H "Authorization: Bearer $TOKEN" \
  "$GATEWAY/v1/runs/roundtable/digest-gw-1a2b3c4d/events"
```

| Event | Data |
|-------|------|
| `step` | `{"name","phase","error?","output?","outputTruncated?"}`: sent when a step's phase changes. A client that connects mid-run first gets each step's current phase. `output` is cut at 4 KiB; the result has the full (64 KiB) output. |
| `activity` | The activity events of the steps that are running (found by the step's `taskId`) |
| `result` | The run record (same as `GET /v1/runs/{namespace}/{run}`) once the run is terminal. The stream closes after it. |
| `error` | `{"error": "..."}` |

A finished run yields its steps and the `result` at once. Step changes are
read from the Chain every second or two, so a step that starts and finishes
between two reads is reported only as its latest phase. Activity is not
replayed, as for tasks.

### `GET /v1/runs?namespace=&chain=&phase=&source=&active=&issue=&limit=`

`{"runs": [...]}`: runs newest first, without `steps` and
`output` (get one run for those). `limit` defaults to 20, at most 200.

- `source=gateway|trigger|pickup|all` picks which runs are listed; the default
  `gateway` lists the runs started through the gateway, as before.
- `active=true` keeps runs that are `Running` or `Pending`.
- `issue=owner/repo#12` keeps the runs of that picked-up issue.

Every record carries `source`, `ageSeconds` (since creation), `runningSeconds`
(while `Running`) and, for runs that carry the pickup annotations (pickup
runs and GitHub-source Trigger runs), `issue: {repo, number}`. `chain` falls
back to the `ai.roundtable.io/run-template` annotation for runs that are not
gateway runs. Get and events work for runs of every source; cancel only for
gateway runs. The `list_runs` MCP tool takes the same filters.

### Waits longer than 10 minutes

`gateway.maxTimeout` (`--max-timeout`) defaults to 30 minutes, so a CronJob
can wait for a 25-minute chain in one request. The gateway sets no read or
write timeout on its HTTP server: `ReadHeaderTimeout` (10s) only bounds
reading the request headers and `IdleTimeout` (2m) only the gap between
requests on a kept-alive connection, so neither cuts a long response.
**Anything between the caller and the gateway needs its own timeout raised**:
`curl --max-time`, an ingress or proxy read timeout (e.g.
`nginx.ingress.kubernetes.io/proxy-read-timeout: "1900"`), a service mesh
route timeout, an MCP client's tool-call timeout. If one of them can't wait
that long, use run-then-poll: start with a short `waitSeconds` and long-poll
`GET /v1/runs/{namespace}/{run}?waitSeconds=` in slices below that limit.

### Example: a CronJob that runs a chain and waits 1500 s

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: blog-drafter
  namespace: roundtable
spec:
  schedule: "0 7 * * 1"
  concurrencyPolicy: Forbid
  jobTemplate:
    spec:
      backoffLimit: 0
      template:
        spec:
          restartPolicy: Never
          containers:
            - name: run
              image: curlimages/curl:8.11.0
              env:
                - name: TOKEN
                  valueFrom:
                    secretKeyRef: {name: roundtable-gateway-token, key: token}
              command: ["/bin/sh", "-c"]
              args:
                - |
                  set -eu
                  code=$(curl -sS --max-time 1560 -o /tmp/run.json -w '%{http_code}' \
                    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
                    http://<fullname>-gateway.roundtable.svc/v1/chains/blog-drafter/runs \
                    -d '{"input": {"week": "'"$(date +%G-W%V)"'"}, "waitSeconds": 1500}')
                  cat /tmp/run.json
                  # 200 = finished; 202 = still running after 1500 s.
                  [ "$code" = 200 ] && grep -q '"phase":"Succeeded"' /tmp/run.json
```

Without a stored token, the same CronJob can authenticate with its own
ServiceAccount: see [Calling from a workload](#calling-from-a-workload).

Moving a kagent caller to these endpoints, with a Python helper: [Migrating from kagent](migrating-from-kagent.md).

### Example: run, then poll

```bash
GW=http://$FULLNAME-gateway.roundtable.svc
AUTH="Authorization: Bearer $TOKEN"

LOC=$(curl -s -D - -o /dev/null -H "$AUTH" -H 'Content-Type: application/json' \
  "$GW/v1/chains/opencti-feed-sync/runs" -d '{"input": "since=24h"}' \
  | awk 'tolower($1)=="location:" {print $2}' | tr -d '\r')

# Long-poll in 5-minute slices until the run is done.
until curl -s -H "$AUTH" "$GW$LOC?waitSeconds=300" | tee /tmp/run.json | jq -e .done >/dev/null; do
  jq -c '{phase, steps: [.steps[]? | {name, phase}]}' /tmp/run.json
done
jq -r '.phase, .output' /tmp/run.json
```

## Schedules

See [Scheduled work](scheduled-work.md) for the whole picture (targets, missed
and overlapping runs, YAML first).

A schedule is a [Trigger](triggers.md) with a `spec.source.schedule`. The
gateway reads schedules, exports them as YAML, creates, replaces and deletes
them, and suspends, resumes and runs them; Triggers with any other source
(NATS, webhook, GitHub) are not schedules and are a **404** on these routes.
Served when `gateway.schedules.enabled` is true (the default), which grants
`get`, `list`, `watch`, `create`, `update`, `patch` and `delete` on Triggers in
the gateway's namespace and `allowedNamespaces` (a ClusterRole if
`allowedNamespaces` holds `"*"`). The suspend, resume and run POSTs patch
annotations only, never the spec.

### `managedBy`

Every schedule says who owns it, so a client knows whether it may change it
through the API:

| `managedBy` | When |
|---|---|
| `git` | The Trigger carries a Flux or Helm ownership label or annotation: `kustomize.toolkit.fluxcd.io/name`, `helm.toolkit.fluxcd.io/name` or `app.kubernetes.io/managed-by: Helm`. Git is the source of truth: change it there; `PUT` and `DELETE` through the gateway are refused with 409. |
| `api` | The Trigger is labelled `app.kubernetes.io/managed-by: roundtable-gateway` (created through the gateway). |
| `other` | Anything else, for example applied with `kubectl`. |

### `GET /v1/schedules?namespace=&table=`

`{"schedules": [...]}`, by name. `table` keeps the schedules of one
RoundTable. Each schedule:

```json
{
  "name": "morning-digest",
  "namespace": "roundtable",
  "table": "fleet-a",
  "cron": "0 7 * * 1-5",
  "timeZone": "Europe/Berlin",
  "description": "at 07:00 on Monday to Friday",
  "target": {"kind": "chain", "summary": "chain digest", "chain": "digest"},
  "suspended": false,
  "concurrency": {"policy": "Forbid", "maxActive": 0},
  "startingDeadlineSeconds": 300,
  "nextRunAt": "2026-11-26T06:00:00Z",
  "lastRunAt": "2026-11-25T06:00:02Z",
  "lastResult": {"time": "2026-11-25T06:00:02Z", "outcome": "started", "run": "digest-trg-1a2b"},
  "managedBy": "git"
}
```

- `table` is the Trigger's `roundTableRef`, else the target Chain's.
- `timeZone` is `UTC` when the Trigger sets none.
- `description` is the cron in words; an expression it cannot put in words
  comes back as `cron <expression>`.
- `target.kind` is `chain` or `knight`; `summary` is a one-line description of
  the target.
- `suspended` is effective: true when `spec.suspend` is set or the suspend
  annotation (set by `POST .../suspend`) is present. `suspendedBy` says which
  (`spec`, `annotation`, or `spec,annotation`) and is omitted when not suspended.
- `concurrency` is `spec.concurrency`: `policy` (`Allow` when unset, `Forbid` or
  `Replace`) and `maxActive` (0: no cap).
- `startingDeadlineSeconds` is the schedule source's deadline for a missed tick;
  omitted when unset.
- `nextRunAt` and `lastRunAt` are `status.nextScheduledAt` and
  `status.lastFiredAt`. `lastResult` is the newest of `status.recentFirings`:
  `outcome` is `started` (with the `run`), `skipped` (with the
  `skipReason`) or, for knight targets once the task has ended, `succeeded`,
  `failed` or `lost`.

### `GET /v1/schedules/{namespace}/{name}`

One schedule, as above. **404** if it does not exist or is not a schedule.

### `GET /v1/schedules/{namespace}/{name}/next?count=`

The next `count` fire times (default 5, max 50; outside 1 to 50 is a **400**)
of the stored cron, evaluated in the schedule's `timeZone`:

```json
{"namespace": "roundtable", "name": "morning-digest", "cron": "0 7 * * 1-5",
 "timeZone": "Europe/Berlin", "suspended": true,
 "times": [{"time": "2026-03-30T05:00:00Z", "local": "2026-03-30T07:00:00+02:00"}]}
```

`time` is RFC 3339 in UTC, `local` the same instant in the time zone (so a
DST change shows in the offset). A suspended schedule still answers, with
`suspended: true`. **404** if it does not exist or is not a schedule. The list
is cron ticks only: skip rules (concurrency policy, starting deadline) are not
applied.

### `GET /v1/schedules/preview?cron=&timeZone=&count=`

The same list for an expression that does not exist yet, to check it before
`POST /v1/schedules`. `timeZone` defaults to UTC. An invalid cron or time zone
is a **422** with the parser's message.

```sh
curl -s -H "$AUTH" "$GW/v1/schedules/preview?cron=0+7+*+*+1-5&timeZone=Europe/Berlin&count=3"
```

### `GET /v1/schedules/{namespace}/{name}/manifest`

The Trigger as YAML (`Content-Type: application/yaml`), ready to commit to
Git: no `status`, `managedFields`, `resourceVersion`, `uid`, `generation`,
`creationTimestamp`, owner references, finalizers or
`kubectl.kubernetes.io/last-applied-configuration`.

```sh
curl -s -H "$AUTH" "$GW/v1/schedules/roundtable/morning-digest/manifest" > morning-digest.yaml
```

Git is the source of truth for what Flux applies: an edit through the API
would be reverted on the next reconcile, so the gateway refuses it instead.
The 409 body says where to change it, naming the Flux Kustomization or
HelmRelease (or Helm release) when its labels say which:

```json
{"error": "conflict: schedule roundtable/morning-digest is managed by Git: change it in Git (applied by Flux Kustomization flux-system/apps), not through the gateway",
 "managedBy": "git"}
```

### Request body

`POST` and `PUT` take a Trigger's name and spec. Unknown fields (labels,
annotations, status) are rejected.

| Field | Notes |
|---|---|
| `name` | Required on `POST` (DNS name, at most 63 characters). On `PUT`, optional and must match the path. |
| `namespace` | `POST`: defaults to the gateway's namespace; must be allowed. `PUT`: optional and must match the path. |
| `spec` | A [`TriggerSpec`](triggers.md): `source.schedule` (`cron`, `timeZone`, `startingDeadlineSeconds`), exactly one of `target.chainRef`, `target.knight` or `target.mission`, `input`, `dedupeKey`, `concurrency`, `suspend`, `roundTableRef`, `ttlSecondsAfterFinished`. |

The gateway checks the CRD's rules before writing: exactly a schedule source
(`spec.source`), a five-field cron the trigger controller can parse
(`spec.source.schedule.cron`, no `TZ=` prefix), an IANA time zone other than
`Local` (`spec.source.schedule.timeZone`), an existing target Chain
(`spec.target.chainRef.name`), Knight (`spec.target.knight.name`) or valid
Mission templates (`spec.target.mission.objective`), the RoundTable when one
is named (`spec.roundTableRef.name`), and the concurrency and TTL bounds. A failure is
a 400 whose `field` names the offending field; so are validation errors the
API server returns (OpenAPI or CEL rules):

```json
{"error": "invalid request: spec.source.schedule.cron: \"every day\" is not a valid cron expression: ...",
 "field": "spec.source.schedule.cron"}
```

### `POST /v1/schedules`

Creates the Trigger, labelled `app.kubernetes.io/managed-by:
roundtable-gateway` and annotated `ai.roundtable.io/caller` with the caller
(the token's name from the token file, or `<namespace>/<serviceaccount>` for
ServiceAccount callers; see [Auth](#auth)). A schedule may
target a Chain (`target.chainRef`), a Knight (`target.knight`) or a Mission
(`target.mission`); the gateway applies the trigger controller's rules to each.
A knight target's Knight must exist (and sit at `roundTableRef` when one is
set). A mission target needs `roundTableRef`, and its `objective` and
`successCriteria` must parse as templates. Neither accepts `spec.input` or
`concurrency.policy: Replace`. Returns 201 with a `Location`, or 409
if a Trigger of that name exists.

```json
{"name": "morning-digest", "namespace": "roundtable", "managedBy": "api", "caller": "jobs/runner",
 "createdAt": "2026-11-25T07:00:00Z", "spec": {"source": {"schedule": {"cron": "0 7 * * 1-5", "timeZone": "Europe/Berlin"}}, "...": "..."}}
```

### `PUT /v1/schedules/{namespace}/{name}`

Replaces the spec of an existing schedule (labels, other annotations and
status are kept; the caller annotation is updated). 404 if there is no such
schedule, 409 if it is Git-managed or changed concurrently (read it and
retry).

### `DELETE /v1/schedules/{namespace}/{name}`

Deletes the schedule; runs it already started are not deleted with it and are
removed by their own `ttlSecondsAfterFinished` (a run created without one is
kept).
404 if there is no such schedule, 409 if it is Git-managed.

### Example: a weekday digest

```bash
GW=http://$FULLNAME-gateway.roundtable.svc
AUTH="Authorization: Bearer $(cat /var/run/secrets/roundtable/gateway/token)"

curl -s -H "$AUTH" -H 'Content-Type: application/json' "$GW/v1/schedules" -d '{
  "name": "morning-digest",
  "spec": {
    "source": {"schedule": {"cron": "0 7 * * 1-5", "timeZone": "Europe/Berlin"}},
    "target": {"chainRef": {"name": "daily-digest"}},
    "input": "Digest for {{ .Event.Time.Format \"2006-01-02\" }}",
    "concurrency": {"policy": "Forbid"}
  }
}'

# Move it to 06:30, then remove it.
curl -s -X PUT -H "$AUTH" -H 'Content-Type: application/json' \
  "$GW/v1/schedules/roundtable/morning-digest" -d '{
  "spec": {
    "source": {"schedule": {"cron": "30 6 * * 1-5", "timeZone": "Europe/Berlin"}},
    "target": {"chainRef": {"name": "daily-digest"}}
  }
}'
curl -s -X DELETE -H "$AUTH" "$GW/v1/schedules/roundtable/morning-digest"
```

### `POST /v1/schedules/{namespace}/{name}/suspend`

Sets `ai.roundtable.io/suspended` to the caller (`token` for the static bearer
tokens). The answer is the effective state:

```json
{"name": "morning-digest", "namespace": "roundtable", "suspended": true, "suspendedBy": "annotation"}
```

### `POST /v1/schedules/{namespace}/{name}/resume`

Removes the annotation. It does **not** clear `spec.suspend`: a schedule
suspended in its spec stays suspended, and the answer says so:

```json
{"name": "morning-digest", "namespace": "roundtable", "suspended": true, "suspendedBy": "spec",
 "message": "spec.suspend is still true: the schedule stays suspended until spec.suspend is cleared where the Trigger is defined"}
```

### `POST /v1/schedules/suspend` and `POST /v1/schedules/resume`

Suspend or resume **every schedule of a table** in one call, for maintenance
or an incident. Body: `{"table": "knights", "namespace": "roundtable"}`
(`table` is required; `namespace` defaults like the other routes). Each
schedule gets the same change as the single-schedule routes: only the
`ai.roundtable.io/suspended` annotation is set or removed, so the Trigger
manifests in Git are untouched, and a resume leaves a schedule suspended in its
spec suspended (its state carries the `message`). Mission-target schedules are
included. The answer is **200** unless the list itself fails; one failing
schedule does not stop the rest:

```json
{"changed": [{"name": "morning-digest", "namespace": "roundtable", "suspended": true, "suspendedBy": "annotation"}],
 "unchanged": [{"name": "weekly", "namespace": "roundtable", "suspended": true, "suspendedBy": "annotation"}],
 "failed": [{"name": "broken", "error": "..."}]}
```

`unchanged` are schedules already in the requested state. The MCP tools are
`suspend_table_schedules` and `resume_table_schedules`.

```bash
curl -s -X POST -H "$AUTH" -d '{"table":"knights"}' "$GW/v1/schedules/suspend"
```

### `POST /v1/schedules/{namespace}/{name}/run`

Fires the schedule once now through the trigger controller's normal firing
path (input rendering, concurrency policy, run labels), even while it is
suspended. The gateway sets `ai.roundtable.io/run-now` (a request ID) and
`ai.roundtable.io/run-now-caller` on the Trigger; the controller starts the
run, annotates it `ai.roundtable.io/caller` with the caller, records the
firing and removes the request. The gateway waits up to 10 seconds for that:

- **201** with `state: started` once the run exists;
- **202** with `state: pending` if the operator has not handled it yet. For a
  Chain target the run name is already known (it is derived from the firing's
  key). For a knight or mission target it is not: `run` is empty and there is
  no `Location`; the task ID or Mission name appears in the schedule's
  `status.recentFirings[].run` once the firing is recorded;
- **409** with `state: skipped` and the reason when the firing started
  nothing (for example `concurrency Forbid: run ... is active`), when an
  earlier run-now request is still waiting (`state: pending`), or when the
  target Chain does not exist.

The answer's `target` is `chain`, `knight` or `mission`, and `Location`
follows it:

- **Chain**: `run` is the Chain run; 201 and 202 carry
  `Location: /v1/schedules/{namespace}/{name}/runs/{run}`.
- **Knight**: `run` is the task ID and the task records the caller; a 201
  carries `Location: /v1/tasks/{taskId}`.
- **Mission**: `run` is the Mission's name; the Mission carries the caller as
  `ai.roundtable.io/caller`, and a 201 carries
  `Location: /v1/missions/{namespace}/{name}`. Like a knight target, `run` is
  empty and there is no `Location` while the request is pending.

```json
{"schedule": "morning-digest", "namespace": "roundtable", "run": "daily-digest-morning-digest-3f9a0c1b2d4e",
 "key": "manual:1795627159000-1a2b3c4d", "target": "chain", "state": "started", "caller": "token"}
```

```bash
curl -s -X POST -H "$AUTH" "$GW/v1/schedules/roundtable/morning-digest/suspend"
curl -s -X POST -H "$AUTH" "$GW/v1/schedules/roundtable/morning-digest/run"
curl -s -X POST -H "$AUTH" "$GW/v1/schedules/roundtable/morning-digest/resume"
```

## Schedule run history

A schedule is a Trigger with a `source.schedule` ([triggers.md](triggers.md)).
These routes read back what its firings did. A Trigger with another source is
a **404**. Served when `gateway.scheduleRuns.enabled` is true (the default).

### `GET /v1/schedules/{namespace}/{name}/runs?limit=`

`{"runs": [...]}`, newest first (`limit` defaults to 20, at most 200). Each
entry:

| Field | Meaning |
|---|---|
| `time` | When the firing was handled |
| `outcome` | `succeeded`, `failed`, `partiallySucceeded`, `lost`, `skipped`, `pending`, `running`, or `unknown` (the run is gone and its outcome was not recorded) |
| `reason` | Why a firing was `skipped` (from `status.recentFirings[].skipReason`) |
| `durationSeconds`, `costUSD` | Once the run finished, when known. For a knight target they come from the task record the knight reported into (while it is kept) |
| `turns`, `toolCalls` | Knight target only, while the task record is kept and the knight reported them; `toolCalls` is omitted, not 0, when not reported |
| `run` | What to fetch the result with: the Chain run's name (a knight target: the task ID). Empty for a skipped firing |
| `resultAvailable` | The result can still be fetched |
| `traceId` | The run's trace: the Chain's or Mission's `status.traceID`, or the task record's `traceId`. Omitted when unknown, invalid (not 32 hex) or the run is gone |
| `key` | The firing's dedupe key |

History is built from the Chains the Trigger started (found by their
`ai.roundtable.io/trigger` label and owner), so it goes back as far as results
are retained, not only the 10 entries of `status.recentFirings`. Those add
the skipped firings and the outcome of runs whose Chain is already deleted.

Each entry's `traceId` is the run's trace, omitted when it is unknown, not a
valid trace ID or the run is gone. It is the same ID the dashboard's "View
trace" link uses.

### `GET /v1/schedules/{namespace}/{name}/runs/{run}`

The run's result, by `target`:

- `chain`: the run's `phase`, `steps` and `output`, the run's final output
  (the one step no other step depends on; with several, read the steps).
- `knight`: `task` with the task's `status` (`pending`, `succeeded`,
  `failed`), `output`, `error`, `model`, `verdict`, `prs` and timestamps. It
  is readable while the gateway's task record is kept.
- `mission`: `mission`, the Mission as `GET /v1/missions/{namespace}/{name}`
  returns it (`phase`, `result`, chains, conditions). It is readable while
  the Mission exists.

A run that does not belong to the schedule (a knight task not among its
firings, a Mission another Trigger created), or whose record or Mission is
gone, is a **404**. `resultAvailable` in the run list is true for knight runs
whose task record still exists and for Missions that still exist.

```bash
GW=http://$FULLNAME-gateway.roundtable.svc
AUTH="Authorization: Bearer $(cat /var/run/secrets/roundtable/gateway/token)"
# The last failed firing of the morning digest and its output.
run=$(curl -s -H "$AUTH" "$GW/v1/schedules/roundtable/morning-digest/runs?limit=50" \
  | jq -r '[.runs[] | select(.outcome=="failed")][0].run')
curl -s -H "$AUTH" "$GW/v1/schedules/roundtable/morning-digest/runs/$run" | jq -r '.phase, .output'
```

Example `GET .../runs?limit=3`:

```json
{"runs": [
  {"run": "morning-digest-1795600000", "time": "2026-11-25T07:00:00Z", "outcome": "succeeded",
   "durationSeconds": 212.4, "costUSD": "0.41", "resultAvailable": true,
   "key": "2026-11-25T07:00:00Z"},
  {"time": "2026-11-24T07:00:00Z", "outcome": "skipped",
   "reason": "missed schedule time 2026-11-24T07:00:00Z by more than startingDeadlineSeconds", "resultAvailable": false},
  {"run": "morning-digest-1795427200", "time": "2026-11-23T07:00:00Z", "outcome": "failed",
   "durationSeconds": 31, "resultAvailable": true}
]}
```

### How long results stay readable

`gateway.scheduleRuns.resultRetention` (default `720h`, 30 days) maps to the
two places a schedule's results are stored today:

- **Chain runs** are Chains owned by the Trigger and deleted by the operator
  `ttlSecondsAfterFinished` after they finish (default 1 day). For a schedule
  source the operator keeps them at least `resultRetention`
  (`--schedule-result-retention`): the longer of the two applies.
  `ttlSecondsAfterFinished: 0` still keeps them forever.
- **Knight-target tasks** are records in the task KV bucket
  (`gateway.taskBucket`), whose TTL is `gateway.taskTTL`. The gateway and the
  operator create the bucket with the longer of `taskTTL` and
  `resultRetention`. **A bucket that already exists keeps its TTL**: raise it
  with `nats kv update roundtable_gateway_tasks --ttl 720h`, or delete the
  bucket (this drops its records) and restart the gateway.

Raising the retention raises the task bucket TTL for all tasks, not only
schedule runs.

## Webhooks

`POST /v1/hooks/{namespace}/{trigger}` is the ingress for
[webhook Triggers](triggers.md#webhook-source): systems that can only send
webhooks (Alertmanager, Flux notification-controller, Home Assistant) start
Round Table work through it. Served when `gateway.hooks.enabled` is true (the
default).

- **Its own auth, outside the API tokens.** The endpoint is not behind the
  bearer-token middleware. Each delivery must carry the Trigger's own
  credential: a bearer token or an HMAC-SHA256 signature of the raw body
  (`X-Hub-Signature-256` or `X-Signature`), from a Secret in the Trigger's
  namespace labelled `ai.roundtable.io/webhook-secret: "true"`. The gateway's
  API tokens are refused there (401), and a Trigger's credential opens no
  other endpoint. Tokens are compared as SHA-256 digests in constant time,
  signatures with `hmac.Equal`.
- **Publish, don't run.** The gateway publishes the body to
  `<subjectPrefix>.events.webhook.<namespace>.<trigger>` on the Trigger's
  RoundTable events stream (JetStream, expecting that stream) and answers
  **202** with the stream sequence. The operator's Trigger consumer starts the
  run. `X-GitHub-Delivery`, `Ce-Id` or `Idempotency-Key` become
  `Nats-Msg-Id` (scoped to the Trigger), so a retried delivery is stored once.
- **Limits.** Body at most `gateway.hooks.maxBodyBytes` (1 MiB, else 413).
  Each Trigger has a token bucket (`gateway.hooks.rate` per second, burst
  `gateway.hooks.burst`, else 429 with `Retry-After`), per replica.
- **Credential read errors.** A credential Secret that is missing, forbidden,
  unlabelled or too short gets the same 401 as a bad token. If the Secret
  cannot be read for another reason (API timeout, 5xx), the answer is a **503**
  with `Retry-After: 5` and a body that does not say whether the credential
  exists, so senders that retry 5xx keep the event.
- **No oracle.** An unknown Trigger, a Trigger without `source.webhook` and a
  namespace outside the gateway's namespace and `allowedNamespaces` all get
  the same 404. A 404 for an unknown name is cached per replica for 5 seconds,
  so repeated probes of that name read the API once; a Trigger created in that
  window is reachable once the entry expires. Found Triggers are read on every
  request.
- **NATS publishers bypass the credential.** The credential guards only this
  HTTP path. Anyone who can publish on
  `<subjectPrefix>.events.webhook.<namespace>.<trigger>` (or `<subjectPrefix>.events.>`)
  fires the Trigger without its credential. NATS users and permissions are not
  managed by the gateway (see issue #234); restrict who can publish to the
  events subjects if that matters for a Trigger.
- **In-cluster senders.** Allow each sender in `gateway.networkPolicy.from`,
  for example Alertmanager and Flux:

  ```yaml
  gateway:
    networkPolicy:
      from:
        - podSelector: {}
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: monitoring
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: flux-system
  ```

  A LAN sender such as Home Assistant needs an `ipBlock` peer and a way to
  reach the Service. Exposing the endpoint to the internet (GitHub webhooks)
  is not covered yet.

Sender configurations (Alertmanager, Flux `generic-hmac`, Home Assistant
`rest_command`) and the response codes are in
[triggers.md](triggers.md#webhook-source).

## Firing a schedule Trigger

`POST /v1/triggers/{namespace}/{name}/fire` fires a
[schedule Trigger](triggers.md). It is the one call the Trigger's CronJob pod
makes (`manager fire-trigger`, below). Served with the webhook endpoint
(`gateway.hooks.enabled`, the default).

- **Who may call.** The request goes through the normal bearer-token auth,
  and then only two callers are accepted: the Trigger's own ServiceAccount,
  `<namespace>/trigger-<name>` (a projected token with the gateway's
  audience, see [Calling from a workload](#calling-from-a-workload)), and a
  static gateway token. Any other ServiceAccount, including a
  `trigger-<name>` ServiceAccount in another namespace, gets **403**.
- **Body (optional).** `{"scheduledTime": "<RFC 3339>"}`, the CronJob's
  scheduled time. Without it the gateway uses now. Either way the time is
  truncated to the minute. An explicit old `scheduledTime` is accepted and
  published, but the trigger controller skips the firing when it is older than
  the schedule's `startingDeadlineSeconds` (`skipReason: missed schedule time
  … by more than startingDeadlineSeconds`). A slot whose run already exists is
  deduped first, whatever its age. Run-now is never skipped for the deadline.
- **Publish, don't run.** The gateway publishes a schedule event to
  `<subjectPrefix>.events.schedule.<namespace>.<name>` on the Trigger's
  RoundTable events stream (its `roundTableRef`, else its target Chain's, else its target knight's, else `default`, the same table the trigger controller reads) and answers **202**:

  ```json
  {"subject": "rt.events.schedule.roundtable.nightly", "stream": "rt_tasks_EVENTS",
   "seq": 42, "key": "2026-11-25T02:00:00Z", "scheduledTime": "2026-11-25T02:00:00Z"}
  ```

  The firing key is the scheduled time in UTC (RFC 3339), the key the
  in-process scheduler records. It travels in the `Roundtable-Event-Key` and
  `Roundtable-Scheduled-Time` headers; the body is
  `{"trigger", "scheduledTime", "key", "caller"}`. Two calls for the same slot
  carry the same key: within the stream's duplicate window JetStream stores
  one message (`"duplicate": true` on the second), and after it the trigger
  controller dedupes by the key. Concurrency and `status.recentFirings` stay
  in the trigger controller.
- **Errors.** 404 for an unknown Trigger or a namespace outside the gateway's
  namespace and `allowedNamespaces`; 422 for a Trigger without
  `source.schedule`; 400 for a bad body; 503 when the Trigger's RoundTable or
  NATS is unavailable.

### `manager fire-trigger`

The operator image carries the client as a subcommand, so the CronJob needs
no other image and no Kubernetes client:

```bash
/manager fire-trigger \
  --gateway http://$FULLNAME-gateway.roundtable.svc \
  --trigger roundtable/nightly \
  --token-file /var/run/secrets/roundtable/gateway/token \
  [--job-name "$JOB_NAME" | --scheduled-time 2026-11-25T02:00:00Z] [--timeout 30s]
```

`--job-name` takes the CronJob's Job name (the pod's
`batch.kubernetes.io/job-name` label), which the CronJob controller builds as
`<cronjob>-<unix minutes of the scheduled time>`, and sends that time as the
firing's scheduled time. A late start, a catch-up run or a retry then keeps
the slot's key. It wins over `--scheduled-time`; a name that does not end in
unix minutes exits 2.

It reads the token, makes the one call and exits 0 on a 202. A refusal, an
unreachable gateway or no answer within `--timeout` (default 30 s) exits 1
with the reason on stderr; bad arguments exit 2. `--gateway` defaults to
`$ROUNDTABLE_GATEWAY_URL`.

## MCP endpoint

The endpoint is `POST /mcp` (streamable HTTP, stateless). Its tools are:

| Tool | Input | Returns |
|------|-------|---------|
| `list_knights` | `namespace?`, `load?` | `{knights: [...]}` |
| `invoke_knight` | `knight`, `task`, `runId?`, `model?`, `timeoutSeconds?`, `namespace?` | Text content is the knight's output. Structured content is the task record. A failed task is returned as a tool error; a plan-quota refusal starts with `usage limited:`. A pending task returns a note to call `get_task`. |
| `get_task` | `taskId` | The task record (same text/error conventions) |
| `list_tasks` | `knight?`, `runId?`, `status?`, `limit?` | `{tasks: [...]}` |
| `list_dead_letters` | `knight?`, `reason?`, `state?`, `limit?` | `{deadLetters: [...]}` |
| `get_dead_letter` | `taskId`, `table?` | The dead-letter view including the original task payload (`task`). A missing entry is a tool error. |
| `retry_task` | `taskId`, `force?`, `table?`, `namespace?` | Same semantics as the retry endpoint. A refused retry is a tool error. Poll the returned `taskId` with `get_task`. |
| `start_mission` | The `POST /v1/missions` body | Text content is the result once done, else a note to call `get_mission`. Structured content is the mission projection. A failed or expired mission is a tool error. Unknown arguments fail the input schema. |
| `get_mission` | `name`, `namespace?`, `waitSeconds?` | The mission projection (same text/error conventions) |
| `list_missions` | `namespace?`, `phase?`, `limit?` | `{missions: [...]}` |
| `list_chains` | `namespace?` | `{chains: [...]}`: the opted-in Chains with their description |
| `run_chain` | `chain`, `input?` (a string or any JSON value), `waitSeconds?`, `namespace?` | Text content is the final step's output once done, else a note to call `get_run`. Structured content is the run record. A failed run is a tool error. |
| `get_run` | `run`, `namespace?`, `waitSeconds?` | The run record (same text/error conventions) |
| `cancel_run` | `run`, `namespace?`, `reason?` | Cancels an unfinished run (see `POST /v1/runs/{namespace}/{run}/cancel`) and returns its record; a finished run is an error |
| `rerun_run` | `run`, `namespace?`, `input?`, `waitSeconds?` | Re-runs a finished run with its input (or `input`) and returns the new run record (see `POST /v1/runs/{namespace}/{run}/rerun`); an unfinished run is an error |
| `list_runs` | `namespace?`, `chain?`, `phase?`, `source?`, `active?`, `issue?`, `limit?` | `{runs: [...]}`, the same filters and records as `GET /v1/runs` |
| `list_knight_snapshots` | `name`, `namespace?` | The `GET /v1/knights/{name}/snapshots` answer |
| `snapshot_knight` | `name`, `namespace?`, `waitSeconds?` | Requests a workspace snapshot (see `POST /v1/knights/{name}/snapshot`); listed only when `gateway.knightSnapshots.enabled` is true (default `false`); when off the REST route answers `403` |
| `list_schedules` | `namespace?`, `table?` | `{schedules: [...]}`: the same list as `GET /v1/schedules` |
| `get_schedule` | `name`, `namespace?` | The schedule projection |
| `schedule_next_runs` | `name`, `namespace?` or `cron`, `timeZone?`; `count?` | The same as `GET .../next` or `GET /v1/schedules/preview`: the next fire times (cron ticks only) |
| `suspend_schedule` | `name`, `namespace?` | The schedule's suspension state. Allowed on Git-managed schedules (it sets an annotation, not the spec) |
| `resume_schedule` | `name`, `namespace?` | The suspension state; a schedule suspended in its spec stays suspended and the message says so |
| `run_schedule` | `name`, `namespace?` | Fires the schedule once now (also while suspended, also Git-managed) and waits briefly for the operator: state `started`, `pending` or `skipped`. A skipped firing is a tool error |
| `list_schedule_runs` | `name`, `namespace?`, `limit?` | `{runs: [...]}`, newest first |
| `get_schedule_run` | `name`, `run`, `namespace?` | Text content is the run's output; a failed run is a tool error. Structured content is the run result |

The mission tools are only listed when `gateway.missions.enabled` is true, the
chain-run tools when `gateway.runs.enabled` is true. The schedule tools
(`list_schedules` to `run_schedule`) are listed when `gateway.schedules.enabled`
is true, and `list_schedule_runs` and `get_schedule_run` when the schedule run
history is enabled. Creating, replacing and deleting schedules is REST only.

`invoke_knight` and `run_chain` send MCP progress notifications while they
wait, when the client passes a progress token: one per tool call the knight
makes (`invoke_knight`) or per step change and tool call (`run_chain`), at most
one every 500 ms (step changes and task starts are always sent). The message
is text such as `tool Bash go test ./...` or `step fetch: Running`.

To register the endpoint with Claude Code:

```bash
claude mcp add --transport http roundtable \
  http://$FULLNAME-gateway.roundtable.svc/mcp \
  --header "Authorization: Bearer $TOKEN"
```

`/mcp` takes the same bearer tokens as the HTTP API: a static token or, from
a pod, its projected ServiceAccount token (see
[Calling from a workload](#calling-from-a-workload)).

Some MCP clients cut tool calls off after a fixed time. If yours does, pass a
`timeoutSeconds` (or `waitSeconds`) below that limit and poll `get_task` (or
`get_run` with `waitSeconds`).

## Limits and notes

- **The gateway cleans up recorded results.** Once a result is saved in the
  task record, the gateway deletes it from a WorkQueue results stream, which
  is what an ack would do there. Nobody else reads gateway results, so without
  this they would pile up forever: the operator creates streams without
  `max_age`. Results in Limits/Interest streams are left to the stream's own
  retention.
- **A gateway restart drops open waits, not tasks.** If a gateway restarts
  while a synchronous invoke is waiting, the caller's connection drops but the
  task keeps running. Find it with `list_tasks` or `GET /v1/tasks?knight=…`,
  then fetch it with `get_task`. The result is read from the results stream.
  The same holds for chain runs and missions: the run keeps going, and
  `GET /v1/runs/{namespace}/{run}?waitSeconds=` (or `get_run`) resumes the
  wait.
- **Follow-up turns depend on the knight runtime.** A runtime that keys
  sessions on `runId` continues the conversation. One that doesn't treats every
  task as fresh.
