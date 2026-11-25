# Invoke gateway

The invoke gateway is the synchronous front door to the knights. Callers send a
task and get the answer back in the same HTTP request. If the answer takes too
long, they get a task ID to poll. The same operations are also served as an MCP
server (streamable HTTP), so OpenClaw, Claude and other MCP clients can register
it the way they register kagent's endpoint.

The gateway speaks the [knight task contract](knight-task-contract.md). It
publishes a `TaskPayload` on `<prefix>.tasks.<domain>.<knight>` and reads the
result from `<prefix>.results.<taskId>`. Knights need no changes.

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

A success is cached in memory until 30 seconds before the token expires and
at most 5 minutes, so a deleted pod's or ServiceAccount's token stops working
within that time. A refusal is cached for 5 seconds. The cache is bounded
(4096 tokens).

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
                  value: http://roundtable-roundtable-operator-gateway.roundtable.svc
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

**Idempotency.** Send an `Idempotency-Key` header (1 to 255 printable ASCII
characters, else **400**) to make the call safe to retry after a dropped
connection. The first call publishes the task and stores the key (scoped to
namespace and knight) next to the task record; a repeat publishes nothing and
returns the original task (**200** or **202** as the current state says) with
the response header `Idempotent-Replay: true`. A repeat with a different
`task`, `model` or `runId` is a **422**. The key lives as long as the task
record (`gateway.taskTTL`); a call that fails before publishing releases it.

- **400**: bad request, bad model, or a namespace that isn't allowed.
- **404**: unknown knight.
- **429**: `maxInFlight` waits are already open.
- **503**: the knight is suspended, or NATS is unavailable.

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

Empty output counts as a failure, as it does in the task contract.

### `GET /v1/tasks/{taskId}`

Returns the task record. If the task is still `pending`, the gateway first
checks the results stream for the result and records it if found.

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
| `404` | No such entry. |
| `409` | The entry isn't `open` (body has the current `state`), or it belongs to a chain: rerun the chain or let its `retryPolicy` handle it. `force` retries the chain step on its own; the chain doesn't continue. |
| `422` | The entry has no original task payload. |

### `GET /v1/knights?namespace=`

Lists knights with their name, domain, model, agent runtime, phase, readiness
and whether they are suspended.

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
ClusterRole if that list holds `"*"`).

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
| `waitSeconds` | Optional. Wait this long for the mission to finish (default 0, capped at `gateway.maxTimeout`). Waits count against `maxInFlight`. |
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
  "createdAt": "…", "startedAt": "…", "completedAt": null, "expiresAt": "…"
}
```

`planning.rawOutput` holds only the first 4 KiB of the planner's output.

### `GET /v1/missions?namespace=&phase=&limit=`

Lists missions in a namespace, newest first, in the same projection but
without chain steps. `phase` matches case-insensitively. The default limit
is 20 and the maximum is 200.

### `DELETE /v1/missions/{namespace}/{name}`

Deletes a mission the gateway created (its chains and ephemeral knights go
with it). Any other mission is a **403**.

### Example: launch a research mission and poll it

```bash
GW=http://roundtable-roundtable-operator-gateway.roundtable.svc
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
| `waitSeconds` | Optional. Wait this long for the run to finish (default 0, capped at `gateway.maxTimeout`). Waits count against `maxInFlight`. |

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

```json
{
  "run": "blog-drafter-gw-1a2b3c4d",
  "chain": "blog-drafter",
  "namespace": "roundtable",
  "runId": "2b9c…",
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
steps have no dependents, `output` is omitted; read `steps`.

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
- A Chain the gateway didn't start is a **404**.
- A task a knight already picked up is not recalled: the knight finishes or
  times out and its result is ignored.

This needs `patch` on Chains, which the chart grants only when
`gateway.runs.enabled` is true.

### `GET /v1/runs/{namespace}/{run}?waitSeconds=`

Returns a run the gateway started (any other Chain is a **404**), with the
same 200/202 convention. `waitSeconds` long-polls (capped at
`gateway.maxTimeout`), so a caller whose connection dropped resumes waiting
where it left off.

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

### `GET /v1/runs?namespace=&chain=&phase=&limit=`

`{"runs": [...]}`: gateway-started runs, newest first, without `steps` and
`output` (get one run for those). `limit` defaults to 20, at most 200.

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
                    http://roundtable-roundtable-operator-gateway.roundtable.svc/v1/chains/blog-drafter/runs \
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
GW=http://roundtable-roundtable-operator-gateway.roundtable.svc
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
- `nextRunAt` and `lastRunAt` are `status.nextScheduledAt` and
  `status.lastFiredAt`. `lastResult` is the newest of `status.recentFirings`:
  `outcome` is `started` (with the `run`), `skipped` (with the
  `skipReason`) or, for knight targets once the task has ended, `succeeded`,
  `failed` or `lost`.

### `GET /v1/schedules/{namespace}/{name}`

One schedule, as above. **404** if it does not exist or is not a schedule.

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

Deletes the schedule; runs it already started are left to their own TTL.
404 if there is no such schedule, 409 if it is Git-managed.

### Example: a weekday digest

```bash
GW=http://roundtable-roundtable-operator-gateway.roundtable.svc
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
| `durationSeconds`, `costUSD` | Once the run finished, when known |
| `run` | What to fetch the result with: the Chain run's name (a knight target: the task ID). Empty for a skipped firing |
| `resultAvailable` | The result can still be fetched |
| `key` | The firing's dedupe key |

History is built from the Chains the Trigger started (found by their
`ai.roundtable.io/trigger` label and owner), so it goes back as far as results
are retained, not only the 10 entries of `status.recentFirings`. Those add
the skipped firings and the outcome of runs whose Chain is already deleted.

### `GET /v1/schedules/{namespace}/{name}/runs/{run}`

The run's result: its `phase`, `steps` and, for a chain target, `output`, the
run's final output (the one step no other step depends on; with several,
read the steps). A run that does not belong to the schedule, or that was
deleted, is a **404**. A knight target (task result) and a mission target
(report) are not served yet and answer **422**; they are the extension points
in `ScheduleRunResult` in `internal/gateway/schedule_runs.go`.

```bash
GW=http://roundtable-operator-gateway.roundtable.svc:8080
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
- **No oracle.** An unknown Trigger, a Trigger without `source.webhook` and a
  namespace outside the gateway's namespace and `allowedNamespaces` all get
  the same 404.
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
  truncated to the minute.
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
  --gateway http://roundtable-gateway.roundtable.svc:8080 \
  --trigger roundtable/nightly \
  --token-file /var/run/secrets/roundtable/trigger/token \
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
| `list_knights` | `namespace?` | `{knights: [...]}` |
| `invoke_knight` | `knight`, `task`, `runId?`, `model?`, `timeoutSeconds?`, `namespace?` | Text content is the knight's output. Structured content is the task record. A failed task is returned as a tool error; a plan-quota refusal starts with `usage limited:`. A pending task returns a note to call `get_task`. |
| `get_task` | `taskId` | The task record (same text/error conventions) |
| `list_tasks` | `knight?`, `runId?`, `status?`, `limit?` | `{tasks: [...]}` |
| `list_dead_letters` | `knight?`, `reason?`, `state?`, `limit?` | `{deadLetters: [...]}` |
| `retry_task` | `taskId`, `force?`, `table?`, `namespace?` | Same semantics as the retry endpoint. A refused retry is a tool error. Poll the returned `taskId` with `get_task`. |
| `start_mission` | The `POST /v1/missions` body | Text content is the result once done, else a note to call `get_mission`. Structured content is the mission projection. A failed or expired mission is a tool error. Unknown arguments fail the input schema. |
| `get_mission` | `name`, `namespace?`, `waitSeconds?` | The mission projection (same text/error conventions) |
| `list_missions` | `namespace?`, `phase?`, `limit?` | `{missions: [...]}` |
| `list_chains` | `namespace?` | `{chains: [...]}`: the opted-in Chains with their description |
| `run_chain` | `chain`, `input?` (a string or any JSON value), `waitSeconds?`, `namespace?` | Text content is the final step's output once done, else a note to call `get_run`. Structured content is the run record. A failed run is a tool error. |
| `get_run` | `run`, `namespace?`, `waitSeconds?` | The run record (same text/error conventions) |
| `cancel_run` | `run`, `namespace?`, `reason?` | Cancels an unfinished run (see `POST /v1/runs/{namespace}/{run}/cancel`) and returns its record; a finished run is an error |
| `list_runs` | `namespace?`, `chain?`, `phase?`, `limit?` | `{runs: [...]}` |

The mission tools are only listed when `gateway.missions.enabled` is true, the
chain-run tools when `gateway.runs.enabled` is true.

`invoke_knight` and `run_chain` send MCP progress notifications while they
wait, when the client passes a progress token: one per tool call the knight
makes (`invoke_knight`) or per step change and tool call (`run_chain`), at most
one every 500 ms (step changes and task starts are always sent). The message
is text such as `tool Bash go test ./...` or `step fetch: Running`.

To register the endpoint with Claude Code:

```bash
claude mcp add --transport http roundtable \
  http://roundtable-roundtable-operator-gateway.roundtable.svc/mcp \
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
