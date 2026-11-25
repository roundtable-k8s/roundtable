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
    objects it writes are Missions and run copies of opted-in Chains, and
    only in the namespaces callers may target (see [Missions](#missions) and
    [Chain runs](#chain-runs)). For [webhooks](#webhooks) it can `get`
    Triggers and `get` (not list) Secrets in those namespaces, and uses only
    Secrets labelled `ai.roundtable.io/webhook-secret: "true"`.
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

## HTTP API

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
| `planner` | Optional. Makes this a meta-mission. `knight` is an existing Knight; omit it to use the knight labelled `ai.roundtable.io/role=planner`. `context` (max 64 KiB), `maxChains` (1-20), `maxKnights` (1-50), `allowSkillGeneration` (see above). |
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
  `create` and `delete` on Chains, granted per namespace like Missions (a
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
