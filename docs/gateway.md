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
    objects it writes are Missions, and only in the namespaces callers may
    target (see [Missions](#missions)).
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

Every endpoint except `/healthz` and `/readyz` requires
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
  is still a 200; read `status` and `error`.
- **202**: still running when the wait ended. The `Location` header is
  `/v1/tasks/{taskId}`.
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

Empty output counts as a failure, as it does in the task contract.

### `GET /v1/tasks/{taskId}`

Returns the task record. If the task is still `pending`, the gateway first
checks the results stream for the result and records it if found.

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

## MCP endpoint

The endpoint is `POST /mcp` (streamable HTTP, stateless). Its tools are:

| Tool | Input | Returns |
|------|-------|---------|
| `list_knights` | `namespace?` | `{knights: [...]}` |
| `invoke_knight` | `knight`, `task`, `runId?`, `model?`, `timeoutSeconds?`, `namespace?` | Text content is the knight's output. Structured content is the task record. A failed task is returned as a tool error. A pending task returns a note to call `get_task`. |
| `get_task` | `taskId` | The task record (same text/error conventions) |
| `list_tasks` | `knight?`, `runId?`, `status?`, `limit?` | `{tasks: [...]}` |
| `list_dead_letters` | `knight?`, `reason?`, `state?`, `limit?` | `{deadLetters: [...]}` |
| `retry_task` | `taskId`, `force?`, `table?`, `namespace?` | Same semantics as the retry endpoint. A refused retry is a tool error. Poll the returned `taskId` with `get_task`. |
| `start_mission` | The `POST /v1/missions` body | Text content is the result once done, else a note to call `get_mission`. Structured content is the mission projection. A failed or expired mission is a tool error. Unknown arguments fail the input schema. |
| `get_mission` | `name`, `namespace?`, `waitSeconds?` | The mission projection (same text/error conventions) |
| `list_missions` | `namespace?`, `phase?`, `limit?` | `{missions: [...]}` |

The mission tools are only listed when `gateway.missions.enabled` is true.

To register the endpoint with Claude Code:

```bash
claude mcp add --transport http roundtable \
  http://roundtable-roundtable-operator-gateway.roundtable.svc/mcp \
  --header "Authorization: Bearer $TOKEN"
```

Some MCP clients cut tool calls off after a fixed time. If yours does, pass a
`timeoutSeconds` below that limit and poll `get_task`.

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
- **Follow-up turns depend on the knight runtime.** A runtime that keys
  sessions on `runId` continues the conversation. One that doesn't treats every
  task as fresh.
