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
    read-only `get`/`list` on Knights and RoundTables.
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
(`gateway.auth.existingSecret`, key `gateway.auth.secretKey`):

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

### `GET /v1/knights?namespace=`

Lists knights with their name, domain, model, agent runtime, phase, readiness
and whether they are suspended.

## MCP endpoint

The endpoint is `POST /mcp` (streamable HTTP, stateless). Its tools are:

| Tool | Input | Returns |
|------|-------|---------|
| `list_knights` | `namespace?` | `{knights: [...]}` |
| `invoke_knight` | `knight`, `task`, `runId?`, `model?`, `timeoutSeconds?`, `namespace?` | Text content is the knight's output. Structured content is the task record. A failed task is returned as a tool error. A pending task returns a note to call `get_task`. |
| `get_task` | `taskId` | The task record (same text/error conventions) |
| `list_tasks` | `knight?`, `runId?`, `status?`, `limit?` | `{tasks: [...]}` |

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
