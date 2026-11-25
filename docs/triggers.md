# Triggers

A `Trigger` starts a run of a Chain when something happens. A run is a copy of
the target Chain whose `spec.input` is rendered from the event (the same kind
of copy issue pickup and the gateway make, through `internal/chainrun`). The
design decisions are in #400.

Three sources exist today:

| Source | Fires on | Default dedupe key |
|---|---|---|
| `nats` | each message on a subject under the table's `<subjectPrefix>.events.` | `Nats-Msg-Id` header, else the stream sequence |
| `webhook` | each authenticated `POST` to the gateway's `/v1/hooks/<namespace>/<name>` | the sender's delivery id (see [Webhook source](#webhook-source)), else the stream sequence |
| `schedule` | each cron time | the schedule time (RFC 3339, UTC) |

`github` is reserved: the API server rejects it until it is implemented.
Exactly one source is set.

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
system of record and everything under [Delivery](#delivery-nats-and-webhook-sources)
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

## Example: schedule

[`examples/trigger-schedule.yaml`](../examples/trigger-schedule.yaml) runs a
Chain every weekday at 07:00 New York time with input computed from the
schedule time. For a fixed input, `Chain.spec.schedule` stays the short form.

## Spec

| Field | Meaning |
|---|---|
| `source.nats.subject` | Subject filter (wildcards allowed). Must sit under `<subjectPrefix>.events.` of `roundTableRef`. |
| `source.webhook.auth` | `bearerTokenSecretRef` or `hmacSecretRef`: a key of a labelled Secret in the Trigger's namespace. |
| `source.webhook.eventType` | Fire only on deliveries with this `Ce-Type` (else `X-GitHub-Event`). |
| `source.schedule.cron` / `timeZone` / `startingDeadlineSeconds` | Standard cron, IANA zone (default UTC), and how late a missed time may still fire. |
| `target.chainRef` | The Chain (same namespace) each firing copies. |
| `input` | Go template rendered into the run's `spec.input`. Empty keeps the target's own input. |
| `dedupeKey` | Go template whose result identifies the event. Defaults: see the table above. |
| `concurrency.policy` | `Allow` (default), `Forbid` or `Replace`. |
| `concurrency.maxActive` | Under `Allow`, the most active runs; a firing at the cap is skipped. 0 means no cap. |
| `suspend` | Stop firing. A NATS or webhook source's consumer is deleted, so events published while suspended don't fire on resume (the gateway still accepts and stores them). |
| `roundTableRef` | The RoundTable whose events stream a NATS or webhook source reads. Required for `nats` and `webhook`. |
| `ttlSecondsAfterFinished` | Delete a finished run this long after it completed (default 86400; 0 keeps runs). |

### Templates

`input` and `dedupeKey` see:

- `.Event.Subject`: the NATS subject (empty for schedule)
- `.Event.Data`: the payload parsed as JSON, or the raw string when it is not JSON
- `.Event.Headers`: header name to value (multiple values joined with `, `)
- `.Event.Time`: when the message was stored in the stream, or the schedule time (in the schedule's zone)
- `.Trigger.Name`, `.Trigger.Namespace`

A missing map key is an error (`missingkey=error`), so `{{ .Event.Data.service }}`
fails on an event without `service` instead of rendering `<no value>`. `toJson`
encodes a value as JSON, for embedding strings in a JSON input safely.

A template that does not parse makes the Trigger not Ready (`InvalidSpec`) and
nothing is read until it is fixed. An event the template cannot render is
recorded in `status.recentFirings` with the reason and terminated: it is never
redelivered.

### Dedupe

The run's name is `<chain>-<trigger>-<hash of dedupe key>`. An event whose key
matches a run that still exists starts nothing, so a key is remembered until
its run is deleted (`ttlSecondsAfterFinished`). The same name makes redelivery
safe: if the operator restarts after creating a run but before acking the
message, the redelivered message finds the run and is skipped.

JetStream also drops a repeated `Nats-Msg-Id` within the stream's duplicate
window (2 minutes) before the Trigger sees it.

## Delivery (NATS and webhook sources)

`RoundTable.spec.nats.createStreams: true` creates an events stream next to the
tasks and results streams:

- name: `spec.nats.eventsStream`, default `<tasksStream>_EVENTS`
- subjects: `<subjectPrefix>.events.>`
- Limits retention, max age `spec.nats.eventsMaxAge` (default 168h)

Each NATS- or webhook-source Trigger gets one durable pull consumer on it,
`trigger-<namespace>_<name>`, filtered to `source.nats.subject` (webhook:
`<subjectPrefix>.events.webhook.<namespace>.<name>`). A new consumer
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

`Ready` reasons: `Listening`, `Suspended`, `InvalidSpec` (template, subject or
cron), `TargetNotFound`, `RoundTableNotFound`, `ConsumerError` (for example no
events stream: set `createStreams` or create the stream yourself).
