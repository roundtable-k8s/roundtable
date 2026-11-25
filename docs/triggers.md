# Triggers

A `Trigger` starts a run of a Chain when something happens. A run is a copy of
the target Chain whose `spec.input` is rendered from the event (the same kind
of copy issue pickup and the gateway make, through `internal/chainrun`). The
design decisions are in #400.

Two sources exist today:

| Source | Fires on | Default dedupe key |
|---|---|---|
| `nats` | each message on a subject under the table's `<subjectPrefix>.events.` | `Nats-Msg-Id` header, else the stream sequence |
| `schedule` | each cron time | the schedule time (RFC 3339, UTC) |

`webhook` (#176) and `github` are reserved: the API server rejects them until
they are implemented. Exactly one source is set.

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

## Example: schedule

[`examples/trigger-schedule.yaml`](../examples/trigger-schedule.yaml) runs a
Chain every weekday at 07:00 New York time with input computed from the
schedule time. For a fixed input, `Chain.spec.schedule` stays the short form.

## Spec

| Field | Meaning |
|---|---|
| `source.nats.subject` | Subject filter (wildcards allowed). Must sit under `<subjectPrefix>.events.` of `roundTableRef`. |
| `source.schedule.cron` / `timeZone` / `startingDeadlineSeconds` | Standard cron, IANA zone (default UTC), and how late a missed time may still fire. |
| `target.chainRef` | The Chain (same namespace) each firing copies. |
| `input` | Go template rendered into the run's `spec.input`. Empty keeps the target's own input. |
| `dedupeKey` | Go template whose result identifies the event. Defaults: see the table above. |
| `concurrency.policy` | `Allow` (default), `Forbid` or `Replace`. |
| `concurrency.maxActive` | Under `Allow`, the most active runs; a firing at the cap is skipped. 0 means no cap. |
| `suspend` | Stop firing. A NATS source's consumer is deleted, so events published while suspended don't fire on resume. |
| `roundTableRef` | The RoundTable whose events stream a NATS source reads. Required for `nats`. |
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

## Delivery (NATS source)

`RoundTable.spec.nats.createStreams: true` creates an events stream next to the
tasks and results streams:

- name: `spec.nats.eventsStream`, default `<tasksStream>_EVENTS`
- subjects: `<subjectPrefix>.events.>`
- Limits retention, max age `spec.nats.eventsMaxAge` (default 168h)

Each NATS-source Trigger gets one durable pull consumer on it,
`trigger-<namespace>_<name>`, filtered to `source.nats.subject`. A new consumer
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
