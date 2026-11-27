# Dashboard

The dashboard is a web UI that shows the whole fleet in one place: knights,
running tasks, Chains, Missions, schedules, queues, failures and cost. It
exists so you can watch everything the fleet does (see [principles](principles.md)).
It ships in the chart and is on by default (`dashboard.enabled: true`).

## Pages

| Route | What it answers |
|-------|-----------------|
| `/` | Command center: how is the fleet doing right now? |
| `/fleet` | Which knights exist, and are they ready? Agent cards and isolation status are served from here. |
| `/running` | What is running at this moment? |
| `/missions` | Which Missions ran, and how did they end? `/missions/:name` shows one. |
| `/roundtables` | Which RoundTables (NATS subject prefixes) exist? |
| `/chat` | Talk to a knight. Sends through the gateway. |
| `/quests` | Which tasks ran, and which knight answered each? |
| `/queues` | What is waiting in each knight's queue? |
| `/failed` | Which knight failed this task? Failed tasks can be retried. |
| `/flow` | A live view of task traffic. |
| `/chronicles` | What did the fleet write up, such as briefings and reports from the vault? |
| `/chains` | Which Chain runs happened, step by step, with their cost? |
| `/triggers`, `/triggers/:namespace/:name` | Which Triggers exist, and when did each last fire? |
| `/schedules`, `/schedules/:namespace/:name`, `/schedules/new`, `/schedules/:namespace/:name/edit` | Which scheduled work is configured? List, detail, create and edit. See [scheduled work](scheduled-work.md). |
| `/costs` | What did last night's schedule cost? |
| `/sessions` | What did a knight's sessions contain? |
| `/architecture` | How are the parts of the fleet connected? |

## What it can change

The dashboard reads Kubernetes with its own ServiceAccount and Role. The Role
is read-only on pods, pod logs, events, Deployments, HorizontalPodAutoscalers,
ScaledObjects, Knights, Chains, RoundTables, Triggers and Missions
(`charts/roundtable-operator/templates/dashboard.yaml`). It never writes to
the Kubernetes API itself.

It reads results and queues from NATS (`dashboard.env.NATS_URL`).

Everything it changes goes through the [invoke gateway](gateway.md), using the
`GATEWAY_URL` and `GATEWAY_TOKEN_FILE` the chart sets when `gateway.enabled`
is true:

- chat messages and task launches, including Mission launch
- dead-letter retry on `/failed`
- schedule create, replace, delete, suspend and resume

The gateway token is the gateway's own token Secret by default. Set
`dashboard.gateway.tokenSecret` / `dashboard.gateway.tokenKey` for a separate
one, or `dashboard.gateway.serviceAccountToken: true` to send a projected
ServiceAccount token instead (this needs `gateway.serviceAccountAuth.enabled`).
Without the gateway the pages that read still work, and the pages that write
do not.

## One fleet

A dashboard shows one fleet: the NATS subject prefix `dashboard.env.FLEET_PREFIX`
(default `fleet-a`) and the JetStream results stream `dashboard.env.FLEET_STREAM`
(default `fleet_a_results`). `dashboard.env.NAMESPACE` is the namespace it watches.
To see another fleet, run another dashboard with other values.

## The vault volume

`dashboard.vault.enabled` defaults to `true`. The dashboard mounts the PVC
named by `dashboard.vault.claimName` (default `roundtable-vault`) at `/vault`
to read briefings and reports. **If that PVC does not exist the dashboard pod
stays Pending.** Set `dashboard.vault.enabled=false` if you have no vault; the
quickstart does (`values-minimal.yaml`).

## Reaching it

- **Port-forward** (the quickstart way):
  `kubectl -n roundtable port-forward svc/roundtable-operator-dashboard 8080:80`.
  The Service is a ClusterIP named `<fullname>-dashboard`.
- **Ingress**: set `dashboard.ingress.enabled`, `dashboard.ingress.host`,
  and optionally `dashboard.ingress.className` and `dashboard.ingress.annotations`.
- **NetworkPolicy**: when the chart's network policies are on, only the
  metrics peers can reach the dashboard's HTTP port. Add other peers, such as
  your ingress controller's namespace, to `networkPolicy.dashboardFrom`.

## Who can use it

The dashboard can launch tasks, retry dead letters and edit schedules. Treat
anyone who can reach it as able to do those things.

**It has no login unless `DASHBOARD_API_KEY` is set.** The dashboard API reads
that environment variable at start-up:

- **Unset or empty**: authentication is off and every `/api` call is open.
  This is the chart's default.
- **Set**: every `/api` call must carry `Authorization: Bearer <key>`. The
  exceptions are `GET /api/health` and the static UI files, plus
  `POST /api/auth/login`, which checks a key sent as `{"apiKey": "..."}`.
  The WebSocket at `/api/ws` also accepts `?api_key=` in the URL, because a
  browser cannot set headers on it; URLs end up in proxy logs, so prefer
  a proxy that authenticates before the request arrives.

**The chart cannot set `DASHBOARD_API_KEY` from a Secret today.** There is no
chart value for it, and `dashboard.env` renders plain `value:` entries only.
Do not put the key in `dashboard.env`: it would sit in plain text in your
values file, the Helm release Secret and the Deployment spec.

The supported posture is therefore:

1. Keep the dashboard cluster-internal and reach it by port-forward or from
   inside the cluster; **or**
2. Put an authenticating proxy (for example oauth2-proxy) in front of the
   Ingress, and restrict who may reach the pod with `networkPolicy.dashboardFrom`.

Do not expose the dashboard on an Ingress with no authentication in front of it.
