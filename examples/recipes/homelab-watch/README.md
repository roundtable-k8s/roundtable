# Homelab watch

An alert from Alertmanager starts a two-step triage on a read-only knight:
step 1 gathers facts about the alert, step 2 writes a short finding with a
suggested fix. Nothing here is about software development; it shows the
platform reacting to events.

```
Alertmanager --POST--> gateway /v1/hooks/roundtable/homelab-alerts
   --> fleet-a.events.webhook.roundtable.homelab-alerts (events stream)
   --> Trigger homelab-alerts --> Chain cluster-triage
         gather (homelab-triage) --> finding (homelab-triage)
```

| File | What |
|---|---|
| [`knight.yaml`](knight.yaml) | The `homelab-triage` Knight, a ServiceAccount and a binding to the built-in `view` ClusterRole |
| [`chain.yaml`](chain.yaml) | The `cluster-triage` Chain: `gather` then `finding` |
| [`trigger.yaml`](trigger.yaml) | The token Secret and the webhook Trigger (input template, `dedupeKey`) |
| [`alertmanager-receiver.yaml`](alertmanager-receiver.yaml) | The Alertmanager route and receiver to merge into your config |

## Before you start

- The operator and gateway are installed with `gateway.hooks.enabled`
  (the default), and a RoundTable named `fleet-a` exists with
  `nats.createStreams: true` (see [`roundtable-fleet.yaml`](../../roundtable-fleet.yaml)).
  Rename `fleet-a` (and the `fleet-a.tasks.` subject) in the manifests if
  your table differs.
- Alertmanager's namespace is allowed by `gateway.networkPolicy.from`
  ([gateway docs](../../../docs/gateway.md#webhooks)).
- The `infrastructure`, `kubernetes` and `shared` skills are in your arsenal;
  change `spec.skills` if not.

## Install

```bash
kubectl apply -f knight.yaml -f chain.yaml
```

### Create the token

The token is a Secret in the Trigger's namespace, labelled
`ai.roundtable.io/webhook-secret: "true"` (the gateway refuses any Secret
without the label). Create it with a real value instead of applying the
placeholder in `trigger.yaml`:

```bash
kubectl -n roundtable create secret generic homelab-alerts-hook \
  --from-literal=token="$(openssl rand -hex 32)"
kubectl -n roundtable label secret homelab-alerts-hook ai.roundtable.io/webhook-secret=true
```

Then delete the Secret document from your copy of `trigger.yaml` (so the
placeholder does not overwrite the real token) and `kubectl apply -f trigger.yaml`.
Rotating the token is changing the Secret; the gateway reads it on every
delivery.

### Point Alertmanager at it

Mount the same token into Alertmanager's pod (copy the Secret into its
namespace first; Secrets do not cross namespaces) and merge
[`alertmanager-receiver.yaml`](alertmanager-receiver.yaml) into its config.
The receiver's essential part:

```yaml
receivers:
  - name: roundtable
    webhook_configs:
      - url: http://<fullname>-gateway.roundtable.svc/v1/hooks/roundtable/homelab-alerts
        send_resolved: true
        http_config:
          authorization:
            type: Bearer
            credentials_file: /etc/alertmanager/secrets/homelab-alerts-hook/token
```

Group by `alertname` and `namespace` (as the snippet does) so the Trigger's
input can show both.

## Test it with curl

From a pod inside the cluster (the gateway is not reachable from outside; that
comes with M6):

```bash
TOKEN=$(kubectl -n roundtable get secret homelab-alerts-hook -o jsonpath='{.data.token}' | base64 -d)
kubectl -n roundtable run curl --rm -it --restart=Never --image=curlimages/curl --env TOKEN="$TOKEN" -- \
  curl -sS -X POST http://<fullname>-gateway.roundtable.svc/v1/hooks/roundtable/homelab-alerts \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -d '{"status":"firing","groupKey":"g1","commonLabels":{"alertname":"KubePodCrashLooping","severity":"warning","namespace":"media"},"commonAnnotations":{"summary":"Pod media/plex is crash looping"},"alerts":[{"status":"firing","fingerprint":"abc123","labels":{"alertname":"KubePodCrashLooping","pod":"plex-0"}}]}'
# {"subject":"fleet-a.events.webhook.roundtable.homelab-alerts","stream":"fleet_a_tasks_EVENTS","seq":42}
```

The gateway answers 202 with the stream position (`<fullname>` is the Helm
release's full name, for example `roundtable-roundtable-operator`). Then:

```bash
kubectl -n roundtable get chains -l ai.roundtable.io/trigger=homelab-alerts
kubectl -n roundtable get trigger homelab-alerts -o yaml   # status.recentFirings
```

Sending the same body again starts nothing: the dedupe key
(`<first alert's fingerprint>/<status>`) matches the existing run. A flapping
alert keeps its fingerprint, so repeated notifications while it fires are one
run, and its resolution is one more. A different status or fingerprint starts
a new run. A run is remembered until it is deleted
(`ttlSecondsAfterFinished`, one day by default), so an alert that fires again
after that starts a new run.

If a field the input template needs is missing from the body, the delivery is
recorded in `status.recentFirings` with the reason and starts nothing.

## What the knight may and may not do

May:

- Read the cluster through the `view` ClusterRole: `get`, `list` and `watch`
  on most namespaced resources, so it can run `kubectl get`, `describe`,
  `logs` and read events.
- Talk to NATS, its model provider and the hosts in `spec.egress.allowedHosts`
  (tool downloads at start).
- Write a finding as the chain's output.

May not:

- Change anything in the cluster: `view` has no write verbs, and no `exec`.
- Read Secrets: `view` excludes them. Nodes and cluster-scoped objects are
  limited to what `view` grants; widen with your own ClusterRole if you want it.
- Reach any other host: egress is default-deny with the allowlist above.
- Apply its own suggested fix. A human does that.

Alert labels and annotations come from outside the knight, so the knight's
prompt tells it to treat them as data. Keep the role read-only: that, not the
prompt, is what limits the damage.

## Variations

- **Flux failures instead:** use an `hmacSecretRef` Trigger, see
  [Flux notification-controller](../../../docs/triggers.md#senders).
- **Quieter:** set `send_resolved: false`, or route only `critical`.
