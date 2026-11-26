# Getting Started

Go from nothing to a finished two-step Chain, visible in the dashboard, on a local
[kind](https://kind.sigs.k8s.io/) cluster in about ten minutes.

## Prerequisites

- [kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation) (with Docker or another kind provider), [Helm](https://helm.sh/docs/intro/install/) 3.12+ and `kubectl`
- A clone of this repository (the script installs the chart from `charts/roundtable-operator` and the manifests live in `examples/quickstart/`)
- One model-provider API key. The examples use Anthropic (`ANTHROPIC_API_KEY`)

```bash
git clone https://github.com/roundtable-k8s/roundtable.git
cd roundtable
```

## 1. Create the cluster and install Round Table

```bash
hack/quickstart-kind.sh
```

The script creates a kind cluster named `roundtable`, installs NATS with JetStream
(pinned `nats` chart version) and the operator chart into the `roundtable` namespace,
then waits for the operator and dashboard Deployments to be Available. Image tags are
the chart's pinned defaults. Because kind has no gVisor or Cilium, it installs the
chart with `isolation.runtimeClassName=""` and `isolation.egress.mode=off`
(see [runtime isolation](runtime-isolation.md) and [egress](egress.md)); do not copy
those settings to a real cluster.

To install a released chart instead of the local one, pass `--chart-version X.Y.Z`
(the OCI chart `oci://ghcr.io/roundtable-k8s/charts/roundtable-operator`). The script
also takes `--cluster` and `--namespace`.

This is the smallest install that works today; a minimal "Core" tier is tracked in
[#182](https://github.com/roundtable-k8s/roundtable/issues/182).

## 2. Store your API key

```bash
kubectl -n roundtable create secret generic llm-api-keys \
  --from-literal=ANTHROPIC_API_KEY=sk-ant-...
```

The quickstart Knight loads this Secret through `spec.envFrom`.

## 3. Apply a RoundTable, a Knight and a Chain

```bash
kubectl apply -f examples/quickstart/roundtable.yaml
kubectl apply -f examples/quickstart/knight.yaml
kubectl -n roundtable get knights -w   # wait for READY true, then Ctrl-C
```

- [`roundtable.yaml`](../examples/quickstart/roundtable.yaml): one RoundTable pointing at the in-cluster NATS (`nats://nats.roundtable.svc:4222`); `createStreams: true` makes the operator create its JetStream streams.
- [`knight.yaml`](../examples/quickstart/knight.yaml): one Knight, `arthur`, consuming `quickstart.tasks.general.>`.
- [`chain.yaml`](../examples/quickstart/chain.yaml): two steps, `research` then `summarize`, the second using `{{ .Steps.research.Output }}`.

```bash
kubectl apply -f examples/quickstart/chain.yaml
```

## 4. Run the Chain

Applying a Chain does not start it. Annotate it with `ai.roundtable.io/run` (any new
value starts a new run):

```bash
kubectl -n roundtable annotate chain hello-chain \
  ai.roundtable.io/run="$(date +%s)" --overwrite
kubectl -n roundtable get chain hello-chain -w   # Running, then Succeeded
```

## 5. See it in the dashboard

```bash
kubectl -n roundtable port-forward svc/roundtable-operator-dashboard 8080:80
```

Open <http://localhost:8080> and open the `hello-chain` run to see both steps and their
output. The same output is in `kubectl -n roundtable get chain hello-chain -o yaml`
under `status.stepStatuses`.

## 6. Ask the knight directly

The invoke gateway (see [gateway.md](gateway.md)) sends a task straight to a Knight and
returns the answer in the same HTTP request. It serves the release namespace by default,
so no extra values are needed. Open a port-forward to the gateway in a second terminal:

```bash
kubectl -n roundtable port-forward svc/roundtable-operator-gateway 8081:80
```

Then, in the first terminal, read the token the chart generated and ask `arthur`:

```bash
TOKEN=$(kubectl -n roundtable get secret roundtable-operator-gateway-token \
  -o jsonpath='{.data.token}' | base64 -d)
curl -s -X POST localhost:8081/v1/knights/arthur/invoke \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"task":"In one sentence, what is a Kubernetes operator?","timeoutSeconds":120}'
```

A `200` response is the finished task record. The answer is in `output` and the
task ID in `taskId` (`status` is `succeeded`, or `failed` with an `error`):

```json
{"taskId": "invoke-arthur-…", "knight": "arthur", "status": "succeeded", "output": "…"}
```

A `202` means the knight is still running when the wait ended. Poll the task with the
same token: `curl -s -H "Authorization: Bearer $TOKEN" localhost:8081/v1/tasks/<taskId>`.

## Clean up

```bash
hack/quickstart-kind.sh --delete
```

## Alternative: your own NATS

On an existing cluster, install the chart yourself
(`helm install roundtable-operator oci://ghcr.io/roundtable-k8s/charts/roundtable-operator -n roundtable --create-namespace`)
and point every NATS URL at your NATS server: `nats.url` in the RoundTable and Knight
manifests, plus `missionDefaultNatsURL`, `gateway.natsURL` and `dashboard.env.NATS_URL`
in the chart (and `dashboard.vault.enabled=false` unless you provide the vault claim).
See [Pointing Round Table at your NATS](install.md#pointing-round-table-at-your-nats).
NATS must have JetStream enabled. Values are in [helm-values.md](helm-values.md). Round Table
needs nothing else: see [dependencies.md](dependencies.md) for the optional add-ons and
`values-minimal.yaml`.

### Running a chain by hand

Annotate the Chain with `ai.roundtable.io/run` — any value works, and it starts a **new run**
(new `runId`, all steps reset) whenever the value differs from `status.lastRunTrigger`:

```bash
kubectl annotate chain hello-chain -n roundtable ai.roundtable.io/run="$(date +%s)" --overwrite
```

Re-applying the same value never re-triggers. If a run is still in flight the trigger waits
and fires when it finishes. Changing `spec.input` never resumes an old run either; it resets
the chain to Idle, so trigger it with the annotation afterwards.

To resume a failed run from its failed (and skipped) steps only, keeping the steps that
already succeeded, use the opt-in `ai.roundtable.io/retry-failed` annotation:

```bash
kubectl annotate chain hello-chain -n roundtable ai.roundtable.io/retry-failed="$(date +%s)" --overwrite
```

Do not set `status.phase=Running` by hand: it resumes the previous run.

### Step timeouts and queueing

A step's `timeout` starts when the knight picks the task up, not when it is published, so
time spent queued behind a busy knight does not count. Pickup is detected from the knight's
durable `<Knight>-consumer` on the tasks stream (the capitalised Knight name, e.g.
`Rt-operator-consumer`). Knights keep a task un-acked while they work on it and ack only after the
result is published, so the ack floor is a completion signal, not a pickup signal. Instead the
consumer's delivered stream sequence reaching the task's sequence (`status.stepStatuses[].taskSeq`)
sets `pickedUpAt`. `spec.maxQueueWaitSeconds` (default 1800s) fails a step whose task is never
picked up. The overall chain `timeout` still counts from the start of the run.

## What's Next?

- [Example Manifests](../examples/) — Real-world Knight, Chain, Mission examples
- [Helm Values Reference](helm-values.md) — Full configuration options
- [Architecture Overview](ARCHITECTURE.md) — How it all works under the hood
- [CRD Design](crd-design.md) — Complete CRD field documentation
- [Gateway](gateway.md) — The HTTP API (its OpenAPI document is served at `/openapi.json`)
- [Keeping Mission Output](keeping-mission-output.md) — Write a Mission's results somewhere durable
- [Local Development](local-development.md) — The Tilt inner loop for working on the operator
