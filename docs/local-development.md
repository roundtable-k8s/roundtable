# Local development with Tilt and kind

Change operator code and see it running in a local [kind](https://kind.sigs.k8s.io/)
cluster within seconds, with one command: `make tilt-up`.

## Prerequisites

- Docker (or another kind provider), [kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation),
  [Helm](https://helm.sh/docs/intro/install/) 3.12+, `kubectl`, [Tilt](https://docs.tilt.dev/install.html) and Go (see `go.mod`)
- A clone of this repository

## Start

```bash
make tilt-up
```

This creates the kind cluster `roundtable-dev` if it is missing (override with
`TILT_KIND_CLUSTER`), selects its kubectl context and runs `tilt up`. Tilt then:

1. creates the `roundtable` namespace and applies the CRDs from `charts/roundtable-operator/crds`
   server-side (they are too large for client-side apply),
2. installs NATS with JetStream from the pinned `nats` chart (`NATS_CHART_VERSION`, the same
   version `hack/quickstart-kind.sh` uses),
3. compiles the operator on the host (`.tilt/manager`, `.tilt/gateway`) and deploys
   `charts/roundtable-operator` with `hack/tilt-values.yaml` (dashboard on, gateway off,
   no gVisor/Cilium/KEDA, no leader election),
4. applies `examples/dev/`: one RoundTable (`dev`) and one Knight (`dev-knight`).

Open the Tilt UI at <http://localhost:10350>. For a non-interactive run, e.g. in CI:
`make tilt-up TILT_ARGS=ci`.

The knight's API key Secret is optional, so its Deployment exists without one but the pod
does not start. To run a knight, create `llm-api-keys` with your provider key:

```bash
kubectl -n roundtable create secret generic llm-api-keys --from-literal=ANTHROPIC_API_KEY=<key>
```

## The loop

Edit a Go file under `cmd/`, `api/`, `internal/` or `pkg/`. Tilt recompiles on the host and
syncs the binary into the running operator pod, which restarts the process in place; no image
rebuild. Changes to the chart or `hack/tilt-values.yaml` re-render and re-apply the manifests.
If you change the types in `api/`, run `make manifests generate` and copy the CRDs to
`charts/roundtable-operator/crds` as usual; the `crds` resource re-applies them.

## Use a local pi-knight image

Build the image and load it into the kind cluster, then set `spec.image` on the dev Knight
(a commented line in `examples/dev/knight.yaml`):

```bash
docker build -t pi-knight-dev:local /path/to/pi-knight
kind load docker-image pi-knight-dev:local --name roundtable-dev
# edit examples/dev/knight.yaml: image: pi-knight-dev:local   (Tilt re-applies it)
```

To change the default for every knight instead, set `images.piKnight.repository` and `tag`
in `hack/tilt-values.yaml`. Use a tag other than `latest` so the kubelet does not try to pull it.

## Tear down

```bash
tilt down            # remove what Tilt deployed, keep the cluster
make tilt-down       # delete the kind cluster
```
