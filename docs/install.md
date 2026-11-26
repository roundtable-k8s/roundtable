# Installing Round Table

The Helm chart is published only as an OCI artifact:
`oci://ghcr.io/roundtable-k8s/charts/roundtable-operator`. There is no classic
Helm repository (`helm repo add` does not apply).

Prerequisites: Kubernetes 1.28+, NATS with JetStream enabled, and an AI model
API key. The chart installs the CRDs, the operator and the dashboard.

## 1. Flux (OCIRepository + HelmRelease)

Use this if your cluster is managed by Flux. The manifests are in
[`examples/flux/`](../examples/flux/):

| File | Resource |
|------|----------|
| [`namespace.yaml`](../examples/flux/namespace.yaml) | Namespace `roundtable` |
| [`ocirepository.yaml`](../examples/flux/ocirepository.yaml) | `OCIRepository` (`source.toolkit.fluxcd.io/v1`) that tracks the chart's semver range |
| [`helmrelease.yaml`](../examples/flux/helmrelease.yaml) | `HelmRelease` (`helm.toolkit.fluxcd.io/v2`) that installs the chart via `chartRef` |

Flux v2.3 or later is required: `chartRef` pointing at an `OCIRepository` and
the `helm.toolkit.fluxcd.io/v2` API are not available in older releases.

The `OCIRepository` uses a `layerSelector` so Flux extracts the Helm chart
tarball layer (`application/vnd.cncf.helm.chart.content.v1.tar+gzip`) from
the OCI artifact. Flux finds the artifact either way, but without the selector
helm-controller cannot load it as a chart.

```bash
kubectl apply -f examples/flux/namespace.yaml \
  -f examples/flux/ocirepository.yaml \
  -f examples/flux/helmrelease.yaml

# Wait for the chart to be fetched and installed
flux get sources oci -n roundtable
flux get helmreleases -n roundtable
kubectl get crds | grep ai.roundtable.io
```

To put these in a GitOps repo, add the three files to the Flux path you
reconcile (for example with a `Kustomization`). Set `values:` in
`helmrelease.yaml` for chart options; see [Helm values](helm-values.md).

The `semver` constraint in `ocirepository.yaml` (`^0.23.0`) takes patch
releases only. Widen it to pick up later 0.x minor releases; check the release
notes first, since minor releases can change the CRDs.

The version pins here, in `examples/flux/ocirepository.yaml` (comment and
`semver`) and in the `helm install` command below are bumped when a release is
cut. If `scripts/check-install-versions.sh` fails after a release, bump those
three.

## 2. Helm (`helm install`)

```bash
helm install roundtable \
  oci://ghcr.io/roundtable-k8s/charts/roundtable-operator \
  --version 0.23.0 \
  --namespace roundtable --create-namespace
```

Pass `--version` to pin the chart. Omit it to get the latest release. Chart
values can be set with `--set` or `-f values.yaml`; see
[Helm values](helm-values.md).

This is the install for a normal cluster. Do the next three steps in order:
install NATS, choose the isolation flags for your cluster, then install the
chart with them and create your first knight.

## Install NATS

Round Table does not bundle NATS (see [dependencies.md](dependencies.md)). Install
the nats-io chart with JetStream on and a file store on a PVC, in the same
namespace as Round Table:

```bash
helm repo add nats https://nats-io.github.io/k8s/helm/charts/
helm upgrade --install nats nats/nats \
  --version 2.12.3 \
  --namespace roundtable --create-namespace \
  --set config.jetstream.enabled=true \
  --set config.jetstream.fileStore.pvc.enabled=true \
  --set config.jetstream.fileStore.pvc.size=10Gi \
  --set natsBox.enabled=false \
  --wait
```

- `config.jetstream.enabled=true` turns JetStream on. Round Table needs it.
- `config.jetstream.fileStore.pvc.enabled=true` keeps the stream data on a PVC, so
  a NATS pod restart keeps the tasks. `hack/quickstart-kind.sh` turns this off for
  kind only; do not copy that to a real cluster.
- `config.jetstream.fileStore.pvc.size` sizes the claim for your streams. Set it
  for your workload; `10Gi` is only a starting point.
- `natsBox.enabled=false` skips the optional debug pod.

With the release named `nats` in the `roundtable` namespace, the Service URL is
`nats://nats.roundtable.svc:4222`. That is the URL the quickstart manifests use.
The chart's `nats.database.svc` default is the author's cluster, not yours, so
always set the URL explicitly (see [Pointing Round Table at your NATS](#pointing-round-table-at-your-nats)).

## Choose your isolation

By default each knight runs under the `gvisor` RuntimeClass and its egress is
default-deny. A cluster without gVisor parks every knight with `Isolated=False`,
reason `RuntimeClassNotFound`, so pick one of these paths and set its flags on the
`helm install` in step 2.

**(a) gVisor is installed.** Name its RuntimeClass `gvisor` and change nothing.
The default values already do this.

**(b) gVisor is not installed, and you accept that.** Turn the RuntimeClass default
off:

```bash
--set-string isolation.runtimeClassName=""
```

Knights then report `Isolated=False`, reason `IsolationDisabled`.

**(c) No CNI that enforces NetworkPolicy.** Turn the egress default off:

```bash
--set isolation.egress.mode=off
```

A cluster with neither gVisor nor an enforcing CNI needs both (b) and (c). Read
[runtime isolation](runtime-isolation.md) for what each choice gives up and
[egress](egress.md) for how the egress allowlist is enforced. Hostname rules need
Cilium; the plain `NetworkPolicy` fallback cannot enforce them.

Check the result with:

```bash
kubectl -n roundtable get knights
```

The `ISOLATED` column shows the `Isolated` condition. It is `True` only with a
RuntimeClass and a Cilium hostname policy. Otherwise it is `False`, and the
condition's reason tells you why: `IsolationDisabled` for (b) and (c),
`FQDNPolicyUnavailable` for gVisor without Cilium.

## Your first knight

Create the Secret that holds your model API key. The quickstart Knight loads it
through `spec.envFrom`:

```bash
kubectl -n roundtable create secret generic llm-api-keys \
  --from-literal=ANTHROPIC_API_KEY=sk-ant-...
```

Apply the RoundTable and the Knight from [`examples/quickstart/`](../examples/quickstart/).
Their NATS URL is `nats://nats.roundtable.svc:4222`, which matches the NATS install
above. If your NATS is elsewhere, replace that URL (`nats.url` in each manifest) when
you apply them, for example from the repo root:

```bash
sed 's#nats://nats.roundtable.svc:4222#nats://YOUR-NATS-HOST:4222#' \
  examples/quickstart/roundtable.yaml | kubectl apply -f -
sed 's#nats://nats.roundtable.svc:4222#nats://YOUR-NATS-HOST:4222#' \
  examples/quickstart/knight.yaml | kubectl apply -f -
```

Wait until the Knight is ready:

```bash
kubectl -n roundtable get knights -w   # wait for READY true, then Ctrl-C
```

Then send one task through the gateway; see [gateway.md](gateway.md).

## Pointing Round Table at your NATS

A NATS URL lives in five places. The chart and CRD defaults name a NATS of the
author's cluster (`nats.database.svc`), so on any other cluster set all of them:

| Where | Field | What it configures |
|-------|-------|--------------------|
| RoundTable manifest | `spec.nats.url` | the table's subjects and streams |
| Knight manifest | `spec.nats.url` | the knight pod's NATS connection |
| Chart value | `missionDefaultNatsURL` | NATS URL given to Mission knights |
| Chart value | `gateway.natsURL` | the gateway's `--nats-url` |
| Chart value | `dashboard.env.NATS_URL` | the dashboard's `NATS_URL` |

```bash
helm install roundtable-operator oci://ghcr.io/roundtable-k8s/charts/roundtable-operator \
  -n roundtable --create-namespace \
  --set-string missionDefaultNatsURL=nats://nats.example.svc:4222 \
  --set-string gateway.natsURL=nats://nats.example.svc:4222 \
  --set-string dashboard.env.NATS_URL=nats://nats.example.svc:4222
```

The dashboard also mounts a vault claim (`roundtable-vault`) by default, and the
chart does not create that PVC. Either create it or set
`dashboard.vault.enabled=false`, or the dashboard pod stays Pending. The kind
quickstart (`hack/quickstart-kind.sh`) sets all of the above.

If a knight is not Ready after installing, see [troubleshooting](troubleshooting.md).

## 3. Upgrading and CRDs

Helm installs the CRDs in the chart's `crds/` directory on first install. Helm
does **not** upgrade or delete them on `helm upgrade` or `helm uninstall`.
If a new chart version changes a CRD, the cluster keeps the old schema unless
you apply the new one.

- **Flux:** `helmrelease.yaml` sets `install.crds` and `upgrade.crds` to
  `CreateReplace`, so Flux applies the chart's CRDs on install and on every
  upgrade. CRDs are replaced, not deleted, so existing Knights, Chains,
  Missions and RoundTables are kept.
- **Helm:** apply the CRDs yourself before `helm upgrade`. Helm created the
  CRDs under the field manager `helm`, so a server-side apply of changed CRDs
  conflicts on fields such as `.spec.versions`. `--force-conflicts` takes
  ownership of those fields; without it the command fails exactly when the CRDs
  changed:

  ```bash
  helm pull oci://ghcr.io/roundtable-k8s/charts/roundtable-operator \
    --version <new-version> --untar --untardir /tmp/rt
  kubectl apply --server-side --force-conflicts -f /tmp/rt/roundtable-operator/crds/
  helm upgrade roundtable \
    oci://ghcr.io/roundtable-k8s/charts/roundtable-operator \
    --version <new-version> --namespace roundtable
  ```

Read the release notes for the target version before upgrading. Check
whether the CRDs or values changed.
