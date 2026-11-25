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

The `OCIRepository` uses a `layerSelector` so Flux takes the Helm chart
tarball layer (`application/vnd.cncf.helm.chart.content.v1.tar+gzip`) out of
the OCI artifact. Without it, Flux cannot find the chart.

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

The `semver` constraint in `ocirepository.yaml` (`^0.22.0`) takes patch
releases only. Widen it to pick up later 0.x minor releases; check the release
notes first, since minor releases can change the CRDs.

## 2. Helm (`helm install`)

```bash
helm install roundtable-operator \
  oci://ghcr.io/roundtable-k8s/charts/roundtable-operator \
  --version 0.22.0 \
  --namespace roundtable --create-namespace
```

Pass `--version` to pin the chart. Omit it to get the latest release. Chart
values can be set with `--set` or `-f values.yaml`; see
[Helm values](helm-values.md).

## 3. Upgrading and CRDs

Helm installs the CRDs in the chart's `crds/` directory on first install. Helm
does **not** upgrade or delete them on `helm upgrade` or `helm uninstall`.
If a new chart version changes a CRD, the cluster keeps the old schema unless
you apply the new one.

- **Flux:** `helmrelease.yaml` sets `install.crds` and `upgrade.crds` to
  `CreateReplace`, so Flux applies the chart's CRDs on install and on every
  upgrade. CRDs are replaced, not deleted, so existing Knights, Chains,
  Missions and RoundTables are kept.
- **Helm:** apply the CRDs yourself before `helm upgrade`:

  ```bash
  helm pull oci://ghcr.io/roundtable-k8s/charts/roundtable-operator \
    --version <new-version> --untar --untardir /tmp/rt
  kubectl apply --server-side -f /tmp/rt/roundtable-operator/crds/
  helm upgrade roundtable-operator \
    oci://ghcr.io/roundtable-k8s/charts/roundtable-operator \
    --version <new-version> --namespace roundtable
  ```

Read the release notes for the target version before upgrading. Check
whether the CRDs or values changed.
