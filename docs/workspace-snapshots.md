# Workspace snapshots and forks

A knight's workspace (`/data`) is a PVC. The operator can take an on-demand CSI
`VolumeSnapshot` of it and start a new knight whose workspace begins as a copy of that
snapshot. Use it to keep a restore point before risky work, or to fork a knight that has
built up files and context (`galahad` becomes `galahad-fork`).

Scheduled snapshots are not supported: a snapshot is taken only when you ask for one.

## Requirements

- A CSI driver that supports snapshots, and a `VolumeSnapshotClass` for it (or a default
  class; see `className` below).
- The external-snapshotter CRDs and **snapshot-controller** installed, so the cluster
  serves `snapshot.storage.k8s.io/v1` `VolumeSnapshot`.
- A knight with the default `workspace.mode: pvc`. Ephemeral workspaces cannot be
  snapshotted.

The operator checks for the API at startup (`pkg/runtime/snapshot_detect.go`). If it was
missing, the operator checks again, at most once a minute, whenever a snapshot or a
`workspace.fromSnapshot` restore is requested, so installing the snapshotter later needs no
operator restart. Knights are then polled for snapshot readiness instead of watched until
the operator next restarts. Without the API the feature does nothing except report the
conditions below. The
chart RBAC already grants the operator `get, list, watch, create, delete` on
`volumesnapshots` (`charts/roundtable-operator/templates/rbac.yaml`). See
[dependencies.md](dependencies.md).

## Take a snapshot

Set the `ai.roundtable.io/snapshot` annotation on the Knight to a value that has not been
used before. The value is arbitrary; a timestamp is convenient:

```bash
kubectl -n roundtable annotate knight galahad ai.roundtable.io/snapshot=$(date +%s)
```

The operator creates a `VolumeSnapshot` named `<knight>-ws-<unix-seconds>` in the knight's
namespace, from the workspace PVC, and records the handled value in
`status.workspace.lastSnapshotRequest`. Re-applying the same value does nothing; use a new
value for each snapshot.

Each snapshot has the label `roundtable.io/knight=<knight>` and is owned by the Knight, so
**deleting the Knight deletes its snapshots**. Copy anything you need to keep (or fork
first).

Choose the class and retention with `spec.workspace.snapshots`:

```yaml
spec:
  workspace:
    snapshots:
      className: csi-snapclass   # empty (default): the cluster's default VolumeSnapshotClass
      keep: 5                    # default 3, minimum 1
```

## Read the snapshots

```bash
kubectl -n roundtable get knight galahad -o jsonpath='{.status.workspace.snapshots}'
kubectl -n roundtable get volumesnapshots -l roundtable.io/knight=galahad
```

`status.workspace.snapshots` lists the knight's snapshots, oldest first. Each entry has
`name`, `createdAt` and `readyToUse` (mirrors the `VolumeSnapshot`'s `status.readyToUse`).
Only a snapshot with `readyToUse: true` can be restored.

The `WorkspaceSnapshots` condition on the Knight (`kubectl describe knight galahad`):

| Status / reason | Meaning |
|---|---|
| `True` / `SnapshotsReady` | Snapshots are in order |
| `False` / `SnapshotAPIUnavailable` | The `VolumeSnapshot` API is not served; install the snapshotter. The operator looks again, at most once a minute, when a snapshot is requested; no restart is needed |
| `False` / `WorkspaceNotPersistent` | The knight uses `workspace.mode: ephemeral` |
| `False` / `SnapshotFailed` | Creating or deleting a `VolumeSnapshot` failed; the message has the error |

The condition is absent while you have not used the annotation and the knight has no
snapshots.

## Retention

`keep` limits how many snapshots of the knight are retained. Once the **newest** snapshot
is `readyToUse`, the oldest ones beyond `keep` are deleted. While the newest is still being
cut, nothing is deleted, so the count can briefly exceed `keep`. Only snapshots the
operator created (owned by the Knight) are counted or deleted; a `VolumeSnapshot` you made
by hand is left alone even if it carries the `roundtable.io/knight` label.

## Fork a knight from a snapshot

Create a new Knight and put the snapshot name in `spec.workspace.fromSnapshot`
([`examples/knight-fork-from-snapshot.yaml`](../examples/knight-fork-from-snapshot.yaml)):

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: Knight
metadata:
  name: galahad-fork
  namespace: roundtable
spec:
  # ... domain, model, nats: use your own NATS subjects, not galahad's
  workspace:
    size: 2Gi
    fromSnapshot: galahad-ws-1767225600
```

The operator creates the workspace PVC with the snapshot as its data source and the
knight's pod mounts it at `/data` as usual. Check progress with the `WorkspaceReady`
condition:

| Status / reason | Meaning |
|---|---|
| `False` / `SnapshotNotReady` | The `VolumeSnapshot` does not exist or is not `readyToUse`. The PVC is **not created** yet, so the knight's pod stays `Pending` until the PVC exists; the operator looks again every 30 seconds |
| `False` / `SnapshotAPIUnavailable` | The `VolumeSnapshot` API is not served. The PVC is not created, so the pod stays `Pending`; the operator looks again every 30 seconds and rechecks the API at most once a minute |
| `True` / `RestoredFromSnapshot` | The PVC was created from the snapshot |
| `True` / `FromSnapshotIgnored` | The PVC already existed (not created from this snapshot), so `fromSnapshot` was not applied |

An existing PVC is kept: to get a restored workspace, create a new knight (a fork).

## Restrictions

- `pvc` mode only. `fromSnapshot` is rejected (at admission) with
  `workspace.existingClaim` (even an empty one) and with `workspace.mode: ephemeral`.
  Snapshots are not taken for ephemeral workspaces. Taking snapshots does work for a knight
  that uses `existingClaim`: the operator snapshots that claim.
- `fromSnapshot` applies only when the PVC is created. An existing PVC is never replaced or
  modified.
- The snapshot must be in the knight's own namespace.
- PVC size is the larger of `workspace.size` (default `1Gi`) and the snapshot's
  `restoreSize`.
- `fromSnapshot` is **not inherited** from RoundTable defaults (`internal/knight/pod_builder.go`),
  so a table-wide default never makes every knight clone one snapshot. Set it on the
  individual Knight.
- The fork is an independent knight: it shares nothing with the source afterwards, and
  needs its own NATS subjects.

## Where each name comes from

| Name | Defined in |
|---|---|
| `spec.workspace.snapshots` (`className`, `keep`), `spec.workspace.fromSnapshot`, `status.workspace` (`lastSnapshotRequest`, `snapshots[]`), `ai.roundtable.io/snapshot` | `api/v1alpha1/knight_types.go` |
| Conditions `WorkspaceSnapshots`, `WorkspaceReady` and their reasons | `api/v1alpha1/conditions.go` |
| Snapshot creation, naming, labels `roundtable.io/knight` and `roundtable.io/snapshot-request`, retention, default `keep` of 3 | `internal/controller/knight_snapshot.go` |
| PVC creation from the snapshot, readiness check, size | `internal/controller/knight_controller.go`, `internal/controller/knight_workspace_restore.go` |
| API detection | `pkg/runtime/snapshot_detect.go`, `cmd/main.go` |
