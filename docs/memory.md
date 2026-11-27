# Knight memory

Knights remember with **plain files on volumes**. There is no memory service:
a knight reads and writes a directory, and the volume behind it decides who
else can see the files and whether they survive. Sharing between knights
follows one convention:

> **Write your own paths, read everyone's.**

Each knight writes only to paths that belong to it and may read everything
else. The operator enforces this with mounts where it can (see
[Enforcement](#enforcement)).

## The mounts

| Mount | Configured by | Mode | Survives pod replacement | Shared between knights |
|-------|---------------|------|--------------------------|------------------------|
| `/data` | `spec.workspace` | read-write | `mode: pvc` (default): yes. `mode: ephemeral`: **no**, an `emptyDir` per pod | No. One PVC per knight, or one `emptyDir` per pod |
| `/vault` | `spec.vault` (or the table's `spec.vault`) | read-only base, read-write `writablePaths` sub-mounts | Yes, it is an existing PVC | Yes, if the claim is ReadWriteMany |
| `/shared` | `RoundTable.spec.sharedWorkspace` | read-write for every knight | Yes, it is an existing PVC | Yes, RWX |
| `/memory` | `spec.memory` on the Knight, or `spec.defaults.memory` on the RoundTable | With `existingClaim`: `/memory/<knight>` read-write, `/memory` read-only. Without it: the knight's own claim at `/memory/<knight>` read-write | Yes | Yes, with a ReadWriteMany `existingClaim` |

- **`/data` (workspace)** is scratch: clones, build output, intermediate
  files. With the default `mode: pvc` it is one ReadWriteOnce PVC and so
  limits the knight to one pod. With `mode: ephemeral` (needed for
  `replicas > 1`) it is lost whenever a pod is replaced or scaled away. Do not
  keep anything there you cannot regenerate, memory files included.
- **`/vault`** is the shared knowledge base (Derek's fleet uses an Obsidian
  vault). It is mounted read-only; each knight lists the sub-paths it may
  write.
- **`/shared`** is a collaboration scratch volume. It is read-write for all
  knights, so it carries **no** ownership convention. Use it for hand-offs, not
  for memory you want protected.
- **`/memory`** is a per-knight ReadWriteMany memory volume kept separate from
  the scratch workspace, so memory survives `mode: ephemeral` workspaces and
  scaling. The operator sets `KNIGHT_MEMORY_DIR=/memory/<knight>` (the
  knight's own directory) and `KNIGHT_MEMORY_ROOT=/memory` in the pod. Without
  `spec.memory` the knight has no `/memory` mount; a vault sub-path works as a
  per-knight directory too (see [Vault](#vault-explicit-writablepaths)).

## Enforcement

| Mount | Does it enforce "write only your own"? |
|-------|----------------------------------------|
| `/vault` | **Yes**, while `readOnly` is true (the default for a knight with `writablePaths`). The base is read-only and only the knight's `writablePaths` are mounted read-write. Writes anywhere else fail with a read-only filesystem error. With `vault.readOnly: false` the **whole vault is writable** and `writablePaths` enforces nothing. |
| `/memory` | **Yes** for a shared `existingClaim`: only `/memory/<knight>` is read-write; the rest is read-only. A per-knight claim is mounted only at `/memory/<knight>`, so other knights' files are not visible to it at all. |
| `/shared` | **No.** Every knight can write everywhere. Keep to the convention by prompt, not by mount. The path is `/shared` unless `RoundTable.spec.sharedWorkspace.mountPath` overrides it. |
| `/data` | Not applicable: it is private to the knight. |

## Vault: explicit `writablePaths`

`writablePaths` has no default. An absent or empty list means a strictly
read-only vault. List every path the knight may write. A knight's own
`spec.vault` replaces the table's `spec.vault`; the two are not merged.

The example below gives a researcher and a writer each their own directory in
the same vault. Each can read the other's notes, and neither can write them.

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: Knight
metadata:
  name: researcher
  namespace: ai
spec:
  domain: research
  model: <your-model>
  skills:
    - general
  nats:
    url: nats://nats.database.svc:4222
    stream: fleet_a_tasks
    resultsStream: fleet_a_results
    subjects:
      - "fleet-a.tasks.research.>"
  vault:
    claimName: obsidian-vault
    readOnly: true
    writablePaths:
      - "Memory/researcher/"
---
apiVersion: ai.roundtable.io/v1alpha1
kind: Knight
metadata:
  name: writer
  namespace: ai
spec:
  domain: writing
  model: <your-model>
  skills:
    - general
  nats:
    url: nats://nats.database.svc:4222
    stream: fleet_a_tasks
    resultsStream: fleet_a_results
    subjects:
      - "fleet-a.tasks.writing.>"
  vault:
    claimName: obsidian-vault
    readOnly: true
    writablePaths:
      - "Memory/writer/"
      - "Briefings/"
```

The vault claim must be ReadWriteMany for several knights to mount it at once.

## Worked example: shared memory claim

One ReadWriteMany claim is set once in the table's `defaults.memory`, and
every knight of the table gets `/memory/<knight>` read-write and `/memory`
read-only. The researcher writes `/memory/researcher` and reads
`/memory/writer`; the writer does the reverse. The full manifests are in
[`examples/knight-memory.yaml`](../examples/knight-memory.yaml), which the test
suite decodes and the CI admission check dry-runs against the API server. The
parts that matter:

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: RoundTable
metadata:
  name: memory-fleet
  namespace: roundtable
spec:
  # ... nats and streams ...
  defaults:
    memory:
      existingClaim: fleet-memory   # a ReadWriteMany PVC you create first
---
apiVersion: ai.roundtable.io/v1alpha1
kind: Knight
metadata:
  name: researcher
  namespace: roundtable
  labels:
    ai.roundtable.io/round-table: memory-fleet   # inherits defaults.memory
spec:
  # ... domain, model, nats ...
```

Points to know:

- **Inheritance needs the table label.** A knight inherits `defaults.memory`
  only when it carries `ai.roundtable.io/round-table: <table>`. A knight that
  sets its own `spec.memory` keeps it and is not in the shared claim.
- **Why both knights inherit, not one with its own claim.** A knight with a
  per-knight claim is mounted only at its own `/memory/<knight>`, so the
  others could not read its notes. Keep every knight that should share memory
  on the table's claim.
- **What a knight sees.** Inside the pod, `$KNIGHT_MEMORY_DIR` is
  `/memory/writer` and `$KNIGHT_MEMORY_ROOT` is `/memory`. The writer can list
  `/memory/researcher` but any write there fails with a read-only filesystem
  error.
- **The claim is yours to create.** With `existingClaim` the operator never
  creates, resizes or deletes it, and it does not check its access mode. Check
  it before the knights start:

  ```bash
  kubectl get pvc fleet-memory -n roundtable -o jsonpath='{.spec.accessModes}'
  # ["ReadWriteMany"]
  ```

## Storage

Sharing needs **ReadWriteMany**. Use a storage class that supports it: CephFS,
NFS or Longhorn RWX. On a ReadWriteOnce class a second knight on another node
cannot attach the claim and its pod stays `Pending`.

The operator checks this only for a claim it creates (`spec.memory` without
`existingClaim`), and only when the knight can run more than one pod
(`replicas` or `maxReplicas` above 1). Then the Knight gets a `MemoryShared`
condition: `True` when the claim is bound ReadWriteMany, and `False` with
reason `MemoryNotRWX` when it is not. Read it with:

```bash
kubectl get knight writer -n roundtable \
  -o jsonpath='{.status.conditions[?(@.type=="MemoryShared")]}'
```

A shared `existingClaim` is not checked, so verify its access mode yourself
(see the worked example).

## Consolidation

Memory consolidation (a "dream" pass that tidies and summarises what a knight
has written) needs no code. Schedule a Chain step that asks the knight to
tidy its own memory:

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: Chain
metadata:
  name: researcher-memory-tidy
  namespace: ai
spec:
  description: "Nightly consolidation of the researcher's memory"
  roundTableRef: {name: fleet-a}
  schedule: "0 3 * * *"
  timeoutSeconds: 600
  steps:
    - name: consolidate
      knightRef: {name: researcher}
      task: "Review Memory/researcher/ in the vault. Merge duplicate notes, drop stale facts, and write a short index at Memory/researcher/INDEX.md. Touch only your own directory."
      timeoutSeconds: 540
```

The step above tidies the vault layout. On a shared claim the same task can
name `/memory/researcher` instead, the knight's own directory.

## Plug-in points

- **A different volume.** Any claim can back `spec.vault`, `sharedWorkspace`
  or `spec.memory`. The runtime sees only a directory.
- **An MCP server.** Memory behind a tool (a knowledge graph, a vector store)
  can be declared with `spec.mcpServers`, see
  [crd-design.md](crd-design.md#7-knight-specmcpservers-claude-knight-only).
- **Non-filesystem memory** is future work; the design is parked in
  pi-knight#28.
