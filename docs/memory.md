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
| `/memory` | `spec.memory` (**pending [#388][388]**) | `/memory/<knight>` read-write, `/memory` read-only | Yes | Yes, with a shared claim |

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
  scaling. `spec.memory` and the `KNIGHT_MEMORY_DIR` variable the runtimes read
  are being built in [#388][388]; this page marks that part as pending and
  will be updated when the fields land on `main`. Until then, use a vault
  sub-path as the per-knight memory directory (below).

## Enforcement

| Mount | Does it enforce "write only your own"? |
|-------|----------------------------------------|
| `/vault` | **Yes.** The base is read-only and only the knight's `writablePaths` are mounted read-write. Writes anywhere else fail with a read-only filesystem error. |
| `/memory` | **Yes** (after #388). Only `/memory/<knight>` is read-write; the rest is read-only. |
| `/shared` | **No.** Every knight can write everywhere. Keep to the convention by prompt, not by mount. |
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
  model: claude-sonnet-4-20250514
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
  model: claude-sonnet-4-20250514
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

## Worked example: shared memory claim (pending #388)

After [#388][388], the same pattern needs no vault: one ReadWriteMany claim is
set once in the table's `defaults.memory`, and every knight gets
`/memory/<knight>` read-write and `/memory` read-only. The researcher writes
`/memory/researcher` and reads `/memory/writer`; the writer does the reverse.
The YAML is omitted here because the field is not on `main` yet, and every
example on this page is validated against the CRDs.

## Storage

Sharing needs **ReadWriteMany**. Use a storage class that supports it: CephFS,
NFS or Longhorn RWX. On a ReadWriteOnce class a second knight on another node
cannot attach the claim and its pod stays `Pending`. After #388 the operator
reports this with the `MemoryNotRWX` condition on the Knight instead of
failing silently.

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

## Plug-in points

- **A different volume.** Any claim can back `spec.vault`, `sharedWorkspace`
  or (after #388) `spec.memory`. The runtime sees only a directory.
- **An MCP server.** Memory behind a tool (a knowledge graph, a vector store)
  can be declared with `spec.mcpServers` once [#211][211] lands.
- **Non-filesystem memory** is future work; the design is parked in
  pi-knight#28.

[388]: https://github.com/roundtable-k8s/roundtable/issues/388
[211]: https://github.com/roundtable-k8s/roundtable/issues/211
