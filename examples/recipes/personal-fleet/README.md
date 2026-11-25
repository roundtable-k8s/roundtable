# Personal fleet

A few persona knights and two jobs, none of them about software development.
A knight is a persona (identity, model, what it may touch); a chain is a job
(a prompt, an order of steps, a schedule). Notes live as files on a shared
volume: each knight writes only its own directory and reads everyone else's.

```
06:30 (Europe/London)  Chain morning-briefing
   research (scout) ──┐
                      ├──> briefing (editor) --> Briefings/<date>.md
   infrastructure (steward) ┘

POST /v1/chains/summarize-note/runs   Chain summarize-note (on demand)
   summarize (editor) --> Briefings/<date>-<run>.md
```

| File | What |
|---|---|
| [`roundtable.yaml`](roundtable.yaml) | The `fleet-a` RoundTable and the `personal-notes` ReadWriteMany PVC |
| [`knights.yaml`](knights.yaml) | The `scout`, `steward` and `editor` Knights, and the steward's read-only ServiceAccount |
| [`chains.yaml`](chains.yaml) | The scheduled `morning-briefing` Chain and the on-demand `summarize-note` Chain |

## Before you start

- The operator is installed and NATS is reachable at the URL in
  `roundtable.yaml`. If you already have a RoundTable named `fleet-a`, delete
  that document from your copy and keep the PVC. Rename `fleet-a` (and the
  `fleet-a.tasks.` subjects) if your table differs.
- The knights run `ghcr.io/roundtable-k8s/claude-knight` and need a model
  provider credential, as any knight does. No credentials are in this recipe.
- Optionally create `Topics.md` in the notes volume with the topics the scout
  should follow.

## Install

```bash
kubectl apply -f roundtable.yaml -f knights.yaml -f chains.yaml
```

Run the briefing once without waiting for 06:30; any new annotation value
starts a fresh run (see [Run the Chain](../../../docs/getting-started.md#4-run-the-chain)):

```bash
kubectl annotate chain morning-briefing -n roundtable \
  ai.roundtable.io/run="$(date +%s)" --overwrite
```

The note lands in `Briefings/<date>.md` on the volume.

## Run a job on demand

`summarize-note` has no schedule. It carries the label
`ai.roundtable.io/invokable: "true"`, which opts it in to
`POST /v1/chains/{name}/runs` on the gateway
([gateway docs](../../../docs/gateway.md#chain-runs)). From a pod inside the
cluster (`<fullname>` is the Helm release's full name, for example
`roundtable-roundtable-operator`):

```bash
TOKEN=$(kubectl -n roundtable get secret <fullname>-gateway-token -o jsonpath='{.data.token}' | base64 -d)
kubectl -n roundtable run curl --rm -it --restart=Never --image=curlimages/curl --env TOKEN="$TOKEN" -- \
  curl -sS -X POST http://<fullname>-gateway.roundtable.svc/v1/chains/summarize-note/runs \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -d '{"input": "Paste the text to summarize here.", "waitSeconds": 600}'
```

`input` is the run's input (`{{ .Input }}` in the step). With `waitSeconds`
the answer is **200** with the run record once it finishes, its `output`
being the summary; without it, or if the wait runs out, **202** with a
`Location` to poll (`GET /v1/runs/roundtable/<run>`). The summary is also kept in `Briefings/<date>-<run>.md`, so runs on one day do not overwrite each other. Each call runs a copy of
the Chain named `summarize-note-gw-<8 hex>`, which the gateway deletes a day
after it finishes.

## What each persona may and may not do

| | scout (research) | steward (infrastructure) | editor |
|---|---|---|---|
| Reads the notes volume | all of it | all of it | all of it |
| Writes the notes volume | `Research/` only | `Infrastructure/` only | `Briefings/` only |
| Network | baseline plus a few news and wiki hosts | baseline only | baseline only |
| Cluster | none | read-only (`view`: no Secrets, no writes, no `exec`), with `kubectl` from `tools.nix` | none |
| Model | `sonnet` | `sonnet` | `haiku` |

- Every knight is isolated under the defaults (gVisor, default-deny egress).
  The baseline (DNS, NATS, the Kubernetes API, model providers) is always
  allowed; the allowlist adds to it, so add hosts only as you need them.
- The notes volume is mounted read-only. Only each knight's `writablePaths`
  are read-write, so a write elsewhere fails with a read-only filesystem
  error; the mount enforces it, not the prompt
  ([memory docs](../../../docs/memory.md#vault-explicit-writablepaths)).
- Web pages and cluster output come from outside: the prompts tell the personas
  to treat them as data. Keep the steward's role read-only, which is what
  limits the damage.
- The scout can read the web and the notes, so a hostile page could ask it to
  put something in `Research/`; it cannot reach `Briefings/` except through
  the editor, who only rewrites what it is handed.

## Use an existing notes volume

Delete the PVC from `roundtable.yaml` and set `vault.claimName` on each knight
to your PVC (for example an Obsidian vault). It must be ReadWriteMany because
three knights mount it. Keep each knight's `writablePaths` to its own
directory; create those directories in the vault first if you want them
there, and change `Briefings/` in `chains.yaml` if you keep briefings
elsewhere (the `outputPath` must sit under the editor's `writablePaths`).

## Change models by cost

Each knight's `spec.model` is its default; a chain step's `model` overrides it
for that job. Persona knights that gather (scout, steward) benefit from a
stronger model; the editor only rewrites, so `haiku` is enough. To spend less,
set `model: haiku` on the scout and steward, or set `model` on a single step
in `chains.yaml` to change one job. To spend more on one run, set a stronger
model on that step.

## Add a persona

1. Add a Knight to `knights.yaml`: a `domain`, a `nats.subjects` entry
   `fleet-a.tasks.<domain>.>`, a short `prompt.identity`, a small
   `egress.allowedHosts` and a `vault` with its own `writablePaths`
   (for example `Finance/`).
2. Give it a step in a chain: `knightRef: {name: <knight>}`, then list it in
   the editor step's `dependsOn` and include `{{ .Steps.<step>.Output }}` in
   the editor's task.
3. `kubectl apply -f knights.yaml -f chains.yaml`.
