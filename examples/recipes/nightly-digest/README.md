# Nightly digest

A nightly job done as a skill with scripts: one persona knight, a skill in the
arsenal that holds the scripts, and a schedule Trigger that sends one task
naming the skill. Collecting, validating and filing live in the skill's
scripts, and validation is in the script that writes the output, so it cannot
be skipped. There is no new chain step type (decision 25 in
[`docs/decisions.md`](../../../docs/decisions.md), epic #570). Here the job is
a read-only digest of what changed in one namespace.

```
Trigger nightly-digest (cron, Forbid)
   |  one task: "use the nightly-digest skill for namespace X"
   v
Knight digest (read-only, isolated)
   scripts/collect  -> JSON of workloads, unhealthy pods, Warning events
   knight           -> writes the digest JSON from that output
   scripts/file     -> validates it, writes $DIGEST_DIR/<ns>-<date>.{json,md},
                       last line "nightly-digest: ok"
```

| File | What |
|---|---|
| [`knight.yaml`](knight.yaml) | The `digest` Knight, its ServiceAccount and the `view` ClusterRole binding; `kubectl` and `jq` from `spec.tools.nix` |
| [`trigger.yaml`](trigger.yaml) | The schedule Trigger: `cron`, `timeZone`, `startingDeadlineSeconds`, `Forbid`, one task for the knight, `timeoutSeconds` |
| [`skill/nightly-digest/`](skill/nightly-digest/) | The skill: `SKILL.md`, `scripts/collect`, `scripts/file`, `tests/run.sh` |

## Before you start

Needs the [shared "Before you start"](../README.md#before-you-start): operator and NATS, a RoundTable `fleet-a`, `spec.nats.url` on the `digest` Knight and model credentials (no aliases).

## Install

A RoundTable named `fleet-a` exists (see
[`roundtable-fleet.yaml`](../../roundtable-fleet.yaml)); rename `fleet-a`
(the Knight's `ai.roundtable.io/round-table` label, the Trigger's
`roundTableRef` and the `fleet-a.tasks.` subject) in the manifests if yours
differs; the label must match the Trigger's `roundTableRef` or the Trigger is
never Ready. First make the
skill available to the knight (next section), then:

```bash
kubectl apply -f knight.yaml -f trigger.yaml
kubectl -n roundtable get trigger nightly-digest   # Ready, Last Fired
```

Change the namespace to digest in the Trigger's `task`, and the schedule in
`cron` / `timeZone`. For another namespace, copy `trigger.yaml` under another
name; the Knight and the skill are shared.

## Build and reference the skill

Skills are mounted under `/skills` from `spec.arsenals`. The knight's
`spec.skills` lists skill *categories*; each arsenal must hold
`<category>/<skill>/`, and the skill-filter sidecar links
`/skills/<category>/<skill>` to the last arsenal that provides it. (Checked in
`internal/knight/pod_builder.go`: `WithSkillFilter`.) Here the category is
`ops` and the skill is `nightly-digest`, so the arsenal needs:

```
<arsenal root>/ops/nightly-digest/SKILL.md
<arsenal root>/ops/nightly-digest/scripts/collect
<arsenal root>/ops/nightly-digest/scripts/file
```

**Git arsenal.** Copy `skill/nightly-digest/` to `ops/nightly-digest/` in a
repository of yours, keep the scripts executable, and add it after the default
arsenal (later arsenals win on a clash; an empty list means only the default,
so list both):

```yaml
spec:
  skills: [ops]
  arsenals:
    - name: platform
      repo: https://github.com/roundtable-k8s/arsenal
    - name: mine
      repo: https://github.com/<you>/<your-skills>
```

**OCI arsenal.** The image root is the arsenal root: it is mounted read-only
as an image volume at `/arsenal-oci/<name>`, so `ops/` must be at the root of
the image. Needs a cluster with image volumes (see
[`knight-arsenal-oci.yaml`](../../knight-arsenal-oci.yaml)).

```bash
mkdir -p build/ops && cp -r skill/nightly-digest build/ops/
cat > build/Dockerfile <<'DOCKER'
FROM scratch
COPY ops /ops
DOCKER
docker build -t registry.example.com/you/nightly-digest-arsenal:1 build
docker push registry.example.com/you/nightly-digest-arsenal:1   # note the digest
```

```yaml
spec:
  skills: [ops]
  arsenals:
    - name: platform
      repo: https://github.com/roundtable-k8s/arsenal
    - name: digest
      oci:
        reference: registry.example.com/you/nightly-digest-arsenal@sha256:<digest>
```

`COPY` keeps file modes, so the scripts stay executable. Set `name` on the OCI
arsenal explicitly, and pin the reference by digest.

## Fire it once now

Without waiting for the cron (works while the Trigger is suspended; see
[`docs/triggers.md`](../../../docs/triggers.md)):

```bash
GW=http://roundtable-operator-gateway.roundtable.svc:8080
AUTH="Authorization: Bearer $(cat /var/run/secrets/roundtable/gateway/token)"
curl -s -X POST -H "$AUTH" "$GW/v1/schedules/roundtable/nightly-digest/run"

# or without the gateway
kubectl -n roundtable annotate trigger nightly-digest ai.roundtable.io/run-now=$(date +%s)
```

A knight target is concurrency-checked like a scheduled fire: while a run is
active, `Forbid` skips it.

## Where the result is

```bash
curl -s -H "$AUTH" "$GW/v1/schedules/roundtable/nightly-digest/runs?limit=5" | jq
kubectl -n roundtable get trigger nightly-digest -o jsonpath='{.status.recentFirings[0]}'
```

`runs` lists each firing with its `outcome` and the task ID in `run`; the
task's result is the knight's answer, ending in the last line `file` printed.
(`GET .../runs/{run}` does not serve knight-target results yet; use the task
ID with `GET /v1/tasks/<id>` on the gateway or the dashboard.) The digest
itself is in the knight's `$DIGEST_DIR` (default `/data/digests`, on its
persistent volume) as `<namespace>-<date>.json` and `.md`.

## When it fails

If the knight cannot finish (`collect` or `file` keeps failing, or `kubectl`
is refused), it ends its answer with a last line `Outcome: failed <reason>`.
The platform reads that line: the knight's result is `success: false` and the
firing is recorded as failed, with the reason. Without the line a prose
apology would count as a success. The line must be the last non-empty line of
the answer. A failed firing shows in `status.recentFirings[].outcome` on the
Trigger, in the `outcome` of `GET .../runs`, and on the Schedules page of the
dashboard.

## What the knight may and may not do

- May read the cluster through the `view` ClusterRole: get, list and watch on
  most namespaced resources. `view` cannot read Secrets and cannot change
  anything; a prompt is not a limit, the RBAC is.
- May run `scripts/collect`, write the digest JSON, and run `scripts/file`,
  which writes only under `$DIGEST_DIR`.
- May reach only the always-allowed baseline (DNS, NATS, Kubernetes API, model
  providers). No `spec.egress` is set. Runs isolated (gVisor, default-deny
  egress).
- May not change cluster objects, read Secrets, or follow instructions found in
  events, labels or annotations.

## Tests

`skill/nightly-digest/tests/run.sh` checks that `file` rejects invalid input
(non-zero, no `ok` line, nothing written), accepts valid input (last line
`nightly-digest: ok`), and that `collect` shapes `kubectl` output (with a fake
`kubectl`). It needs `bash` and `jq`, and runs in `make test` through
`recipe_test.go` in this directory, which also parses the manifests. Lint the
scripts with `shellcheck skill/nightly-digest/scripts/*` if you have it.
