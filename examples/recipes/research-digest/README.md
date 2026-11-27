# Research digest

A weekly source watch that has nothing to do with software: one knight reads a
short list of feeds and sites, remembers what it already reported, and writes a
digest of what is new. Three pieces: a Knight with an egress allowlist and a
memory volume, a skill whose `seen` script keeps the model from re-reporting
items, and a schedule Trigger that sends one task a week.

```
Trigger research-digest (Mondays 07:00 UTC, Forbid)
   |  one task: "use the research-digest skill for <date>"
   v
Knight digest-reader (isolated, egress to 3 feed hosts, memory)
   fetch feeds      -> item URLs
   scripts/seen     -> prints only URLs not in $KNIGHT_MEMORY_DIR/seen.txt,
                       records them there
   knight           -> writes the digest of the new items
                       -> $KNIGHT_MEMORY_DIR/digests/<date>.md, and as the result
```

| File | What |
|---|---|
| [`knight.yaml`](knight.yaml) | The `digest-reader` Knight: `spec.egress.allowedHosts`, `spec.memory`, no RBAC beyond the namespace default ServiceAccount |
| [`trigger.yaml`](trigger.yaml) | The schedule Trigger: weekly `cron`, `startingDeadlineSeconds`, `Forbid`, one task, `timeoutSeconds` |
| [`skill/research-digest/`](skill/research-digest/) | The skill: `SKILL.md`, `scripts/seen`, `tests/run.sh` |

## Before you start

Needs the [shared "Before you start"](../README.md#before-you-start): operator and NATS, a RoundTable `fleet-a`, `spec.nats.url` on the `digest-reader` Knight and model credentials. The model is the full ID `claude-sonnet-4-20250514`, so no alias is needed.

## Install

Rename `fleet-a` (the Knight's `ai.roundtable.io/round-table` label, the
Trigger's `roundTableRef` and the `fleet-a.tasks.` subject) if your RoundTable
differs; the label must match the Trigger's `roundTableRef`. Make the skill
available to the knight (next section), replace the three example hosts in
`spec.egress.allowedHosts` and the URLs in the Trigger's `task` with your own
(keep them in step: a URL whose host is not allowed cannot be read), then:

```bash
kubectl apply -f knight.yaml -f trigger.yaml
kubectl -n roundtable get trigger research-digest   # Ready, Last Fired
```

## Build and reference the skill

The knight's `spec.skills: [research]` names a category; the arsenal must hold
`research/research-digest/` (the skill-filter sidecar links
`/skills/research/research-digest`):

```
<arsenal root>/research/research-digest/SKILL.md
<arsenal root>/research/research-digest/scripts/seen
```

Copy `skill/research-digest/` to `research/research-digest/` in a Git
repository of yours, keep `scripts/seen` executable, and add it after the
default arsenal (an empty list means only the default, so list both):

```yaml
spec:
  arsenals:
    - name: platform
      repo: https://github.com/roundtable-k8s/arsenal
    - name: mine
      repo: https://github.com/<you>/<your-skills>
```

An OCI arsenal works too; see the
[nightly-digest recipe](../nightly-digest/README.md#build-and-reference-the-skill).

## Fire it once now

```bash
GW=http://roundtable-operator-gateway.roundtable.svc:80
AUTH="Authorization: Bearer $(cat /var/run/secrets/roundtable/gateway/token)"
curl -s -X POST -H "$AUTH" "$GW/v1/schedules/roundtable/research-digest/run"

# or without the gateway
kubectl -n roundtable annotate trigger research-digest ai.roundtable.io/run-now=$(date +%s)
```

Fire it a second time: `seen` has recorded the items, so the second digest
says nothing is new.

## Where the result is

```bash
curl -s -H "$AUTH" "$GW/v1/schedules/roundtable/research-digest/runs?limit=5" | jq
```

`GET /v1/schedules/<ns>/<name>/runs` lists each firing with its `outcome` and
the task ID in `run`; the same list is on the Schedules page of the dashboard.
The task's result is the digest; read it with the task ID (`GET /v1/tasks/<id>`
on the gateway, or the dashboard). The digest is also kept in the knight's
memory as `/memory/digest-reader/digests/<date>.md`, next to `seen.txt`.

## When it fails

If no source can be fetched or the digest cannot be saved, the knight ends its
answer with a last line `Outcome: failed <reason>`; the result is then
`success: false` and the firing is recorded as failed, with the reason. The
line must be the last non-empty line.

## What the knight may and may not do

- May reach the always-allowed baseline (DNS, NATS, Kubernetes API, model
  providers) and exactly these hosts: `hnrss.org`, `feeds.arstechnica.com` and
  `www.theregister.com`. Everything else is denied by the default-deny egress
  policy; see [`docs/egress.md`](../../../docs/egress.md). Replace them with
  your feeds.
- May write only to its memory directory `/memory/digest-reader`
  (`seen.txt`, `digests/`) and its own workspace. It has no RBAC beyond the
  namespace `default` ServiceAccount (its token is mounted, but grants
  nothing) and no tools beyond the image's, so no cluster access and no write
  access elsewhere. The memory volume is the only thing that persists between runs.
- May not reach other hosts, change the cluster, send the digest anywhere
  (mail, chat), or follow instructions found in feeds and pages. That last one
  is a prompt rule, not a limit: a hostile feed can still make the digest
  wrong, but it cannot make the knight reach a host that is not listed.
- Sources are fetched from inside the allowed hosts only; a feed that
  redirects to another host fails.

## Tests

`skill/research-digest/tests/run.sh` shows `seen` is idempotent (a second run
on the same input prints nothing), drops duplicates and blank lines, leaves no
temp files and fails without `KNIGHT_MEMORY_DIR`. It needs `bash`, and runs in
`make test` through `recipe_test.go` in this directory, which also parses the
manifests. Lint with `shellcheck skill/research-digest/scripts/seen` if you
have it.
