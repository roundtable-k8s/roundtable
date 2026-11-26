# Migrating from kagent

This guide turns a kagent agent into a **job Chain run by a persona Knight**
and moves its caller from kagent's A2A endpoint to the
[gateway](gateway.md) API. You don't need to read the operator code.

## Agent = job, Knight = persona

- A **Knight** is a persona: identity, default model, tools and limits
  (`spec.prompt`, `spec.model`, `spec.tools`, `spec.serviceAccountName`,
  `spec.vault`, `spec.env`).
- A **Chain** is a job: its own prompt (the step `task`), its own `model` and
  its own input.
- A kagent agent is a job, not a persona. Port each agent to a Chain.

**Grouping rule:** jobs share a persona only when they need the same trust
level: the same tools, the same Kubernetes and vault access, the same
network reach. A job that needs more than the others gets its own persona.

- *Shared:* `triage-alert` and `summarize-report` only read text they are
  given and need no tools. Both run on the tool-less persona `reader` in
  [`examples/knight-persona-jobs.yaml`](../examples/knight-persona-jobs.yaml).
- *Kept apart:* a job that writes notes to the vault (`vault.writablePaths`)
  or reads the cluster (`serviceAccountName`) must not run on `reader`.
  Give it its own Knight, even if its prompt looks similar. Anything on a
  persona is available to every job on it.

## Field mapping

| kagent | Round Table |
|--------|-------------|
| `AgentTemplate.systemPrompt` | The job Chain step's `task`, verbatim, then `{{ .Input }}` for the caller's request (`ChainStep.Task`, `Chain.spec.input`) |
| `ModelConfig` | The step's `model` (`ChainStep.Model`): a model ID or a RoundTable `modelAliases` name. It must suit the knight's runtime: Claude models on claude-knight, `provider/model` on pi-knight |
| `Agent` (+ `Harness`) | A persona `Knight`: `spec.image` (claude-knight or pi-knight), `spec.prompt.identity` / `spec.prompt.instructions` (who it is and its limits only, not the job), `spec.tools.nix`, `spec.serviceAccountName`, `spec.concurrency`, `spec.workspace` |
| Vault MCP server | `spec.vault` (`claimName`, `readOnly`, `writablePaths`), mounted at `/vault` |
| `Harness` tool settings | `spec.env`: `CLAUDE_DISALLOWED_TOOLS` on claude-knight |
| `RemoteMCPServer` | `spec.mcpServers` (`name`, `url`, optional `headersFrom` to send a token from a Secret key as a header such as `Authorization`, see [`examples/knight-mcp-headers.yaml`](../examples/knight-mcp-headers.yaml)). The operator allows the server's URL in the knight's egress by itself. A CLI tool in `spec.tools.nix` or a skill also works. `MCP_SERVERS_JSON` in `spec.env` is the alternative on claude-knight |
| `EgressPolicy` | `spec.egress` per-knight allow-lists. A knight is isolated by default (deny-all) and reaches only what its egress allows, see [egress](egress.md) |
| Sandbox / isolation | `spec.runtimeClassName` (e.g. gVisor), see [runtime isolation](runtime-isolation.md) |

### Tool-less is not tool-free on claude-knight

A persona with no `spec.tools` on claude-knight still has Claude Code's
built-in tools (Bash, Read, Write, WebFetch, ...) and the knight-to-knight
tools. Disable what the persona must not use with `CLAUDE_DISALLOWED_TOOLS`
(built-in names and `mcp__<server>__<tool>` names, comma-separated):

```yaml
env:
  - name: CLAUDE_DISALLOWED_TOOLS
    value: "Bash,Read,Write,Edit,MultiEdit,Glob,Grep,WebFetch,WebSearch,NotebookEdit,Task,Agent,TodoWrite,mcp__roundtable__nats_request,mcp__roundtable__nats_publish"
```

## The job Chain

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: Chain
metadata:
  name: triage-alert
  namespace: roundtable
  labels:
    ai.roundtable.io/invokable: "true"   # required for the gateway to run it
spec:
  steps:
    - name: triage
      knightRef: {name: reader}
      model: haiku
      task: |
        <the kagent systemPrompt, verbatim>

        {{ .Input }}
```

Leave out `schedule` for agents that another system calls: those are called,
not scheduled, and so are these Chains. An agent that kagent ran from a CronJob
becomes a scheduled job instead, see [Scheduled jobs](#scheduled-jobs).

## Scheduled jobs

A job that kagent ran on a cadence (a CronJob that sent the agent a message)
does not need a Chain or a caller. The standard migrated job
([decision 25](decisions.md)) is a **skill with scripts** in the arsenal plus a
**schedule that sends one task** to a persona knight naming the skill and
script. The skill's script does the real work and the validation; the knight
runs it. Round Table runs the schedule as an operator-managed CronJob, so the
old wrapper CronJob and its access rules go away.

Layout of the skill: `<group>/<skill>/SKILL.md` and
`<group>/<skill>/scripts/`, mounted in the knight at
`/skills/<group>/<skill>/scripts/`. The Trigger:

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: Trigger
metadata:
  name: nightly-file-report
  namespace: roundtable
spec:
  source:
    schedule:
      cron: "30 3 * * *"
      timeZone: Europe/London
      startingDeadlineSeconds: 3600
  target:
    knight:
      name: tristan
      task: |
        Use the ops/nightly-report skill and run its run-job script
        (/skills/ops/nightly-report/scripts/run-job) for {{ .Event.Time.Format "2006-01-02" }}.
        Reply with the script's last output line.
      timeoutSeconds: 1800
  concurrency:
    policy: Forbid
```

What happens on missed, overlapping, lost and deleted runs is in
[Scheduled work](scheduled-work.md#when-a-run-is-missed-overlapping-lost-or-deleted).

## Moving a cron job

A cron job that ran a script (collect some data, ask an agent to judge it,
file the result) moves in four steps: shape the script, change the persona
knight, send one task from a Trigger, then run old and new side by side before
the cutover.

### 1. The script contract

Split the script into two modes. The agent does only the judging between them.

- `--collect` prints one JSON document: the items to judge, the instructions
  and the shape of the answer. It **writes nothing**.
- `--file ANSWERS.json` validates the answer and does **every write**. When the
  answer is bad it exits non-zero with a reason on stderr. When it succeeds it
  prints a fixed last line, `<job>: ok`.
- The old wrapper mode (the one the cron job calls) stays in the script until
  the cron job retires, so both can run side by side.

```sh
S=/skills/ops/nightly-report/scripts
$S/run-job --collect > /tmp/collect.json   # items, instructions, answer shape; no writes
# the knight reads collect.json and writes /tmp/answers.json
$S/run-job --file /tmp/answers.json        # validates, writes, prints "nightly-report: ok"
```

### 2. Change the persona knight

- **The script** arrives as a skill in an arsenal: `spec.arsenals` takes a git
  source or an OCI image ([`examples/knight-arsenal-oci.yaml`](../examples/knight-arsenal-oci.yaml)).
  Pin an OCI reference by digest (`@sha256:...`), not by tag.
- **Interpreters** the script needs (`python3`, `jq`) go in `spec.tools.nix`.
- **Vault output**: `spec.vault.writablePaths` lists only the output folder
  the job writes to, nothing wider.
- **Model**: a job that needs a different model than the knight sets
  `target.model` on the Trigger (a model ID or a `modelAliases` name); it
  applies to that task only.

```yaml
spec:
  tools:
    nix: [python3, jq]
  vault:
    claimName: vault
    writablePaths:
      - Reports/nightly-new
```

### 3. The Trigger task text

The task names the skill and the two commands, and tells the knight what its
answer is:

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: Trigger
metadata:
  name: nightly-report
  namespace: roundtable
spec:
  source:
    schedule:
      cron: "30 3 * * *"
      timeZone: Europe/London
      startingDeadlineSeconds: 3600
  target:
    knight:
      name: tristan
      model: haiku
      task: |
        Use the ops/nightly-report skill for {{ .Event.Time.Format "2006-01-02" }}.
        Run `run-job --collect`, judge the items as its instructions say, write
        your answer in the shape it gives to /tmp/answers.json, then run
        `run-job --file /tmp/answers.json`. Your answer is the last line `file`
        printed.
        If `--file` rejects the answer and you cannot fix it, end with
        `Outcome: failed <reason>`. Never claim success without the `ok` line.
      timeoutSeconds: 1800
  concurrency:
    policy: Forbid
```

### Failing loudly

A cron job that fails shows up as a failed Job. A knight that merely says it
could not finish shows up as a success. To make a failure visible, the task
text tells the knight to end with a final line:

```text
Outcome: failed <reason>
```

That line turns the task into `success: false` (the knight runtime
parses it, `parseTaskFailure` in knights `packages/core/src/structured.ts`). The firing is then recorded with `outcome: failed` in the
Trigger's `status.recentFirings`, in the schedule run history, and on the
Schedules page of the dashboard:

```sh
kubectl -n roundtable get trigger nightly-report -o jsonpath='{.status.recentFirings[0]}'
```

Tell the knight to use it when `--file` rejects the answer and it cannot fix
it, and never to claim success without the script's `ok` line. See
[Scheduled work](scheduled-work.md#a-job-that-is-one-task-naming-a-skill-and-a-script)
and [See and control schedules](scheduled-work.md#see-and-control-schedules).

### Side by side, then cutover

1. Deploy the new Trigger writing to a **separate** folder: a different
   `OUTPUT_DIR` for the script and a matching `writablePaths` entry
   (`Reports/nightly-new` next to the old `Reports/nightly`). Leave the old
   cron job running.
2. Let both run for 3 to 7 runs. Compare the dated outputs by hand, one day at
   a time: same days written, same items, differences you can explain. Check
   `status.recentFirings` for failed or skipped firings.
3. Cutover: point `OUTPUT_DIR` and `writablePaths` at the real folder (drop the
   `-new` one), and suspend the old cron job
   (`kubectl -n <ns> patch cronjob <name> -p '{"spec":{"suspend":true}}'`).
4. Delete the old cron job a week later, once nothing needed it.

A job that another job links to must keep its output path at cutover: change
the folder the new job writes to, not the path its readers use.

## The caller

kagent's CronJobs send an A2A `SendMessage` to
`http://kagent-controller.kagent.svc:8083/agents/kagent/<name>`, block, and
read the last text artifact.

Before (A2A JSON-RPC):

```json
{"jsonrpc": "2.0", "id": "1", "method": "SendMessage",
 "params": {"message": {"role": "user", "parts": [{"kind": "text", "text": "<alert text>"}]}}}
```

```json
{"jsonrpc": "2.0", "id": "1",
 "result": {"artifacts": [{"parts": [{"kind": "text", "text": "<answer>"}]}]}}
```

After (`POST /v1/chains/triage-alert/runs` on the gateway, with
`Authorization: Bearer <token>`):

```json
{"input": "<alert text>", "waitSeconds": 600}
```

```json
{"run": "triage-alert-gw-1a2b3c4d", "chain": "triage-alert", "namespace": "roundtable",
 "phase": "Succeeded", "done": true,
 "steps": [{"name": "triage", "phase": "Succeeded", "output": "<answer>"}],
 "output": "<answer>"}
```

`output` is the final step's output. The answer is **200** when the run
finished and **202** when it is still running after `waitSeconds`; poll
`GET /v1/runs/{namespace}/{run}?waitSeconds=`. See
[Chain runs](gateway.md#chain-runs) for every field, and
[Waits longer than 10 minutes](gateway.md#waits-longer-than-10-minutes) for
timeouts in front of the gateway (kagent waits ran 300 s to 1500 s).

### Python helper

Standard library only. `run_chain` returns the output and raises
`RunChainError` when the run fails, the gateway rejects the request or
`timeout` passes. It raises the subclass `UsageLimited` when a step is
refused because the knight's plan quota is used up
([#355](https://github.com/roundtable-k8s/roundtable/issues/355)).

A usage-limited refusal is not a failure: the chain defers that step until
the plan window resets. The run stays not `done`, and the step shows
`phase: "Deferred"`, usually with the refusal in `error` (a refused loop step
can be `Deferred` with `error` empty). The helper raises as soon as
it sees such a step, so the caller can retry later instead of waiting out
`timeout`.

```python
import json
import time
import urllib.error
import urllib.request


class RunChainError(Exception):
    """The run failed, or the gateway rejected the request."""


class UsageLimited(RunChainError):
    """A step was refused because the knight's plan quota is used up.

    The chain defers the step until the plan window resets; it has not failed.
    Retry later (`resets` carries the refusal text, which may name the reset time; it can be empty).
    """

    def __init__(self, message, resets=""):
        super().__init__(message)
        self.resets = resets


def _call(method, url, token, body=None, timeout=60):
    req = urllib.request.Request(
        url,
        method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.load(resp)  # 200 and 202 both carry the run record
    except urllib.error.HTTPError as e:
        raise RunChainError("gateway %s: %s" % (e.code, e.read().decode(errors="replace"))) from e


def run_chain(base_url, token, chain, input, timeout=900, namespace=None):
    """Run a Chain through the gateway and return the final step's output.

    Raises UsageLimited if a step is deferred by a plan-quota refusal,
    RunChainError if the run fails or `timeout` seconds pass.
    """
    base = base_url.rstrip("/")
    deadline = time.monotonic() + timeout
    body = {"input": input, "waitSeconds": min(int(timeout), 1500)}
    if namespace:
        body["namespace"] = namespace
    # The HTTP timeout must outlast waitSeconds.
    rec = _call("POST", base + "/v1/chains/%s/runs" % chain, token, body, timeout=body["waitSeconds"] + 30)
    while True:
        for step in rec.get("steps") or []:
            if step.get("phase") == "Deferred":
                err = step.get("error", "")
                raise UsageLimited("step %s deferred: %s" % (step.get("name"), err or "usage limit"), err)
        if rec.get("done"):
            break
        left = deadline - time.monotonic()
        if left <= 0:
            raise RunChainError("run %s not finished after %ss" % (rec.get("run"), timeout))
        wait = int(min(left, 300))
        url = "%s/v1/runs/%s/%s?waitSeconds=%d" % (base, rec["namespace"], rec["run"], wait)
        rec = _call("GET", url, token, timeout=wait + 30)
    if rec.get("phase") != "Succeeded":
        errs = ["%s: %s" % (s.get("name"), s.get("error")) for s in rec.get("steps") or [] if s.get("error")]
        raise RunChainError("run %s %s. %s" % (rec.get("run"), rec.get("phase"), "; ".join(errs)))
    if "output" not in rec:
        raise RunChainError("run %s has several final steps; read steps" % rec.get("run"))
    return rec["output"]
```

```python
try:
    out = run_chain(GATEWAY, TOKEN, "triage-alert", alert_text, timeout=600)
except UsageLimited as e:
    ...  # retry later; e.resets holds the refusal text, which may name the reset time
```

## Checklist

For a chat or API agent (a Chain):

1. Create the persona Knight, or reuse one at the same trust level.
2. Add the job Chain: the old `systemPrompt` as the step `task`, ending in
   `{{ .Input }}`, a step `model`, the `ai.roundtable.io/invokable` label.
3. Run old and new on the same inputs and compare the answers.
4. Switch the caller to the gateway (`run_chain` or the `run_chain` MCP tool).
5. Delete the kagent Agent.

For a cron job (a scheduled knight):

1. Split the script into `--collect` and `--file`, keeping the old wrapper mode.
2. Ship it as a skill in an arsenal (OCI pinned by digest); add `spec.tools.nix`
   and a `spec.vault.writablePaths` entry for the new output folder only.
3. Add the Trigger: the task names the skill and both commands, and says to end
   with `Outcome: failed <reason>` when `--file` rejects the answer.
4. Run side by side for 3 to 7 runs into the separate folder; compare.
5. Cut over: switch the folder (keep paths other jobs link to), suspend the old
   cron job, delete it a week later.
