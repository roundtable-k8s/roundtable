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

| kagent | Round Table | Not supported yet |
|--------|-------------|-------------------|
| `AgentTemplate.systemPrompt` | The job Chain step's `task`, verbatim, then `{{ .Input }}` for the caller's request (`ChainStep.Task`, `Chain.spec.input`) | |
| `ModelConfig` | The step's `model` (`ChainStep.Model`): a model ID or a RoundTable `modelAliases` name. It must suit the knight's runtime: Claude models on claude-knight, `provider/model` on pi-knight | |
| `Agent` (+ `Harness`) | A persona `Knight`: `spec.image` (claude-knight or pi-knight), `spec.prompt.identity` / `spec.prompt.instructions` (who it is and its limits only, not the job), `spec.tools.nix`, `spec.serviceAccountName`, `spec.concurrency`, `spec.workspace` | |
| Vault MCP server | `spec.vault` (`claimName`, `readOnly`, `writablePaths`), mounted at `/vault` | |
| `Harness` tool settings | `spec.env`: `CLAUDE_DISALLOWED_TOOLS` and `MCP_SERVERS_JSON` on claude-knight | |
| `RemoteMCPServer` | A CLI tool in `spec.tools.nix`, a skill, or `MCP_SERVERS_JSON` on claude-knight | Declarative MCP servers: [#211](https://github.com/roundtable-k8s/roundtable/issues/211) |
| `EgressPolicy` | `spec.egress` per-knight allow-lists, see [egress](egress.md) | Deny-all by default: [#387](https://github.com/roundtable-k8s/roundtable/issues/387) |
| Sandbox / isolation | `spec.runtimeClassName` (e.g. gVisor), see [runtime isolation](runtime-isolation.md) | |

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

Leave out `schedule`: kagent agents are called, not scheduled, and so are
these Chains.

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

1. Create the persona Knight, or reuse one at the same trust level.
2. Add the job Chain: the old `systemPrompt` as the step `task`, ending in
   `{{ .Input }}`, a step `model`, the `ai.roundtable.io/invokable` label.
3. Run old and new side by side on the same inputs and compare the answers.
4. Switch the caller to the gateway (`run_chain` or the `run_chain` MCP tool).
5. Delete the kagent Agent.
