# Decision register

The product decisions taken with the owner during two whiteboard sessions (2026-11-24 and 2026-11-25), numbered 1 to 31. The [principles](principles.md) say what Round Table believes; this register records the individual decisions behind them and where they led.

This is a record, not a design document. Where the owner is quoted, the words are kept as spoken.

## How to use the register

- **Cite the number**: "decision 8", "decision 30". Issues and pull requests already do.
- A later decision can replace an earlier one. When it does, both entries say so: the old one is marked **Superseded by N** and the new one says **Supersedes M**. Nothing is deleted.
- **Status** marks each decision: **Standing** (in force, work may be open), **Done** (the work it led to is merged), **Parked**, or **Superseded**. Status was checked against the linked issues on 2026-11-25. No decision is superseded yet; decisions 25 and 30 refine or extend earlier ones.
- The register does not change decisions. To change one, add a new decision as described below.

## How a decision is added

Open a pull request that adds a new, dated entry at the end of the register with the next free number: the decision in one or two sentences, the reasoning, the owner's quoted words where there are any, and the issues it affects. If it replaces an earlier decision, update that entry's status in the same pull request. Cite the new number from the issues it affects.

## A. What Round Table is

### 1. An opinionated open-source reference platform
*2026-11-24. Status: Done (see [principles](principles.md)).*

A showcase people can copy. Not a company, and not a neutral CNCF-style tool. The owner's own fleets are the showcase. The opinions are written down as a principles doc. The install has to work but does not need to be generic. Standards are adopted only where they are part of an opinion.

### 2. Platform principles (the DNA)
*2026-11-24. Status: Done; extended by decision 30.*

1. Agents are infrastructure (CRDs, GitOps, reviewed like code).
2. Specialists, not generalists.
3. Messages, not calls (NATS JetStream).
4. Any model, chosen by cost.
5. Humans set goals, agents do the rest.
6. Isolated by default.
7. Reproducible capabilities (skills in git, pinned Nix tools).
8. You can watch everything.

A ninth, "don't rebuild the wheel", was added by decision 30 (#579, closed).

### 3. Not platform DNA: recipes
*2026-11-24. Status: Standing.*

The self-improving dev factory, the Obsidian vault and briefings are only how the owner uses Round Table. They become **recipes**. See [recipe: fleet self-improvement](recipe-fleet-self-improvement.md).

### 4. Extract the fleet-specific code from the core
*2026-11-24. Status: Standing. Led to #399 (generic triggers and recipes), #176 (closed).*

The fleet-specific code was built in to get things working first. Approach:

- a generic **trigger** mechanism ("when X happens, start chain Y with input"; X = schedule, webhook, NATS message or GitHub event; it grows out of #176);
- re-implement issuePickup and prFeedback as GitHub triggers, keeping the old fields as deprecated aliases;
- move the pipeline's GitHub checks into chain steps or skills;
- package the results as recipes (dev factory, personal fleet, homelab watch).

Needed a new epic.

### 5. Three rungs of work, all built from the same primitives
*2026-11-24. Status: Standing.*

1. Ask a knight: a single call.
2. Run a chain: a declared, repeatable job.
3. Launch a mission: a large multi-agent, multi-step goal. A planner composes standing knights and chains.

Missions are the showcase but the least proven part, so one real end-to-end mission run comes before new mission features.

## B. Interaction

### 6. The primary interface is the HTTP/MCP API, plus dashboard chat built on it
*2026-11-24. Status: Standing. Led to #176 (phase 1, closed), ui#162. See [gateway](gateway.md).*

The invoke gateway is the primary interface, packaged with the operator, and dashboard chat is built on it.

- The gateway is core and ON by default in the chart.
- The API covers all three rungs: ask a knight, run a chain (#176 phase 1), launch a mission, and status/results.
- The dashboard uses the same API for chat (ui#162), dispatch and run-now, not NATS directly.
- No CLI for now.
- M6 mostly dissolves: Claude is just an MCP client, and what remains is exposing the API safely outside the cluster (auth, Cloudflare).

### 7. No approval gates. Autonomy is the point.
*2026-11-24. Status: Done. Led to closing #376.*

Safety comes from constraint: least-privilege, narrowly scoped credentials; sandboxing; network limits; budgets; after-the-fact observability and revocation. If an action is too dangerous unattended, the knight does not get the capability. #376 was closed. Human checkpoints, if anyone wants them, are a recipe concern.

## D. Trust and safety

### 8. Every knight is isolated by default
*2026-11-24. Status: Standing. Led to #164 (PR #383), #212, #347, #348, #349, #368 (all closed). See [runtime isolation](runtime-isolation.md) and [egress](egress.md).*

gVisor plus default-deny egress. Opting out must be explicit in the spec, and each knight reaches only its allow-listed hosts.

- Hard order before the old fleet returns: #164 (PR #383) → #212 gVisor on the cluster → #347–#349 per-knight egress → turn the defaults on in the chart → add an allowlist to every restore draft.
- Agent Sandbox is the backend for ephemeral and mission knights (#368); gVisor runs under both backends.

### 9. Credentials stay in the pod for now
*2026-11-24. Status: Standing. Related: #169 (open).*

As narrow and short-lived as possible. Egress credential injection (#169, the Envoy AI Gateway idea) is a future exploration.

## C. Knights working together

### 10. Memory is pluggable with a simple default
*2026-11-24. Status: Standing. Led to #371 (closed), #211 (open), pi-knight#28. See [memory](memory.md).*

- Default: plain files on a PVC.
- Shared memory is an RWX volume several knights mount (the owner: PVCs plus the Obsidian vault).
- A per-knight RWX memory volume, separate from the scratch workspace, fixes memory being lost on scaling and restarts. To be filed as work.
- Convention: each knight writes only its own paths and reads everyone else's (`writablePaths`; #371 was the bug that broke this).
- Plug-in point: a volume or an MCP server (#211).
- Stretch: non-filesystem shared memory (NATS KV/object store, vector DB) for clusters without RWX or for semantic recall.

### 11. Agent cards and discovery, on NATS
*2026-11-24. Status: Standing. Led to pi-knight#15 (roster), #174 (open). See [agent cards](agent-cards.md).*

A2A-inspired; everything stays on NATS.

- The **RoundTable controller writes and maintains a card per knight** in NATS KV (key `<table>.<knight>`). It is generated from the Knight spec: domain, model, tools, and skills with descriptions from SKILL.md. Optional overrides go in the spec. The card exists even when the knight is scaled to zero, and a KV watch gives a live roster.
- Knight pods register as **NATS micro services** (`$SRV.PING/INFO/STATS`) for liveness and load.
- A `find_knights` tool in both runtimes.
- Keep the card fields close to A2A's schema, so the gateway could serve them to outside A2A agents later.

### 12. Never lose a task, never silently repeat one
*2026-11-24. Status: Done. Led to #177 (closed).*

- Execution stays at most once automatically.
- A task whose knight dies or that fails goes to a dead-letter stream with the reason (via the JetStream max-deliveries advisory, so nothing vanishes).
- The dashboard shows failed and lost tasks, and **Retry** (dashboard or API) republishes with an attempt number so the knight checks for partial work.
- Chains keep their own step retries.
- #177 was rewritten to this.

## E. Leaving kagent

### 13. A kagent agent is a JOB, not a persona
*2026-11-24. Status: Standing. Led to #214, #369 (both open). Refined by decision 25.*

Knight = persona; chain = job. Each kagent agent becomes a chain run by one persona knight:

| Knight | Chains |
|---|---|
| galahad | security-brief |
| kay | feeds-digest, night-editor |
| tristan | nightwatch |
| librarian | reading-processor, link-suggester, blog-scout, blog-drafter, voice-processor |
| its own knight (or kay) | intel-triage, the pilot |

- Retire flux-ops, k8s-reader, claude, feeds-curator and voice-librarian. Their personas cover them, or they are unused.
- Callers (vault-jobs CronJobs, OpenCTI) call the API and run the chain. That needs:
  - an API "run chain and wait for the result" mode;
  - synchronous waits longer than 10 min (callers wait 1500 s);
  - chain input passed through the API.
- Rule: group jobs into one persona only when they need the same trust level (that is why librarian is separate from patsy).
- Morning brief, old chain vs night-shift: not yet answered.
- #214 and #369 were rewritten around "personas run jobs as chains". See [migrating from kagent](migrating-from-kagent.md).

## F. Telemetry

### 14. One trace per chain run or mission
*2026-11-24. Status: Standing. Led to #153 (open), #154 (closed).*

Tasks are child spans; a single "ask a knight" call is its own trace.

### 15. Capture prompts and responses in traces (on)
*2026-11-24. Status: Standing. See [observability](observability.md).*

Without them there is no way to tell where something went wrong.

- Redact credentials before export with the existing activity redaction helper.
- Who can view traces and how long they are kept follows from that.

### 16. Rename the knight metrics to one consistent `roundtable_*` scheme now
*2026-11-24. Status: Standing. Led to #154 (closed), #157 (open).*

- Every signal (metrics, traces, logs, activity events, cards) carries the **table** as a dimension, since there are many tables per install.
- The owner: "spend some cycles to come up with one polished standard". That is the #154 conventions work, done properly, as its own effort.

### 17. OTel did not work out of the box (fact as of 2026-11-24)
*2026-11-24. Status: Parked for the backend choice; the facts have since moved on (see [observability](observability.md)).*

- No component emitted OTel yet: operator, gateway, dashboard, pi-knight and claude-knight had no OTel SDK.
- The cluster had no trace backend or collector. It had Prometheus, VictoriaLogs plus fluent-bit, and Grafana.
- Claude Code has its own built-in OTel export, which claude-knight could switch on (claude-knight#2).
- **Backend choice: PARKED** until a clear winner exists (Tempo vs an LLM viewer such as Langfuse or Phoenix vs both).
- Still instrument with vendor-neutral OTel (GenAI conventions) and put a collector in front, so the backend can be plugged in later without code changes.

### 18. One polished, opinionated observability standard for agent fleets
*2026-11-24. Status: Standing. Ties to #153, #154, #156–#159, #331–#333.*

Round Table defines one standard, the way ELK, Prometheus + Grafana or the Victoria stack are standards for their eras.

- It covers collection (OTel collector), metrics, logs, traces with prompt content, dashboards and alerts, all with one naming scheme and the table as a dimension.
- It ships with the platform: a chart option, companion chart or recipe.
- It needs dedicated evaluation cycles (a spike issue). The backend pick stays parked until it is clear, but the evaluation starts.

### Open at the time of the first session

- Morning brief: the old chain vs night-shift.

## M8 whiteboard, 2026-11-25

### 19. Gateway callers authenticate with their Kubernetes ServiceAccount token
*2026-11-25. Status: Done. Led to #559 (closed).*

No stored caller tokens. The owner: "Serviceaccount token sounds like the right call".

### 20. Start very permissive
*2026-11-25. Status: Standing. Led to #559 (closed).*

Any authenticated in-cluster ServiceAccount may use the whole gateway API; the caller is logged. No per-table or per-chain grants in this release. The owner: "im not looking to make it harder to write chains ect. We can always do another pass for the next release".

### 21. Scheduled work is native to Round Table, run by operator-managed Kubernetes CronJobs
*2026-11-25. Status: Standing. Led to #564 (closed), #565 (open). See [triggers](triggers.md).*

A schedule can target a single task to a knight or a chain. The operator creates and owns the CronJob and the ServiceAccount it runs as, so no access rules are written by hand. The owner: "I want native, but I want to be able to schedule a task, or chain. and have the operator manage native kubernetes cronjobs. This would be much cleaner in my opinion, and solves the rbac too as its the operator managing it". The vault-jobs CronJobs that only wrap a kagent call are replaced by these; a wrapper script stays only where it does real work. #564 is the knight target; #565 is the CronJobs (after #559).

### 22. The personal table runs on the Claude plan
*2026-11-25. Status: Standing. Related: knights#99.*

claude-knight, not OpenRouter. The owner: "we should be using the claude plan in the personal table". Local models for pi-knight are tracked in knights#99.

### 23. The personal table moves to its own NATS names
*2026-11-25. Status: Standing.*

`personal.tasks.>` and `personal.results.>`, leaving the old fleet's names. The owner: "I'm good to migrate to a new nats personal.tasks ect."

### 24. Bring the whole roster back
*2026-11-25. Status: Standing. Related: #369 (open).*

The owner: "may as well bring the whole roster". patsy, bedivere, percival, agravain and watchman join the six already restored. Idle knights scale to zero (`lifecycle.suspendPolicy: auto`).

### 25. Straight to native for the kagent jobs, nightwatch first, no bridge through the old scripts
*2026-11-25. Status: Standing. Refines decision 13. Related: #214 (open).*

A migrated job is a skill with scripts in the arsenal plus a schedule that sends ONE TASK to a persona knight naming that skill; no chain needed and no new step type. The owner: "if we've got the scripts, I'd prefer to just use a task. the agent can do multistep without a chain. and all we have to say is to use this skill and this script." Validation lives in the skill's filing script.

### 26. Full CRUD for schedules: YAML in Git first, plus API and a dashboard page
*2026-11-25. Status: Standing. Related: #565 (open).*

The owner: "We'll need the whole crud pipeline, and it would be nice to have something in the ui too. I do everything declaritively so make sure we can create these as yaml in cluster."

Orchestrator defaults, **not yet confirmed by the owner**: Git-managed schedules are read-only in the UI (run now, suspend, resume excepted) with YAML export for UI-created ones; tristan does not get the Syncthing API key, and the rescan stays outside the job.

### 27. A planner per table, and pi-knight on local models
*2026-11-25. Status: Done for the planner (#566, closed); Standing for local models (knights#99).*

Both were filed at the owner's request.

### 28. Keep the sandbox on the reviewer; change how reviews work
*2026-11-25. Status: Standing. Led to an issue in the cluster repository.*

rt-lead stays under gVisor. Reviews read CI results (`gh pr checks`, failed logs) instead of re-running test suites or full builds; one targeted test at most, to settle a specific doubt. The owner: "keep the sandbox, change how reviews work".

### 29. MCP servers are listed directly on each knight; no catalogue on the table
*2026-11-25. Status: Done. Led to #577 (closed). Extended by decision 30.*

The owner expects Envoy AI Gateway to host and manage MCP centrally later, which would make a Round Table catalogue redundant. The owner: "does it make more sense to add them directly to the knights? I'm going to assume we'll add the envoy ai gateway to the cluster at some point to centrally host and manage mcp". #577 was rescoped to the one part useful either way: the operator derives the egress allowance from each mcpServers URL.

### 30. Don't rebuild the wheel
*2026-11-25. Status: Standing as a rule; the principle text is Done (#579, closed). Extends decisions 2 and 29.*

Round Table is not an MCP management tool. It does not manage which tools a knight may use, and it must not grow features that belong to a purpose-built component such as Envoy AI Gateway (MCP hosting and management, and by the same reasoning model routing and whatever else such a component already does well). The rule holds for most things: where something we would otherwise build already exists in a tool we can run beside Round Table, use that tool and keep Round Table to what only it does.

The owner, 2026-11-25: "we don't manage what tools are availble to the knights. note this on the whiteboard the roundtable is not an mcp management tool, we should not bake in extra feature that we should be getting from something like envoy ai gateway. this theorem stands for most things. don't rebuild the wheel where we don't have to."

Consequences so far: no table MCP catalogue (decision 29); kay keeps every tool its MCP servers offer (no toolFilter added).

## The orchestrator becomes a knight

### 31. `merlin`: the orchestrator is a knight on `roundtable-dev`, on Fable for everything
*2026-11-25, answered by the owner as short questions. Status: Standing.*

merlin does what the external session's 30-minute check does. Purpose: less context held in one long session, and a more hands-off factory.

- Check every 30 minutes (a scheduled task naming a skill and its scripts).
- **Decides open design questions itself from the principles and the decision register**, records the reasoning on the issue; product-direction and taste questions go to the owner.
- **Writes a few issues per check** when the queue runs low (the grooming recipe), no bulk mission.
- **In the cluster: read-only, plus it may send tasks to knights over NATS and retry dead-lettered tasks through the gateway.** Every change goes through a PR.
- **Cluster repository: opens PRs only; rt-lead reviews and merges them** like any other knight PR.
- **May re-run failed CI jobs**: the owner adds Actions read/write to the GitHub App.
- **Reports by dashboard chat on demand and an ntfy alert when blocked.** No daily vault note, no pinned issue.
- **Hand-over: run both for a day** (merlin does the checks, the session watches), then the session's checks stop.

## After the whiteboards

### 32. M6 stays parked: the API is not exposed publicly for a long while
*2026-11-26, given by the owner as an instruction to the orchestrator. Status: Parked. Refines decision 6. Affects #234, #235, #236, #237, #238.*

Nothing that publishes the gateway outside the cluster is built now. The owner's instruction, as it reached the orchestrator: "M6 stays parked; the owner does not want the API exposed publicly for a long while."

- #235 (connector profile), #236 (publishing the connector), #237 (origin provenance) and the epic #238 carry the `parked` label and stay in the M6 milestone. They come back only when the owner asks for public exposure.
- #234 (NATS users and permissions for every component) moved to M7, because it is useful without public exposure: it is judged as least-privilege work inside the cluster (decision 8), not as a step toward M6.
- NATS credentials generated inside the cluster by the chart or the operator are not an owner secret. Credentials the owner must create or store are.
- Decision 6 stands for everything inside the cluster: the gateway is still the primary interface. Only its last bullet, exposing the API safely outside the cluster, waits.
