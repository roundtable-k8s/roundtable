# Recipes

A recipe is a fleet packaged as a directory you can copy: the Knights, Chains
and Triggers, plus a README with how to install and test it and what the
knights may and may not do. Each recipe is applied on top of a RoundTable (see
[`../roundtable-fleet.yaml`](../roundtable-fleet.yaml)).

## Before you start

Every recipe needs these; each recipe README says in one line which apply.

- **Operator installed**, with NATS (JetStream enabled) reachable: follow
  [`docs/getting-started.md`](../../docs/getting-started.md).
- **A RoundTable named `fleet-a`.** If yours has another name, rename `fleet-a`
  in the recipe: the Knights' `ai.roundtable.io/round-table` label, `roundTableRef`
  on Triggers and Chains, `spec.nats.subjectPrefix` and the `fleet-a.tasks.`
  subjects. Change them together.
- **The NATS URL, on the RoundTable and on every Knight.** A Knight does not
  take the RoundTable's URL; one without `spec.nats.url` connects to the CRD
  default, `nats://nats.database.svc:4222` (the author's server). No recipe
  Knight sets a URL, so add `spec.nats.url` to each Knight, and replace the
  URL in any RoundTable the recipe ships (see
  [Adapting the examples](../README.md#adapting-the-examples) for a `sed` line).
- **Model credentials.** Create the Secret your Knights read (for example
  `llm-api-keys` with `ANTHROPIC_API_KEY`); no credentials are in a recipe.
- **Model aliases.** A recipe that uses an alias such as `deep` or `fast` needs
  it in the RoundTable's `spec.modelAliases`; otherwise use a model name.

| Recipe | What it does |
|---|---|
| [`homelab-watch/`](homelab-watch/) | An Alertmanager webhook Trigger starts a read-only triage chain on a knight ([triggers docs](../../docs/triggers.md#webhook-source)) |
| [`personal-fleet/`](personal-fleet/) | Three persona knights (research, infrastructure, editor) on a shared notes volume, a scheduled `morning-briefing` Chain and an on-demand chain run through the gateway |
| [`dev-factory/`](dev-factory/) | Issue to merged PR: GitHub Triggers for pickup and PR feedback, the PR pipeline and feedback Chains, post-run checks as chain steps, a lead knight and a coder |
| [`backlog-grooming/`](backlog-grooming/) | A schedule Trigger starts a lead knight that reads a repository's open epics and files `groomed` sub-issues, capped per run, never `rt:ready` and never touching `needs-human` |
| [`nightly-digest/`](nightly-digest/) | A schedule Trigger sends one task to a read-only persona knight naming a skill; the skill's `collect` and `file` scripts gather, validate and write a namespace digest, with no chain |
| [`research-digest/`](research-digest/) | A weekly schedule Trigger sends one task to a knight with an egress allowlist and memory; the skill's `seen` script drops items already reported, and the digest is saved in memory |
