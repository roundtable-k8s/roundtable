# Documentation index

Find a page by what you want to do. The sections go in the order a reader
usually needs them. The [README](../README.md) gives the overview.

## Start here

- [Getting started](getting-started.md): from nothing to a two-step Chain on a local kind cluster in about ten minutes.
- [Install](install.md): install the Helm chart on an existing cluster, with Flux or plain Helm.
- [Troubleshooting](troubleshooting.md): first-install problems, keyed on the conditions, reasons and events the operator sets.
- [Dependencies](dependencies.md): Kubernetes and NATS with JetStream are required; every other add-on is optional.

## Run a fleet

- [Triggers](triggers.md): start a Chain run, or send one task to a knight, from a cron time, a NATS message, a webhook or a GitHub event.
- [Scheduled work](scheduled-work.md): run a task, a Chain or a Mission on a cron cadence, written as a Trigger.
- [Dashboard](dashboard.md): the web UI's pages, what it can change, how to reach it and who can use it.
- [Invoke gateway](gateway.md): the synchronous HTTP front door to the knights, with a task ID to poll for slow answers.
- [Egress](egress.md): default-deny egress for knights that read untrusted web pages, feeds or issues, and how to allow a host.
- [Runtime isolation](runtime-isolation.md): knights run under a sandboxing RuntimeClass (gVisor) by default, and how to opt out.
- [Memory](memory.md): knights remember with plain files on volumes; there is no memory service.
- [Workspace snapshots](workspace-snapshots.md): take a CSI VolumeSnapshot of a knight's `/data` and start a new knight from it.
- [Observability](observability.md): how traces, metrics and logs are emitted, how to wire them to a backend, and the dashboards and alerts.
- [Migrating from kagent](migrating-from-kagent.md): turn a kagent agent into a job Chain run by a persona Knight.

## Reference

- [Helm values](helm-values.md): the full chart configuration reference.
- [CRD design](crd-design.md): the Knight, RoundTable, Chain, Mission and Trigger fields.
- [Knight task contract](knight-task-contract.md): how the operator hands a task to a knight runtime over NATS and what it expects back.
- [Agent cards](agent-cards.md): the JSON document each knight publishes saying who it is, what it can do and how to reach it.

## How it works

- [Principles](principles.md): what Round Table is and the principles it is built on.
- [Decisions](decisions.md): the decision register, with the owner's words where they were quoted.
- [Architecture](ARCHITECTURE.md): how the system runs today, covering the NATS/JetStream topology, the knight environment and request flow.

## Recipes

- [Issue pickup](issue-pickup.md): the operator polls GitHub for `rt:ready` issues, claims each one and starts a chain.
- [PR pipeline](pr-pipeline.md): the standard Chain that takes a Ready GitHub issue to an open pull request.
- [PR feedback](pr-feedback.md): the chain that responds when a PR from the pipeline goes red, leaves the merge queue or conflicts.
- [Fleet self-improvement](recipe-fleet-self-improvement.md): a periodic review in which knights report missing or broken tools and skills.
- [Keeping Mission output](keeping-mission-output.md): copy a Mission's results to a ConfigMap so they outlive the Mission.
- [Conditional steps](conditional-steps.md): `when` and bounded `loop` on Chain steps, evaluated by the operator without a knight task.
- Complete fleets you can copy are in [`examples/recipes/`](../examples/recipes/).

## Contributing and history

- [Local development](local-development.md): change operator code and see it run in a local kind cluster with `make tilt-up`.
- [Releasing](releasing.md): how to cut a release: tag only a commit whose CI run passed, after the chart version is set.
- [Design notes](design/): design records for individual features, such as the telemetry conventions, the observability standard and the runtime backends decision.
- [ADRs](adr/): architecture decision records, such as the use of standard Kubernetes types in the CRDs.
- [Architecture refactor](architecture-refactor.md): **historical**, a past design that may not match the code.
- [Dynamic missions (phase 3)](phase-3-dynamic-missions.md): **historical**, a past design that may not match the code.
