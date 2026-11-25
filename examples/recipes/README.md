# Recipes

A recipe is a fleet packaged as a directory you can copy: the Knights, Chains
and Triggers, plus a README with how to install and test it and what the
knights may and may not do. Each recipe is applied on top of a RoundTable (see
[`../roundtable-fleet.yaml`](../roundtable-fleet.yaml)).

| Recipe | What it does |
|---|---|
| [`homelab-watch/`](homelab-watch/) | An Alertmanager webhook Trigger starts a read-only triage chain on a knight ([triggers docs](../../docs/triggers.md#webhook-source)) |
| [`personal-fleet/`](personal-fleet/) | Three persona knights (research, infrastructure, editor) on a shared notes volume, a scheduled `morning-briefing` Chain and an on-demand chain run through the gateway |

More recipes (the dev factory) follow.
