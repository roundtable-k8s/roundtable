# Recipe: fleet self-improvement

A periodic "what are we missing?" review for any table. Each knight reports
missing or broken tools and skills in a fixed shape, and a lead knight turns
the answers into a report, or into issues. It is a recipe, not platform
behaviour: one Chain, no new CRD, controller or approval UI. See
[principles](principles.md#platform-and-recipes).

Example: [`examples/chain-fleet-self-improvement.yaml`](../examples/chain-fleet-self-improvement.yaml).

**Knights never change their own manifests or skills.** The recipe only
produces a report (or issues and pull requests) that the owner reviews and
merges the usual way.

## Shape

| Step | Runs | What it does |
|---|---|---|
| `roster` | the lead | Lists the table's knights (`find_knights`, backed by the [agent cards](agent-cards.md)), or uses `spec.input` `{"knights": [...]}` when given. Returns `{"knights": [{"name", "domain"}]}`. |
| `assess` | the lead, once per knight | A [`forEach`](crd-design.md) over `steps.roster` (`path: .knights`, `maxParallel: 4`, `failurePolicy: continue`). The lead asks each knight for its self-assessment with `nats_request`. The lead's `spec.concurrency` must be at least `maxParallel`, or the extra items queue behind it. Worst case is roster 600s + 5 waves (`maxItems: 20` / 4) x 900s + report 1200s = 6300s, inside the chain `timeoutSeconds: 7200`; raise that if you raise `maxItems` or the step timeout. A step has a single `knightRef`, so the lead is the one that runs every item. |
| `report` | the lead | Reads the join (`[{index, item, phase, output, error}]`), groups the answers and writes one markdown report with `outputPath`. |

A commented `file_issues` variant files issues (or opens pull requests)
against the fleet's own manifests and arsenal repos. Grant the lead only the
repos it needs.

## Assessment shape

Each knight answers with this JSON; empty lists mean nothing to report:

```json
{
  "missing_tools": [
    {"name": "kubectl-neat", "nixpkgs": "kubectl-neat", "why": "Cleaning manifests before diffing them took 6 extra steps"}
  ],
  "missing_skills": [
    {"name": "cve-triage", "why": "Re-derived the CVSS/EPSS triage by hand on every advisory"}
  ],
  "broken_tools": [
    {"name": "web-search", "symptom": "Returns HTTP 429 after 3 queries"}
  ],
  "efficiency_gaps": [
    "Fetching the same release notes in every run; a cached summary would save about 20% of tokens"
  ]
}
```

## Schedule and output

- Set `spec.schedule` (cron) and `spec.timeZone`; the example runs Mondays at
  06:00. Trigger a run by hand with the `ai.roundtable.io/run` annotation, see
  [examples](../examples/README.md#scheduled-briefing).
- The report goes where `outputPath` points, written by `outputKnightRef`
  (here `Reports/<date>-<chain>.md`). Change the path to a vault folder the
  lead may write to.
- A knight that does not answer (`phase` is not `Succeeded`) is listed under
  "No answer" and does not fail the run (`failurePolicy: continue`).

## Out of scope

Feeding each knight's recent failed tasks into the assessment, and a proposal
CRD or approval UI.
