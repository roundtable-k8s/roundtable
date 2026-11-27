# Agent cards

Every knight has an agent card: a JSON document saying who the knight is,
what it can do and how to reach it. The RoundTable controller writes and
maintains the cards in a NATS KV bucket; knights, the dashboard and other
clients only read them. A KV watch on a table's keys is a live roster.

The card's top level follows [A2A's AgentCard](https://a2a-protocol.org/latest/specification/#agentcard)
so the gateway could serve it to outside A2A agents later. Everything Round
Table specific lives under `roundtable`.

## Bucket and keys

| | |
|---|---|
| Bucket | `roundtable-cards` (operator env `CARDS_KV_BUCKET`, chart `cards.bucket`) |
| Key | `<table>.<knight>`: the RoundTable name and the Knight name |
| History | 1 (only the current card) |
| TTL | none |

The operator creates the bucket if it is missing. Knight pods get the bucket
name as `KNIGHT_CARDS_BUCKET` and their table as `ROUNDTABLE_TABLE`.

A card exists for every member knight of a table (see `spec.knightSelector`),
including knights that are suspended or scaled to zero. It is deleted when the
knight is deleted, when the knight leaves the table (labels or selector
changed) and when the table is deleted. A knight that belongs to two tables
has a card under each.

The controller writes a card only when its content changed, so a watch sees
real changes, not resyncs. A Knight spec or status change reaches the card
within seconds.

## Schema (version 1)

```json
{
  "schemaVersion": 1,
  "name": "galahad",
  "description": "Security analyst: CVE triage and the daily security brief.",
  "version": "7",
  "defaultInputModes": ["text/plain"],
  "defaultOutputModes": ["text/plain"],
  "skills": [
    {"id": "cve-triage", "name": "cve-triage", "description": "Triage a CVE ...", "tags": ["security"], "examples": []}
  ],
  "roundtable": {
    "table": "personal",
    "knight": "galahad",
    "domain": "security",
    "model": "claude-sonnet-4-20250514",
    "tools": ["nmap", "whois"],
    "tags": ["security", "daily"],
    "taskSubject": "fleet-a.tasks.security.galahad",
    "runtime": "deployment",
    "phase": "Ready",
    "readyReplicas": 1,
    "suspended": false,
    "skillsSource": "runtime",
    "runtimeSkills": [{"name": "cve-triage", "description": "Triage a CVE ...", "category": "security"}],
    "updatedAt": "2026-11-24T10:00:00Z"
  }
}
```

| Field | Source |
|---|---|
| `name` | Knight name |
| `description` | `spec.card.description`, else the first paragraph of `spec.prompt.identity`, else `"<Name>, <domain> knight"` |
| `version` | Knight `metadata.generation` |
| `skills` | See [Skills](#skills) |
| `roundtable.model` | `status.model` (the effective model), else `spec.model` |
| `roundtable.tools` | `spec.tools.nix` |
| `roundtable.tags` | `spec.card.tags` (omitted when unset) |
| `roundtable.taskSubject` | `<prefix>.tasks.<domain>.<knight>`; the prefix comes from the knight's first `spec.nats.subjects` entry, else the table's `spec.nats.subjectPrefix` |
| `roundtable.runtime` | `spec.runtime` (`deployment` or `sandbox`) |
| `roundtable.phase`, `readyReplicas` | Knight status |
| `roundtable.suspended` | `spec.suspend` |
| `roundtable.skillsSource` | `spec` (categories only) or `runtime` (reported by a running knight) |
| `roundtable.runtimeSkills` | The skills the knight last reported, kept while it is at zero |
| `roundtable.updatedAt` | When the card content last changed |

## Skills

The skills themselves (`SKILL.md` files) reach the knight pod through
git-sync; the operator never sees them. Each running knight registers as the
NATS micro service `roundtable-knight` and puts its loaded skills in the
service INFO metadata (`table`, `knight`, and `skills`: a JSON array of
`{name, description, category}`).

When a member knight has ready pods, the controller sends
`$SRV.INFO.roundtable-knight` (1 s timeout, all responders, at most about once
a minute per table), takes the responses whose `knight` (case-insensitive) and
`table` metadata match, and stores their skills in the card. Each reported
skill becomes `{id: name, name, description, tags: [category]}`. If the knight
does not answer, the card keeps the skills it already has, so a knight scaled
to zero keeps its skill descriptions.

Before a knight has ever reported skills, `skills` lists its `spec.skills`
categories: `{id: <category>, name: <category>, description: "<category> skills", tags: [<category>]}`.

## Overrides

`Knight.spec.card` overrides the generated card:

```yaml
spec:
  card:
    description: "Security analyst: CVE triage and the daily security brief."
    tags: [security, daily]
    skills:
      - id: cve-triage
        description: Triage a CVE against the fleet's images.
        examples: ["Is CVE-2026-1234 exploitable in our cluster?"]
      - id: threat-model        # not generated: added to the card
        description: Threat-model a design doc.
        tags: [security]
```

`spec.card.skills` entries are merged over the generated skills by `id`: set
fields (`description`, `tags`, `examples`) replace the generated ones; an `id`
that is not generated adds a skill.

## How a knight finds another knight

Knights read the same cards. The `find_knights` tool lists the other knights
of the knight's table from them, and `nats_request` takes a knight's name and
looks up its domain on the card, so a knight can delegate without being told
the target's domain. The roster is per table: a knight sees only the cards
under its own `ROUNDTABLE_TABLE`, and never itself.

`find_knights` takes four optional parameters (all from `FindKnightsQuery`):

| Parameter | Type | Matches |
|---|---|---|
| `query` | string | Case-insensitive text in the knight name, the card `description`, or a skill's `id` or `description`. A knight that matches only on a skill is listed with just the matching skills. |
| `domain` | string | Exact match on the card's domain (the Knight's `spec.domain`). |
| `skill` | string | A skill `id` or one of its `tags`, exact and case-insensitive. Only the matching skills are listed. |
| `includeSuspended` | boolean | Default `true`. Suspended (scaled to zero) knights are listed with status `suspended`; a task sent to them waits until they wake. `false` drops them. |

With no parameters it lists every other knight. Results are sorted by name.
Each match gives the name, domain, description, the skills that matched, and a
status:

- `ready`: the knight has ready replicas (or `phase` is `Ready`).
- `suspended`: `spec.suspend` is set.
- `not ready`: neither of the above.

A `live` flag is added when a pod answered a micro service PING for the knight
within about 300 ms. It is omitted when nobody answered, so its absence means
"no liveness information", not "not live". The PING is only sent when there is
at least one match.

The knight reads the roster from a KV watch on the table's keys. Until that
watch has loaded, and again after it ends (it is restarted with back-off from
5 s to 60 s), each lookup reads the bucket directly, so results are current
either way.

An example exchange (plain text, the tool's own output format):

```text
knight: find_knights {"domain": "security"}
tool:   1 knight(s) at your table:
        - galahad (domain: security, ready, live): Security analyst: CVE triage and the daily security brief.
            - cve-triage: Triage a CVE against the fleet's images.

knight: nats_request {"knight": "galahad", "task": "Is CVE-2026-1234 exploitable in our cluster?"}
```

Nothing in that exchange passes `domain` to `nats_request`: the domain comes
from galahad's card. An unknown name is rejected with the valid names at the
table. Passing `domain` explicitly skips the lookup.

The fields that decide what a knight finds are on the Knight:

- `spec.domain`: the card's domain, used by the `domain` filter and by the task subject.
- `spec.card.description` (else the first paragraph of `spec.prompt.identity`): the description that `query` matches.
- `spec.card.tags` and the skills (`spec.skills` categories, the skills a running knight reports, and `spec.card.skills`): what `skill` and `query` match. See [Skills](#skills) and [Overrides](#overrides).
- `spec.suspend`: the status, and whether `includeSuspended: false` drops the knight.

When there is no roster, `find_knights` answers
`No roster available: this knight has no agent cards (ROUNDTABLE_TABLE or KNIGHT_CARDS_BUCKET is not set).`
This happens on older operators that do not set `KNIGHT_CARDS_BUCKET`, or when
the knight has no table. `nats_request` then needs an explicit `domain`.

## Watching the roster

```bash
# Every card of the table, then every change as it happens
nats kv watch roundtable-cards 'personal.*'

# One card
nats kv get roundtable-cards personal.galahad --raw | jq .

# The running pods behind the cards (liveness and load)
nats micro ls roundtable-knight
```
