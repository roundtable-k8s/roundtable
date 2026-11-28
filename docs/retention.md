# Retention

Finished work piles up on a busy table. Each kind has its own limit; the ones
marked "none by default" keep today's behaviour until you set them.

| What | Bounded by | Default | Recommended for a busy table |
|---|---|---|---|
| Finished pickup Chains | `issuePickup.ttlSecondsAfterFinished` (age) and `issuePickup.maxFinishedChains` (count) | 7 days; count: none | `maxFinishedChains: 200` |
| Finished Trigger runs (not pickup-generated) | `ttlSecondsAfterFinished` and `maxFinishedRuns` | 1 day; count: none | `maxFinishedRuns: 200` |
| Task results in the results stream | `nats.resultsMaxAge` (needs `createStreams: true`) | none | `168h`; longer than the longest step timeout and the gateway task TTL |
| Events stream | `nats.eventsMaxAge` | 168h | |
| Dead-letter records | `nats.deadLetterMaxAge` | 720h (30 days) | |
| Missions | `ttlSeconds` on the Mission (or the Trigger's `target.mission.ttlSeconds`) | per Mission | |

An age limit alone does not bound the count: a table that starts hundreds of
runs a day holds all of them inside the TTL window. Set the count cap too.

## Count cap

When `maxFinishedChains` (pickup) or `maxFinishedRuns` (Trigger) is set, each
pass deletes the oldest finished Chains beyond that number, by
`status.completedAt`. Failed and PartiallySucceeded Chains count toward the
cap but are deleted last, so recent failures stay inspectable. Running or
pending Chains, and pickup Chains whose post-run work is not done, are never
deleted. `0` or unset means no cap. The TTL still applies on top of the cap.

A Trigger treats an event as already handled while a run with its key exists.
When the count cap (or the TTL) deletes a run, that key is free again, so a
polled source (for example a GitHub source) can fire the same event again if it
still returns it. Set the cap well above the number of events a source can
still return, or keep the TTL longer than the source's look-back window.

Each pass logs how many Chains it deleted by reason (`ttl`, `count`) and
increments `roundtable_finished_chains_deleted_total{source,reason}`.
