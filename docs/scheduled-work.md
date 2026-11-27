# Scheduled work

Scheduled work is a [Trigger](triggers.md) with `spec.source.schedule`. It runs
a task, a Chain or a Mission on a cron cadence. Write it as YAML and apply it
(or commit it to Git) first; the gateway API and the dashboard read and control
the same Trigger. ([Decision 21](decisions.md): scheduled work is native, run by
operator-managed CronJobs.)

How it runs, in short: the operator creates a CronJob `trigger-<name>` and a
ServiceAccount of the same name, owned by the Trigger. Each cron time the
CronJob starts a short pod that calls the gateway, which publishes the firing;
the Trigger controller then starts the target. The details are in
[Schedule source](triggers.md#schedule-source).

## The three targets

Every schedule has exactly one `target`. All blocks below are complete
Triggers in the `roundtable` namespace; the fuller files are in
[`examples/`](../examples/README.md).

### One task to a knight

[`examples/trigger-knight-task.yaml`](../examples/trigger-knight-task.yaml), no
Chain needed ([Knight target](triggers.md#knight-target)):

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: Trigger
metadata:
  name: nightly-briefing
  namespace: roundtable
spec:
  source:
    schedule:
      cron: "0 2 * * *"
      timeZone: Europe/London
      startingDeadlineSeconds: 3600
  target:
    knight:
      name: kay
      task: |
        Write the nightly fleet briefing for {{ .Event.Time.Format "Monday 2006-01-02" }}
        to Briefings/.
      timeoutSeconds: 1800
  concurrency:
    policy: Forbid
```

### A Chain run

[`examples/trigger-schedule.yaml`](../examples/trigger-schedule.yaml)
([Schedule source](triggers.md#schedule-source)):

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: Trigger
metadata:
  name: weekday-security-report
  namespace: roundtable
spec:
  source:
    schedule:
      cron: "0 7 * * 1-5"
      timeZone: America/New_York
      startingDeadlineSeconds: 3600
  target:
    chainRef: {name: security-report}
  input: |
    Generate the security report for {{ .Event.Time.Format "Monday 2006-01-02" }}.
  concurrency:
    policy: Forbid
```

For a fixed input, `Chain.spec.schedule` is the short form.

### A Mission

[`examples/trigger-mission.yaml`](../examples/trigger-mission.yaml)
([Mission target](triggers.md#mission-target)); `roundTableRef` is required:

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: Trigger
metadata:
  name: weekly-backlog-review
  namespace: roundtable
spec:
  roundTableRef: {name: fleet-a}
  source:
    schedule:
      cron: "0 8 * * 1"
      timeZone: Europe/London
      startingDeadlineSeconds: 7200
  target:
    mission:
      objective: |
        Review the open issue backlog as of {{ .Event.Time.Format "2006-01-02" }}:
        group issues by theme, flag stale ones, and propose the five to do next.
        Read-only: do not comment on, label or close anything.
      successCriteria: A ranked list of five issues with a one-line reason each.
      costBudgetUSD: "2.50"
      timeoutSeconds: 3600
      ttlSeconds: 86400
  concurrency:
    policy: Forbid
```

## A job that is one task naming a skill and a script

The standard migrated job ([decision 25](decisions.md)) is a **skill with
scripts** in the arsenal, plus a schedule that sends **one task** to a persona
knight telling it to use that skill and script. The agent does the multi-step
work itself, so no Chain is needed, and the skill's script does the validation
and filing.

The skill layout, as in the arsenal's
[`project/pr-postrun`](https://github.com/roundtable-k8s/arsenal/tree/main/project/pr-postrun):

```text
<group>/<skill>/
  SKILL.md        # when to use it, the exact commands, what the last output line means
  scripts/        # mounted in the knight's pod at /skills/<group>/<skill>/scripts/
    run-job
```

`SKILL.md` names the script path (`S=/skills/<group>/<skill>/scripts`) and the
result line the script prints. The knight must have the arsenal that carries
the skill (`spec.arsenals`).

The Trigger (the group, skill and script names here are placeholders for your
own):

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

A knight reports a failure the script cannot hide by ending its answer with a
final line `Outcome: failed <reason>`. That makes the task `success: false`, so
the firing shows as failed in `status.recentFirings` instead of succeeded. See
[Failing loudly](migrating-from-kagent.md#failing-loudly).

## When a run is missed, overlapping, lost or deleted

| Situation | What happens | Rests on |
|---|---|---|
| **Missed run** (cluster or CronJob controller down at the cron time) | With `startingDeadlineSeconds` set, a missed time still fires only if fewer than that many seconds have passed since it was due. The deadline is also applied when the firing is consumed: a firing that reaches the operator later than that (after downtime, or while the Trigger was not Ready) is recorded as skipped and starts no run. Unset, the CronJob controller decides: it catches up once, and none after more than 100 missed times. The operator does not catch up. | `trigger_cronjob.go` (`StartingDeadlineSeconds: src.StartingDeadlineSeconds`); the field comment on `TriggerScheduleSource`; `reconcileSchedule` comment in `trigger_controller.go` |
| **Late or retried firing of the same slot** | Deduplicated: the key is the schedule time (UTC, RFC 3339, read from the Job name), so a second firing with that key starts nothing. | [`triggers.md`](triggers.md#schedule-source), [`gateway.md`](gateway.md#firing-a-schedule-trigger); default dedupe key table |
| **Overlap: previous run still active** | The CronJob's `concurrencyPolicy` (from `spec.concurrency.policy`, `cronConcurrencyPolicy`) only governs the seconds-long firing pod. The Trigger controller decides what happens to the run itself: `Forbid` skips the firing (`skipReason` `concurrency Forbid: run <name> is active` for a Chain target; `task <id>` for a knight target, `mission <name>` for a mission target), `Allow` starts it (skipped only at `maxActive`), `Replace` deletes the active Chain run first. A skipped firing is **not queued**: it is recorded in `status.recentFirings[].skipReason` and not retried. `Replace` is rejected for knight and mission targets. | `trigger_controller.go` (concurrency switch near `activeRuns`); `trigger_cronjob.go` (`cronConcurrencyPolicy`) |
| **Lost run** | A knight-target task with no result within `target.knight.timeoutSeconds` (default 3600) is `outcome: lost`, and stops counting as active. A Mission target is `lost` when it expired or was already cleaned up. | `trigger_knight.go`, `trigger_mission.go` (`TriggerFiringLost`) |
| **Suspended** | `spec.suspend` or the `ai.roundtable.io/suspended` annotation suspends the CronJob (`Suspend: trg.Suspended()`). Run now still fires. | `trigger_cronjob.go`; [`triggers.md`](triggers.md#suspend-and-run-now-without-editing-the-spec) |
| **Trigger deleted** | The CronJob and ServiceAccount are owned by the Trigger and are deleted with it. Chain runs and Missions it started are owned by it too; knight tasks are not, they stay in the gateway's task records until `gateway.taskTTL`. | `trigger_cronjob.go` (`SetControllerReference`); `chainrun.go`; `trigger_mission.go` |
| **`source.schedule` removed** | The CronJob and ServiceAccount are deleted (`deleteScheduleObjects`). | `trigger_controller.go` |

## See and control schedules

```sh
kubectl -n roundtable get triggers                                # Ready, Last Fired, Suspended
kubectl -n roundtable get cronjobs -l ai.roundtable.io/trigger    # schedules, last run, suspended
kubectl -n roundtable get jobs -l ai.roundtable.io/trigger        # recent firing pods
kubectl -n roundtable get trigger nightly-briefing -o jsonpath='{.status.recentFirings[0]}'
kubectl -n roundtable annotate trigger nightly-briefing ai.roundtable.io/suspended=me   # suspend
kubectl -n roundtable annotate trigger nightly-briefing ai.roundtable.io/suspended-     # resume
```

A knight firing that ends `failed` or `lost` records why in `status.recentFirings[].reason`, as a Warning Event (`TaskFailed`, `TaskLost`) on the Trigger, and in the `reason` of the runs API (`GET /v1/schedules/{namespace}/{name}/runs`).

Suspend the Trigger, not the CronJob: a CronJob edited by hand is reset at the
next reconcile.

To suspend or resume every schedule of a table at once (maintenance, an
incident), call `POST /v1/schedules/suspend` or `/resume` with
`{"table": "..."}` ([gateway](gateway.md#post-v1schedulessuspend-and-post-v1schedulesresume)),
or the MCP tools `suspend_table_schedules` and `resume_table_schedules`. It sets
the same annotation per schedule, so Git is untouched.

The gateway has full CRUD for schedules ([Schedules](gateway.md#schedules)):
list and get, create, replace and delete, `suspend`, `resume`, and `run` (fire
once now), plus the run history
([Schedule run history](gateway.md#schedule-run-history)). To get a schedule
as YAML ready to commit, call
`GET /v1/schedules/{namespace}/{name}/manifest`:

```sh
curl -s -H "$AUTH" "$GW/v1/schedules/roundtable/nightly-briefing/manifest" > nightly-briefing.yaml
```

A schedule applied from Git (Flux or Helm labels) reports `managedBy: git` and
is read-only through the API: `PUT` and `DELETE` are refused with 409, because
the next reconcile would revert them. Suspend, resume and run now still work,
since they only patch annotations.

The schedule views (`GET /v1/schedules`, `list_schedules`) report each schedule's `concurrency` (policy, maxActive), `startingDeadlineSeconds` and effective suspension (`suspended`, `suspendedBy`).
