# Warning events

Every Warning event the operator emits, by reason. See them with
`kubectl -n <ns> get events --field-selector type=Warning --sort-by=.lastTimestamp`
or `kubectl -n <ns> describe <kind> <name>`. Reasons are exact strings; two
reasons that look alike (`Failed`) are emitted by more than one controller, so
read the object kind and the message too.

The table is checked against the code by `scripts/check-events-doc.sh`
(run by `make helm-check`): a Warning reason in `internal/` that is missing
here, or listed here and not emitted, fails the build. A call whose reason is a
variable carries a `// event-reasons:` comment naming what it can emit; keep it
current when you add a reason. Normal events are not listed. For conditions
see [troubleshooting](troubleshooting.md) and [observability](observability.md).
A cause marked "inferred" is not stated by the code; it is the likely reason.

## Knight

| Reason | Emitted by | When it fires | Look at next |
| --- | --- | --- | --- |
| `ReconcileFailed` | Knight controller | A reconcile step returned an error; the knight goes Degraded. The message has the error | `kubectl describe knight <name>`, then the operator log |
| `RuntimeClassNotFound` | Knight controller | The RuntimeClass named in `spec.runtimeClassName` does not exist; the knight is parked | `kubectl get runtimeclass`; [runtime isolation](runtime-isolation.md) |
| `SandboxUnavailable` | Knight controller | `runtime: sandbox` is requested but the Sandbox CRD is not served; the knight is parked | Install the agent-sandbox CRDs, or use `runtime: deployment` |
| `MaxDeliverIgnored` | Knight controller | `spec.nats.maxDeliver` is above 1; it is deprecated and ignored (once per generation) | Remove the field; see the `DeprecatedFieldsUsed` condition |
| `ConsumerRecreated` | Knight controller | The JetStream consumer was recreated because a field NATS cannot update differed | `nats consumer info <stream> <consumer>`; a one-off after a spec change is normal |
| `ConsumerDeleteAbandoned` | Knight controller | Deleting the consumer failed repeatedly, so the Knight is deleted anyway and the consumer is left | Delete it by hand: `nats consumer rm <stream> <consumer>` |
| `ServiceNotFound` | Knight controller | An egress `allowedServices` entry names a Service that does not exist | `kubectl get svc`; fix `spec.egress`; [egress](egress.md) |
| `InClusterAllowedHost` | Knight controller | An `allowedHosts` entry names an in-cluster Service host | Use `allowedServices` for in-cluster targets; [egress](egress.md) |
| `MemoryNotRWX` | Knight controller | The memory PVC is not ReadWriteMany but the knight can run more than one pod | Set `spec.memory.storageClassName` to an RWX class and recreate the PVC; [memory](memory.md) |
| `UnsupportedRuntime` | Knight controller | Autoscaling is requested with a runtime other than `deployment`; one pod runs | Use `runtime: deployment` or drop `maxReplicas`/`suspendPolicy: auto` |
| `KEDAUnavailable` | Knight controller | Autoscaling is requested but the operator started without KEDA; a fixed replica count runs | Install KEDA and restart the operator, or set `--enable-keda` |
| `InvalidFlakeRef` | Knight controller | A `spec.tools.flakes` ref is invalid (it got past admission) | Fix the ref; the message names the problem |
| `ToolsImageNotConfigured` | Knight controller | `spec.tools` is set but the operator has no `toolsImage.repository`; the tools are not installed | Set `toolsImage.repository` in the chart ([troubleshooting](troubleshooting.md)) |
| `ToolsImageMissing` | Knight controller | A published tools image is gone from the registry; it is rebuilt | Registry retention (cause inferred: garbage collection); wait for the rebuild |
| `ToolsImageBuildFailed` | Knight controller | The tools image build Job failed; the pod rollout is held | `kubectl logs job/<job>`; fix `spec.tools` |

## RoundTable

| Reason | Emitted by | When it fires | Look at next |
| --- | --- | --- | --- |
| `BudgetExceeded` | RoundTable controller | Total cost passed `policies.costBudgetUSD`; Available goes False | `kubectl get roundtable <name> -o yaml` status cost; raise the budget |
| `DeprecatedFieldsUsed` | RoundTable controller | A deprecated field is set (`policies.costResetSchedule`), once per generation | Remove the field; the message names it |
| `PlannerFailed` | RoundTable controller | Choosing the planner knight failed; the message has the error | `kubectl describe roundtable <name>`; `spec.planner` |
| `PlannerKnightMissing` | RoundTable controller | `spec.planner.knightRef` names a Knight that does not exist | `kubectl get knights`; fix the ref |
| `PlannerKnightNotInTable` | RoundTable controller | The planner Knight exists but is not selected by `spec.knightSelector` | Fix the selector or the ref |
| `IssuePickupFailed` | IssuePickup controller (on the RoundTable) | A poll or trigger of issue pickup failed; the message has the error, retried on the next poll | [issue pickup](issue-pickup.md); the GitHub token and rate limit |
| `SplitParentNeedsHuman` | IssuePickup controller (on the RoundTable) | Not every sub-issue of a split parent finished; the parent is labelled needs-human | The issue named in the message; [issue pickup](issue-pickup.md) |
| `MergeStepUnverified` | IssuePickup controller (on the Chain) | The chain's merge step did not queue the PR; the operator enabled auto-merge itself | The PR named in the message; [PR pipeline](pr-pipeline.md) |
| `MergeNeedsHuman` | IssuePickup controller (on the RoundTable) | An open PR could not be queued for auto-merge; the message has the error | The PR in the message; [PR pipeline](pr-pipeline.md) |
| `ChainFailedNeedsHuman` | IssuePickup controller (on the RoundTable) | The chain for a picked-up issue failed | `kubectl describe chain <name>`; the issue in the message |
| `PRFeedbackFailed` | PRFeedback controller (on the RoundTable) | A poll or trigger of PR feedback failed; retried on the next poll | [PR feedback](pr-feedback.md); the GitHub token |
| `PRFeedbackEscalated` | PRFeedback controller (on the RoundTable) | CI or merge conflicts still failing after the allowed fix rounds; the PR is labelled for a person | The PR in the message; [PR feedback](pr-feedback.md) |

## Chain

| Reason | Emitted by | When it fires | Look at next |
| --- | --- | --- | --- |
| `Failed` | Chain controller (Chain), Mission controller and planner (Mission) | Chain: the run failed or timed out. Mission: planning or the mission failed. The message says which | `kubectl get chain <name> -o yaml` step errors; or the Mission's status |
| `RunCancelled` | Chain controller | A run was cancelled; unfinished steps are marked cancelled | The message has the reason; re-annotate to start a new run |
| `NoStepsExecuted` | Chain controller | A run finished without executing any step because every output was restored from cache | Expected with unchanged inputs; clear the cache to force a run |
| `StepWhenError` | Chain controller | A step's `when:` expression did not evaluate | The step's `when` in `kubectl get chain -o yaml` |
| `InvalidStepModel` | Chain controller | A step's model is invalid for its knight; the step fails | The step's `model` and the knight's allowed models |
| `StepResumeRunIDError` | Chain controller | A step's `resumeRunId` template did not resolve | The step's `resumeRunId` template |
| `FanOutFailed` | Chain controller | A fan-out step failed; pending items are skipped. The message has the cause | The message; the step's fan-out settings and item results in `kubectl get chain -o yaml` |
| `LoopExhausted` | Chain controller | A loop step used all its iterations without meeting its stop condition | The loop's `maxIterations` and stop condition |
| `LoopFailed` | Chain controller | A loop step failed | The message; the failing body step |
| `StepEmptyOutput` | Chain controller | A step returned empty output and is treated as failed | The knight's logs for that task |
| `StepJSONRetry` | Chain controller | A step's output had no parseable JSON; the step is retried once | The step's output schema and prompt |
| `StepInvalidJSON` | Chain controller | A step's output was still invalid after the retry; the step fails | The step's output; its schema |
| `StepProviderOverloaded` | Chain controller | A transient provider error (rate limit or overload); the step is retried after a wait | Nothing if it recovers; the provider status page otherwise (retry count is in the message) |
| `RetryIgnored` | Chain controller | The retry-failed annotation was set but there is no failed step | Remove the annotation |
| `CronTriggerSkipped` | Chain controller | A scheduled run was skipped because the previous run is still going | The previous run's duration; [scheduled work](scheduled-work.md) |
| `ScheduleTriggerConflict` | Chain controller | A Trigger with the schedule's name exists and is not owned by this Chain, so `spec.schedule` is not run | Rename or delete that Trigger |

## Trigger

| Reason | Emitted by | When it fires | Look at next |
| --- | --- | --- | --- |
| `EventRejected` | Trigger controller | A trigger firing was skipped; the message has the skip reason | [triggers](triggers.md) |
| `FireJobFailed` | Trigger controller | The CronJob-backed trigger's Job failed | `kubectl get jobs,pods`; the Job logs |
| `ClaimFailed` | Trigger controller | Claiming a GitHub item failed; retried on the next poll | The GitHub token and its scopes; [triggers](triggers.md) |
| `TaskFailed` | Trigger controller | The knight task a firing sent failed | The knight's logs; the dead-letter entry |
| `TaskLost` | Trigger controller | A firing's task was lost (never answered) | The dead-letter stream ([knight task contract](knight-task-contract.md#delivery-and-failure)) |

## Mission

| Reason | Emitted by | When it fires | Look at next |
| --- | --- | --- | --- |
| `Timeout` | Mission controller | The mission exceeded its TTL ("Mission exceeded TTL") and moves to cleanup | The mission's TTL setting and status |
| `ResultsNotRetained` | Mission controller | Storing the mission's results to NATS KV failed during cleanup (once); cleanup continues without a `resultsRef` | The `ResultsRetained` condition; NATS KV health; [keeping mission output](keeping-mission-output.md) |
| `BriefingPartialDelivery` | Mission briefing | The briefing reached fewer knights than it was sent to | `kubectl get knights`; NATS consumer state of the missing ones |
| `DeprecatedPlanner` | Mission planner | The planner was found by the deprecated `role=planner` label | Set `spec.planner.knightRef` on the RoundTable |

## Completion webhooks (Chain and Mission)

| Reason | Emitted by | When it fires | Look at next |
| --- | --- | --- | --- |
| `NotificationRejected` | Notification helper | The completion webhook URL is not in the allowlist | The operator's webhook allowlist setting |
| `NotificationFailed` | Notification helper | The webhook failed permanently (no more retries) | The message has the error; the receiver |
| `NotificationRetrying` | Notification helper | A delivery failed and will be retried | The receiver, if it keeps repeating |
