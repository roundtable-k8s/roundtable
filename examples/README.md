# Examples

Real-world examples of Round Table resources.

| File | Description |
|------|-------------|
| [`knight-basic.yaml`](knight-basic.yaml) | A single knight — security domain with NATS config |
| [`knight-scaled.yaml`](knight-scaled.yaml) | A horizontally scaled knight — ephemeral workspace, KEDA 0..N on pending tasks |
| [`roundtable-fleet.yaml`](roundtable-fleet.yaml) | Full fleet config with warm pool, policies, and NATS streams |
| [`chain-sequential.yaml`](chain-sequential.yaml) | Multi-step chain with output templating between steps |
| [`chain-model-override.yaml`](chain-model-override.yaml) | Per-step model override with RoundTable `modelAliases` |
| [`chain-fan-out.yaml`](chain-fan-out.yaml) | `forEach` fan-out/fan-in over a planner's JSON list |
| [`chain-pr-pipeline.yaml`](chain-pr-pipeline.yaml) | PR pipeline: plan → implement (or `forEach` over sub-issues) → review → bounded fix `loop` → merge, or escalate via `when`. Rendered from the chart's `prPipeline` values ([docs](../docs/pr-pipeline.md)) |
| [`roundtable-issue-pickup.yaml`](roundtable-issue-pickup.yaml) | Issue pickup: poll `rt:ready` issues and start one PR pipeline Chain per issue ([docs](../docs/issue-pickup.md)) |
| [`chain-pr-feedback.yaml`](chain-pr-feedback.yaml) | PR pipeline feedback: the operator feeds a red CI run or merge-queue rejection to the PR's author knight (`resumeRunId` resumes its session), then the lead re-queues, or escalates. Rendered from the chart's `prPipeline.feedback` values ([docs](../docs/pr-feedback.md)) |
| [`mission-ephemeral.yaml`](mission-ephemeral.yaml) | Mission using existing fleet knights |
| [`mission-meta.yaml`](mission-meta.yaml) | Meta-mission — planner auto-generates chains and knights |

## Quick Start

```bash
# 1. Install the operator
helm install roundtable oci://ghcr.io/roundtable-k8s/charts/roundtable-operator \
  --namespace roundtable --create-namespace

# 2. Create a fleet
kubectl apply -f roundtable-fleet.yaml

# 3. Deploy a knight
kubectl apply -f knight-basic.yaml

# 4. Watch it come online
kubectl get knights -n roundtable -w

# 5. Run a chain
kubectl apply -f chain-sequential.yaml

# 6. Launch a mission
kubectl apply -f mission-ephemeral.yaml
kubectl get missions -n roundtable -w
```
