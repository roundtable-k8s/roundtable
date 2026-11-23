# Getting Started

Deploy your first AI agent fleet in 5 minutes.

## Prerequisites

- Kubernetes cluster (1.28+)
- Helm 3.12+
- NATS JetStream running in-cluster
- An LLM API key (Anthropic, OpenAI, etc.)

## 1. Install the Operator

```bash
helm install roundtable oci://ghcr.io/roundtable-k8s/charts/roundtable-operator \
  --namespace roundtable --create-namespace \
  --set images.piKnight.tag=latest
```

## 2. Create a NATS Secret

```bash
kubectl create secret generic llm-api-keys \
  --namespace roundtable \
  --from-literal=ANTHROPIC_API_KEY=sk-ant-...
```

## 3. Deploy a RoundTable

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: RoundTable
metadata:
  name: my-fleet
  namespace: roundtable
spec:
  nats:
    url: nats://nats.database.svc:4222
    subjectPrefix: my-fleet
    tasksStream: my_fleet_tasks
    resultsStream: my_fleet_results
    createStreams: true
  defaults:
    model: claude-sonnet-4-20250514
    concurrency: 2
    taskTimeoutSeconds: 120
```

```bash
kubectl apply -f roundtable.yaml
kubectl get rt -n roundtable
```

## 4. Deploy Your First Knight

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: Knight
metadata:
  name: my-first-knight
  namespace: roundtable
spec:
  domain: general
  model: claude-sonnet-4-20250514
  skills:
    - general
  nats:
    url: nats://nats.database.svc:4222
    stream: my_fleet_tasks
    resultsStream: my_fleet_results
    subjects:
      - "my-fleet.tasks.general.>"
```

```bash
kubectl apply -f knight.yaml
kubectl get knights -n roundtable -w
# Wait for Phase: Ready
```

## 5. Send a Task

```bash
# Install NATS CLI: https://github.com/nats-io/natscli
nats pub my-fleet.tasks.general.my-first-knight '{
  "task_id": "test-1",
  "task": "What is Kubernetes? Explain in 2 sentences."
}' --server nats://nats.database.svc:4222

# Watch for results
nats sub "my-fleet.results.>" --server nats://nats.database.svc:4222
```

## 6. Create a Chain

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: Chain
metadata:
  name: hello-chain
  namespace: roundtable
spec:
  timeoutSeconds: 600
  input: "Tell me about Kubernetes operators"
  steps:
    - name: research
      knightRef: {name: my-first-knight}
      prompt: "Research this topic: {{ .Input }}"
    - name: summarize
      knightRef: {name: my-first-knight}
      prompt: "Summarize this in 3 bullet points: {{ .Steps.research.Output }}"
```

```bash
kubectl apply -f chain.yaml
kubectl get chains -n roundtable -w
```

### Running a chain by hand

Annotate the Chain with `ai.roundtable.io/run` — any value works, and it starts a **new run**
(new `runId`, all steps reset) whenever the value differs from `status.lastRunTrigger`:

```bash
kubectl annotate chain hello-chain -n roundtable ai.roundtable.io/run="$(date +%s)" --overwrite
```

Re-applying the same value never re-triggers. If a run is still in flight the trigger waits
and fires when it finishes. Changing `spec.input` never resumes an old run either; it resets
the chain to Idle, so trigger it with the annotation afterwards.

To resume a failed run from its failed (and skipped) steps only, keeping the steps that
already succeeded, use the opt-in `ai.roundtable.io/retry-failed` annotation:

```bash
kubectl annotate chain hello-chain -n roundtable ai.roundtable.io/retry-failed="$(date +%s)" --overwrite
```

Do not set `status.phase=Running` by hand: it resumes the previous run.

### Step timeouts and queueing

A step's `timeout` starts when the knight picks the task up, not when it is published, so
time spent queued behind a busy knight does not count. Pickup is detected from the knight's
durable `<knight>-consumer` on the tasks stream: knights ack each task as they receive it, so
the consumer's ack floor passing the task's stream sequence (`status.stepStatuses[].taskSeq`)
sets `pickedUpAt`. `spec.maxQueueWaitSeconds` (default 1800s) fails a step whose task is never
picked up. The overall chain `timeout` still counts from the start of the run.

## What's Next?

- [Example Manifests](../examples/) — Real-world Knight, Chain, Mission examples
- [Helm Values Reference](helm-values.md) — Full configuration options
- [Architecture Overview](ARCHITECTURE.md) — How it all works under the hood
- [API Reference](api-reference.md) — Complete CRD field documentation
