# ADR: runtime backends, isolation, scaling and egress

Status: accepted (roundtable-k8s/roundtable#161; picture confirmed on the issue 2026-11-25)

## Context

Knights are long-lived NATS JetStream consumers with session state on a PVC.
We needed to decide which workload backends are supported, how gVisor, KEDA and
egress policy fit each one, and whether kubernetes-sigs/agent-sandbox or Agent
Substrate add anything that Deployment + gVisor + KEDA lacks. The epic is #160;
this ADR records the outcome and must agree with it.

Two earlier comments on #161 looked like they conflicted ("use agent-sandbox"
and "Deployment + gVisor + KEDA"). They describe different layers, not
alternatives.

## The layers

Three layers, chosen independently:

| Layer | What it is | Choices |
|---|---|---|
| Isolation runtime | How the pod's containers run on the node | default `runc`, or a RuntimeClass. **gVisor** (`runsc`) is the standard; Kata is not planned |
| Backend (workload controller) | Which object owns the knight's pod | **Deployment** (default) or **Sandbox** (kubernetes-sigs/agent-sandbox) |
| Scaling and idle | Who starts and stops pods | KEDA, `spec.suspend`, agent-sandbox `operatingMode` and warm pools |

agent-sandbox is an orchestrator. Upstream says it "delegates low-level
container isolation to secure Sandbox Runtimes (like gVisor or Kata Containers)
by managing Pods configured to use these runtimes (via `RuntimeClass`)". gVisor
is what an agent-sandbox pod runs under, not an alternative to it.

## Decisions

1. **Two backends, both supported.** `runtime: deployment` and
   `runtime: sandbox`, selected per knight. Both get their pod spec from the
   same builder (`KnightReconciler.BuildPodSpec`, `internal/controller/knight_controller.go`;
   the sandbox backend calls it through `BuildDrainablePodSpec`), so anything set
   there reaches both.
2. **The chart default stays `runtime: deployment`.** It needs no extra
   controller. Sandbox is opt-in per knight.
3. **gVisor on both backends through `runtimeClassName`** (#164). The field goes
   into the Deployment's pod template or into `Sandbox.spec.podTemplate.spec`.
   The cluster provides the RuntimeClass (see [runtime-isolation.md](../runtime-isolation.md)).
4. **KEDA for Deployments and for sandbox pools.** See the scaling table below.
5. **One egress allowlist API, rendered per backend** (#168). See below.
6. **Supported agent-sandbox range:** v1.0 and later, API
   `agents.x-k8s.io/v1beta1` (checked against v1.0.12, 2026-11-20; upstream
   releases weekly). The backend already uses `v1beta1` (`SandboxGVK` in
   `pkg/runtime/sandbox_detect.go`, #366); upstream removed `v1alpha1` in v1.0.0.
   Remaining port work is tracked in #163.
7. **Mission knights: a bare `Sandbox` per knight now** (#483, outcome of #368).
   A mission knight with `runtime: sandbox` is one `Sandbox` whose
   `spec.shutdownTime` is the mission's expiry (start + `ttlSeconds`); the
   operator's hand-rolled warm pool (`RoundTable.spec.warmPool`) is removed.
   Role pools (`SandboxTemplate` + `SandboxWarmPool` + `SandboxClaim`) only after
   a spike; warm starts come back through them if the spike supports it.

## Backend comparison

| | Deployment backend (default) | Sandbox backend |
|---|---|---|
| Needs | nothing extra | agent-sandbox v1.0+ installed |
| Object | `Deployment`, 1..N identical pods | one `Sandbox` = one pod with a stable name; volumes kept across suspend |
| Good for | standing knights, horizontally scaled knights (coders 1..3) | singleton stateful knights; short-lived mission knights with a lifetime (`shutdownTime`, `ttlSecondsAfterFinished` on a claim) |
| Suspend | `spec.suspend` scales to 0; KEDA can also scale to 0 | `operatingMode: Suspended` (pod deleted, object and volumes kept) |

### What agent-sandbox adds

- A singleton with a stable pod name and explicit `operatingMode: Running | Suspended`.
- Lifetime control for short-lived mission knights.
- `SandboxTemplate`, `SandboxWarmPool` and `SandboxClaim`: pre-warmed pods handed out on claim, with the pool sized by KEDA (the pool has a `scale` subresource).
- A managed, shared default-deny NetworkPolicy per template.
- The SIG Apps API that other tooling and managed clouds build on.

These are the reason the Sandbox backend stays (it was ported in #366). The condition
"if they add nothing, close the sandbox-controller issues" is not met.

### What it does not give

- More than one pod per knight. Horizontally scaled knights stay on Deployments.
- KEDA-driven wake for a single Sandbox: a v1beta1 `Sandbox` has no `scale` subresource.
- Auto suspend/resume and snapshot-based suspend are upstream work in progress (roadmap "Planned"; KEP 694 names a snapshot provider). They are not part of v1beta1 today and this ADR does not depend on them.

### Fit with long-lived JetStream consumers and PVC state

- Durable consumers live on the NATS server, so tasks queue while a knight's pod is gone. The Deployment backend already relies on this for KEDA scale-to-zero.
- The workspace PVC is created by the Knight controller and referenced from the pod spec (the pod builder adds it through `PodBuilder.WithWorkspace()`, and the sandbox backend adds no volume claim templates), so it survives suspend on either backend.
- Not yet verified on a cluster: a `runtime: sandbox` knight under gVisor completing a task after suspend and resume. That check is tracked under #163.

## Scaling and idle, per backend

| Case | Mechanism |
|---|---|
| Deployment knight | KEDA `ScaledObject` on the knight's JetStream consumer lag. `maxReplicas > 1` scales out; `lifecycle.suspendPolicy: auto` scales to 0 and back (#166) |
| Sandbox, standing knight | `spec.suspend` set by hand, which the operator maps to `operatingMode: Suspended`. **No automatic wake** until upstream ships auto suspend/resume. Knights that need lag-driven scale-to-zero use the Deployment backend |
| Sandbox, pooled knights | KEDA sizes the `SandboxWarmPool`, down to 0 (upstream `examples/keda-scale-to-zero`). Applies once mission knights use pools |

## Egress per backend

One allowlist API on Knight and RoundTable (#168), rendered per backend:

- **Any knight pod, both backends:** the operator renders a policy selecting the
  knight's pods by label: a `CiliumNetworkPolicy` with `toFQDNs` where Cilium is
  present, a plain `NetworkPolicy` otherwise. This covers Deployment knights and
  directly created Sandboxes; the Sandbox backend already labels its pods
  (`sandboxLabels`, #366).
- **Sandboxes from a `SandboxTemplate`** (pools, claims): the template's
  `networkPolicy` is a shared, controller-managed NetworkPolicy. Left empty,
  upstream's default allows the public internet and blocks RFC1918, link-local
  and cluster DNS, which would cut a knight off from in-cluster NATS. The operator
  must render explicit NATS and DNS rules there. It is L3/L4 only; hostname rules
  still need Cilium.

See [egress.md](../egress.md). Credentials stay in the pod for now, as narrow and
short-lived as possible; injection at egress (#169) is future exploration.

## Rejected

- **Agent Substrate** (`kagent-dev/substrate`; #165 closed). As of 2026-11 (reasons not re-checked
  against Substrate's repo), it multiplexes actors onto shared worker pools with suspend to object storage, which is a
  different execution model from one long-lived consumer per knight. It adds an
  object-storage dependency and node prerequisites (Talos needed a userns patch),
  is vendor-led rather than SIG-governed, and its main extra, credential
  injection, is not in scope (#169).
- **A custom waker** that watches consumer lag and flips `operatingMode`. It
  would duplicate what KEDA does for Deployments and what upstream plans.
- **A custom egress proxy.** NetworkPolicy and CiliumNetworkPolicy cover the
  allowlist without a new data-path component.
- **Kata** as a first-class runtime: not planned; it can still be named through
  `runtimeClassName`.

## Consequences

- Fleets keep working with zero extra controllers; sandbox users install agent-sandbox v1.0+.
- Sandbox knights have no lag-driven wake; this is accepted until upstream ships it.
- Related work: #163 (port), #164 (`runtimeClassName`), #166 (scaling), #168 (egress), #212 (cluster install, closed).
