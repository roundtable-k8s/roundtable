# Egress allowlists

A knight that reads web pages, feeds or issues can be prompt-injected: hostile
text tells it to send data somewhere or to fetch and run something. An egress
allowlist limits where a hijacked knight can send anything. Together with
keeping credentials out of pods (#169, #173) it caps the damage.

## End state and what is live today

The model (whiteboard decision 8, #377) is **isolated by default**: every
knight is behind a default-deny egress policy and reaches only the baseline
below plus its allow-listed hosts. Opting out is explicit on the Knight.

| Step | Status |
| --- | --- |
| `Knight.spec.egress` renders a per-knight policy (#348) | live |
| `RoundTable.spec.defaults.egress` gives every knight of a table an allowlist (#349) | live |
| Default-deny for every knight, with an explicit per-knight opt-out (#387) | live (chart `isolation.egress.mode: deny`, the default) |

The allowlist is resolved in this order: the knight's `egress`, then the
table's `defaults.egress`, then an empty allowlist (the baseline only). With
the install-wide `isolation.egress.mode: "off"` the last step is skipped, so
knights without an allowlist get no policy.

There is no table-level opt-out: a table can only supply an allowlist. Opt-outs
stay on the Knight:

```yaml
spec:
  isolation:
    egress: unrestricted   # no egress policy at all
    reason: "crawls arbitrary research sources"
```

`reason` is required (at least 10 characters); the knight then reports
`Isolated=False`, reason `OptedOut`. See
[runtime-isolation.md](runtime-isolation.md) for the `Isolated` condition and
the `roundtable_knight_isolated` metric.

## The baseline

Every knight's policy allows, even with an empty allowlist:

| What | Why | Rendered as (Cilium) |
| --- | --- | --- |
| Cluster DNS | name resolution, and Cilium's DNS proxy for `toFQDNs` | `toEndpoints` on 53 with a DNS rule |
| The knight's NATS server(s) | tasks and results | `toEndpoints` (in-cluster) or the port to any destination |
| The Kubernetes API | knights use it on purpose (`automountServiceAccountToken: true`) | `toEntities: [kube-apiserver]` |
| `isolation.egress.baseHosts` (`KNIGHT_EGRESS_BASE_HOSTS`, default `openrouter.ai`, `api.anthropic.com`) | the model providers | `toFQDNs` on 443 |
| The hosts of the knight's arsenal repos (`spec.arsenal.repo`, `spec.arsenals[].repo`) | the git-sync sidecars share the pod's network | `toFQDNs` on the URL's port (443 for https, 22 for ssh / `git@host:`) |

The knight's own `allowedHosts` are added on top, on its `allowedPorts`.

## The fields

```yaml
# Knight.spec.egress, or RoundTable.spec.defaults.egress (same type)
egress:
  allowedHosts:            # exact hosts or "*." wildcards; empty = the baseline only
    - api.anthropic.com
    - "*.githubusercontent.com"   # subdomains only, not githubusercontent.com itself
  allowedPorts: [443]      # TCP, default 443
```

- A knight's own `egress` wins over the table's. Allowlists are **not merged**
  (same rule as `resources`); an explicit `egress: {}` is its own value and
  means the baseline only.
- The table default applies to the table's standing knights (those labelled
  `ai.roundtable.io/round-table`).
- The operator renders `CiliumNetworkPolicy knight-<name>-egress` allowing the
  baseline and each allowed host on the allowed ports. Everything else is
  denied.
- Mission knights are Knights too and get the same policy and the same
  opt-out rules (only their template or inline spec can opt out). Under
  default-deny the operator no longer creates the per-mission
  `mission-<name>-isolation` NetworkPolicy, which allowed 443 to anywhere and
  would have widened the knight's policy (NetworkPolicies are additive).

## Cilium vs fallback

| Cluster | Result | `EgressRestricted` condition |
| --- | --- | --- |
| `cilium.io/v2` `CiliumNetworkPolicy` is served | Hostname allowlist enforced with `toFQDNs` | `True`, reason `FQDNPolicyApplied` |
| Not served | A plain `NetworkPolicy` allows DNS, NATS and, to **any** destination, the Kubernetes API ports (443, 6443), the baseline hosts' ports and the allowed **ports**; hostnames cannot be enforced | `False`, reason `FQDNPolicyUnavailable` |
| Applying the policy failed | See the condition message | `False`, reason `EgressPolicyFailed` |

Do not treat a fallback cluster as isolated: check `EgressRestricted` before
relying on the allowlist.

```sh
kubectl get knight galahad -o jsonpath='{.status.conditions[?(@.type=="EgressRestricted")]}'
```

## By runtime backend

- **`runtime: deployment`** and **per-knight Sandboxes** (`runtime: sandbox`
  without a template): the operator's policy above. It selects pods by the
  labels `app.kubernetes.io/name=knight` and `app.kubernetes.io/instance=<name>`
  only, so it covers both.
- **Sandboxes created from an agent-sandbox `SandboxTemplate`** get the
  template's `networkPolicy`, not the operator's policy. Upstream's default
  blocks private ranges and cluster DNS, so NATS and DNS must be allowed
  explicitly in the template, and a `networkPolicy` cannot express hostnames:
  the template can only filter by IP/CIDR and port. Use the template for
  coarse limits and expect no per-host allowlist there.

## Finding the hosts a knight needs

1. Start the knight with a generous allowlist (or temporarily with
   `isolation.egress: unrestricted` and a reason) on a Cilium cluster with
   Hubble.
2. Run its normal tasks, then list what it contacted:

   ```sh
   hubble observe --namespace roundtable --pod galahad --protocol dns --since 1h -o jsonpb \
     | jq -r '.flow.l7.dns.query' | sort -u
   hubble observe --namespace roundtable --pod galahad --verdict DROPPED --since 1h
   ```

3. Add the hosts the task really needs to `allowedHosts`; re-run and watch for
   `DROPPED` verdicts. A host that appears only in a dropped flow is either a
   missing entry or something the knight should not reach.
4. Remember CDN and redirect hosts (for example `*.githubusercontent.com`).

## Starter allowlists

[`examples/egress/`](../examples/egress/) has starter allowlists for GitHub
read access, package documentation, model providers, a research knight and a
feed-reading knight. They are not bound to anything by default: copy what you
need into a knight or a table's `defaults.egress`.

## Out of scope

Mission-level allowlists chosen by the planner, and egress-denied metrics or
span events (#153).
