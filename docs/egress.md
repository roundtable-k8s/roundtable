# Egress allowlists

A knight that reads web pages, feeds or issues can be prompt-injected: hostile
text tells it to send data somewhere or to fetch and run something. An egress
allowlist limits where a hijacked knight can send anything. Together with
keeping credentials out of pods (#169, #173) it caps the damage.

## End state and what is live today

The intended model (whiteboard decision 8, #377) is **isolated by default**:
every knight reaches only DNS, its NATS server and its allow-listed hosts, and
opting out is explicit on the Knight. Two steps get there:

| Step | Status |
| --- | --- |
| `Knight.spec.egress` renders a per-knight policy (#348) | live |
| `RoundTable.spec.defaults.egress` gives every knight of a table an allowlist (this page, #349) | live |
| Default-deny for every knight, with an explicit per-knight opt-out (#387) | **not yet**: until #387 lands a knight with no `egress` (own or inherited) has no egress policy |

There is no table-level opt-out: a table can only supply an allowlist. Opt-outs
stay on the Knight (#387).

## The fields

```yaml
# Knight.spec.egress, or RoundTable.spec.defaults.egress (same type)
egress:
  allowedHosts:            # exact hosts or "*." wildcards; empty = DNS and NATS only
    - api.anthropic.com
    - "*.githubusercontent.com"   # subdomains only, not githubusercontent.com itself
  allowedPorts: [443]      # TCP, default 443
```

- A knight's own `egress` wins over the table's. Allowlists are **not merged**
  (same rule as `resources`); an explicit `egress: {}` is its own value and
  means DNS and NATS only.
- The table default applies to the table's standing knights (those labelled
  `ai.roundtable.io/round-table`).
- The operator renders `CiliumNetworkPolicy knight-<name>-egress` allowing DNS
  (with a DNS proxy rule, which FQDN tracking needs), the knight's NATS server
  and each allowed host on the allowed ports. Everything else is denied.

## Cilium vs fallback

| Cluster | Result | `EgressRestricted` condition |
| --- | --- | --- |
| `cilium.io/v2` `CiliumNetworkPolicy` is served | Hostname allowlist enforced with `toFQDNs` | `True`, reason `FQDNPolicyApplied` |
| Not served | A plain `NetworkPolicy` allows DNS, NATS and the allowed **ports** to **any** destination; hostnames cannot be enforced | `False`, reason `FQDNPolicyUnavailable` |
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

1. Start the knight with a generous allowlist (or none, before default-deny) on a
   Cilium cluster with Hubble.
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
