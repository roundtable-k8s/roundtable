# GitHub tokens for knights

A knight gets GitHub credentials as files, never as env vars, so a rotated
Secret reaches the knight without a pod restart. There are two kinds:

- **The default token**: `spec.github.tokenSecretRef` on the RoundTable (or the
  knight's own override). It is mounted at
  `/var/run/secrets/roundtable/github/token` and its path is in
  `GITHUB_TOKEN_FILE`.
- **Additional tokens**: `spec.github.additionalTokens` on the RoundTable. Each
  one is for a single GitHub owner or `owner/repo`, for example a second GitHub
  App installed on another account.

The runtime side (the git credential helper, the `gh` shim and the prompt
context that pick a token per repository) is roundtable-k8s/knights#209. This
page is the pod contract it reads.

## The field

```yaml
apiVersion: ai.roundtable.io/v1alpha1
kind: RoundTable
spec:
  github:
    tokenSecretRef: {name: roundtable-github-token, key: token}
    additionalTokens:
      - match: dapperdivers/dapper-cluster      # owner or owner/repo
        tokenSecretRef: {name: dapper-cluster-github-token, key: token}
        knights: [rt-operator, rt-lead]         # optional; empty = every knight of the table
```

| Field | Meaning |
|-------|---------|
| `match` | Owner (`dapperdivers`) or repository (`dapperdivers/dapper-cluster`). Pattern `^[A-Za-z0-9_.-]+(/[A-Za-z0-9_.-]+)?$`. It is the list key, so each value appears at most once. |
| `tokenSecretRef` | Required. Secret and key in the table's namespace. `optional: true` lets the pod start while the Secret or key is missing. |
| `knights` | Optional. Knight names the token is for. Empty means every knight of the table. |

The list holds at most 10 items. Only a RoundTable has `additionalTokens`: the
Knight's `spec.github` has no such field (the API server drops it). A Knight
gets its additional tokens from its table, found through the
`ai.roundtable.io/round-table` label (or the legacy `ai.roundtable.io/table`).

### Per-knight limit

With `knights` set, only Knights with those names get the token. Mission
knights are named `<mission>-<knight>` and never match a name, so **a mission
knight gets an additional token only when its `knights` list is empty**. The
ephemeral RoundTable of a mission copies the parent table's `spec.github`,
`additionalTokens` included.

### Opt-out

A Knight opts out of all of its table's additional tokens:

```yaml
kind: Knight
spec:
  github:
    disableAdditionalTokens: true
```

The default token is not affected.

## In the pod

For each item that applies to the knight, item `<i>` (its index in
`additionalTokens`, counted over the whole list) is mounted read-only into the
main container:

```
/var/run/secrets/roundtable/github-additional/<i>/token
```

The whole Secret volume is mounted (no `subPath`), so the kubelet refreshes the
file when the Secret changes. Sidecars do not get the tokens. The directory is
separate from the default token's `/var/run/secrets/roundtable/github`.

`GITHUB_ADDITIONAL_TOKENS` lists the mounted tokens as a JSON array in list
order, with paths only:

```json
[{"match":"dapperdivers/dapper-cluster","file":"/var/run/secrets/roundtable/github-additional/0/token"}]
```

The variable is unset when no additional token applies to the knight. No env
var holds a token value. Because the indexes come from the whole list, a knight
that gets only item 1 sees `.../github-additional/1/token`; read the paths from
the env var rather than building them.

Changing `additionalTokens` (adding an item, editing `knights`, a new Secret
name) changes the knight's pod template, so the pods roll. Rotating the token
inside the same Secret does not.

## Rules for dapper-cluster

The `dapperdivers/dapper-cluster` token lets knights open PRs on the cluster
repository. Knights that use it follow these rules:

- Branch names start with `knight/`.
- Changes stay under `kubernetes/apps/roundtable/`.
- Never touch RBAC, token, namespace or encrypted (SOPS) files.
- Open PRs only. rt-lead reviews and merges them ([decision 31](decisions.md), #288).
