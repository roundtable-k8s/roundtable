# Egress starter allowlists

Starters for `Knight.spec.egress` and `RoundTable.spec.defaults.egress`; see
[`docs/egress.md`](../../docs/egress.md). Nothing here is applied by default:
the files are examples to copy from. Each file holds one Knight or RoundTable.

| File | Allows |
| --- | --- |
| [`github-read.yaml`](github-read.yaml) | `github.com`, `api.github.com`, `*.githubusercontent.com` |
| [`package-docs.yaml`](package-docs.yaml) | Go, npm, Python and Rust package documentation and registries |
| [`model-providers.yaml`](model-providers.yaml) | `openrouter.ai`, `api.anthropic.com`, as a table default |
| [`research.yaml`](research.yaml) | A research knight: search and a short list of reference sites |
| [`feed-reader.yaml`](feed-reader.yaml) | A feed-reading knight: its feed hosts and nothing else |

A knight's own `egress` replaces the table's; allowlists are not merged, so a
knight that needs model access *and* GitHub lists both.
