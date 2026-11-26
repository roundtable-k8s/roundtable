# Observability standard: backend scores and shortlist

Status: desk evaluation, input to the live trial. Issue: #709, first slice of #385.

This doc scores the candidate backends listed in #385 against the criteria
listed in #385, and ends with a shortlist of at most two for the live trial. It
does not pick a backend (decision 17 keeps the pick parked), does not set
retention or access rules, and adds no criteria beyond those in #385 plus
principle 9 ([principles](../principles.md), "Don't rebuild the wheel"), which
#385 names as the tie-breaker.

It builds on [telemetry conventions](telemetry-conventions.md) (decisions 14 to
16) and [observability](../observability.md) (the `roundtable_*` metrics).

## How the scores were made

- **Desk work only.** Nothing was installed or run. Every cell comes from the
  candidate's own documentation or repository, read on 2026-11-26.
- **Sources.** The published documentation sites were not reachable from the
  environment that wrote this doc, so each link points at the Markdown or MDX
  source of the page in the candidate's own repository (the same text the site
  is built from), pinned to the default branch. Versions are the latest release
  tags on that date: Tempo v3.1.1, VictoriaTraces v0.13.0, Langfuse v4.88.0,
  arize-phoenix v22.1.0.
- **Scale.** `2` meets the criterion as documented. `1` meets it in part, or
  only with extra configuration or components. `0` does not meet it as
  documented. `?` could not be scored from documentation.
- **`unverified`** marks a claim that documentation did not let us check. It is
  never a guess: it is a question for the trial. Statements about a candidate
  that come only from the #385 text, and that we did not re-check, say so.
- The scores are an input to a decision, not a ranking formula. They are not
  summed.

## Scores

Columns are the candidates from #385. Rows are the criteria from #385
(numbered as there; criterion 7 is split into its three parts) plus principle
9 as a row of its own. The two "Others worth a look" in #385 (SigNoz,
HyperDX/ClickStack, Jaeger v2, OpenLIT, Opik, Laminar) were not scored: #385
says to include them only if they beat a listed candidate on a criterion, and
no criterion in this desk pass showed a gap that needs one (see
[Not scored](#not-scored)).

| Criterion | Grafana LGTM (Tempo) | Victoria family (VictoriaTraces) | OpenSearch / ELK | Langfuse | Arize Phoenix |
|---|---|---|---|---|---|
| **1. Follow one task end to end, with prompts readable** | **1.** Tempo stores any OTLP trace and queries it with TraceQL ([docs][tempo-docs]), so the whole chain or mission tree is stored. Prompt text is shown as raw span attributes in Grafana's trace view. A dedicated LLM view is **unverified**: the Tempo docs contain no `gen_ai` page, and the "Grafana LLM observability" page cited in #385 was not reachable (it is not in the Tempo repository). | **1.** Stores OTLP spans and serves the Jaeger query API, read in Grafana or the Jaeger frontend ([querying][vt-query]). Whole tree is stored; prompt text is a raw attribute. No view understands `gen_ai.*`: the VictoriaTraces docs contain no `gen_ai` or LLM page. | **?** Trace analytics via Data Prepper's OTel trace source ([docs][os-prepper-trace]) and Jaeger data ([docs][os-jaeger]). Whether prompt attributes render readably is **unverified**. Elastic APM was **not checked** (its docs are not in a repository we could read). | **1.** Maps OTLP spans to its LLM data model and shows messages per generation ([OTel endpoint][lf-otel]). By default it focuses on LLM-relevant spans (SDK spans, spans with `gen_ai.*`, known LLM instrumentors); the doc describes this for the SDK filter, so whether plain chain, NATS and operator spans show on the raw OTLP endpoint is **unverified**. | **2.** Trace collector and UI in one; converts `gen_ai.*` to OpenInference at ingest so messages, tool calls and token counts render ([release note][px-semconv]). Non-LLM spans are not discussed: **unverified**. |
| **2. Cost per knight, per job (chain), per table** | **1.** No cost computation in Tempo itself (no cost page in its docs). Cost comes from the existing `roundtable_*` Prometheus metrics and the `roundtable.cost.usd` span attribute; Tempo's metrics-generator ([docs][tempo-mg]) can derive Prometheus metrics from spans. Per chain cost already exists as `roundtable_chain_step_cost_usd_total` ([observability][rt-obs]). | **1.** Same as Tempo: no cost feature in its docs; cost stays in Prometheus metrics. VictoriaTraces can be alerted on through vmalert ([docs][vt-vmalert]). | **?** No cost feature found in the docs read. **Unverified.** | **?** The repository README lists cost tracking ([README][lf-readme]). Per knight, chain and table breakdowns, and whether cost is computed from `gen_ai.usage.*` or must be supplied, are **unverified**. | **1.** Computes cost from token counts and a built-in model price table, rolled up per trace and project ([docs][px-cost]). It reads OpenInference token attributes, so it depends on the `gen_ai.*` conversion. Per chain and per table grouping is **unverified**. |
| **3. Homelab-light** | **1.** Separate Apache-licensed `operations/` manifests and a [docker example][tempo-docker] exist. The configuration reference mentions a Kafka-fed block-builder ([config][tempo-config]); whether a small install needs Kafka is **unverified**. Memory and storage at rest: **unverified**. Full LGTM would also add Loki, but the standard can keep VictoriaLogs (Grafana already has it as a datasource, per #385). | **2.** Single binary, single node documented, retention by time or by disk usage ([README][vt-readme]). The README claims up to 3.7x less RAM and 2.6x less CPU than Tempo; that is a vendor claim, **unverified**. Cost of "light": v0.x, no tail sampling yet ([roadmap][vt-roadmap]). | **0.** #385 calls it the heaviest option. We did not measure; the documentation pages read give no footprint numbers: **unverified**. | **0.** Documented minimum infrastructure is Postgres (2 CPU, 4 GiB), ClickHouse (2 CPU, 8 GiB), Redis or Valkey (1 CPU, 1.5 GiB), web and worker containers (2 CPU, 4 GiB each) and blob storage ([scaling][lf-scaling]). The Helm chart v2 also needs cert-manager and the ClickHouse operator installed first ([Helm][lf-helm]). | **2.** One container with SQLite by default, or PostgreSQL for production ([architecture][px-arch]). Cost of "light": SQLite is for single-user and development use; production wants Postgres. |
| **4. Open source, self-hostable, licence named** | **1.** AGPL-3.0, with `operations/` and `pkg/tempopb` under Apache-2.0 ([LICENSING][tempo-lic]). Grafana itself is AGPL-3.0 (repository metadata). No paid tier gating found in what we read. | **2.** Apache-2.0 ([LICENSE][vt-lic]); single and cluster versions are free ([README][vt-readme]). The [roadmap][vt-roadmap] lists some features as enterprise only (per-tenant statistics, vtstorage node discovery). | **1.** OpenSearch is Apache-2.0 (repository metadata). The Elasticsearch and Kibana repositories report no single licence ([Elasticsearch][es-repo]); the licence terms that apply to Elastic APM are **unverified**. | **1.** MIT core, but directories named `ee/` are under a separate licence ([LICENSE][lf-lic]). Project-level roles, data retention policies, audit logs and server-side data masking need a paid licence key ([license keys][lf-key]). That breaks "no feature in the standard may depend on a paid tier" for access and retention. | **1.** Elastic License 2.0 ([LICENSE][px-lic]): source-available, not an OSI open-source licence; the full limitations clause was not read here. No paid gating of the features scored here found ([RBAC][px-rbac], [retention][px-ret]). |
| **5. OTel GenAI conventions, and attribute churn** | **1.** Ingests any attribute, so `gen_ai.*` is stored whatever its name; nothing renders it as an LLM call (see 1). Churn (`gen_ai.system` to `gen_ai.provider.name`) costs nothing in storage; it only matters to queries and dashboards we write. | **1.** Same as Tempo: stores whatever arrives, renders nothing LLM-specific. | **?** **Unverified** (no `gen_ai` page found in the docs read). | **1.** Maps received traces to its data model and "supports additional attributes popular in the OTel GenAI ecosystem"; it says the conventions are still evolving and asks for reports on mismatches ([property mapping][lf-otel]). Which `gen_ai.*` names are mapped, and whether `gen_ai.provider.name` is, are **unverified** from the prose read. | **1.** Conversion table in the [release note][px-semconv] (arize-phoenix 15.10.0 or later). It names `gen_ai.system` and `gen_ai.output.message` (singular). Our [conventions](telemetry-conventions.md) emit `gen_ai.provider.name` and `gen_ai.output.messages` (plural), so whether the current names convert is **unverified** and is a trial question. Existing OpenInference attributes win over converted ones. |
| **6. Multi-table: filter and group by `table`, ideally per-table access and retention** | **1.** Filter and group by `roundtable.table` in TraceQL is expected to work on any span attribute (**unverified**, not read on the TraceQL page). Multi-tenancy exists (`multitenancy_enabled`, off by default; tenant from the `X-Scope-OrgID` header) ([tenant IDs][tempo-tenant]). Tempo trusts that header and expects an authenticating reverse proxy to set it ([authentication][tempo-auth]). A tenant per table is possible but needs a collector pipeline per table. | **1.** Multitenancy by `(AccountID, ProjectID)`, thousands of tenants are fine, but it does no per-tenant authorisation: use vmauth or similar ([README][vt-readme]). Filtering by attribute in the UI: **unverified**. | **?** **Unverified.** | **1.** Organisations and projects are its tenancy; project-level roles are paid (see 4) ([RBAC][lf-rbac]). Mapping the `table` span attribute to a filter or to a project: **unverified**. | **0** for isolation: one instance is one tenant; multi-team means multiple instances ([architecture][px-arch]). Filtering by attribute inside a project: **unverified**. Roles are `admin`, `member`, `viewer` ([RBAC][px-rbac]). |
| **7a. Access control for traces with prompt content** | **1.** None built in: put an authenticating proxy in front and use Grafana's own roles (the latter not read here: **unverified**) ([authentication][tempo-auth]). | **1.** None built in; vmauth per tenant ([README][vt-readme]). | **?** **Unverified.** | **1.** Free tier has user authentication and basic roles; project-level roles are paid ([license keys][lf-key]). | **1.** Three roles, all in the free edition ([RBAC][px-rbac]); no per-project isolation inside an instance. |
| **7b. Retention controls** | **1.** Compactor retention is configurable ([config][tempo-config]); retention per tenant: **unverified**. | **1.** One global `-retentionPeriod` (default 7 days) plus disk-usage caps; no per-tenant retention found ([README][vt-readme]). | **?** **Unverified.** | **0** in the free edition: data retention policies need a licence key ([license keys][lf-key]). | **2.** Per-project policies by age or trace count, default policy settable with `PHOENIX_DEFAULT_RETENTION_POLICY_DAYS`; default is keep forever ([retention][px-ret]). |
| **7c. Alerting: does it reuse Prometheus rules and Alertmanager?** | **2.** Metrics-generator produces Prometheus-compatible metrics from spans ([docs][tempo-mg]), so existing Prometheus rules and Alertmanager apply. Alerting on `roundtable_*` metrics needs no trace backend at all. | **2.** vmalert can evaluate rules against VictoriaTraces ([docs][vt-vmalert]); the README says alerts on VictoriaTraces' own metrics can run in vmalert or Prometheus ([README][vt-readme]). The cluster already runs vmalert (per #385). | **?** **Unverified.** | **0.** No Prometheus or Alertmanager integration found in the pages read: **unverified**. Alerting on `roundtable_*` metrics is unaffected, because those are in Prometheus. | **1.** The server can expose a Prometheus listener ([configuration][px-config]), so alerts on the viewer itself reuse Alertmanager. Alerts on trace content: **unverified**. |
| **Principle 9: what a cluster that already runs Prometheus and an OTel collector must add** | **Tempo**, an OTLP receiver on it, a Grafana Tempo datasource, and (if span-derived metrics are wanted) metrics-generator with remote write to Prometheus. Loki is not needed if VictoriaLogs stays. Grafana, Prometheus and Alertmanager are reused. | **VictoriaTraces** (one binary), the collector's OTLP exporter pointing at it, and a Grafana Jaeger datasource. Also reuses vmalert and VictoriaLogs where the cluster has them. | **OpenSearch** cluster, **Data Prepper** (or the collector with an OpenSearch exporter: **unverified**), and OpenSearch Dashboards. Reuses nothing the cluster runs. | **Langfuse web and worker, Postgres, ClickHouse, Redis or Valkey, blob storage**, plus cert-manager and the ClickHouse operator for the Helm chart; the collector sends OTLP over HTTP only (no gRPC) ([OTel endpoint][lf-otel]). Reuses the collector; Grafana and Prometheus play no part. | **Phoenix** (one container; Postgres for production) and a collector pipeline to its OTLP port (gRPC 4317 or HTTP 6006) ([configuration][px-config]). Reuses the collector and Prometheus; Grafana plays no part for traces. |

### What each candidate does with what the instrumentation already emits

The instrumentation contract is in [telemetry conventions](telemetry-conventions.md).

| | Decision 14: one trace per chain run or mission | Decision 15: prompt and response content on spans | Decision 16: `roundtable_*` metrics with `table` |
|---|---|---|---|
| **Tempo** | Stores the whole tree; shown as a waterfall in Grafana. Nothing LLM-aware. Very long traces (a chain that runs for hours) and any per-trace size or duration limits: **unverified**. | Stored as span attributes. Per-attribute size limits in Tempo: **unverified** (the conventions cap each content attribute at 16 KiB before export). Content can be dropped per pipeline in the collector. | Not involved: metrics stay in Prometheus. Span-derived metrics are optional. |
| **VictoriaTraces** | Same as Tempo, read through the Jaeger API. Very long traces: **unverified**. | Stored as span attributes; same collector-side control. | Not involved. |
| **OpenSearch / ELK** | **Unverified.** | **Unverified.** | Not involved. |
| **Langfuse** | Whole-trace display for LLM spans; non-LLM spans may be hidden by its default filter (see criterion 1). It groups by session and user when those attributes are on every span ([propagation][lf-otel]), so `roundtable.table` on every span is needed. | Maps messages from `gen_ai.*` attributes; server-side masking is paid, so redaction must happen in the collector. | Not involved: metrics stay in Prometheus. |
| **Phoenix** | LLM spans render fully after conversion; handling of non-LLM chain and NATS spans is **unverified**. | Messages, tool calls and token counts render from `gen_ai.*` ([release note][px-semconv]); attribute-name mismatch noted in criterion 5. | Not involved: metrics stay in Prometheus. |

Cost metrics for every candidate are already answered by Prometheus: per table
(`roundtable_table_cost_usd`), per chain (`roundtable_chain_step_cost_usd_total`)
and per knight (`roundtable_knight_llm_cost_usd_total`) ([observability][rt-obs],
[conventions](telemetry-conventions.md)). The trace backend adds drill-down,
not the totals.

## Not scored

SigNoz, HyperDX/ClickStack, Jaeger v2, OpenLIT, Opik and Laminar were not
scored. #385 says to include them only if they beat a listed candidate on a
criterion, and this desk pass did not find a criterion that the scored
candidates all fail in a way one of these is known to fix. This is an omission
of evidence, not a verdict: nothing about them was read.

## Shortlist for the live trial

At most two, chosen from the scores and the tie-breakers in #385 (principle 9,
content stays on and redacted, one trace per chain run or mission must read
well).

### 1. Grafana Tempo (general store, read in the existing Grafana)

Why: reuses Grafana, Prometheus, Alertmanager and the collector the cluster
already has (principle 9); Apache or AGPL, no paid tier found; mature release
line (v3.1.1); stores every span of the chain, the NATS hop and the knight.
Where it falls short: no LLM-specific view, so prompts read as raw attributes.

VictoriaTraces is the closest runner-up and scores higher on footprint and
licence, but it is a v0.x line whose README warns that on-disk structures and
APIs may change without backward compatibility, and it has no LLM view either.
It is left out to stay at two; the trial question on Tempo's footprint is the
one that could bring it back.

Questions only a live trial can answer:

- What do Tempo's memory, CPU and storage look like at rest and after the
  sample load, and does a small install need Kafka (the config reference
  mentions a Kafka-fed block-builder)?
- Can a chain run with two steps, a mission and an "ask" be followed end to end
  in Grafana, and is prompt text with the 16 KiB content cap readable there?
- Does Tempo accept a trace that stays open for the length of a long chain run,
  and are there duration or size limits that split it?
- Does filtering and grouping by `roundtable.table` work in TraceQL and in the
  dashboards, and is a tenant per table worth the extra pipelines?
- Can per-tenant retention be set, and does the metrics-generator add anything
  the `roundtable_*` metrics lack?
- How does it cope with `gen_ai.system` to `gen_ai.provider.name` in queries?

### 2. Arize Phoenix (LLM viewer, fed from the same collector)

Why: the only candidate documented to render `gen_ai.*` spans as LLM calls
without a vendor SDK, with computed cost; one container; free retention and
roles. Prefer it over Langfuse because Langfuse's retention and project roles
are paid features and its footprint is the largest of the five.

Open licence point, which is a question for the owner and not for the trial:
Phoenix is under Elastic License 2.0, not an OSI open-source licence. If
criterion 4 is read strictly as "OSI open source", Phoenix drops out and
Langfuse is excluded by its paid-gated features; then the shortlist would be
Tempo and VictoriaTraces with no LLM viewer.

Questions only a live trial can answer:

- Do our spans convert? Our conventions emit `gen_ai.provider.name` and
  `gen_ai.output.messages`; the conversion table names `gen_ai.system` and
  `gen_ai.output.message`.
- Do the non-LLM spans (chain run root, NATS hop, task) show in the trace view
  at all, or only the LLM ones?
- Is the cost it computes equal to `roundtable.cost.usd` for the same span,
  especially for claude-knight's native `claude_code.*` spans?
- Can it group by `roundtable.table`, and with one instance per table out of
  scope, is a single instance with a table filter acceptable for access?
- Memory, CPU and storage with SQLite and with Postgres after the sample load.

### Both?

#385 asks whether "both" is worth the extra moving parts. The trial should
feed one collector to both with the same sample spans and answer: does the LLM
viewer show something Grafana cannot (prompt view, cost per trace), and is that
worth running a second store with a second access and retention model? The
collector can strip content attributes per pipeline, so content could go to
only one of them.

## Links

Sources are in each candidate's own repository, default branch, read on
2026-11-26.

[tempo-docs]: https://github.com/grafana/tempo/tree/main/docs/sources/tempo
[tempo-mg]: https://github.com/grafana/tempo/blob/main/docs/sources/tempo/metrics-from-traces/metrics-generator/_index.md
[tempo-docker]: https://github.com/grafana/tempo/blob/main/docs/sources/tempo/docker-example.md
[tempo-config]: https://github.com/grafana/tempo/blob/main/docs/sources/tempo/configuration/_index.md
[tempo-lic]: https://github.com/grafana/tempo/blob/main/LICENSING.md
[tempo-tenant]: https://github.com/grafana/tempo/blob/main/docs/sources/tempo/configuration/tenant-ids.md
[tempo-auth]: https://github.com/grafana/tempo/blob/main/docs/sources/tempo/operations/authentication.md
[vt-readme]: https://github.com/VictoriaMetrics/VictoriaTraces/blob/master/docs/victoriatraces/README.md
[vt-query]: https://github.com/VictoriaMetrics/VictoriaTraces/tree/master/docs/victoriatraces/querying
[vt-vmalert]: https://github.com/VictoriaMetrics/VictoriaTraces/blob/master/docs/victoriatraces/vmalert.md
[vt-roadmap]: https://github.com/VictoriaMetrics/VictoriaTraces/blob/master/docs/victoriatraces/roadmap.md
[vt-lic]: https://github.com/VictoriaMetrics/VictoriaTraces/blob/master/LICENSE
[os-prepper-trace]: https://github.com/opensearch-project/documentation-website/blob/main/_data-prepper/pipelines/configuration/sources/otel-trace-source.md
[os-jaeger]: https://github.com/opensearch-project/documentation-website/blob/main/_observing-your-data/trace/trace-analytics-jaeger.md
[es-repo]: https://github.com/elastic/elasticsearch
[lf-otel]: https://github.com/langfuse/langfuse-docs/blob/main/content/integrations/native/opentelemetry/index.mdx
[lf-readme]: https://github.com/langfuse/langfuse/blob/main/README.md
[lf-scaling]: https://github.com/langfuse/langfuse-docs/blob/main/content/self-hosting/configuration/scaling.mdx
[lf-helm]: https://github.com/langfuse/langfuse-docs/blob/main/content/self-hosting/deployment/kubernetes-helm.mdx
[lf-lic]: https://github.com/langfuse/langfuse/blob/main/LICENSE
[lf-key]: https://github.com/langfuse/langfuse-docs/blob/main/content/self-hosting/license-key.mdx
[lf-rbac]: https://github.com/langfuse/langfuse-docs/blob/main/content/docs/administration/rbac.mdx
[px-semconv]: https://github.com/Arize-ai/phoenix/blob/main/docs/phoenix/release-notes/05-2026/05-15-2026-otel-semconv-conversion.mdx
[px-cost]: https://github.com/Arize-ai/phoenix/blob/main/docs/phoenix/tracing/how-to-tracing/cost-tracking.mdx
[px-arch]: https://github.com/Arize-ai/phoenix/blob/main/docs/phoenix/self-hosting/architecture.mdx
[px-lic]: https://github.com/Arize-ai/phoenix/blob/main/LICENSE
[px-rbac]: https://github.com/Arize-ai/phoenix/blob/main/docs/phoenix/settings/access-control-rbac.mdx
[px-ret]: https://github.com/Arize-ai/phoenix/blob/main/docs/phoenix/settings/data-retention.mdx
[px-config]: https://github.com/Arize-ai/phoenix/blob/main/docs/phoenix/self-hosting/configuration.mdx
[rt-obs]: ../observability.md
