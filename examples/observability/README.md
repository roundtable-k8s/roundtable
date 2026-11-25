# Observability example: Jaeger behind the chart's OTel collector

A copy-paste way to see traces from the operator, gateway, dashboard and
knights. The chart's optional collector ([docs](../../docs/helm-values.md#opentelemetry-collector))
receives OTLP, redacts secrets and exports traces to Jaeger. Jaeger is only a
try-it-out viewer: it keeps traces in memory, so they are lost on restart. The
chart stays backend-agnostic; swap the exporter in `values-telemetry.yaml` for
your own store.

Prompt and response text is captured by default (`telemetry.captureContent`),
redacted before export. With this example, those redacted prompts sit in
Jaeger's memory until the pod restarts.

Files:

- [`jaeger.yaml`](jaeger.yaml) — Jaeger all-in-one (`jaegertracing/all-in-one:1.62.0`,
  in-memory, OTLP on) as a Deployment and Service in `roundtable`.
- [`values-telemetry.yaml`](values-telemetry.yaml) — enables telemetry and the
  collector, with an `otlp/jaeger` exporter in the traces pipeline.

## Apply

Install or upgrade the operator with the values file, then deploy Jaeger into
the same namespace (the exporter endpoint assumes `roundtable`):

```bash
kubectl create namespace roundtable --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f examples/observability/jaeger.yaml

helm upgrade --install roundtable oci://ghcr.io/roundtable-k8s/charts/roundtable-operator \
  --namespace roundtable --create-namespace \
  -f examples/observability/values-telemetry.yaml

kubectl rollout status deploy/jaeger -n roundtable
kubectl get deploy -n roundtable   # the collector: <release>-roundtable-operator-otel-collector
```

The operator, gateway and dashboard send OTLP to the collector Service, which
forwards traces to `jaeger.roundtable.svc.cluster.local:4317`.

## Open the Jaeger UI

```bash
kubectl port-forward -n roundtable svc/jaeger 16686:16686
```

Then open <http://localhost:16686> and pick the `roundtable-operator`,
`roundtable-gateway` or `roundtable-ui` service. Run a task or chain to produce
traces.

## Not for production

Jaeger here has no persistence, no authentication and no retention limit. Use a
real trace store and access control for anything beyond trying Round Table out.
