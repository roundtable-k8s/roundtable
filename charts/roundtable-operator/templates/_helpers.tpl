{{/*
Expand the name of the chart.
*/}}
{{- define "roundtable-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "roundtable-operator.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "roundtable-operator.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "roundtable-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "roundtable-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "roundtable-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Service account name
*/}}
{{- define "roundtable-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "roundtable-operator.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Name of the Secret holding the gateway bearer token: the user-supplied one,
or the chart-generated <fullname>-gateway-token.
*/}}
{{- define "roundtable-operator.gatewayTokenSecret" -}}
{{- default (printf "%s-gateway-token" (include "roundtable-operator.fullname" .)) .Values.gateway.auth.existingSecret }}
{{- end }}

{{/*
OpenTelemetry env for a component's container. Call with
(dict "root" . "service" "<service.name>"). Renders the OTEL_* variables when
telemetry.enabled, otherwise only OTEL_SDK_DISABLED=true. Resource attributes
are sorted by key (range over a map is ordered) for stable output.
*/}}
{{- define "roundtable-operator.telemetryEnv" -}}
{{- $t := .root.Values.telemetry -}}
{{- if $t.enabled -}}
{{- $endpoint := $t.otlp.endpoint }}
{{- if and (not $endpoint) $t.collector.enabled }}
{{- $endpoint = include "roundtable-operator.collectorEndpoint" .root }}
{{- end }}
{{- if $endpoint }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ $endpoint | quote }}
{{- end }}
- name: OTEL_EXPORTER_OTLP_PROTOCOL
  value: {{ $t.otlp.protocol | quote }}
- name: OTEL_TRACES_SAMPLER
  value: parentbased_traceidratio
- name: OTEL_TRACES_SAMPLER_ARG
  value: {{ $t.sampling.ratio | toString | quote }}
- name: OTEL_SERVICE_NAME
  value: {{ .service | quote }}
{{- $attrs := list }}
{{- range $k, $v := $t.resourceAttributes }}
{{- $attrs = append $attrs (printf "%s=%v" $k $v) }}
{{- end }}
{{- if $attrs }}
- name: OTEL_RESOURCE_ATTRIBUTES
  value: {{ join "," $attrs | quote }}
{{- end }}
- name: ROUNDTABLE_TELEMETRY_CAPTURE_CONTENT
  value: {{ $t.captureContent | quote }}
{{- else }}
- name: OTEL_SDK_DISABLED
  value: "true"
{{- end }}
{{- end }}

{{/*
Name of the optional OpenTelemetry Collector Deployment and Service.
*/}}
{{- define "roundtable-operator.collectorName" -}}
{{- printf "%s-otel-collector" (include "roundtable-operator.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
In-cluster OTLP endpoint of the optional collector: gRPC (4317) unless
telemetry.otlp.protocol is http/protobuf (4318).
*/}}
{{- define "roundtable-operator.collectorEndpoint" -}}
{{- $port := ternary 4318 4317 (eq .Values.telemetry.otlp.protocol "http/protobuf") -}}
{{- printf "http://%s.%s:%d" (include "roundtable-operator.collectorName" .) .Release.Namespace (int $port) }}
{{- end }}

{{/*
Selector labels of the optional collector. Deliberately a different
app.kubernetes.io/name from the operator so the operator's Deployment selector
and NetworkPolicy never match collector pods.
*/}}
{{- define "roundtable-operator.collectorSelectorLabels" -}}
app.kubernetes.io/name: roundtable-otel-collector
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
