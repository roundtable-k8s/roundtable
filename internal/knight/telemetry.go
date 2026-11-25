/*
Copyright 2026 dapperdivers.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package knight

import (
	"os"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// Telemetry is the OpenTelemetry configuration the operator passes on to every
// knight pod. It is read once from the operator's own environment (set by the
// Helm chart) so users configure telemetry in one place, never per knight.
type Telemetry struct {
	Endpoint       string
	Protocol       string
	Sampler        string
	SamplerArg     string
	ResourceAttrs  string // operator-level extra attributes, "k=v,k=v"
	SDKDisabled    string
	CaptureContent string
}

var (
	telemetryOnce   sync.Once
	telemetryLoaded Telemetry
)

// LoadTelemetry reads the telemetry settings from the operator environment.
func LoadTelemetry() Telemetry {
	return Telemetry{
		Endpoint:       os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		Protocol:       os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL"),
		Sampler:        os.Getenv("OTEL_TRACES_SAMPLER"),
		SamplerArg:     os.Getenv("OTEL_TRACES_SAMPLER_ARG"),
		ResourceAttrs:  os.Getenv("OTEL_RESOURCE_ATTRIBUTES"),
		SDKDisabled:    os.Getenv("OTEL_SDK_DISABLED"),
		CaptureContent: os.Getenv("ROUNDTABLE_TELEMETRY_CAPTURE_CONTENT"),
	}
}

// processTelemetry returns the operator environment's settings, read once.
func processTelemetry() Telemetry {
	telemetryOnce.Do(func() { telemetryLoaded = LoadTelemetry() })
	return telemetryLoaded
}

// Env returns the OTEL_* env for a knight's app container. Without an
// endpoint it returns only OTEL_SDK_DISABLED=true. OTEL_SERVICE_NAME is never
// set: each runtime names itself. The attributes reference POD_NAME,
// POD_NAMESPACE and NODE_NAME via $(VAR) expansion, so those downward-API
// vars must be defined earlier in the container's env.
func (t Telemetry) Env(k *aiv1alpha1.Knight, image string) []corev1.EnvVar {
	if t.Endpoint == "" {
		return []corev1.EnvVar{{Name: "OTEL_SDK_DISABLED", Value: "true"}}
	}
	attrs := []string{}
	if t.ResourceAttrs != "" {
		attrs = append(attrs, t.ResourceAttrs)
	}
	attrs = append(attrs, "roundtable.knight="+k.Name)
	if rt := telemetryRuntime(image); rt != "" {
		attrs = append(attrs, "roundtable.knight.runtime="+rt)
	}
	table := KnightTable(k)
	if table == "" {
		table = "none"
	}
	attrs = append(attrs, "roundtable.table="+table)
	if m := k.Labels[aiv1alpha1.LabelMission]; m != "" {
		attrs = append(attrs, "roundtable.mission="+m)
	}
	attrs = append(attrs,
		"k8s.namespace.name=$(POD_NAMESPACE)",
		"k8s.pod.name=$(POD_NAME)",
		"k8s.node.name=$(NODE_NAME)",
	)

	capture := t.CaptureContent
	if capture == "" {
		capture = "true"
	}
	env := []corev1.EnvVar{{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: t.Endpoint}}
	for _, kv := range [][2]string{
		{"OTEL_EXPORTER_OTLP_PROTOCOL", t.Protocol},
		{"OTEL_TRACES_SAMPLER", t.Sampler},
		{"OTEL_TRACES_SAMPLER_ARG", t.SamplerArg},
		{"OTEL_SDK_DISABLED", t.SDKDisabled},
	} {
		if kv[1] != "" {
			env = append(env, corev1.EnvVar{Name: kv[0], Value: kv[1]})
		}
	}
	return append(env,
		corev1.EnvVar{Name: "OTEL_RESOURCE_ATTRIBUTES", Value: strings.Join(attrs, ",")},
		corev1.EnvVar{Name: "ROUNDTABLE_TELEMETRY_CAPTURE_CONTENT", Value: capture},
	)
}

// telemetryRuntime maps the knight image to the telemetry runtime value
// ("pi" or "claude"); empty when unknown.
func telemetryRuntime(image string) string {
	switch AgentRuntimeForImage(image) {
	case AgentRuntimeClaude:
		return "claude"
	case AgentRuntimePi:
		return "pi"
	}
	return ""
}

// mergeMissingEnv appends the vars from add whose names are not set in
// userEnv (the knight's spec.env and builder extra env), so those win over
// operator defaults. Names already in env are not checked.
func mergeMissingEnv(env, add, userEnv []corev1.EnvVar) []corev1.EnvVar {
	have := map[string]bool{}
	for _, e := range userEnv {
		have[e.Name] = true
	}
	for _, e := range add {
		if !have[e.Name] {
			env = append(env, e)
		}
	}
	return env
}
