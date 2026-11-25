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

package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"text/template"

	"k8s.io/apimachinery/pkg/types"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// renderTemplate renders Go templates in the task string with step outputs and input.
func (r *ChainReconciler) renderTemplate(chain *aiv1alpha1.Chain, taskStr string) (string, error) {
	return r.renderTemplateWith(chain, taskStr, nil)
}

// renderTemplateWith renders a task template with extra top-level data
// (forEach items add Item, item, ItemIndex and ItemJSON).
func (r *ChainReconciler) renderTemplateWith(chain *aiv1alpha1.Chain, taskStr string, extra map[string]any) (string, error) {
	if !strings.Contains(taskStr, "{{") {
		return taskStr, nil
	}

	// Build template data
	steps := make(map[string]map[string]string)
	for _, ss := range chain.Status.StepStatuses {
		steps[ss.Name] = stepTemplateData(ss)
	}

	data := map[string]any{
		"Steps":  steps,
		"Input":  chain.Spec.Input,
		"Fields": inputFields(chain.Spec.Input),
	}
	maps.Copy(data, extra)

	tmpl, err := template.New("task").Parse(taskStr)
	if err != nil {
		return "", fmt.Errorf("template parse error: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("template execute error: %w", err)
	}

	return buf.String(), nil
}

// resolveNATSConfig looks up the chain's RoundTable and returns the NATS configuration.
// Falls back to defaultNATSConfig if no roundTableRef is specified.
func (r *ChainReconciler) resolveNATSConfig(ctx context.Context, chain *aiv1alpha1.Chain) (natsConfig, error) {
	if aiv1alpha1.RefName(chain.Spec.RoundTableRef) == "" {
		return natsConfig{}, fmt.Errorf("chain %s/%s has no roundTableRef configured", chain.Namespace, chain.Name)
	}

	rt := &aiv1alpha1.RoundTable{}
	if err := r.Get(ctx, types.NamespacedName{Name: aiv1alpha1.RefName(chain.Spec.RoundTableRef), Namespace: chain.Namespace}, rt); err != nil {
		return natsConfig{}, fmt.Errorf("RoundTable %q not found: %w", aiv1alpha1.RefName(chain.Spec.RoundTableRef), err)
	}

	return natsConfig{
		SubjectPrefix: rt.Spec.NATS.SubjectPrefix,
		TasksStream:   rt.Spec.NATS.TasksStream,
		ResultsStream: rt.Spec.NATS.ResultsStream,
		ModelAliases:  rt.Spec.ModelAliases,
		Table:         rt.Name,
	}, nil
}

// publishTask publishes a task to NATS JetStream.
// It returns the tasks-stream sequence of the published message.
func (r *ChainReconciler) publishTask(ctx context.Context, nc natsConfig, domain, knightName string, payload natspkg.TaskPayload) (uint64, error) {
	natsCli, err := r.natsClient()
	if err != nil {
		return 0, err
	}

	subject := natspkg.TaskSubject(nc.SubjectPrefix, domain, knightName)
	return natsCli.PublishJSONSeqCtx(ctx, subject, payload, natspkg.WithTable(nc.Table))
}

// inputFields returns the top-level fields of spec.input when it is a JSON
// object (else an empty map), for templates that must pick single fields
// instead of rendering all of .Input, e.g. {{ .Fields.url }}.
func inputFields(input string) map[string]any {
	fields := map[string]any{}
	if strings.HasPrefix(strings.TrimSpace(input), "{") {
		if err := json.Unmarshal([]byte(input), &fields); err != nil {
			return map[string]any{}
		}
	}
	return fields
}
