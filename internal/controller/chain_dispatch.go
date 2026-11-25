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
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
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

// dispatchReadySteps starts every Pending step whose retry backoff has passed
// and whose dependencies are done: it skips or fails it on its when
// expression, hands loops and fan-outs to their reconcilers and publishes
// plain steps. While a quota-deferred step waits for its plan window it
// dispatches nothing and returns held=true with the wait until the earliest
// deferred step is due.
func (r *ChainReconciler) dispatchReadySteps(ctx context.Context, chain *aiv1alpha1.Chain, run *chainRun) (wait time.Duration, held bool) {
	log := logf.FromContext(ctx)

	// #321: a quota-deferred step parks the chain: nothing further is
	// dispatched (so nothing fails and nothing escalates, no needs-human)
	// until the refused plan window resets; then the deferred step(s) run
	// again on their primary knight and the chain picks up where it left off.
	if next := wakeDeferredSteps(chain, time.Now()); next != nil {
		log.Info("Chain has a quota-deferred step; holding further steps", "requeueAfter", next.Time)
		return time.Until(next.Time), true
	}

	// Find ready steps and publish
	for i := range chain.Spec.Steps {
		step := &chain.Spec.Steps[i]
		ss := run.statusMap[step.Name]
		if ss.Phase != aiv1alpha1.ChainStepPhasePending {
			continue
		}
		if !stepReady(chain, run, step, ss) {
			continue
		}

		// when: skip the step without dispatching anything. A resumed loop or
		// fan-out (retry-failed) already passed its gate.
		if step.When != "" && ss.Loop == nil && ss.FanOut == nil && !r.passesWhen(ctx, chain, step, ss) {
			continue
		}

		// loop: run the body round after round.
		if step.Loop != nil {
			r.startLoop(chain, step, ss)
			r.reconcileLoop(ctx, run.nc, chain, step, ss, run.failing)
			continue
		}

		// forEach: fan out into one task per item.
		if step.ForEach != nil {
			if msg := r.startFanOut(ctx, chain, step, ss); msg != "" {
				r.failFanOutStep(chain, ss, msg)
				continue
			}
			r.reconcileFanOut(ctx, run.nc, chain, step, ss)
			continue
		}

		r.dispatchStep(ctx, chain, run.nc, step, ss)
	}
	return 0, false
}

// stepReady reports whether a Pending step may start: its retry backoff (if
// retrying) has passed and every dependency succeeded, was skipped, or failed
// with continueOnFailure.
func stepReady(chain *aiv1alpha1.Chain, run *chainRun, step *aiv1alpha1.ChainStep, ss *aiv1alpha1.ChainStepStatus) bool {
	// Check if retry backoff applies (per-step policy overrides chain-level)
	if ss.Retries > 0 && ss.CompletedAt != nil {
		retryPolicy := chain.Spec.RetryPolicy
		spec := run.specMap[step.Name]
		if spec != nil && spec.Retry != nil {
			retryPolicy = &aiv1alpha1.ChainRetryPolicy{
				BackoffSeconds: spec.Retry.BackoffSeconds,
			}
		}
		if retryPolicy != nil {
			backoff := time.Duration(retryPolicy.BackoffSeconds) * time.Second
			if time.Since(ss.CompletedAt.Time) < backoff {
				return false
			}
		}
	}

	// Check dependencies
	ready := true
	for _, dep := range step.DependsOn {
		depStatus := run.statusMap[dep]
		depSpec := run.specMap[dep]
		if depStatus == nil {
			ready = false
			break
		}
		switch depStatus.Phase {
		case aiv1alpha1.ChainStepPhaseSucceeded, aiv1alpha1.ChainStepPhaseSkipped:
			// OK — a step skipped by its when expression doesn't block
			// its dependents (a chain-failure skip ends the run anyway).
		case aiv1alpha1.ChainStepPhaseFailed:
			if depSpec != nil && depSpec.ContinueOnFailure {
				// OK
			} else {
				ready = false
			}
		default:
			ready = false
		}
	}
	return ready
}

// passesWhen evaluates the step's when expression. A false expression skips
// the step and an evaluation error fails it; both return false.
func (r *ChainReconciler) passesWhen(ctx context.Context, chain *aiv1alpha1.Chain, step *aiv1alpha1.ChainStep, ss *aiv1alpha1.ChainStepStatus) bool {
	log := logf.FromContext(ctx)
	ok, err := r.evaluateWhen(ctx, chain, step)
	if err != nil {
		log.Info("Step when expression failed", "step", step.Name, "error", err.Error())
		ss.Phase = aiv1alpha1.ChainStepPhaseFailed
		ss.Error = fmt.Sprintf("when: %v", err)
		now := metav1.Now()
		ss.CompletedAt = &now
		r.Recorder.Eventf(chain, corev1.EventTypeWarning, "StepWhenError", "Step %s: when: %v", step.Name, err)
		return false
	}
	if !ok {
		log.Info("Skipping step, when is false", "step", step.Name)
		skipStep(ss, stepReasonWhenFalse)
		r.Recorder.Eventf(chain, corev1.EventTypeNormal, "StepSkipped", "Step %s skipped: when is false", step.Name)
		return false
	}
	return true
}

// dispatchStep renders a plain step's task, resolves its knight and model and
// publishes it, moving the step to Running. Render, model and resumeRunId
// errors fail the step; a missing knight or a publish error leaves it Pending
// for the next reconcile.
func (r *ChainReconciler) dispatchStep(ctx context.Context, chain *aiv1alpha1.Chain, nc natsConfig, step *aiv1alpha1.ChainStep, ss *aiv1alpha1.ChainStepStatus) {
	log := logf.FromContext(ctx)

	// Render task template
	taskStr, err := r.renderTemplate(chain, step.Task)
	if err != nil {
		log.Error(err, "Failed to render template", "step", step.Name)
		ss.Phase = aiv1alpha1.ChainStepPhaseFailed
		ss.Error = fmt.Sprintf("template render error: %v", err)
		now := metav1.Now()
		ss.CompletedAt = &now
		return
	}

	// A plan-quota refusal re-runs on the step's fallback knight with NO
	// model override (#321): the cheap/fast/deep aliases are Claude models.
	knightName, stepModel := step.KnightRef.Name, step.Model
	if ss.Knight != "" && ss.Knight != step.KnightRef.Name {
		knightName, stepModel = ss.Knight, ""
	}

	// Get knight domain
	knight := &aiv1alpha1.Knight{}
	if err := r.Get(ctx, types.NamespacedName{Name: knightName, Namespace: chain.Namespace}, knight); err != nil {
		log.Error(err, "Failed to get knight", "knightRef", knightName)
		return
	}

	// Resolve the per-step model override. A model the knight's runtime
	// can't run is a spec error: fail the step (no retry) rather than let
	// the knight silently fall back to its default model.
	model, err := knightpkg.ResolveModel(stepModel, nc.ModelAliases, knightpkg.EffectiveImage(knightpkg.WithTableDefaults(ctx, r.Client, knight), r.DefaultKnightImage))
	if err != nil {
		log.Info("Rejected step model", "step", step.Name, "knight", knightName, "reason", err.Error())
		r.Recorder.Eventf(chain, corev1.EventTypeWarning, "InvalidStepModel", "Step %s: %v", step.Name, err)
		ss.Phase = aiv1alpha1.ChainStepPhaseFailed
		ss.Error = fmt.Sprintf("invalid model for knight %q: %v", knightName, err)
		ss.Model = ""
		now := metav1.Now()
		ss.CompletedAt = &now
		return
	}

	// The run ID shares the final subject token with the timestamp (joined
	// by "-") so the result subject keeps the same token count and the
	// wildcard fallback in pollResult still matches.
	taskID := fmt.Sprintf("chain-%s-%s.%s-%d", chain.Name, step.Name, chain.Status.RunID, time.Now().UnixMilli())

	// resumeRunId: carry an earlier run's ID so the knight resumes that
	// session. It does not change the chain's own run identity (KV keys,
	// taskId).
	runID := chain.Status.RunID
	if step.ResumeRunID != "" {
		resumed, err := evalString(step.ResumeRunID, r.conditionVars(ctx, chain, step.ResumeRunID))
		if err != nil {
			log.Info("Step resumeRunId expression failed", "step", step.Name, "error", err.Error())
			ss.Phase = aiv1alpha1.ChainStepPhaseFailed
			ss.Error = fmt.Sprintf("resumeRunId: %v", err)
			now := metav1.Now()
			ss.CompletedAt = &now
			r.Recorder.Eventf(chain, corev1.EventTypeWarning, "StepResumeRunIDError", "Step %s: resumeRunId: %v", step.Name, err)
			return
		}
		if resumed != "" {
			runID = resumed
		}
	}

	if ss.Reason == stepReasonJSONRetry {
		taskStr = jsonRetryTask(taskStr, ss.Error)
	}

	payload := natspkg.TaskPayload{
		TaskID:    taskID,
		ChainName: chain.Name,
		StepName:  step.Name,
		RunID:     runID,
		Task:      taskStr,
		Model:     model,
		Effort:    step.Effort,
	}

	seq, err := r.publishTask(withRunSpan(ctx, chain, plainStepSpanKey(step.Name, taskID)), nc, knight.Spec.Domain, knightName, payload)
	if err != nil {
		log.Error(err, "Failed to publish task", "step", step.Name)
		return
	}

	now := metav1.Now()
	ss.Phase = aiv1alpha1.ChainStepPhaseRunning
	ss.StartedAt = &now
	ss.PickedUpAt = nil
	ss.TaskSeq = seq
	ss.TaskID = taskID
	ss.Model = model
	ss.Knight = knightName
	ss.Error = ""
	log.Info("Published step task", "step", step.Name, "taskId", taskID, "knight", knightName, "model", model)
}
