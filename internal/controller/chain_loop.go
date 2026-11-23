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
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// Step phase reasons recorded in ChainStepStatus.Reason.
const (
	stepReasonWhenFalse   = "WhenFalse"
	stepReasonChainFailed = "ChainFailed"
)

const (
	// errEmptyOutput is the step error for a knight result with no content.
	errEmptyOutput = "knight returned empty output"
	// defaultLoopMaxIterations mirrors the CRD default for loop.maxIterations.
	defaultLoopMaxIterations = 3
	// stepOutputStatusLimit is how much of an output the status keeps.
	stepOutputStatusLimit = 4000
	// loopLastTemplateLimit bounds {{ .Loop.Last.Output }} in body tasks.
	loopLastTemplateLimit = 64 * 1024
)

// validateConditionsAndLoops compiles every when / loop.until expression and
// checks loop settings. It returns the ChainValid reason with the error.
func validateConditionsAndLoops(chain *aiv1alpha1.Chain) (string, error) {
	names := make(map[string]bool, len(chain.Spec.Steps))
	for _, s := range chain.Spec.Steps {
		names[s.Name] = true
	}
	for _, step := range chain.Spec.Steps {
		if step.When != "" {
			if _, err := compileCondition(step.When); err != nil {
				return aiv1alpha1.ReasonInvalidCondition, fmt.Errorf("step %q when: %w", step.Name, err)
			}
		}
		if step.ResumeRunID != "" {
			if step.Loop != nil || step.ForEach != nil {
				return aiv1alpha1.ReasonInvalidCondition, fmt.Errorf("step %q: resumeRunId is not valid on loop or forEach steps", step.Name)
			}
			if _, err := compileStringExpr(step.ResumeRunID); err != nil {
				return aiv1alpha1.ReasonInvalidCondition, fmt.Errorf("step %q resumeRunId: %w", step.Name, err)
			}
		}
		lp := step.Loop
		if lp == nil {
			if step.KnightRef.Name == "" || step.Task == "" {
				return aiv1alpha1.ReasonInvalidLoop, fmt.Errorf("step %q needs knightRef and task (only loop steps may omit them)", step.Name)
			}
			continue
		}
		var extra []string
		if step.KnightRef.Name != "" {
			extra = append(extra, "knightRef")
		}
		if step.Task != "" {
			extra = append(extra, "task")
		}
		if step.ForEach != nil {
			extra = append(extra, "forEach")
		}
		if step.Model != "" {
			extra = append(extra, "model")
		}
		if step.Retry != nil {
			extra = append(extra, "retry")
		}
		if step.OutputPath != "" {
			extra = append(extra, "outputPath")
		}
		if len(extra) > 0 {
			return aiv1alpha1.ReasonInvalidLoop, fmt.Errorf("loop step %q must not set %s (set them on its body steps)", step.Name, strings.Join(extra, ", "))
		}
		if lp.From != "" {
			if !names[lp.From] || lp.From == step.Name {
				return aiv1alpha1.ReasonInvalidLoop, fmt.Errorf("loop step %q from references unknown step %q", step.Name, lp.From)
			}
			if !slices.Contains(step.DependsOn, lp.From) {
				return aiv1alpha1.ReasonInvalidLoop, fmt.Errorf("loop step %q from step %q must be listed in dependsOn", step.Name, lp.From)
			}
		}
		if len(lp.Steps) == 0 {
			return aiv1alpha1.ReasonInvalidLoop, fmt.Errorf("loop step %q has no body steps", step.Name)
		}
		seen := map[string]bool{}
		for _, b := range lp.Steps {
			if b.Name == "" || b.KnightRef.Name == "" || b.Task == "" {
				return aiv1alpha1.ReasonInvalidLoop, fmt.Errorf("loop step %q: every body step needs name, knightRef and task", step.Name)
			}
			if seen[b.Name] {
				return aiv1alpha1.ReasonInvalidLoop, fmt.Errorf("loop step %q has duplicate body step %q", step.Name, b.Name)
			}
			seen[b.Name] = true
		}
		if _, err := compileCondition(lp.Until); err != nil {
			return aiv1alpha1.ReasonInvalidCondition, fmt.Errorf("loop step %q until: %w", step.Name, err)
		}
	}
	return "", nil
}

// stepTemplateData is the per-step entry of .Steps in task templates.
func stepTemplateData(ss aiv1alpha1.ChainStepStatus) map[string]string {
	d := map[string]string{
		"Output":     ss.Output,
		"Error":      ss.Error,
		"Phase":      string(ss.Phase),
		"Reason":     ss.Reason,
		"Outcome":    "",
		"Iterations": "0",
	}
	if ss.Loop != nil {
		d["Outcome"] = string(ss.Loop.Outcome)
		d["Iterations"] = strconv.Itoa(int(ss.Loop.Iteration))
	}
	return d
}

// mentions reports whether name appears in any of the expressions. It is a
// cheap over-approximation used to decide which full outputs to load.
func mentions(exprs []string, name string) bool {
	for _, e := range exprs {
		if strings.Contains(e, name) {
			return true
		}
	}
	return false
}

// fullOutput returns the full output stored in KV for key (current run only),
// falling back to the (possibly truncated) status copy.
func (r *ChainReconciler) fullOutput(ctx context.Context, chain *aiv1alpha1.Chain, key, fallback string) string {
	if full, ok := r.kvOutput(ctx, chain, key); ok {
		return full
	}
	return fallback
}

// conditionVars builds the CEL variables shared by when and until: input and
// steps. Outputs of steps the expressions mention are read in full from KV.
func (r *ChainReconciler) conditionVars(ctx context.Context, chain *aiv1alpha1.Chain, exprs ...string) map[string]any {
	steps := make(map[string]any, len(chain.Status.StepStatuses))
	for _, ss := range chain.Status.StepStatuses {
		out := ss.Output
		if ss.Phase == aiv1alpha1.ChainStepPhaseSucceeded && mentions(exprs, ss.Name) {
			out = r.fullOutput(ctx, chain, chain.Name+"."+ss.Name, out)
		}
		entry := map[string]any{
			"name":       ss.Name,
			"phase":      string(ss.Phase),
			"output":     clipForCEL(out),
			"error":      ss.Error,
			"reason":     ss.Reason,
			"outcome":    "",
			"iterations": int64(0),
		}
		if ss.Loop != nil {
			entry["outcome"] = string(ss.Loop.Outcome)
			entry["iterations"] = int64(ss.Loop.Iteration)
		}
		steps[ss.Name] = entry
	}
	return map[string]any{"input": chain.Spec.Input, "steps": steps}
}

// evaluateWhen decides whether a ready step runs. It returns run=false with
// a nil error when the step must be skipped.
func (r *ChainReconciler) evaluateWhen(ctx context.Context, chain *aiv1alpha1.Chain, step *aiv1alpha1.ChainStep) (bool, error) {
	if step.When == "" {
		return true, nil
	}
	return evalCondition(step.When, r.conditionVars(ctx, chain, step.When))
}

// skipStep marks a step Skipped with a reason.
func skipStep(ss *aiv1alpha1.ChainStepStatus, reason string) {
	now := metav1.Now()
	ss.Phase = aiv1alpha1.ChainStepPhaseSkipped
	ss.Reason = reason
	ss.CompletedAt = &now
}

// ---- loops ----

func loopMaxIterations(lp *aiv1alpha1.ChainLoop) int32 {
	if lp.MaxIterations > 0 {
		return lp.MaxIterations
	}
	return defaultLoopMaxIterations
}

func loopBodyKey(chain *aiv1alpha1.Chain, loopName, bodyName string) string {
	return chain.Name + "." + loopName + "." + bodyName
}

// startLoop initialises (or, after retry-failed, resumes) a loop step.
func (r *ChainReconciler) startLoop(chain *aiv1alpha1.Chain, spec *aiv1alpha1.ChainStep, ss *aiv1alpha1.ChainStepStatus) {
	if ss.Loop == nil {
		ls := &aiv1alpha1.ChainLoopStatus{MaxIterations: loopMaxIterations(spec.Loop)}
		for _, b := range spec.Loop.Steps {
			ls.Steps = append(ls.Steps, aiv1alpha1.ChainLoopStepStatus{Name: b.Name, Phase: aiv1alpha1.ChainStepPhasePending})
		}
		ss.Loop = ls
		r.Recorder.Eventf(chain, corev1.EventTypeNormal, "LoopStarted", "Loop step %s started (max %d rounds)", spec.Name, ls.MaxIterations)
	}
	now := metav1.Now()
	ss.Phase = aiv1alpha1.ChainStepPhaseRunning
	ss.StartedAt = &now
	ss.CompletedAt = nil
	ss.Error = ""
	ss.Reason = ""
}

// resetLoopForRetry prepares a failed or exhausted loop for retry-failed: a
// failed body step re-runs in the same round; an exhausted loop gets another
// spec.loop.maxIterations rounds, continuing from its last round.
func resetLoopForRetry(ls *aiv1alpha1.ChainLoopStatus, spec *aiv1alpha1.ChainStep) *aiv1alpha1.ChainLoopStatus {
	if ls == nil || spec == nil || spec.Loop == nil || ls.Iteration == 0 {
		return nil
	}
	if ls.Outcome == aiv1alpha1.ChainLoopExhausted {
		ls.MaxIterations += loopMaxIterations(spec.Loop)
	}
	ls.Outcome = ""
	for i := range ls.Steps {
		bs := &ls.Steps[i]
		if bs.Phase == aiv1alpha1.ChainStepPhaseFailed || bs.Phase == aiv1alpha1.ChainStepPhaseRunning {
			bs.Phase = aiv1alpha1.ChainStepPhasePending
			bs.Error = ""
			bs.Retries = 0
			bs.TaskID = ""
			bs.CompletedAt = nil
		}
	}
	return ls
}

// loopInFlight reports whether a loop has a body task running.
func loopInFlight(ls *aiv1alpha1.ChainLoopStatus) bool {
	if ls == nil {
		return false
	}
	for _, bs := range ls.Steps {
		if bs.Phase == aiv1alpha1.ChainStepPhaseRunning {
			return true
		}
	}
	return false
}

// nextBodyIndex returns the first body step not yet succeeded in the current
// round, or -1 when the round is complete.
func nextBodyIndex(ls *aiv1alpha1.ChainLoopStatus) int {
	for i, bs := range ls.Steps {
		if bs.Iteration != ls.Iteration || bs.Phase != aiv1alpha1.ChainStepPhaseSucceeded {
			return i
		}
	}
	return -1
}

// loopLast returns "last" — the step before body index idx in the loop's flow
// (idx == len(body) means "after the round") — with its full output.
func (r *ChainReconciler) loopLast(ctx context.Context, chain *aiv1alpha1.Chain, spec *aiv1alpha1.ChainStep, ls *aiv1alpha1.ChainLoopStatus, idx int) map[string]string {
	empty := map[string]string{"step": "", "phase": "", "output": "", "error": ""}
	prev := idx - 1
	if prev < 0 && ls.Iteration > 1 {
		prev = len(ls.Steps) - 1
	}
	if prev >= 0 && prev < len(ls.Steps) {
		bs := ls.Steps[prev]
		out := bs.Output
		if bs.Phase == aiv1alpha1.ChainStepPhaseSucceeded {
			out = r.fullOutput(ctx, chain, loopBodyKey(chain, spec.Name, bs.Name), out)
		}
		return map[string]string{"step": bs.Name, "phase": string(bs.Phase), "output": out, "error": bs.Error}
	}
	if from := spec.Loop.From; from != "" {
		for _, ss := range chain.Status.StepStatuses {
			if ss.Name != from {
				continue
			}
			out := ss.Output
			if ss.Phase == aiv1alpha1.ChainStepPhaseSucceeded {
				out = r.fullOutput(ctx, chain, chain.Name+"."+from, out)
			}
			return map[string]string{"step": from, "phase": string(ss.Phase), "output": out, "error": ss.Error}
		}
	}
	return empty
}

// evalUntil evaluates the loop's until expression with last = loopLast(idx).
func (r *ChainReconciler) evalUntil(ctx context.Context, chain *aiv1alpha1.Chain, spec *aiv1alpha1.ChainStep, ls *aiv1alpha1.ChainLoopStatus, idx int) (bool, error) {
	expr := spec.Loop.Until
	vars := r.conditionVars(ctx, chain, expr)
	last := r.loopLast(ctx, chain, spec, ls, idx)
	vars["last"] = map[string]any{
		"step": last["step"], "phase": last["phase"], "output": clipForCEL(last["output"]), "error": last["error"],
	}
	body := make(map[string]any, len(ls.Steps))
	for _, bs := range ls.Steps {
		out := bs.Output
		if bs.Phase == aiv1alpha1.ChainStepPhaseSucceeded && mentions([]string{expr}, bs.Name) {
			out = r.fullOutput(ctx, chain, loopBodyKey(chain, spec.Name, bs.Name), out)
		}
		body[bs.Name] = map[string]any{
			"phase": string(bs.Phase), "output": clipForCEL(out), "error": bs.Error, "iteration": int64(bs.Iteration),
		}
	}
	vars["iteration"] = int64(ls.Iteration)
	vars["maxIterations"] = int64(ls.MaxIterations)
	vars["body"] = body
	return evalCondition(expr, vars)
}

// finishLoop ends a loop with the given outcome and sets the step's phase.
func (r *ChainReconciler) finishLoop(ctx context.Context, chain *aiv1alpha1.Chain, spec *aiv1alpha1.ChainStep, ss *aiv1alpha1.ChainStepStatus, outcome aiv1alpha1.ChainLoopOutcome, msg string) {
	ls := ss.Loop
	ls.Outcome = outcome
	now := metav1.Now()
	ss.CompletedAt = &now
	last := r.loopLast(ctx, chain, spec, ls, nextOrEnd(ls))
	full := last["output"]
	ss.Output = truncateStatusOutput(full, chain.Name+"."+ss.Name)

	switch outcome {
	case aiv1alpha1.ChainLoopConverged:
		ss.Phase = aiv1alpha1.ChainStepPhaseSucceeded
		ss.Error = ""
		ss.Reason = string(outcome)
		r.Recorder.Eventf(chain, corev1.EventTypeNormal, "LoopConverged", "Loop step %s converged after %d round(s)", ss.Name, ls.Iteration)
	case aiv1alpha1.ChainLoopExhausted:
		ss.Reason = string(outcome)
		if spec.Loop.OnExhausted == aiv1alpha1.ChainLoopExhaustedContinue {
			ss.Phase = aiv1alpha1.ChainStepPhaseSucceeded
			ss.Error = ""
		} else {
			ss.Phase = aiv1alpha1.ChainStepPhaseFailed
			ss.Error = msg
		}
		r.Recorder.Eventf(chain, corev1.EventTypeWarning, "LoopExhausted", "Loop step %s: %s", ss.Name, msg)
	default:
		ss.Phase = aiv1alpha1.ChainStepPhaseFailed
		ss.Error = msg
		ss.Reason = string(outcome)
		r.Recorder.Eventf(chain, corev1.EventTypeWarning, "LoopFailed", "Loop step %s: %s", ss.Name, msg)
	}
	if ss.Phase == aiv1alpha1.ChainStepPhaseSucceeded {
		r.storeStepOutputToKV(ctx, chain.Name, chain.Status.RunID, ss.Name, full, "", "", ss.StartedAt, &now)
	}
}

// nextOrEnd is the flow position whose predecessor is the loop's latest
// finished step: the next body step of the round, or the end of the round.
func nextOrEnd(ls *aiv1alpha1.ChainLoopStatus) int {
	if ls.Iteration == 0 {
		return 0
	}
	if i := nextBodyIndex(ls); i >= 0 {
		return i
	}
	return len(ls.Steps)
}

func truncateStatusOutput(out, key string) string {
	if len(out) <= stepOutputStatusLimit {
		return out
	}
	return strings.ToValidUTF8(out[:stepOutputStatusLimit], "") +
		"\n\n... [truncated — full output in NATS KV bucket 'chain-outputs', key '" + key + "']"
}

// reconcileLoop advances a Running loop step: it collects the in-flight body
// task, then either dispatches the next body task, starts the next round, or
// ends the loop. failing stops it from dispatching more work.
func (r *ChainReconciler) reconcileLoop(ctx context.Context, nc natsConfig, chain *aiv1alpha1.Chain, spec *aiv1alpha1.ChainStep, ss *aiv1alpha1.ChainStepStatus, failing bool) {
	log := logf.FromContext(ctx).WithValues("step", ss.Name)
	if ss.Loop == nil {
		r.startLoop(chain, spec, ss)
	}
	ls := ss.Loop

	// 1. Collect the in-flight body task.
	for i := range ls.Steps {
		bs := &ls.Steps[i]
		if bs.Phase == aiv1alpha1.ChainStepPhaseRunning && i < len(spec.Loop.Steps) {
			r.collectBodyTask(ctx, nc, chain, spec, &spec.Loop.Steps[i], bs)
		}
	}
	for _, bs := range ls.Steps {
		if bs.Phase == aiv1alpha1.ChainStepPhaseFailed {
			r.finishLoop(ctx, chain, spec, ss, aiv1alpha1.ChainLoopFailed,
				fmt.Sprintf("body step %s failed in round %d: %s", bs.Name, bs.Iteration, bs.Error))
			return
		}
	}
	if loopInFlight(ls) {
		return
	}

	// 2. Advance: entry check, next body step, or end of round.
	for {
		if ls.Iteration == 0 {
			if spec.Loop.From != "" {
				done, err := r.evalUntil(ctx, chain, spec, ls, 0)
				if err != nil {
					r.finishLoop(ctx, chain, spec, ss, aiv1alpha1.ChainLoopFailed, fmt.Sprintf("until: %v", err))
					return
				}
				if done {
					r.finishLoop(ctx, chain, spec, ss, aiv1alpha1.ChainLoopConverged, "")
					return
				}
			}
			ls.Iteration = 1
			continue
		}
		idx := nextBodyIndex(ls)
		if idx < 0 {
			done, err := r.evalUntil(ctx, chain, spec, ls, len(ls.Steps))
			if err != nil {
				r.finishLoop(ctx, chain, spec, ss, aiv1alpha1.ChainLoopFailed, fmt.Sprintf("until: %v", err))
				return
			}
			if done {
				r.finishLoop(ctx, chain, spec, ss, aiv1alpha1.ChainLoopConverged, "")
				return
			}
			if ls.Iteration >= ls.MaxIterations {
				r.finishLoop(ctx, chain, spec, ss, aiv1alpha1.ChainLoopExhausted,
					fmt.Sprintf("loop exhausted: until still false after %d round(s)", ls.Iteration))
				return
			}
			ls.Iteration++
			log.Info("Loop starting next round", "round", ls.Iteration)
			continue
		}
		if failing {
			skipStep(ss, stepReasonChainFailed)
			return
		}
		r.dispatchBodyTask(ctx, nc, chain, spec, ss, idx)
		if bs := ls.Steps[idx]; bs.Phase == aiv1alpha1.ChainStepPhaseFailed {
			r.finishLoop(ctx, chain, spec, ss, aiv1alpha1.ChainLoopFailed,
				fmt.Sprintf("body step %s failed in round %d: %s", bs.Name, bs.Iteration, bs.Error))
		}
		return
	}
}

// collectBodyTask polls the result of a running body task and applies
// timeout and retry handling.
func (r *ChainReconciler) collectBodyTask(ctx context.Context, nc natsConfig, chain *aiv1alpha1.Chain, spec *aiv1alpha1.ChainStep, body *aiv1alpha1.ChainLoopStep, bs *aiv1alpha1.ChainLoopStepStatus) {
	log := logf.FromContext(ctx).WithValues("step", spec.Name, "body", bs.Name)
	if bs.TaskID == "" {
		return
	}
	if bs.PickedUpAt == nil && bs.StartedAt != nil {
		if bs.TaskSeq == 0 {
			bs.PickedUpAt = bs.StartedAt
		} else if r.ackFloorReached(ctx, nc, body.KnightRef.Name, bs.TaskSeq) {
			now := metav1.Now()
			bs.PickedUpAt = &now
		}
	}
	timing := &aiv1alpha1.ChainStep{TimeoutSeconds: body.TimeoutSeconds}
	if timing.TimeoutSeconds <= 0 {
		timing.TimeoutSeconds = 120
	}
	var errMsg, output string
	if msg := taskDeadlineError(chain, timing, bs.StartedAt, bs.PickedUpAt); msg != "" {
		errMsg = msg
	} else {
		result, err := r.pollResult(ctx, nc, chain.Name, spec.Name+"-"+bs.Name, bs.TaskID)
		if err != nil {
			log.Error(err, "Failed to poll loop body result")
			return
		}
		if result == nil {
			return
		}
		if result.Model != "" {
			bs.Model = result.Model
		}
		errMsg, output = result.GetError(), result.GetOutput()
		if errMsg == "" && isEmptyStepOutput(output) {
			errMsg = errEmptyOutput
		}
	}
	now := metav1.Now()
	bs.CompletedAt = &now
	key := loopBodyKey(chain, spec.Name, bs.Name)
	if errMsg != "" {
		bs.Error = truncateUTF8(errMsg, itemErrorLimit)
		if rp := stepRetryPolicy(chain, &aiv1alpha1.ChainStep{Retry: body.Retry}); rp != nil && bs.Retries < rp.MaxRetries {
			bs.Retries++
			bs.Phase = aiv1alpha1.ChainStepPhasePending
			log.Info("Retrying loop body step", "round", bs.Iteration, "retry", bs.Retries)
			return
		}
		bs.Phase = aiv1alpha1.ChainStepPhaseFailed
		return
	}
	bs.Phase = aiv1alpha1.ChainStepPhaseSucceeded
	bs.Error = ""
	bs.Output = truncateStatusOutput(output, key)
	r.storeStepOutputToKV(ctx, chain.Name, chain.Status.RunID, spec.Name+"."+bs.Name, output, "", body.KnightRef.Name, bs.StartedAt, &now)
	r.Recorder.Eventf(chain, corev1.EventTypeNormal, "LoopStepCompleted", "Loop step %s: %s completed round %d", spec.Name, bs.Name, bs.Iteration)
}

// dispatchBodyTask publishes body step idx for the current round.
func (r *ChainReconciler) dispatchBodyTask(ctx context.Context, nc natsConfig, chain *aiv1alpha1.Chain, spec *aiv1alpha1.ChainStep, ss *aiv1alpha1.ChainStepStatus, idx int) {
	log := logf.FromContext(ctx).WithValues("step", spec.Name)
	ls := ss.Loop
	body := &spec.Loop.Steps[idx]
	bs := &ls.Steps[idx]

	if bs.Iteration != ls.Iteration {
		// First attempt in this round.
		bs.Iteration = ls.Iteration
		bs.Retries = 0
		bs.Error = ""
		bs.CompletedAt = nil
		bs.Phase = aiv1alpha1.ChainStepPhasePending
	} else if bs.Retries > 0 && bs.CompletedAt != nil {
		if rp := stepRetryPolicy(chain, &aiv1alpha1.ChainStep{Retry: body.Retry}); rp != nil &&
			time.Since(bs.CompletedAt.Time) < time.Duration(rp.BackoffSeconds)*time.Second {
			return
		}
	}
	fail := func(msg string) {
		now := metav1.Now()
		bs.Phase = aiv1alpha1.ChainStepPhaseFailed
		bs.Error = truncateUTF8(msg, itemErrorLimit)
		bs.CompletedAt = &now
	}

	last := r.loopLast(ctx, chain, spec, ls, idx)
	bodySteps := make(map[string]map[string]string, len(ls.Steps))
	for _, s := range ls.Steps {
		bodySteps[s.Name] = map[string]string{"Output": s.Output, "Error": s.Error, "Phase": string(s.Phase)}
	}
	taskStr, err := r.renderTemplateWith(chain, body.Task, map[string]any{"Loop": map[string]any{
		"Iteration":     int(ls.Iteration),
		"MaxIterations": int(ls.MaxIterations),
		"Last": map[string]string{
			"Step": last["step"], "Phase": last["phase"], "Error": last["error"],
			"Output": truncateUTF8(last["output"], loopLastTemplateLimit),
		},
		"Steps": bodySteps,
	}})
	if err != nil {
		fail(fmt.Sprintf("template render error: %v", err))
		return
	}

	knight := &aiv1alpha1.Knight{}
	if err := r.Get(ctx, types.NamespacedName{Name: body.KnightRef.Name, Namespace: chain.Namespace}, knight); err != nil {
		log.Error(err, "Failed to get knight", "knightRef", body.KnightRef.Name)
		return
	}
	model, err := knightpkg.ResolveModel(body.Model, nc.ModelAliases, knightpkg.EffectiveImage(knight, r.DefaultKnightImage))
	if err != nil {
		r.Recorder.Eventf(chain, corev1.EventTypeWarning, "InvalidStepModel", "Step %s/%s: %v", spec.Name, body.Name, err)
		bs.Model = ""
		fail(fmt.Sprintf("invalid model for knight %q: %v", body.KnightRef.Name, err))
		return
	}

	// Body tasks carry the chain runId, so a knight that already worked on
	// this run resumes the same session (the coder sees its own PR context).
	taskID := fmt.Sprintf("chain-%s-%s-%s-r%d.%s-%d", chain.Name, spec.Name, body.Name, ls.Iteration, chain.Status.RunID, time.Now().UnixMilli())
	payload := natspkg.TaskPayload{
		TaskID:    taskID,
		ChainName: chain.Name,
		StepName:  spec.Name + "." + body.Name,
		RunID:     chain.Status.RunID,
		Task:      taskStr,
		Model:     model,
	}
	seq, err := r.publishTask(nc, knight.Spec.Domain, body.KnightRef.Name, payload)
	if err != nil {
		log.Error(err, "Failed to publish loop body task", "body", body.Name)
		return
	}
	now := metav1.Now()
	bs.Phase = aiv1alpha1.ChainStepPhaseRunning
	bs.TaskID = taskID
	bs.TaskSeq = seq
	bs.StartedAt = &now
	bs.PickedUpAt = nil
	bs.CompletedAt = nil
	bs.Model = model
	log.Info("Published loop body task", "body", body.Name, "round", ls.Iteration, "taskId", taskID, "knight", body.KnightRef.Name, "model", model)
}
