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
	"text/template"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
	"github.com/roundtable-k8s/roundtable/pkg/metrics"
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
func stepTemplateData(ss aiv1alpha1.ChainStepStatus) map[string]any {
	d := map[string]any{
		"Output":     ss.Output,
		"Error":      ss.Error,
		"Phase":      string(ss.Phase),
		"Reason":     ss.Reason,
		"Outcome":    "",
		"Iterations": "0",
		"PRs":        []templatePR{},
		"Verdict":    "",
	}
	if ss.Loop != nil {
		d["Outcome"] = string(ss.Loop.Outcome)
		d["Iterations"] = strconv.Itoa(int(ss.Loop.Iteration))
	}
	return d
}

// templatePR is a pull request of a step's structured result, as templates
// see it in .Steps.<name>.PRs. State is what the knight reported the task did
// with the PR (opened, updated, ...); it is empty when unknown.
type templatePR struct {
	URL    string
	Repo   string
	Number int
	State  string
}

// templateFuncs are the helpers available in task templates.
var templateFuncs = template.FuncMap{"prURLs": prURLs}

// prURLs joins the URLs of a step's PRs with spaces ("" when there are none),
// e.g. {{ prURLs .Steps.implement.PRs }} or {{ prURLs .Steps.implement }}.
func prURLs(v any) string {
	if m, ok := v.(map[string]any); ok {
		v = m["PRs"]
	}
	prs, _ := v.([]templatePR)
	urls := make([]string, 0, len(prs))
	for _, p := range prs {
		if p.URL != "" {
			urls = append(urls, p.URL)
		}
	}
	return strings.Join(urls, " ")
}

// fillStructured sets PRs and Verdict on a template entry from the step's
// structured result (KV entry, else the prose fallback over out).
func (r *ChainReconciler) fillStructured(ctx context.Context, chain *aiv1alpha1.Chain, d map[string]any, key, out string) {
	prs, verdict := r.structuredVars(ctx, chain, key, out, true)
	d["PRs"] = templatePRs(prs)
	d["Verdict"] = verdict
}

// templatePRs converts the structuredVars PR maps to template values. Missing
// or mistyped fields yield zero values rather than panicking.
func templatePRs(prs []any) []templatePR {
	list := make([]templatePR, 0, len(prs))
	for _, p := range prs {
		m, _ := p.(map[string]any)
		n, _ := m["number"].(int64)
		url, _ := m["url"].(string)
		repo, _ := m["repo"].(string)
		action, _ := m["action"].(string)
		list = append(list, templatePR{URL: url, Repo: repo, Number: int(n), State: action})
	}
	return list
}

// templateWantsStructured reports whether a task template may use the
// structured fields, so the KV is only read for templates that need it.
func templateWantsStructured(task string) bool {
	return strings.Contains(task, "PRs") || strings.Contains(task, "Verdict") || strings.Contains(task, "prURLs")
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

// structuredVars returns the "prs" and "verdict" values for a step: the
// structured fields the knight reported (kept in the KV entry) or, failing
// that, the prose fallback over out. Steps that did not succeed, or that the
// expressions do not mention, get empty values without touching KV.
func (r *ChainReconciler) structuredVars(ctx context.Context, chain *aiv1alpha1.Chain, key, out string, load bool) (prs []any, verdict string) {
	prs = []any{}
	if !load {
		return prs, ""
	}
	st := natspkg.ParseStructured(out)
	if v, ok := r.kvEntry(ctx, key); ok && v.RunID == chain.Status.RunID {
		if v.Verdict != "" {
			st.Verdict = v.Verdict
		}
		if len(v.PRs) > 0 {
			st.PRs = v.PRs
		}
	}
	for _, p := range st.PRs {
		prs = append(prs, map[string]any{"repo": p.Repo, "number": int64(p.Number), "url": p.URL, "action": p.Action})
	}
	return prs, st.Verdict
}

// conditionVars builds the CEL variables shared by when and until: input and
// steps. Outputs of steps the expressions mention are read in full from KV.
func (r *ChainReconciler) conditionVars(ctx context.Context, chain *aiv1alpha1.Chain, exprs ...string) map[string]any {
	steps := make(map[string]any, len(chain.Status.StepStatuses))
	for _, ss := range chain.Status.StepStatuses {
		out := ss.Output
		load := ss.Phase == aiv1alpha1.ChainStepPhaseSucceeded && mentions(exprs, ss.Name)
		if load {
			out = r.fullOutput(ctx, chain, chain.Name+"."+ss.Name, out)
		}
		prs, verdict := r.structuredVars(ctx, chain, chain.Name+"."+ss.Name, out, load)
		entry := map[string]any{
			"name":       ss.Name,
			"phase":      string(ss.Phase),
			"output":     clipForCEL(out),
			"error":      ss.Error,
			"reason":     ss.Reason,
			"prs":        prs,
			"verdict":    verdict,
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

// loopBodySpec returns the spec body step called name, or nil when the loop
// body no longer has one (e.g. it was edited mid-run).
func loopBodySpec(spec *aiv1alpha1.ChainStep, name string) *aiv1alpha1.ChainLoopStep {
	if spec == nil || spec.Loop == nil {
		return nil
	}
	for i := range spec.Loop.Steps {
		if spec.Loop.Steps[i].Name == name {
			return &spec.Loop.Steps[i]
		}
	}
	return nil
}

// nextBodyIndex returns the index (into the loop status) of the first body
// step not yet succeeded in the current round, or -1 when the round is
// complete. Status entries are paired with spec steps by name; entries with no
// matching spec step are skipped.
func nextBodyIndex(ls *aiv1alpha1.ChainLoopStatus, spec *aiv1alpha1.ChainStep) int {
	for i, bs := range ls.Steps {
		if loopBodySpec(spec, bs.Name) == nil {
			continue // status entry whose spec step was removed: never dispatched
		}
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

// loopLastKey is the KV key of the step loopLast returns ("" when none).
func (r *ChainReconciler) loopLastKey(chain *aiv1alpha1.Chain, spec *aiv1alpha1.ChainStep, ls *aiv1alpha1.ChainLoopStatus, idx int) string {
	prev := idx - 1
	if prev < 0 && ls.Iteration > 1 {
		prev = len(ls.Steps) - 1
	}
	if prev >= 0 && prev < len(ls.Steps) {
		return loopBodyKey(chain, spec.Name, ls.Steps[prev].Name)
	}
	if spec.Loop.From != "" {
		return chain.Name + "." + spec.Loop.From
	}
	return ""
}

// evalUntil evaluates the loop's until expression with last = loopLast(idx).
func (r *ChainReconciler) evalUntil(ctx context.Context, chain *aiv1alpha1.Chain, spec *aiv1alpha1.ChainStep, ls *aiv1alpha1.ChainLoopStatus, idx int) (bool, error) {
	expr := spec.Loop.Until
	vars := r.conditionVars(ctx, chain, expr)
	last := r.loopLast(ctx, chain, spec, ls, idx)
	lastPRs, lastVerdict := r.structuredVars(ctx, chain, r.loopLastKey(chain, spec, ls, idx), last["output"], last["phase"] == string(aiv1alpha1.ChainStepPhaseSucceeded))
	vars["last"] = map[string]any{
		"step": last["step"], "phase": last["phase"], "output": clipForCEL(last["output"]), "error": last["error"],
		"prs": lastPRs, "verdict": lastVerdict,
	}
	body := make(map[string]any, len(ls.Steps))
	for _, bs := range ls.Steps {
		out := bs.Output
		load := bs.Phase == aiv1alpha1.ChainStepPhaseSucceeded && mentions([]string{expr}, bs.Name)
		if load {
			out = r.fullOutput(ctx, chain, loopBodyKey(chain, spec.Name, bs.Name), out)
		}
		prs, verdict := r.structuredVars(ctx, chain, loopBodyKey(chain, spec.Name, bs.Name), out, load)
		body[bs.Name] = map[string]any{
			"phase": string(bs.Phase), "output": clipForCEL(out), "error": bs.Error, "iteration": int64(bs.Iteration),
			"prs": prs, "verdict": verdict,
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
	last := r.loopLast(ctx, chain, spec, ls, nextOrEnd(ls, spec))
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
func nextOrEnd(ls *aiv1alpha1.ChainLoopStatus, spec *aiv1alpha1.ChainStep) int {
	if ls.Iteration == 0 {
		return 0
	}
	if i := nextBodyIndex(ls, spec); i >= 0 {
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
		if bs.Phase != aiv1alpha1.ChainStepPhaseRunning {
			continue
		}
		if body := loopBodySpec(spec, bs.Name); body != nil {
			r.collectBodyTask(ctx, nc, chain, spec, body, bs, ss)
		}
	}
	// A quota-deferred loop (#321) parks the chain: no further rounds or body
	// tasks are dispatched until the refused plan window resets.
	if ss.Phase == aiv1alpha1.ChainStepPhaseDeferred {
		return
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
		idx := nextBodyIndex(ls, spec)
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
// timeout, retry and plan-quota (#321) handling. ss is the loop step's own
// status: a quota refusal without a usable fallback defers the loop.
func (r *ChainReconciler) collectBodyTask(ctx context.Context, nc natsConfig, chain *aiv1alpha1.Chain, spec *aiv1alpha1.ChainStep, body *aiv1alpha1.ChainLoopStep, bs *aiv1alpha1.ChainLoopStepStatus, ss *aiv1alpha1.ChainStepStatus) {
	log := logf.FromContext(ctx).WithValues("step", spec.Name, "body", bs.Name)
	if bs.TaskID == "" {
		return
	}
	if bs.PickedUpAt == nil && bs.StartedAt != nil {
		if bs.TaskSeq == 0 {
			bs.PickedUpAt = bs.StartedAt
		} else if r.taskDelivered(ctx, nc, loopTaskKnight(body, bs), bs.TaskSeq) {
			now := metav1.Now()
			bs.PickedUpAt = &now
		}
	}
	timing := &aiv1alpha1.ChainStep{TimeoutSeconds: body.TimeoutSeconds}
	if timing.TimeoutSeconds <= 0 {
		timing.TimeoutSeconds = 120
	}
	var errMsg, output string
	var structured natspkg.Structured
	var res *natspkg.TaskResult
	// A published result wins over the deadline (#776).
	result, err := r.pollResult(withRunSpan(ctx, chain, roundSpanKey(groupStepSpanKey(spec.Name, ss.StartedAt), bs.Iteration)), nc, chain.Name, spec.Name+"-"+bs.Name, bs.TaskID)
	if err != nil {
		log.Error(err, "Failed to poll loop body result")
		result = nil
	}
	if result != nil {
		res = result
		if result.Model != "" {
			bs.Model = result.Model
		}
		r.recordUsage(ctx, chain, ss, loopTaskKnight(body, bs), result)
		errMsg, output = result.GetError(), result.GetOutput()
		structured = result.Structured()
		if errMsg == "" && isEmptyStepOutput(output) {
			errMsg = errEmptyOutput
		}
	} else if msg := taskDeadlineError(chain, timing, bs.StartedAt, bs.PickedUpAt); msg != "" {
		errMsg = msg
	} else {
		return
	}
	now := metav1.Now()
	bs.CompletedAt = &now
	key := loopBodyKey(chain, spec.Name, bs.Name)
	if errMsg != "" {
		// #321: a plan-quota refusal re-runs the body step on the loop
		// step's fallback knight (no model override), or defers the whole
		// loop until the plan window resets. Neither fails the chain.
		if resetsAt, refused := quotaRefusal(res); refused {
			bs.Error = ""
			bs.CompletedAt = nil
			bs.PickedUpAt = nil
			if fb := loopFallbackKnight(spec, bs); fb != "" {
				bs.Knight = fb
				bs.Model = ""
				bs.Phase = aiv1alpha1.ChainStepPhasePending
				log.Info("Plan-quota refusal: re-running loop body on fallback knight", "round", bs.Iteration, "fallback", fb)
				r.Recorder.Eventf(chain, corev1.EventTypeNormal, "StepFallbackQuota",
					"Loop %s/%s refused for plan quota, re-running on fallback knight %s (no model override)", spec.Name, bs.Name, fb)
				return
			}
			if ss != nil {
				until := metav1.NewTime(quotaDeferUntil(resetsAt))
				ss.Phase = aiv1alpha1.ChainStepPhaseDeferred
				ss.Reason = stepReasonQuotaRefused
				ss.DeferUntil = &until
				bs.Phase = aiv1alpha1.ChainStepPhasePending
				bs.TaskID = ""
				bs.TaskSeq = 0
				bs.StartedAt = nil
				log.Info("Plan-quota refusal: deferring loop until the plan window resets", "loop", spec.Name, "deferUntil", ss.DeferUntil.Unix())
				r.Recorder.Eventf(chain, corev1.EventTypeNormal, "StepQuotaRefused",
					"Loop %s refused for plan quota, deferred until %s", spec.Name, ss.DeferUntil)
			}
			return
		}
		// #486: a transient provider failure defers the loop for a retry.
		deferred, msg := r.deferLoopForProvider(ctx, chain, spec, bs, ss, res, errMsg)
		if deferred {
			return
		}
		errMsg = msg
		bs.Error = truncateUTF8(errMsg, itemErrorLimit)
		recordLoopHistory(ss, bs, aiv1alpha1.ChainStepPhaseFailed, "")
		if rp := stepRetryPolicy(chain, &aiv1alpha1.ChainStep{Retry: body.Retry}); rp != nil && bs.Retries < rp.MaxRetries {
			bs.Retries++
			bs.Phase = aiv1alpha1.ChainStepPhasePending
			log.Info("Retrying loop body step", "round", bs.Iteration, "retry", bs.Retries)
			return
		}
		bs.Phase = aiv1alpha1.ChainStepPhaseFailed
		return
	}
	if ss != nil {
		ss.ProviderRetries = 0
		ss.TransientReason = ""
	}
	bs.Phase = aiv1alpha1.ChainStepPhaseSucceeded
	bs.Error = ""
	bs.Output = truncateStatusOutput(output, key)
	recordLoopHistory(ss, bs, aiv1alpha1.ChainStepPhaseSucceeded, output)
	r.storeStepOutputToKV(ctx, chain.Name, chain.Status.RunID, spec.Name+"."+bs.Name, output, "", body.KnightRef.Name, bs.StartedAt, &now, structured)
	r.Recorder.Eventf(chain, corev1.EventTypeNormal, "LoopStepCompleted", "Loop step %s: %s completed round %d", spec.Name, bs.Name, bs.Iteration)
}

// deferLoopForProvider handles a body task that failed with a transient model
// provider error (#486, HTTP 529 Overloaded...): the loop is deferred with the
// 1/2/4/8 min backoff and the body task re-runs after the wake, on the loop's
// fallback knight from the second retry on. Retries are counted on the loop
// step (ss.ProviderRetries) and reset when a body task succeeds. It returns
// true when the loop was deferred; otherwise the (possibly annotated, once
// retries are exhausted) error message to fail with.
func (r *ChainReconciler) deferLoopForProvider(ctx context.Context, chain *aiv1alpha1.Chain, spec *aiv1alpha1.ChainStep, bs *aiv1alpha1.ChainLoopStepStatus, ss *aiv1alpha1.ChainStepStatus, res *natspkg.TaskResult, errMsg string) (bool, string) {
	reason, transient := transientProviderReason(res)
	if !transient || ss == nil {
		return false, errMsg
	}
	if int(ss.ProviderRetries) >= r.ProviderMaxRetries {
		if ss.ProviderRetries > 0 {
			errMsg = fmt.Sprintf("%s (provider error; retried %d times)", errMsg, ss.ProviderRetries)
		}
		return false, errMsg
	}
	log := logf.FromContext(ctx).WithValues("step", spec.Name, "body", bs.Name)
	ss.ProviderRetries++
	metrics.ChainProviderRetriesTotal.WithLabelValues(r.chainTable(ctx, chain), chain.Name, spec.Name, reason).Inc()
	wait := providerBackoff(ss.ProviderRetries)
	until := metav1.NewTime(time.Now().Add(wait))
	ss.Phase = aiv1alpha1.ChainStepPhaseDeferred
	ss.Reason = stepReasonProviderOverloaded
	ss.TransientReason = reason
	ss.DeferUntil = &until
	bs.Error = ""
	bs.CompletedAt = nil
	bs.PickedUpAt = nil
	bs.Phase = aiv1alpha1.ChainStepPhasePending
	bs.TaskID = ""
	bs.TaskSeq = 0
	bs.StartedAt = nil
	if ss.ProviderRetries >= providerFallbackAfter {
		if fb := loopFallbackKnight(spec, bs); fb != "" {
			bs.Knight = fb
			bs.Model = ""
		}
	}
	log.Info("Model provider overloaded: retrying loop body after backoff", "round", bs.Iteration, "retry", ss.ProviderRetries, "wait", wait.String())
	r.Recorder.Eventf(chain, corev1.EventTypeWarning, "StepProviderOverloaded",
		"Loop %s/%s hit a transient provider error (%s; retry %d/%d in %s): %s", spec.Name, bs.Name, reason, ss.ProviderRetries, r.ProviderMaxRetries, wait, truncateUTF8(errMsg, 200))
	return true, errMsg
}

const (
	loopHistoryMaxRounds    = 20
	loopHistorySummaryBytes = 1024
)

// recordLoopHistory appends the finished execution of body step bs to the
// loop's bounded history. Only the last min(maxIterations, 20) rounds are
// kept. It never touches the fields that templates and until read.
func recordLoopHistory(ss *aiv1alpha1.ChainStepStatus, bs *aiv1alpha1.ChainLoopStepStatus, phase aiv1alpha1.ChainStepPhase, output string) {
	if ss == nil || ss.Loop == nil {
		return
	}
	ls := ss.Loop
	e := aiv1alpha1.ChainLoopHistoryEntry{
		Round:         bs.Iteration,
		Step:          bs.Name,
		Phase:         phase,
		TaskID:        bs.TaskID,
		StartedAt:     bs.StartedAt,
		FinishedAt:    bs.CompletedAt,
		Error:         bs.Error,
		OutputSummary: truncateUTF8(output, loopHistorySummaryBytes),
	}
	if phase != aiv1alpha1.ChainStepPhaseFailed {
		e.Error = ""
	}
	ls.History = append(ls.History, e)
	keep := ls.MaxIterations
	if keep <= 0 || keep > loopHistoryMaxRounds {
		keep = loopHistoryMaxRounds
	}
	cutoff := bs.Iteration - keep // rounds <= cutoff are dropped
	drop := 0
	for drop < len(ls.History) && ls.History[drop].Round <= cutoff {
		drop++
	}
	if drop > 0 {
		ls.History = append([]aiv1alpha1.ChainLoopHistoryEntry(nil), ls.History[drop:]...)
	}
}

// dispatchBodyTask publishes body step idx for the current round.
func (r *ChainReconciler) dispatchBodyTask(ctx context.Context, nc natsConfig, chain *aiv1alpha1.Chain, spec *aiv1alpha1.ChainStep, ss *aiv1alpha1.ChainStepStatus, idx int) {
	log := logf.FromContext(ctx).WithValues("step", spec.Name)
	ls := ss.Loop
	bs := &ls.Steps[idx]
	body := loopBodySpec(spec, bs.Name)
	if body == nil {
		return // no spec step by this name: leave the status entry as is
	}

	if bs.Iteration != ls.Iteration {
		// First attempt in this round: back to the primary knight (a fallback
		// only sticks for re-runs within the same round, so the primary's
		// model alias applies again from the next round on).
		bs.Iteration = ls.Iteration
		bs.Retries = 0
		bs.Error = ""
		bs.CompletedAt = nil
		bs.Knight = ""
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
	bodySteps := make(map[string]map[string]any, len(ls.Steps))
	for _, s := range ls.Steps {
		bodySteps[s.Name] = map[string]any{"Output": s.Output, "Error": s.Error, "Phase": string(s.Phase), "PRs": []templatePR{}, "Verdict": ""}
		if s.Phase == aiv1alpha1.ChainStepPhaseSucceeded && templateWantsStructured(body.Task) {
			r.fillStructured(ctx, chain, bodySteps[s.Name], loopBodyKey(chain, spec.Name, s.Name), s.Output)
		}
	}
	taskStr, err := r.renderTemplateWith(ctx, chain, body.Task, map[string]any{"Loop": map[string]any{
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

	// A plan-quota refusal re-runs the body task on the loop step's fallback
	// knight with NO model override (#321): the aliases are Claude models.
	knightName, bodyModel := body.KnightRef.Name, body.Model
	if bs.Knight != "" && bs.Knight != body.KnightRef.Name {
		knightName, bodyModel = bs.Knight, ""
	}
	knight := &aiv1alpha1.Knight{}
	if err := r.Get(ctx, types.NamespacedName{Name: knightName, Namespace: chain.Namespace}, knight); err != nil {
		log.Error(err, "Failed to get knight", "knightRef", knightName)
		return
	}
	model, err := knightpkg.ResolveModel(bodyModel, nc.ModelAliases, knightpkg.EffectiveImage(knightpkg.WithTableDefaults(ctx, r.Client, knight), r.DefaultKnightImage))
	if err != nil {
		r.Recorder.Eventf(chain, corev1.EventTypeWarning, "InvalidStepModel", "Step %s/%s: %v", spec.Name, body.Name, err)
		bs.Model = ""
		fail(fmt.Sprintf("invalid model for knight %q: %v", knightName, err))
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
		Effort:    body.Effort,
	}
	roundCtx := withRunSpan(ctx, chain, roundSpanKey(groupStepSpanKey(spec.Name, ss.StartedAt), ls.Iteration))
	seq, err := r.publishTask(roundCtx, nc, knight.Spec.Domain, knightName, payload)
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
	bs.Knight = knightName
	log.Info("Published loop body task", "body", body.Name, "round", ls.Iteration, "taskId", taskID, "knight", knightName, "model", model)
}
