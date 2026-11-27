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
	"encoding/json"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/pkg/metrics"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// defaultMaxQueueWait bounds how long a task may sit unclaimed by its knight.
const defaultMaxQueueWait = 1800 * time.Second

func maxQueueWait(chain *aiv1alpha1.Chain) time.Duration {
	if chain.Spec.MaxQueueWaitSeconds > 0 {
		return time.Duration(chain.Spec.MaxQueueWaitSeconds) * time.Second
	}
	return defaultMaxQueueWait
}

// detectPickup records ss.PickedUpAt once the knight has picked the task up.
//
// Pickup signal: knights consume the tasks stream through a durable
// "<Knight>-consumer" and keep each message un-acked while they work on it
// (in-progress acks), acking only after the result is published. The ack floor
// is therefore a completion signal; the consumer's delivered stream sequence
// reaching the task's sequence is the pickup signal (a pull consumer on a
// WorkQueue stream delivers in stream order). No knight runtime change is
// needed. A step without a recorded sequence (published by an older operator)
// falls back to the legacy behaviour: the clock starts at publish time.
func (r *ChainReconciler) detectPickup(ctx context.Context, nc natsConfig, ss *aiv1alpha1.ChainStepStatus, spec *aiv1alpha1.ChainStep) {
	if ss.PickedUpAt != nil || ss.StartedAt == nil {
		return
	}
	if ss.TaskSeq == 0 || spec == nil {
		ss.PickedUpAt = ss.StartedAt
		return
	}
	// The task may be re-running on the fallback knight (#321); track the
	// knight it was actually published to.
	knight := spec.KnightRef.Name
	if ss.Knight != "" {
		knight = ss.Knight
	}
	if r.taskDelivered(ctx, nc, knight, ss.TaskSeq) {
		now := metav1.Now()
		ss.PickedUpAt = &now
		logf.FromContext(ctx).Info("Knight picked up step task", "step", ss.Name, "queuedFor", now.Sub(ss.StartedAt.Time).Round(time.Second).String())
	}
}

// taskDelivered reports whether the knight's task consumer has been delivered
// the tasks-stream message with sequence seq (i.e. picked the task up).
func (r *ChainReconciler) taskDelivered(ctx context.Context, nc natsConfig, knightRef string, seq uint64) bool {
	natsCli, err := r.natsClient()
	if err != nil {
		return false
	}
	delivered, err := natsCli.ConsumerDeliveredSeq(ctx, nc.TasksStream, natspkg.KnightTaskConsumerName(knightRef))
	if err != nil {
		logf.FromContext(ctx).V(1).Info("Could not read knight consumer state", "knight", knightRef, "error", err.Error())
		return false
	}
	return delivered >= seq
}

// pollResult checks for a result message for a given chain step.
// If taskID is provided, polls for that exact result subject; otherwise falls back to wildcard.
func (r *ChainReconciler) pollResult(ctx context.Context, nc natsConfig, chainName, stepName, taskID string) (*natspkg.TaskResult, error) {
	log := logf.FromContext(ctx)

	natsCli, err := r.natsClient()
	if err != nil {
		return nil, err
	}

	// Use exact taskID subject when available (prevents stale result replay)
	var subject string
	if taskID != "" {
		subject = natspkg.ResultSubject(nc.SubjectPrefix, taskID)
	} else {
		taskPrefix := fmt.Sprintf("chain-%s-%s", chainName, stepName)
		subject = natspkg.ResultSubjectWildcard(nc.SubjectPrefix, taskPrefix)
	}

	// Durable consumer with explicit ack (compatible with both Limits and
	// WorkQueue retention), deleted again after the poll. FetchOne acks the
	// result (required for WorkQueue retention).
	consumerName := natspkg.ChainConsumerName(chainName, stepName)

	msg, err := natsCli.FetchOne(ctx, nc.ResultsStream,
		natspkg.ResultConsumerConfig(consumerName, subject), 0)

	// Clean up the consumer
	defer func() {
		_ = natsCli.DeleteConsumer(ctx, nc.ResultsStream, consumerName)
	}()

	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, nil // No result yet
	}

	_, consumeSpan := natspkg.StartConsumeSpan(ctx, msg, natspkg.WithTable(nc.Table), natspkg.WithTaskID(taskID))
	consumeSpan.End()

	log.Info("Received result message", "subject", msg.Subject(), "dataLen", len(msg.Data()))

	var result natspkg.TaskResult
	if err := json.Unmarshal(msg.Data(), &result); err != nil {
		log.Error(err, "Failed to unmarshal result", "subject", msg.Subject(), "rawPreview", string(msg.Data()[:min(200, len(msg.Data()))]))
		return nil, fmt.Errorf("unmarshal result: %w", err)
	}

	log.Info("Parsed result", "taskId", result.GetTaskID(), "outputLen", len(result.GetOutput()), "error", result.GetError())

	return &result, nil
}

// collectResults advances every running step: loops and fan-outs reconcile
// their children, plain steps check their timeouts and poll NATS for a result.
func (r *ChainReconciler) collectResults(ctx context.Context, chain *aiv1alpha1.Chain, run *chainRun) {
	for i := range chain.Status.StepStatuses {
		ss := &chain.Status.StepStatuses[i]
		if ss.Phase == aiv1alpha1.ChainStepPhaseRunning {
			r.collectStepResult(ctx, chain, run, ss)
		}
	}
	refreshChainUsage(chain)
}

// collectStepResult advances one running step.
func (r *ChainReconciler) collectStepResult(ctx context.Context, chain *aiv1alpha1.Chain, run *chainRun, ss *aiv1alpha1.ChainStepStatus) {
	log := logf.FromContext(ctx)
	nc := run.nc
	spec := run.specMap[ss.Name]
	if spec != nil && spec.ForEach != nil {
		r.reconcileFanOut(ctx, nc, chain, spec, ss)
		return
	}
	if spec != nil && spec.Loop != nil {
		r.reconcileLoop(ctx, nc, chain, spec, ss, run.failing)
		return
	}
	// Skip polling if no taskID — step was set to Running by a previous
	// operator version or before the status was persisted. Requeue will
	// handle it once the taskID is saved.
	if ss.TaskID == "" {
		log.Info("Step running but no taskID — skipping poll, will requeue", "step", ss.Name)
		return
	}
	// The step timeout runs from knight pickup, not from publish, so
	// time spent queued behind a busy knight doesn't eat the budget.
	r.detectPickup(ctx, nc, ss, spec)
	// A result already published wins over the deadline (#776): after a
	// restart or a late read the clock can say "timed out" while a good
	// result sits in the stream.
	result, err := r.pollResult(ctx, nc, chain.Name, ss.Name, ss.TaskID)
	if err != nil {
		log.Error(err, "Failed to poll result", "step", ss.Name)
		result = nil
	}
	if result == nil && err == nil {
		// The result may have been acked off the stream by an earlier
		// reconcile whose status write failed (#683); recover it from KV.
		result = r.recoverStepResultFromKV(ctx, chain, ss)
	}
	if result != nil {
		r.applyStepResult(ctx, nc, chain, spec, ss, result)
		return
	}
	if ss.StartedAt != nil && spec != nil {
		now := metav1.Now()
		if ss.PickedUpAt != nil {
			if time.Since(ss.PickedUpAt.Time) > time.Duration(spec.TimeoutSeconds)*time.Second {
				log.Info("Step timed out", "step", ss.Name)
				ss.Phase = aiv1alpha1.ChainStepPhaseFailed
				ss.Error = fmt.Sprintf("step timed out after %ds", spec.TimeoutSeconds)
				ss.CompletedAt = &now
				return
			}
		} else if wait := maxQueueWait(chain); time.Since(ss.StartedAt.Time) > wait {
			log.Info("Step not picked up in time", "step", ss.Name)
			ss.Phase = aiv1alpha1.ChainStepPhaseFailed
			ss.Error = fmt.Sprintf("task not picked up by knight within %ds (max queue wait)", int64(wait.Seconds()))
			ss.CompletedAt = &now
			return
		}
	}
}

// applyStepResult records a step's result: success, failure (with retry), a
// plan-quota refusal, or a JSON-output retry.
func (r *ChainReconciler) applyStepResult(ctx context.Context, nc natsConfig, chain *aiv1alpha1.Chain, spec *aiv1alpha1.ChainStep, ss *aiv1alpha1.ChainStepStatus, result *natspkg.TaskResult) {
	log := logf.FromContext(ctx)
	now := metav1.Now()
	ss.CompletedAt = &now
	if result.Model != "" {
		ss.Model = result.Model
	}
	knight := ss.Knight
	if knight == "" && spec != nil {
		knight = spec.KnightRef.Name
	}
	r.recordUsage(ctx, chain, ss, knight, result)
	resultErr := result.GetError()
	resultOutput := result.GetOutput()
	if resultErr == "" && isEmptyStepOutput(resultOutput) {
		resultErr = errEmptyOutput
		r.Recorder.Eventf(chain, corev1.EventTypeWarning, "StepEmptyOutput",
			"Step %s returned empty output, treating as failure", ss.Name)
	}
	if resultErr != "" {
		r.applyStepError(ctx, chain, spec, ss, result, resultErr)
	} else if jsonRetry, failMsg := checkJSONOutput(chain, ss, resultOutput); jsonRetry {
		// Later steps parse this output as JSON: re-run once with a reminder.
		_, perr := parseJSONDocument(resultOutput)
		ss.Phase = aiv1alpha1.ChainStepPhasePending
		ss.Reason = stepReasonJSONRetry
		ss.Error = perr.Error()
		ss.CompletedAt = nil
		log.Info("Retrying step: output has no parseable JSON", "step", ss.Name, "error", perr.Error())
		r.Recorder.Eventf(chain, corev1.EventTypeWarning, "StepJSONRetry",
			"Step %s output has no parseable JSON block, retrying once: %v", ss.Name, perr)
	} else if failMsg != "" {
		ss.Phase = aiv1alpha1.ChainStepPhaseFailed
		ss.Error = failMsg
		ss.Output = truncateStatusOutput(resultOutput, chain.Name+"."+ss.Name)
		if spec != nil {
			r.storeStepOutputToKV(ctx, chain.Name, chain.Status.RunID, ss.Name, "", failMsg, spec.KnightRef.Name, ss.StartedAt, &now)
		}
		r.Recorder.Eventf(chain, corev1.EventTypeWarning, "StepInvalidJSON", "Step %s: %s", ss.Name, failMsg)
	} else {
		r.applyStepSuccess(ctx, nc, chain, spec, ss, result, resultOutput, &now)
	}
}

// applyStepError handles a failed result: a plan-quota refusal moves the step
// to its fallback knight or defers it; any other error fails the step, unless
// its retry policy re-runs it.
func (r *ChainReconciler) applyStepError(ctx context.Context, chain *aiv1alpha1.Chain, spec *aiv1alpha1.ChainStep, ss *aiv1alpha1.ChainStepStatus, result *natspkg.TaskResult, resultErr string) {
	log := logf.FromContext(ctx)
	// #321: a plan-quota refusal is "not yet", not a failure. With a
	// fallback knight the same step re-runs there (no model override:
	// the aliases resolve to Claude models); otherwise the step is
	// deferred until the plan window resets and the chain requeues.
	// Neither path fails the chain or escalates.
	if resetsAt, refused := quotaRefusal(result); refused {
		if fb := stepFallbackKnight(spec, ss); fb != "" {
			ss.Phase = aiv1alpha1.ChainStepPhasePending
			ss.Reason = stepReasonFallbackQuota
			ss.Error = ""
			ss.CompletedAt = nil
			ss.Knight = fb
			ss.Model = ""
			log.Info("Plan-quota refusal: re-running step on fallback knight", "step", ss.Name, "fallback", fb)
			r.Recorder.Eventf(chain, corev1.EventTypeNormal, "StepFallbackQuota",
				"Step %s refused for plan quota, re-running on fallback knight %s (no model override)", ss.Name, fb)
		} else {
			ss.Phase = aiv1alpha1.ChainStepPhaseDeferred
			ss.Reason = stepReasonQuotaRefused
			ss.Error = resultErr
			ss.CompletedAt = nil
			until := metav1.NewTime(quotaDeferUntil(resetsAt))
			ss.DeferUntil = &until
			log.Info("Plan-quota refusal: deferring step until the plan window resets", "step", ss.Name, "deferUntil", ss.DeferUntil.Unix())
			r.Recorder.Eventf(chain, corev1.EventTypeNormal, "StepQuotaRefused",
				"Step %s refused for plan quota, deferred until %s", ss.Name, ss.DeferUntil)
		}
		return
	}
	// #481: a transient provider failure (529 Overloaded...) is retried after
	// a backoff, on the fallback knight once it has retried twice.
	if reason, transient := transientProviderReason(result); transient {
		if int(ss.ProviderRetries) < r.ProviderMaxRetries {
			ss.ProviderRetries++
			metrics.ChainProviderRetriesTotal.WithLabelValues(r.chainTable(ctx, chain), chain.Name, ss.Name, reason).Inc()
			ss.Phase = aiv1alpha1.ChainStepPhaseDeferred
			ss.Reason = stepReasonProviderOverloaded
			ss.TransientReason = reason
			ss.Error = resultErr
			ss.CompletedAt = nil
			wait := providerBackoff(ss.ProviderRetries)
			until := metav1.NewTime(time.Now().Add(wait))
			ss.DeferUntil = &until
			if ss.ProviderRetries >= providerFallbackAfter {
				if fb := stepFallbackKnight(spec, ss); fb != "" {
					ss.Knight = fb
					ss.Model = ""
				}
			}
			log.Info("Model provider overloaded: retrying step after backoff", "step", ss.Name, "retry", ss.ProviderRetries, "wait", wait.String())
			r.Recorder.Eventf(chain, corev1.EventTypeWarning, "StepProviderOverloaded",
				"Step %s hit a transient provider error (%s; retry %d/%d in %s): %s", ss.Name, reason, ss.ProviderRetries, r.ProviderMaxRetries, wait, truncateUTF8(resultErr, 200))
			return
		}
		if ss.ProviderRetries > 0 {
			resultErr = fmt.Sprintf("%s (provider error; retried %d times)", resultErr, ss.ProviderRetries)
		}
	}
	ss.Phase = aiv1alpha1.ChainStepPhaseFailed
	ss.Error = resultErr
	// Check retry (per-step policy overrides chain-level)
	retryPolicy := chain.Spec.RetryPolicy
	if spec != nil && spec.Retry != nil {
		retryPolicy = &aiv1alpha1.ChainRetryPolicy{
			MaxRetries:     spec.Retry.MaxAttempts,
			BackoffSeconds: spec.Retry.BackoffSeconds,
		}
	}
	if retryPolicy != nil && ss.Retries < retryPolicy.MaxRetries {
		ss.Retries++
		ss.Phase = aiv1alpha1.ChainStepPhasePending
		ss.CompletedAt = nil
		ss.Error = ""
		log.Info("Retrying step", "step", ss.Name, "retry", ss.Retries, "maxRetries", retryPolicy.MaxRetries)
		return
	}
	// The result is acked off the stream; keep a final failure in KV so a lost
	// status write does not turn it into a timeout (#683). A retried failure is
	// not stored: its entry could be mistaken for the retry's own result.
	if spec != nil {
		now := metav1.Now()
		r.storeStepOutputToKV(ctx, chain.Name, chain.Status.RunID, ss.Name, "", resultErr, spec.KnightRef.Name, ss.StartedAt, &now)
	}
}

// applyStepSuccess marks the step Succeeded, stores its full output in KV,
// truncates the status copy and dispatches the outputPath artifact write.
func (r *ChainReconciler) applyStepSuccess(ctx context.Context, nc natsConfig, chain *aiv1alpha1.Chain, spec *aiv1alpha1.ChainStep, ss *aiv1alpha1.ChainStepStatus, result *natspkg.TaskResult, resultOutput string, now *metav1.Time) {
	log := logf.FromContext(ctx)
	ss.Phase = aiv1alpha1.ChainStepPhaseSucceeded
	ss.Output = resultOutput
	ss.Reason = ""

	r.Recorder.Eventf(chain, corev1.EventTypeNormal, "StepCompleted", "Step %s completed", ss.Name)

	// Store full output to NATS KV (best-effort)
	if spec != nil {
		r.storeStepOutputToKV(ctx, chain.Name, chain.Status.RunID, ss.Name, resultOutput, "", spec.KnightRef.Name, ss.StartedAt, now, result.Structured())
	}

	// Truncate CRD status output to avoid etcd bloat (4000 chars allows
	// meaningful summaries for template resolution while staying well
	// under etcd's 1.5MB object limit — 10 steps × 4KB = 40KB max)
	ss.Output = truncateStatusOutput(ss.Output, chain.Name+"."+ss.Name)

	// Best-effort artifact write if outputPath is set
	if spec != nil && spec.OutputPath != "" {
		outputPath, err := r.renderOutputPath(chain, spec)
		if err != nil {
			log.Error(err, "Failed to render outputPath", "step", ss.Name)
		} else {
			if err := r.writeArtifact(ctx, nc, chain, spec.Name, outputPath, resultOutput); err != nil {
				log.Error(err, "Failed to dispatch artifact write", "step", ss.Name, "path", outputPath)
			} else {
				log.Info("Dispatched artifact write", "step", ss.Name, "path", outputPath)
			}
		}
	}
}
