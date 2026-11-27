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
	"slices"
	"strings"
	"text/template"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// renderOutputPath renders template variables in the outputPath.
func (r *ChainReconciler) renderOutputPath(chain *aiv1alpha1.Chain, step *aiv1alpha1.ChainStep) (string, error) {
	path := step.OutputPath
	if !strings.Contains(path, "{{") {
		return path, nil
	}

	data := map[string]string{
		"Date":  time.Now().UTC().Format("2006-01-02"),
		"Chain": chain.Name,
		"Step":  step.Name,
	}

	tmpl, err := template.New("outputPath").Parse(path)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// writeArtifact dispatches a write task to the outputKnightRef knight.
func (r *ChainReconciler) writeArtifact(ctx context.Context, nc natsConfig, chain *aiv1alpha1.Chain, stepName, outputPath, content string) error {
	natsCli, err := r.natsClient()
	if err != nil {
		return err
	}

	knightName := aiv1alpha1.RefName(chain.Spec.OutputKnightRef)
	if knightName == "" {
		knightName = defaultOutputKnight
	}

	// Get knight to find its domain
	knight := &aiv1alpha1.Knight{}
	if err := r.Get(ctx, types.NamespacedName{Name: knightName, Namespace: chain.Namespace}, knight); err != nil {
		return fmt.Errorf("output knight %q not found: %w", knightName, err)
	}

	taskID := fmt.Sprintf("chain-%s-%s-artifact.%s-%d", chain.Name, stepName, chain.Status.RunID, time.Now().UnixMilli())

	// The task instructs the knight to write the content to the path
	task := fmt.Sprintf("Write the following content to the file at path '%s'. Create any missing directories. Write ONLY the content below, do not modify or summarize it.\n\n---\n%s", outputPath, content)

	payload := natspkg.TaskPayload{
		TaskID:    taskID,
		ChainName: chain.Name,
		StepName:  stepName + "-artifact",
		RunID:     chain.Status.RunID,
		Task:      task,
	}

	subject := natspkg.TaskSubject(nc.SubjectPrefix, knight.Spec.Domain, knightName)
	return natsCli.PublishJSONCtx(ctx, subject, payload, natspkg.WithTable(nc.Table))
}

// emptyOutputSentinels are placeholder strings produced by knights when an
// agent session yields no real content. They carry no usable output and must
// not be treated as a successful step result.
var emptyOutputSentinels = []string{
	"[No output from agent]",
}

// isEmptyStepOutput reports whether a step output contains no usable content.
func isEmptyStepOutput(output string) bool {
	trimmed := strings.TrimSpace(output)
	if trimmed == "" {
		return true
	}
	return slices.Contains(emptyOutputSentinels, trimmed)
}

// storeStepOutputToKV stores the full step output to the NATS KV "chain-outputs" bucket.
// This is best-effort — failures are logged but do not block chain execution.
func (r *ChainReconciler) storeStepOutputToKV(ctx context.Context, chainName, runID, stepName, output, errStr, knight string, startedAt, completedAt *metav1.Time, structured ...natspkg.Structured) {
	r.storeStepOutputToKVWithResult(ctx, chainName, runID, stepName, output, errStr, knight, startedAt, completedAt, nil, structured...)
}

// storeStepOutputToKVWithResult is storeStepOutputToKV plus the result's model,
// tokens, cost and billing, so a step recovered from KV keeps them. res may be nil.
func (r *ChainReconciler) storeStepOutputToKVWithResult(ctx context.Context, chainName, runID, stepName, output, errStr, knight string, startedAt, completedAt *metav1.Time, res *natspkg.TaskResult, structured ...natspkg.Structured) {
	log := logf.FromContext(ctx)

	// Never persist empty outputs — a poisoned KV entry would be restored as a
	// "successful" step on later runs, silently skipping real work.
	if errStr == "" && isEmptyStepOutput(output) {
		log.Info("Refusing to store empty step output to KV", "step", stepName)
		return
	}

	natsCli, err := r.natsClient()
	if err != nil {
		log.Error(err, "Failed to connect NATS for KV store", "step", stepName)
		return
	}

	var durationStr string
	if startedAt != nil && completedAt != nil {
		durationStr = completedAt.Time.Sub(startedAt.Time).String()
	}

	kvValue := map[string]any{
		"output":   output,
		"error":    errStr,
		"knight":   knight,
		"runId":    runID,
		"duration": durationStr,
		"storedAt": time.Now().UTC().Format(time.RFC3339),
	}

	if res != nil {
		if res.Model != "" {
			kvValue["model"] = res.Model
		}
		if res.Cost > 0 {
			kvValue["cost"] = res.Cost
		}
		if res.Tokens != nil {
			kvValue["tokens"] = res.Tokens
		}
		if res.Billing != "" {
			kvValue["billing"] = res.Billing
		}
	}

	if len(structured) > 0 {
		if len(structured[0].PRs) > 0 {
			kvValue["prs"] = structured[0].PRs
		}
		if structured[0].Verdict != "" {
			kvValue["verdict"] = structured[0].Verdict
		}
	}

	data, err := json.Marshal(kvValue)
	if err != nil {
		log.Error(err, "Failed to marshal KV value", "step", stepName)
		return
	}

	key := chainName + "." + stepName
	if err := natsCli.KVPut(ctx, "chain-outputs", key, data); err != nil {
		log.Error(err, "Failed to store step output to KV", "key", key)
	} else {
		log.Info("Stored step output to NATS KV", "bucket", "chain-outputs", "key", key, "size", len(data))
	}
}

// stepKVEntry is the decoded value of a chain-outputs KV entry.
type stepKVEntry struct {
	kvRecord
	StoredAt time.Time // zero when absent or unparseable
}

// readStepKV reads and decodes the chain-outputs entry at key.
func readStepKV(ctx context.Context, natsCli natspkg.Client, key string) (*stepKVEntry, error) {
	data, err := natsCli.KVGet(ctx, "chain-outputs", key)
	if err != nil {
		return nil, err
	}
	e := &stepKVEntry{}
	if err := json.Unmarshal(data, &e.kvRecord); err != nil {
		return nil, fmt.Errorf("unmarshal KV value: %w", err)
	}
	if e.StoredAtRaw != "" {
		if t, perr := time.Parse(time.RFC3339, e.StoredAtRaw); perr == nil {
			e.StoredAt = t
		}
	}
	return e, nil
}

// recoverStepResultFromKV returns the result of a Running step whose result
// message was already acked off the results stream but whose status write was
// lost (conflict or API error), as stored in the chain-outputs KV by
// storeStepOutputToKV. It returns nil unless the entry belongs to the current
// run, was stored at or after the step started, and holds a real output or an
// error: stale entries must never complete a step.
func (r *ChainReconciler) recoverStepResultFromKV(ctx context.Context, chain *aiv1alpha1.Chain, ss *aiv1alpha1.ChainStepStatus) *natspkg.TaskResult {
	log := logf.FromContext(ctx)
	if ss.StartedAt == nil || chain.Status.RunID == "" {
		return nil
	}
	natsCli, err := r.natsClient()
	if err != nil {
		return nil
	}
	entry, err := readStepKV(ctx, natsCli, chain.Name+"."+ss.Name)
	if err != nil {
		return nil
	}
	if entry.RunID != chain.Status.RunID || entry.StoredAt.IsZero() {
		return nil
	}
	// storedAt has one-second resolution; the step's StartedAt is truncated
	// the same way by the API server.
	if entry.StoredAt.Before(ss.StartedAt.Truncate(time.Second)) {
		return nil
	}
	if entry.Error == "" && isEmptyStepOutput(entry.Output) {
		return nil
	}
	log.Info("Recovered step result from KV (result already acked, status write was lost)", "step", ss.Name)
	// Carry the knight's reported PRs and verdict: applyStepSuccess rewrites the
	// KV entry from this result, and must not replace them with parsed values.
	return &natspkg.TaskResult{
		TaskID: ss.TaskID, Output: entry.Output, Error: entry.Error, PRs: entry.PRs, Verdict: entry.Verdict,
		Model: entry.Model, Cost: entry.Cost, Tokens: entry.Tokens, Billing: entry.Billing,
	}
}

// restoreStepOutputsFromKV attempts to restore step outputs from NATS KV.
// Returns the number of steps successfully restored.
func (r *ChainReconciler) restoreStepOutputsFromKV(ctx context.Context, chain *aiv1alpha1.Chain) int {
	log := logf.FromContext(ctx)

	natsCli, err := r.natsClient()
	if err != nil {
		log.Error(err, "Failed to connect NATS for KV restore")
		return 0
	}

	restored := 0
	for i := range chain.Status.StepStatuses {
		ss := &chain.Status.StepStatuses[i]
		if ss.Phase != aiv1alpha1.ChainStepPhasePending {
			continue // Only restore pending steps
		}

		key := chain.Name + "." + ss.Name
		entry, err := readStepKV(ctx, natsCli, key)
		if err != nil {
			log.V(1).Info("No stored output found for step", "step", ss.Name, "key", key, "error", err.Error())
			continue
		}
		output, errStr := entry.Output, entry.Error

		// A stored entry with no usable output is poison left by a run that
		// produced nothing — leave the step Pending so it re-executes, and
		// delete the entry so it cannot mask future runs.
		if errStr == "" && isEmptyStepOutput(output) {
			log.Info("Skipping restore of empty stored output, deleting poisoned KV entry", "step", ss.Name, "key", key)
			if err := natsCli.KVDelete(ctx, "chain-outputs", key); err != nil {
				log.Error(err, "Failed to delete poisoned KV entry", "key", key)
			}
			continue
		}

		// Only outputs produced by the current run may be restored — entries
		// from earlier runs (or pre-run-identity entries with no runId) would
		// silently stand in for work this run never did.
		storedRunID := entry.RunID
		if storedRunID == "" || storedRunID != chain.Status.RunID {
			log.V(1).Info("Skipping stored output from a different run", "step", ss.Name, "storedRunId", storedRunID)
			continue
		}

		if errStr != "" {
			ss.Phase = aiv1alpha1.ChainStepPhaseFailed
			ss.Error = errStr
			log.Info("Restored failed step from KV", "step", ss.Name)
		} else {
			ss.Phase = aiv1alpha1.ChainStepPhaseSucceeded
			ss.Output = output
			if len(ss.Output) > 4000 {
				ss.Output = ss.Output[:4000] + "\n\n... [truncated — full output in NATS KV bucket 'chain-outputs', key '" + chain.Name + "." + ss.Name + "']"
			}
			log.Info("Restored successful step from KV", "step", ss.Name, "outputLen", len(output))
		}

		// Set completion time (approximate)
		now := metav1.Now()
		ss.CompletedAt = &now
		restored++
	}

	return restored
}

// defaultOutputKnight is the knight that receives chain output when spec.outputKnightRef is unset.
const defaultOutputKnight = "gawain"
