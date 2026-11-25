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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/jsonpath"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// Fan-out bounds. They keep a forEach step's share of the Chain object small
// regardless of how many items it has.
const (
	// fanOutJoinLimit is the approximate byte budget for all item outputs in
	// the joined step output; each item gets an equal share (min itemOutputMin).
	fanOutJoinLimit = 32000
	itemOutputMin   = 128
	itemErrorLimit  = 1024
	// itemJSONLimit / itemJSONTotal bound the item echoed into each join entry.
	itemJSONLimit = 512
	itemJSONTotal = 16000
	itemJSONMin   = 64
	// maxItemDetails caps the detailed per-item entries kept in status.
	maxItemDetails = 50
	// defaultForEachMaxItems mirrors the CRD default for forEach.maxItems.
	defaultForEachMaxItems = 50
)

// Item phase characters stored in ChainFanOutStatus.ItemPhases.
const (
	itemPending   = 'P'
	itemRunning   = 'R'
	itemSucceeded = 'S'
	itemFailed    = 'F'
	itemSkipped   = 'K'
)

var itemPhaseByChar = map[byte]aiv1alpha1.ChainStepPhase{
	itemPending:   aiv1alpha1.ChainStepPhasePending,
	itemRunning:   aiv1alpha1.ChainStepPhaseRunning,
	itemSucceeded: aiv1alpha1.ChainStepPhaseSucceeded,
	itemFailed:    aiv1alpha1.ChainStepPhaseFailed,
	itemSkipped:   aiv1alpha1.ChainStepPhaseSkipped,
}

// validateForEach checks forEach / maxParallel / failurePolicy settings.
func validateForEach(chain *aiv1alpha1.Chain) error {
	names := make(map[string]bool, len(chain.Spec.Steps))
	for _, s := range chain.Spec.Steps {
		names[s.Name] = true
	}
	for _, step := range chain.Spec.Steps {
		if step.ForEach == nil {
			if step.MaxParallel != 0 || step.FailurePolicy != "" {
				return fmt.Errorf("step %q sets maxParallel/failurePolicy without forEach", step.Name)
			}
			continue
		}
		fe := step.ForEach
		switch {
		case fe.From == "input":
		case strings.HasPrefix(fe.From, "steps."):
			src := strings.TrimPrefix(fe.From, "steps.")
			if !names[src] || src == step.Name {
				return fmt.Errorf("step %q forEach.from references unknown step %q", step.Name, src)
			}
			if !slices.Contains(step.DependsOn, src) {
				return fmt.Errorf("step %q forEach.from step %q must be listed in dependsOn", step.Name, src)
			}
		default:
			return fmt.Errorf("step %q forEach.from must be \"input\" or \"steps.<name>\", got %q", step.Name, fe.From)
		}
		if fe.Path != "" {
			if err := jsonpath.New("forEach").Parse(normalizeJSONPath(fe.Path)); err != nil {
				return fmt.Errorf("step %q forEach.path is not valid JSONPath: %w", step.Name, err)
			}
		}
		if !strings.Contains(step.Task, ".Item") && !strings.Contains(step.Task, ".item") {
			return fmt.Errorf("step %q uses forEach but its task never references {{ .Item }}, {{ .ItemJSON }} or {{ .ItemIndex }}", step.Name)
		}
	}
	return nil
}

func normalizeJSONPath(p string) string {
	p = strings.TrimSpace(p)
	if strings.HasPrefix(p, "{") {
		return p
	}
	if !strings.HasPrefix(p, ".") && !strings.HasPrefix(p, "$") {
		p = "." + p
	}
	return "{" + p + "}"
}

var fencedJSON = regexp.MustCompile("(?s)```(?:json)?\\s*\\n(.*?)```")

// parseJSONDocument decodes a JSON document, tolerating prose around it when
// the JSON sits in a fenced code block (knights often wrap their output).
func parseJSONDocument(s string) (any, error) {
	decode := func(in string) (any, error) {
		dec := json.NewDecoder(strings.NewReader(in))
		dec.UseNumber()
		var v any
		if err := dec.Decode(&v); err != nil {
			return nil, err
		}
		if dec.More() {
			return nil, fmt.Errorf("trailing data after JSON value")
		}
		return v, nil
	}
	trimmed := strings.TrimSpace(s)
	v, err := decode(trimmed)
	if err == nil {
		return v, nil
	}
	for _, m := range fencedJSON.FindAllStringSubmatch(trimmed, -1) {
		if fv, ferr := decode(strings.TrimSpace(m[1])); ferr == nil {
			return fv, nil
		}
	}
	return nil, fmt.Errorf("not valid JSON (and no fenced JSON block found): %w", err)
}

// extractItems evaluates path against doc and returns the resulting list.
func extractItems(doc any, path string) ([]any, error) {
	if path == "" {
		list, ok := doc.([]any)
		if !ok {
			return nil, fmt.Errorf("document is a %T, not a JSON array (set forEach.path to select the list)", doc)
		}
		return list, nil
	}
	jp := jsonpath.New("forEach")
	if err := jp.Parse(normalizeJSONPath(path)); err != nil {
		return nil, err
	}
	results, err := jp.FindResults(doc)
	if err != nil {
		return nil, err
	}
	var values []any
	for _, rs := range results {
		for _, rv := range rs {
			if rv.IsValid() && rv.CanInterface() {
				values = append(values, rv.Interface())
			}
		}
	}
	// One value that is itself a list is the list; anything else (several
	// values, or one scalar/object) forms the list.
	if len(values) == 1 {
		if list, ok := values[0].([]any); ok {
			return list, nil
		}
	}
	if values == nil {
		values = []any{}
	}
	return values, nil
}

// uniqueItems keeps the first item for each distinct value under key (as
// JSON, so 7 and 7.0 match but 7 and "7" do not). An empty key keeps every item.
func uniqueItems(items []any, key string) ([]any, error) {
	if key == "" {
		return items, nil
	}
	seen := make(map[string]bool, len(items))
	out := make([]any, 0, len(items))
	for i, it := range items {
		obj, ok := it.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("item %d is a %T, not an object", i, it)
		}
		v, ok := obj[key]
		if !ok {
			return nil, fmt.Errorf("item %d has no key %q", i, key)
		}
		if n, ok := v.(json.Number); ok {
			if f, err := n.Float64(); err == nil {
				v = f
			}
		}
		id, _ := json.Marshal(v)
		if seen[string(id)] {
			continue
		}
		seen[string(id)] = true
		out = append(out, it)
	}
	return out, nil
}

func hashItems(items []any) string {
	data, _ := json.Marshal(items)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:8])
}

// forEachSource returns the raw document a forEach step fans out over. A
// prior step's full output is read from NATS KV (status output is truncated);
// the status copy is the fallback.
func (r *ChainReconciler) forEachSource(ctx context.Context, chain *aiv1alpha1.Chain, fe *aiv1alpha1.ChainForEach) (string, error) {
	if fe.From == "input" {
		return chain.Spec.Input, nil
	}
	src := strings.TrimPrefix(fe.From, "steps.")
	for _, ss := range chain.Status.StepStatuses {
		if ss.Name != src {
			continue
		}
		if ss.Phase != aiv1alpha1.ChainStepPhaseSucceeded {
			return "", fmt.Errorf("forEach source step %q has not succeeded (phase %s)", src, ss.Phase)
		}
		if full, ok := r.kvOutput(ctx, chain, chain.Name+"."+src); ok {
			return full, nil
		}
		return ss.Output, nil
	}
	return "", fmt.Errorf("forEach source step %q not found", src)
}

// kvRecord is the part of a chain-outputs entry the controller reads back.
type kvRecord struct {
	Output  string          `json:"output"`
	Error   string          `json:"error"`
	RunID   string          `json:"runId"`
	PRs     []natspkg.PRRef `json:"prs"`
	Verdict string          `json:"verdict"`
}

// kvEntry reads a stored entry of the current run from the chain-outputs bucket.
func (r *ChainReconciler) kvEntry(key string) (kvRecord, bool) {
	var v kvRecord
	natsCli, err := r.natsClient()
	if err != nil {
		return v, false
	}
	data, err := natsCli.KVGet("chain-outputs", key)
	if err != nil {
		return v, false
	}
	return v, json.Unmarshal(data, &v) == nil && v.RunID != "" // caller checks the run
}

// kvOutput reads a stored output of the current run from the chain-outputs bucket.
func (r *ChainReconciler) kvOutput(ctx context.Context, chain *aiv1alpha1.Chain, key string) (string, bool) {
	v, ok := r.kvEntry(key)
	if !ok || v.RunID != chain.Status.RunID {
		return "", false
	}
	if v.Error != "" {
		return v.Error, true
	}
	logf.FromContext(ctx).V(1).Info("Read output from KV", "key", key)
	return v.Output, true
}

// resolveForEachItems resolves the item list of a forEach step.
func (r *ChainReconciler) resolveForEachItems(ctx context.Context, chain *aiv1alpha1.Chain, step *aiv1alpha1.ChainStep) ([]any, error) {
	raw, err := r.forEachSource(ctx, chain, step.ForEach)
	if err != nil {
		return nil, err
	}
	doc, err := parseJSONDocument(raw)
	if err != nil {
		return nil, fmt.Errorf("forEach source %s: %w", step.ForEach.From, err)
	}
	items, err := extractItems(doc, step.ForEach.Path)
	if err != nil {
		return nil, fmt.Errorf("forEach path %q: %w", step.ForEach.Path, err)
	}
	if items, err = uniqueItems(items, step.ForEach.UniqueBy); err != nil {
		return nil, fmt.Errorf("forEach uniqueBy %q: %w", step.ForEach.UniqueBy, err)
	}
	maxItems := step.ForEach.MaxItems
	if maxItems <= 0 {
		maxItems = defaultForEachMaxItems
	}
	if len(items) > int(maxItems) {
		return nil, fmt.Errorf("forEach yielded %d items, more than maxItems=%d", len(items), maxItems)
	}
	return items, nil
}

// startFanOut initialises (or, after retry-failed, resumes) a forEach step and
// marks it Running. It returns an error message when the step cannot start.
func (r *ChainReconciler) startFanOut(ctx context.Context, chain *aiv1alpha1.Chain, step *aiv1alpha1.ChainStep, ss *aiv1alpha1.ChainStepStatus) string {
	if ss.FanOut == nil {
		items, err := r.resolveForEachItems(ctx, chain, step)
		if err != nil {
			return err.Error()
		}
		ss.FanOut = &aiv1alpha1.ChainFanOutStatus{
			Total:      int32(len(items)),
			ItemsHash:  hashItems(items),
			ItemPhases: strings.Repeat(string(rune(itemPending)), len(items)),
		}
		r.Recorder.Eventf(chain, corev1.EventTypeNormal, "FanOutStarted", "Step %s fans out over %d item(s)", step.Name, len(items))
	}
	now := metav1.Now()
	ss.Phase = aiv1alpha1.ChainStepPhaseRunning
	ss.StartedAt = &now
	ss.CompletedAt = nil
	ss.Error = ""
	summariseFanOut(ss.FanOut)
	return ""
}

// itemRunID derives the per-item run identity, so every item gets a fresh
// agent session and nothing bleeds between items or runs.
func itemRunID(runID string, idx int) string {
	return fmt.Sprintf("%s-i%d", runID, idx)
}

func itemDetail(fo *aiv1alpha1.ChainFanOutStatus, idx int) *aiv1alpha1.ChainItemStatus {
	for i := range fo.Items {
		if int(fo.Items[i].Index) == idx {
			return &fo.Items[i]
		}
	}
	return nil
}

func setItemPhase(fo *aiv1alpha1.ChainFanOutStatus, idx int, c byte) {
	b := []byte(fo.ItemPhases)
	if idx >= 0 && idx < len(b) {
		b[idx] = c
		fo.ItemPhases = string(b)
	}
}

func summariseFanOut(fo *aiv1alpha1.ChainFanOutStatus) {
	fo.Pending, fo.Running, fo.Succeeded, fo.Failed, fo.Skipped = 0, 0, 0, 0, 0
	for i := 0; i < len(fo.ItemPhases); i++ {
		switch fo.ItemPhases[i] {
		case itemPending:
			fo.Pending++
		case itemRunning:
			fo.Running++
		case itemSucceeded:
			fo.Succeeded++
		case itemFailed:
			fo.Failed++
		case itemSkipped:
			fo.Skipped++
		}
	}
}

// skipPendingItems marks every not-yet-dispatched item Skipped.
func skipPendingItems(fo *aiv1alpha1.ChainFanOutStatus) {
	fo.ItemPhases = strings.ReplaceAll(fo.ItemPhases, string(rune(itemPending)), string(rune(itemSkipped)))
	fo.Items = slices.DeleteFunc(fo.Items, func(it aiv1alpha1.ChainItemStatus) bool {
		return it.Phase == aiv1alpha1.ChainStepPhasePending
	})
	summariseFanOut(fo)
}

// resetFailedItems re-queues failed, skipped and stale running items for
// retry-failed, keeping succeeded items so a resumed fan-out never redoes them.
func resetFailedItems(fo *aiv1alpha1.ChainFanOutStatus) int {
	b := []byte(fo.ItemPhases)
	n := 0
	for i, c := range b {
		if c == itemFailed || c == itemSkipped || c == itemRunning {
			b[i] = itemPending
			n++
		}
	}
	fo.ItemPhases = string(b)
	fo.Items = slices.DeleteFunc(fo.Items, func(it aiv1alpha1.ChainItemStatus) bool {
		return it.Phase != aiv1alpha1.ChainStepPhaseSucceeded
	})
	summariseFanOut(fo)
	return n
}

func truncateUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return strings.ToValidUTF8(s[:n], "") + " …[truncated]"
}

func itemOutputBudget(total int32) int {
	if total <= 0 {
		return fanOutJoinLimit
	}
	return max(itemOutputMin, fanOutJoinLimit/int(total))
}

func stepRetryPolicy(chain *aiv1alpha1.Chain, spec *aiv1alpha1.ChainStep) *aiv1alpha1.ChainRetryPolicy {
	if spec != nil && spec.Retry != nil {
		return &aiv1alpha1.ChainRetryPolicy{MaxRetries: spec.Retry.MaxAttempts, BackoffSeconds: spec.Retry.BackoffSeconds}
	}
	return chain.Spec.RetryPolicy
}

// taskDeadlineError returns a non-empty failure message when a task has
// exceeded its step timeout (measured from pickup) or the max queue wait.
func taskDeadlineError(chain *aiv1alpha1.Chain, spec *aiv1alpha1.ChainStep, startedAt, pickedUpAt *metav1.Time) string {
	if startedAt == nil || spec == nil {
		return ""
	}
	if pickedUpAt != nil {
		if time.Since(pickedUpAt.Time) > time.Duration(spec.TimeoutSeconds)*time.Second {
			return fmt.Sprintf("step timed out after %ds", spec.TimeoutSeconds)
		}
		return ""
	}
	if wait := maxQueueWait(chain); time.Since(startedAt.Time) > wait {
		return fmt.Sprintf("task not picked up by knight within %ds (max queue wait)", int64(wait.Seconds()))
	}
	return ""
}

// reconcileFanOut advances a Running forEach step: it collects item results,
// dispatches pending items up to maxParallel and joins the step once every
// item is terminal.
func (r *ChainReconciler) reconcileFanOut(ctx context.Context, nc natsConfig, chain *aiv1alpha1.Chain, spec *aiv1alpha1.ChainStep, ss *aiv1alpha1.ChainStepStatus) {
	log := logf.FromContext(ctx).WithValues("step", ss.Name)
	fo := ss.FanOut
	if fo == nil {
		if msg := r.startFanOut(ctx, chain, spec, ss); msg != "" {
			r.failFanOutStep(chain, ss, msg)
			return
		}
		fo = ss.FanOut
	}
	budget := itemOutputBudget(fo.Total)
	retryPolicy := stepRetryPolicy(chain, spec)

	// 1. Collect results of in-flight items.
	for i := range fo.Items {
		it := &fo.Items[i]
		if it.Phase != aiv1alpha1.ChainStepPhaseRunning || it.TaskID == "" {
			continue
		}
		idx := int(it.Index)
		if it.PickedUpAt == nil && it.StartedAt != nil {
			if it.TaskSeq == 0 {
				it.PickedUpAt = it.StartedAt
			} else if r.taskDelivered(ctx, nc, spec.KnightRef.Name, it.TaskSeq) {
				now := metav1.Now()
				it.PickedUpAt = &now
			}
		}
		var errMsg, output string
		done := false
		if msg := taskDeadlineError(chain, spec, it.StartedAt, it.PickedUpAt); msg != "" {
			errMsg, done = msg, true
		} else {
			result, err := r.pollResult(ctx, nc, chain.Name, fmt.Sprintf("%s-%d", ss.Name, idx), it.TaskID)
			if err != nil {
				log.Error(err, "Failed to poll item result", "item", idx)
				continue
			}
			if result == nil {
				continue
			}
			done = true
			errMsg, output = result.GetError(), result.GetOutput()
			if errMsg == "" && isEmptyStepOutput(output) {
				errMsg = errEmptyOutput
			}
		}
		if !done {
			continue
		}
		now := metav1.Now()
		it.CompletedAt = &now
		if errMsg != "" {
			if retryPolicy != nil && it.Retries < retryPolicy.MaxRetries {
				it.Retries++
				it.Phase = aiv1alpha1.ChainStepPhasePending
				it.Error = truncateUTF8(errMsg, itemErrorLimit)
				setItemPhase(fo, idx, itemPending)
				log.Info("Retrying item", "item", idx, "retry", it.Retries)
				continue
			}
			it.Phase = aiv1alpha1.ChainStepPhaseFailed
			it.Error = truncateUTF8(errMsg, itemErrorLimit)
			setItemPhase(fo, idx, itemFailed)
			r.storeStepOutputToKV(ctx, chain.Name, chain.Status.RunID, fmt.Sprintf("%s.%d", ss.Name, idx), "", errMsg, spec.KnightRef.Name, it.StartedAt, &now)
			continue
		}
		it.Phase = aiv1alpha1.ChainStepPhaseSucceeded
		it.Error = ""
		it.Output = truncateUTF8(output, budget)
		setItemPhase(fo, idx, itemSucceeded)
		r.storeStepOutputToKV(ctx, chain.Name, chain.Status.RunID, fmt.Sprintf("%s.%d", ss.Name, idx), output, "", spec.KnightRef.Name, it.StartedAt, &now)
	}
	summariseFanOut(fo)

	// 2. failFast: a failed item stops further dispatch.
	if fo.Failed > 0 && spec.FailurePolicy != aiv1alpha1.ChainFailurePolicyContinue {
		skipPendingItems(fo)
	}

	// 3. Dispatch pending items up to maxParallel.
	if fo.Pending > 0 {
		r.dispatchItems(ctx, nc, chain, spec, ss)
	}

	// 4. Join when every item is terminal.
	r.finalizeFanOutIfDone(ctx, chain, spec, ss)
	compactItemDetails(fo)
}

// dispatchItems publishes pending items while fewer than maxParallel are in flight.
func (r *ChainReconciler) dispatchItems(ctx context.Context, nc natsConfig, chain *aiv1alpha1.Chain, spec *aiv1alpha1.ChainStep, ss *aiv1alpha1.ChainStepStatus) {
	log := logf.FromContext(ctx).WithValues("step", ss.Name)
	fo := ss.FanOut
	knight := &aiv1alpha1.Knight{}
	if err := r.Get(ctx, types.NamespacedName{Name: spec.KnightRef.Name, Namespace: chain.Namespace}, knight); err != nil {
		log.Error(err, "Failed to get knight", "knightRef", spec.KnightRef.Name)
		return
	}
	// Every item runs on the step's model override (#217); a model the
	// knight's runtime can't run fails the whole step before any dispatch.
	model, err := knightpkg.ResolveModel(spec.Model, nc.ModelAliases, knightpkg.EffectiveImage(knightpkg.WithTableDefaults(ctx, r.Client, knight), r.DefaultKnightImage))
	if err != nil {
		r.Recorder.Eventf(chain, corev1.EventTypeWarning, "InvalidStepModel", "Step %s: %v", spec.Name, err)
		ss.Model = ""
		r.failFanOutStep(chain, ss, fmt.Sprintf("invalid model for knight %q: %v", spec.KnightRef.Name, err))
		return
	}
	ss.Model = model

	limit := int(spec.MaxParallel)
	if limit <= 0 {
		limit = int(knightpkg.Concurrency(knightpkg.WithTableDefaults(ctx, r.Client, knight)))
	}
	slots := limit - int(fo.Running)
	if slots <= 0 {
		return
	}

	var items []any
	retryPolicy := stepRetryPolicy(chain, spec)
	for idx := 0; idx < len(fo.ItemPhases) && slots > 0; idx++ {
		if fo.ItemPhases[idx] != itemPending {
			continue
		}
		it := itemDetail(fo, idx)
		if it != nil && it.Retries > 0 && it.CompletedAt != nil && retryPolicy != nil &&
			time.Since(it.CompletedAt.Time) < time.Duration(retryPolicy.BackoffSeconds)*time.Second {
			continue
		}
		if items == nil {
			var err error
			if items, err = r.resolveForEachItems(ctx, chain, spec); err != nil {
				r.failFanOutStep(chain, ss, err.Error())
				return
			}
			if h := hashItems(items); h != fo.ItemsHash || len(items) != int(fo.Total) {
				r.failFanOutStep(chain, ss, "forEach item list changed during the run")
				return
			}
		}
		item := items[idx]
		itemJSON, _ := json.Marshal(item)
		taskStr, err := r.renderTemplateWith(chain, spec.Task, map[string]any{
			"Item": item, "item": item, "ItemIndex": idx, "ItemJSON": string(itemJSON),
		})
		now := metav1.Now()
		if it == nil {
			fo.Items = append(fo.Items, aiv1alpha1.ChainItemStatus{Index: int32(idx)})
			it = &fo.Items[len(fo.Items)-1]
		}
		if err != nil {
			it.Phase = aiv1alpha1.ChainStepPhaseFailed
			it.Error = truncateUTF8(fmt.Sprintf("template render error: %v", err), itemErrorLimit)
			it.CompletedAt = &now
			setItemPhase(fo, idx, itemFailed)
			continue
		}

		runID := itemRunID(chain.Status.RunID, idx)
		taskID := fmt.Sprintf("chain-%s-%s-%d.%s-%d", chain.Name, spec.Name, idx, runID, time.Now().UnixMilli())
		payload := natspkg.TaskPayload{
			TaskID:    taskID,
			ChainName: chain.Name,
			StepName:  spec.Name,
			RunID:     runID,
			Task:      taskStr,
			Model:     model,
		}
		seq, err := r.publishTask(ctx, nc, knight.Spec.Domain, spec.KnightRef.Name, payload)
		if err != nil {
			log.Error(err, "Failed to publish item task", "item", idx)
			return
		}
		it.Phase = aiv1alpha1.ChainStepPhaseRunning
		it.RunID = runID
		it.TaskID = taskID
		it.TaskSeq = seq
		it.StartedAt = &now
		it.PickedUpAt = nil
		it.CompletedAt = nil
		it.Output = ""
		setItemPhase(fo, idx, itemRunning)
		slots--
		log.Info("Published item task", "item", idx, "taskId", taskID)
	}
	summariseFanOut(fo)
	// failFast also applies to items that failed at render time.
	if fo.Failed > 0 && spec.FailurePolicy != aiv1alpha1.ChainFailurePolicyContinue {
		skipPendingItems(fo)
	}
}

func (r *ChainReconciler) failFanOutStep(chain *aiv1alpha1.Chain, ss *aiv1alpha1.ChainStepStatus, msg string) {
	now := metav1.Now()
	ss.Phase = aiv1alpha1.ChainStepPhaseFailed
	ss.Error = msg
	ss.CompletedAt = &now
	if ss.FanOut != nil {
		b := []byte(ss.FanOut.ItemPhases)
		for i, c := range b {
			if c == itemPending {
				b[i] = itemSkipped
			}
		}
		ss.FanOut.ItemPhases = string(b)
		summariseFanOut(ss.FanOut)
	}
	r.Recorder.Eventf(chain, corev1.EventTypeWarning, "FanOutFailed", "Step %s: %s", ss.Name, msg)
}

// finalizeFanOutIfDone joins a fan-out step once no item is pending or running.
func (r *ChainReconciler) finalizeFanOutIfDone(ctx context.Context, chain *aiv1alpha1.Chain, spec *aiv1alpha1.ChainStep, ss *aiv1alpha1.ChainStepStatus) {
	fo := ss.FanOut
	if fo == nil || ss.Phase != aiv1alpha1.ChainStepPhaseRunning {
		return
	}
	summariseFanOut(fo)
	if fo.Pending > 0 || fo.Running > 0 {
		return
	}
	now := metav1.Now()
	ss.CompletedAt = &now
	ss.Output = r.joinFanOut(ctx, chain, spec, ss)
	switch {
	case fo.Failed == 0 && fo.Skipped == 0:
		ss.Phase = aiv1alpha1.ChainStepPhaseSucceeded
		ss.Error = ""
	case spec.FailurePolicy == aiv1alpha1.ChainFailurePolicyContinue && fo.Succeeded > 0:
		ss.Phase = aiv1alpha1.ChainStepPhaseSucceeded
		ss.Error = ""
	default:
		ss.Phase = aiv1alpha1.ChainStepPhaseFailed
		ss.Error = fmt.Sprintf("%d/%d item(s) failed, %d skipped", fo.Failed, fo.Total, fo.Skipped)
	}
	r.storeStepOutputToKV(ctx, chain.Name, chain.Status.RunID, ss.Name, ss.Output, "", spec.KnightRef.Name, ss.StartedAt, &now)
	r.Recorder.Eventf(chain, corev1.EventTypeNormal, "FanOutJoined", "Step %s joined: %d succeeded, %d failed, %d skipped of %d",
		ss.Name, fo.Succeeded, fo.Failed, fo.Skipped, fo.Total)
}

type joinEntry struct {
	Index  int             `json:"index"`
	Item   json.RawMessage `json:"item,omitempty"`
	Phase  string          `json:"phase"`
	Output string          `json:"output,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// joinFanOut builds the step output: a JSON array with one entry per item.
func (r *ChainReconciler) joinFanOut(ctx context.Context, chain *aiv1alpha1.Chain, spec *aiv1alpha1.ChainStep, ss *aiv1alpha1.ChainStepStatus) string {
	fo := ss.FanOut
	budget := itemOutputBudget(fo.Total)
	jsonBudget := itemJSONLimit
	if fo.Total > 0 {
		jsonBudget = min(itemJSONLimit, max(itemJSONMin, itemJSONTotal/int(fo.Total)))
	}
	var items []any
	if fo.Total > 0 {
		if got, err := r.resolveForEachItems(ctx, chain, spec); err == nil && hashItems(got) == fo.ItemsHash {
			items = got
		}
	}
	entries := make([]joinEntry, 0, len(fo.ItemPhases))
	for idx := 0; idx < len(fo.ItemPhases); idx++ {
		e := joinEntry{Index: idx, Phase: string(itemPhaseByChar[fo.ItemPhases[idx]])}
		if idx < len(items) {
			if raw, err := json.Marshal(items[idx]); err == nil {
				if len(raw) > jsonBudget {
					raw, _ = json.Marshal(truncateUTF8(string(raw), jsonBudget))
				}
				e.Item = raw
			}
		}
		if it := itemDetail(fo, idx); it != nil {
			e.Output, e.Error = it.Output, it.Error
		} else if fo.ItemPhases[idx] == itemSucceeded || fo.ItemPhases[idx] == itemFailed {
			key := fmt.Sprintf("%s.%s.%d", chain.Name, ss.Name, idx)
			if out, ok := r.kvOutput(ctx, chain, key); ok {
				if fo.ItemPhases[idx] == itemFailed {
					e.Error = truncateUTF8(out, itemErrorLimit)
				} else {
					e.Output = truncateUTF8(out, budget)
				}
			} else {
				e.Output = "[output not retained in status; see NATS KV chain-outputs key " + key + "]"
			}
		}
		entries = append(entries, e)
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(entries)
	return strings.TrimSpace(buf.String())
}

// compactItemDetails bounds the detailed item entries: in-flight and
// retry-pending items are always kept, then failed items, then the most
// recently finished ones, up to maxItemDetails in total.
func compactItemDetails(fo *aiv1alpha1.ChainFanOutStatus) {
	var active, terminal []aiv1alpha1.ChainItemStatus
	for _, it := range fo.Items {
		switch it.Phase {
		case aiv1alpha1.ChainStepPhaseSucceeded, aiv1alpha1.ChainStepPhaseFailed, aiv1alpha1.ChainStepPhaseSkipped:
			terminal = append(terminal, it)
		default:
			active = append(active, it)
		}
	}
	keep := max(0, maxItemDetails-len(active))
	if len(terminal) > keep {
		slices.SortStableFunc(terminal, func(a, b aiv1alpha1.ChainItemStatus) int {
			af, bf := a.Phase == aiv1alpha1.ChainStepPhaseFailed, b.Phase == aiv1alpha1.ChainStepPhaseFailed
			if af != bf {
				if af {
					return -1
				}
				return 1
			}
			return completedAt(b).Compare(completedAt(a))
		})
		terminal = terminal[:keep]
	}
	fo.Items = append(active, terminal...)
	slices.SortFunc(fo.Items, func(a, b aiv1alpha1.ChainItemStatus) int { return int(a.Index) - int(b.Index) })
	finished := int(fo.Succeeded + fo.Failed)
	fo.OmittedItems = int32(max(0, finished-len(terminal)))
}

func completedAt(it aiv1alpha1.ChainItemStatus) time.Time {
	if it.CompletedAt == nil {
		return time.Time{}
	}
	return it.CompletedAt.Time
}
