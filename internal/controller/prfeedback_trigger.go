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
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// The trigger engine of PR feedback (#528): with spec.prFeedback.engine:
// trigger the PR feedback controller generates one Trigger per author, with
// a github source that selects the author's open pull requests with a
// conflict, failed checks or a rejected merge queue attempt, and stops its
// own poller for the table.
//
// What is generic lives in the github source: the state selectors, one
// event per failure (head commit and failure kind in the key), one run per
// pull request started again per failure, the attempt cap (maxRunsPerItem)
// and the escalation (onMaxRuns, whose label also stops selecting the pull
// request). What stays here is what only PR feedback knows: finding the
// author knight and its run in the chain run that opened the pull request
// (prFeedbackAuthor), and the trimmed, fenced job log (prFeedbackLog). The
// Trigger controller calls both for Triggers this file generates. Flaky-test
// issues stay with the builtin engine.

// prFeedbackDefaultTriggerTTL keeps a pull request's finished run, and with
// it the run count against maxAttempts, for a week after its last run.
const prFeedbackDefaultTriggerTTL = int32(7 * 24 * 3600)

// prFeedbackEngine is the table's PR feedback engine, defaulted.
func prFeedbackEngine(cfg *aiv1alpha1.PRFeedbackSpec) aiv1alpha1.PRFeedbackEngine {
	if cfg != nil && cfg.Engine == aiv1alpha1.PRFeedbackEngineTrigger {
		return aiv1alpha1.PRFeedbackEngineTrigger
	}
	return aiv1alpha1.PRFeedbackEngineBuiltin
}

// controllingTable returns the RoundTable named by trg's label that also
// controls trg, or "".
func controllingTable(trg *aiv1alpha1.Trigger, label string) string {
	table := trg.Labels[label]
	if table == "" {
		return ""
	}
	ref := metav1.GetControllerOf(trg)
	if ref == nil || ref.Kind != "RoundTable" || ref.Name != table ||
		!strings.HasPrefix(ref.APIVersion, aiv1alpha1.GroupVersion.Group+"/") {
		return ""
	}
	return table
}

// prFeedbackTable returns the RoundTable whose PR feedback generated trg, or
// "".
func prFeedbackTable(trg *aiv1alpha1.Trigger) string {
	return controllingTable(trg, aiv1alpha1.LabelPRFeedbackRoundTable)
}

// prFeedbackAuthors is the table's watched authors, defaulted.
func prFeedbackAuthors(cfg *aiv1alpha1.PRFeedbackSpec) []string {
	if len(cfg.Authors) == 0 {
		return []string{prFeedbackDefaultAuthor}
	}
	return cfg.Authors
}

var nonNameChars = regexp.MustCompile(`[^a-z0-9]+`)

// prFeedbackTriggerNames maps each generated Trigger's name to its author:
// "<table>-pr-feedback-<author>", the author lowercased with other
// characters turned into dashes. Authors that would share a name get a
// hash of the login appended.
func prFeedbackTriggerNames(rt *aiv1alpha1.RoundTable, cfg *aiv1alpha1.PRFeedbackSpec) map[string]string {
	authors := prFeedbackAuthors(cfg)
	slug := func(a string) string {
		return strings.Trim(nonNameChars.ReplaceAllString(strings.ToLower(a), "-"), "-")
	}
	count := map[string]int{}
	for _, a := range authors {
		count[slug(a)]++
	}
	out := make(map[string]string, len(authors))
	for _, a := range authors {
		s := slug(a)
		if s == "" || count[s] > 1 {
			s = strings.Trim(s+"-"+dedupeHash(a)[:6], "-")
		}
		out[rt.Name+aiv1alpha1.PRFeedbackTriggerInfix+s] = a
	}
	return out
}

// prFeedbackTriggerInput renders the input the builtin engine gives the
// feedback Chain. knight, runId, issue and log are added to the event by the
// Trigger controller (prFeedbackAuthor, prFeedbackLog).
func prFeedbackTriggerInput(maxAttempts int32) string {
	return `{"repo": {{ toJson .Event.Data.repo }}, "pr": {{ toJson .Event.Data.number }}, "url": {{ toJson .Event.Data.url }}, ` +
		`"knight": {{ toJson (index .Event.Data "knight") }}, "runId": {{ toJson (index .Event.Data "runId") }}, ` +
		fmt.Sprintf(`"attempt": {{ .Run.Attempt }}, "maxAttempts": %d, `, maxAttempts) +
		`"kind": {{ toJson .Event.Data.failureKind }}, "headSha": {{ toJson .Event.Data.headSha }}, ` +
		`"failedChecks": {{ toJson (pluck "name" .Event.Data.failedChecks) }}, "failureUrl": {{ toJson .Event.Data.failureUrl }}` +
		`{{ with index .Event.Data "log" }}, "log": {{ toJson . }}{{ end }}` +
		`{{ with index .Event.Data "issue" }}, "issue": {{ toJson . }}{{ end }}}`
}

// prFeedbackEscalationComment is the builtin engine's escalation comment.
const prFeedbackEscalationComment = `{{ if eq .Event.Data.failureKind "conflict" }}` +
	`Still conflicting with the base branch after {{ .Run.Attempt }} fix round(s) (head {{ .Event.Data.headSha }}).` +
	`{{ else }}CI is still failing after {{ .Run.Attempt }} fix round(s) ({{ .Event.Data.failureKind }}: ` +
	`{{ range $i, $c := .Event.Data.failedChecks }}{{ if $i }}, {{ end }}{{ $c.name }}{{ end }}, {{ .Event.Data.failureUrl }}).{{ end }}` +
	` Escalating to a human.`

// prFeedbackTriggerSpec builds a generated Trigger's spec from the table's
// prFeedback fields for one author.
func prFeedbackTriggerSpec(rt *aiv1alpha1.RoundTable, cfg *aiv1alpha1.PRFeedbackSpec, author string) aiv1alpha1.TriggerSpec {
	maxAttempts := cfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = prFeedbackDefaultAttempts
	}
	ignore := cfg.IgnoreChecks
	if ignore == nil {
		ignore = []string{prFeedbackDefaultIgnore}
	}
	ttl := prFeedbackDefaultTriggerTTL
	return aiv1alpha1.TriggerSpec{
		Source: aiv1alpha1.TriggerSource{GitHub: &aiv1alpha1.TriggerGitHubSource{
			Repositories:   append([]string(nil), cfg.Repos...),
			Kind:           aiv1alpha1.TriggerGitHubKindPullRequest,
			Query:          aiv1alpha1.TriggerGitHubQuery{State: aiv1alpha1.TriggerGitHubStateOpen},
			PollInterval:   &metav1.Duration{Duration: prFeedbackPollDuration(cfg)},
			Author:         author,
			Checks:         aiv1alpha1.TriggerGitHubChecksFailing,
			IgnoreChecks:   append([]string{}, ignore...),
			Mergeable:      aiv1alpha1.TriggerGitHubMergeableConflicting,
			MergeQueue:     aiv1alpha1.TriggerGitHubMergeQueueRejected,
			MaxRunsPerItem: maxAttempts,
			OnMaxRuns: &aiv1alpha1.TriggerGitHubHandBack{
				AddLabels: []string{orDefault(cfg.EscalationLabel, prFeedbackDefaultEscalation)},
				Comment:   prFeedbackEscalationComment,
			},
		}},
		Target: aiv1alpha1.TriggerTarget{ChainRef: &corev1.LocalObjectReference{Name: feedbackChainName(cfg)}},
		Input:  prFeedbackTriggerInput(maxAttempts),
		// One fix round at a time, like the builtin engine's single
		// feedback Chain.
		Concurrency: aiv1alpha1.TriggerConcurrency{
			Policy:    aiv1alpha1.TriggerConcurrencyAllow,
			MaxActive: 1,
		},
		Suspend:                 rt.Spec.Suspend,
		RoundTableRef:           &corev1.LocalObjectReference{Name: rt.Name},
		TTLSecondsAfterFinished: &ttl,
	}
}

// reconcileFeedbackTriggers creates or updates the generated Triggers while
// the table runs PR feedback on the trigger engine and deletes them (and
// their runs) otherwise. It returns the generated Triggers' names. A Trigger
// of a generated name that the table does not control is never touched.
func (r *PRFeedbackReconciler) reconcileFeedbackTriggers(ctx context.Context, rt *aiv1alpha1.RoundTable) ([]string, error) {
	log := logf.FromContext(ctx)
	cfg := rt.Spec.PRFeedback
	want := map[string]string{}
	if cfg != nil && cfg.Enabled && !rt.Spec.Ephemeral && prFeedbackEngine(cfg) == aiv1alpha1.PRFeedbackEngineTrigger && rt.DeletionTimestamp.IsZero() {
		want = prFeedbackTriggerNames(rt, cfg)
	}

	owned := &aiv1alpha1.TriggerList{}
	if err := r.List(ctx, owned, client.InNamespace(rt.Namespace),
		client.MatchingLabels{aiv1alpha1.LabelPRFeedbackRoundTable: rt.Name}); err != nil {
		return nil, fmt.Errorf("list PR feedback Triggers: %w", err)
	}
	var errs []error
	for i := range owned.Items {
		trg := &owned.Items[i]
		if _, ok := want[trg.Name]; ok || !metav1.IsControlledBy(trg, rt) || !trg.DeletionTimestamp.IsZero() {
			continue
		}
		if err := r.Delete(ctx, trg); client.IgnoreNotFound(err) != nil {
			errs = append(errs, fmt.Errorf("delete Trigger %s: %w", trg.Name, err))
			continue
		}
		log.Info("Deleted PR feedback Trigger", "trigger", trg.Name)
		r.Recorder.Eventf(rt, corev1.EventTypeNormal, "PRFeedbackTriggerDeleted", "Deleted Trigger %s", trg.Name)
	}

	names := slices.Sorted(maps.Keys(want))
	for _, name := range names {
		if err := r.applyFeedbackTrigger(ctx, rt, name, prFeedbackTriggerSpec(rt, cfg, want[name])); err != nil {
			errs = append(errs, err)
		}
	}
	return names, errors.Join(errs...)
}

// applyFeedbackTrigger creates or updates one generated Trigger.
func (r *PRFeedbackReconciler) applyFeedbackTrigger(ctx context.Context, rt *aiv1alpha1.RoundTable, name string, desired aiv1alpha1.TriggerSpec) error {
	if len(name) > 63 {
		return fmt.Errorf("engine trigger: Trigger name %q is longer than 63 characters; shorten the RoundTable name or the author", name)
	}
	existing := &aiv1alpha1.Trigger{}
	err := r.Get(ctx, types.NamespacedName{Namespace: rt.Namespace, Name: name}, existing)
	if client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("get Trigger %s: %w", name, err)
	}
	if err != nil {
		trg := &aiv1alpha1.Trigger{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: rt.Namespace,
				Labels: map[string]string{aiv1alpha1.LabelPRFeedbackRoundTable: rt.Name},
			},
			Spec: desired,
		}
		if err := controllerutil.SetControllerReference(rt, trg, r.Scheme()); err != nil {
			return err
		}
		if err := r.Create(ctx, trg); err != nil {
			return fmt.Errorf("create Trigger %s: %w", name, err)
		}
		logf.FromContext(ctx).Info("Created PR feedback Trigger", "trigger", name)
		r.Recorder.Eventf(rt, corev1.EventTypeNormal, "PRFeedbackTriggerCreated",
			"Created Trigger %s: PR feedback runs on the trigger engine", name)
		return nil
	}
	if !metav1.IsControlledBy(existing, rt) {
		return fmt.Errorf("engine trigger: Trigger %s exists and is not owned by this RoundTable", name)
	}
	if !existing.DeletionTimestamp.IsZero() {
		return nil // recreated once the old one is gone
	}
	if equality.Semantic.DeepEqual(existing.Spec, desired) && existing.Labels[aiv1alpha1.LabelPRFeedbackRoundTable] == rt.Name {
		return nil
	}
	existing.Spec = desired
	if existing.Labels == nil {
		existing.Labels = map[string]string{}
	}
	existing.Labels[aiv1alpha1.LabelPRFeedbackRoundTable] = rt.Name
	if err := r.Update(ctx, existing); err != nil {
		return fmt.Errorf("update Trigger %s: %w", name, err)
	}
	logf.FromContext(ctx).Info("Updated PR feedback Trigger", "trigger", name)
	return nil
}

// prFeedbackPollDuration is the table's poll interval.
func prFeedbackPollDuration(cfg *aiv1alpha1.PRFeedbackSpec) time.Duration {
	return time.Duration(prFeedbackInterval(cfg)) * time.Second
}

// setPRFeedbackEngineStatus records the engine in use, the generated
// Triggers, and (trigger engine) that flaky-test issues are not filed.
func setPRFeedbackEngineStatus(rt *aiv1alpha1.RoundTable, st *aiv1alpha1.PRFeedbackStatus, triggers []string) {
	engine := prFeedbackEngine(rt.Spec.PRFeedback)
	st.Engine = engine
	st.Triggers = nil
	if engine != aiv1alpha1.PRFeedbackEngineTrigger {
		meta.RemoveStatusCondition(&st.Conditions, aiv1alpha1.ConditionPRFeedbackFlakeReportingSkipped)
		return
	}
	st.Triggers = triggers
	meta.SetStatusCondition(&st.Conditions, metav1.Condition{
		Type:   aiv1alpha1.ConditionPRFeedbackFlakeReportingSkipped,
		Status: metav1.ConditionTrue,
		Reason: aiv1alpha1.ReasonPRFeedbackTriggerEngine,
		Message: "PR feedback runs on the trigger engine, which files no flaky-test issues: " +
			"a check that failed and passed on the same commit starts a fix round. Set spec.prFeedback.engine: builtin to file them.",
		ObservedGeneration: rt.Generation,
	})
}

// patchEngineStatus records the engine status (and, on the trigger engine,
// the Triggers' reconcile error as lastError) when it changed.
func (r *PRFeedbackReconciler) patchEngineStatus(ctx context.Context, rt *aiv1alpha1.RoundTable, triggers []string, triggerErr error) error {
	orig := rt.DeepCopy()
	st := rt.Status.PRFeedback
	if st == nil {
		st = &aiv1alpha1.PRFeedbackStatus{}
		rt.Status.PRFeedback = st
	}
	setPRFeedbackEngineStatus(rt, st, triggers)
	if prFeedbackEngine(rt.Spec.PRFeedback) == aiv1alpha1.PRFeedbackEngineTrigger {
		st.ActiveRun = ""
		st.LastError = ""
		if triggerErr != nil {
			st.LastError = truncateUTF8(triggerErr.Error(), 1024)
		}
	}
	if equality.Semantic.DeepEqual(orig.Status.PRFeedback, st) {
		return nil
	}
	return r.Status().Patch(ctx, rt, client.MergeFrom(orig))
}

// withEventFields returns event data with extra fields added (a copy; data
// that is not a JSON object is returned as is).
func withEventFields(data any, extra map[string]any) any {
	m, ok := data.(map[string]any)
	if !ok {
		return data
	}
	out := maps.Clone(m)
	maps.Copy(out, extra)
	return out
}

// prFeedbackAuthor finds the author knight of a PR feedback Trigger's pull
// request: the event fields knight, runId and issue. reason is set (and
// nothing fires) while the chain run that opened the pull request is still
// running, or when no chain run opened it and the table has no
// fallbackKnightRef.
func (r *TriggerReconciler) prFeedbackAuthor(ctx context.Context, trg *aiv1alpha1.Trigger, table string, item githubItem) (map[string]any, string, error) {
	rt := &aiv1alpha1.RoundTable{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: trg.Namespace, Name: table}, rt); err != nil {
		return nil, "", fmt.Errorf("get RoundTable %q: %w", table, err)
	}
	author, running, err := traceAuthor(ctx, r.reader(), trg.Namespace, aiv1alpha1.RefName(trg.Spec.Target.ChainRef), item.Repo, item.Number)
	if err != nil {
		return nil, "", err
	}
	if running {
		return nil, fmt.Sprintf("the chain run that opened %s is still running", item.key()), nil
	}
	if author.knight == "" && rt.Spec.PRFeedback != nil && rt.Spec.PRFeedback.FallbackKnightRef != nil {
		author.knight = rt.Spec.PRFeedback.FallbackKnightRef.Name
	}
	if author.knight == "" {
		return nil, fmt.Sprintf("no chain run opened %s and spec.prFeedback.fallbackKnightRef is unset", item.key()), nil
	}
	extra := map[string]any{"knight": author.knight, "runId": author.runID}
	if author.issue > 0 {
		extra["issue"] = author.issue
	}
	return extra, "", nil
}

// prFeedbackLog returns the trimmed, fenced log of the failed jobs of a PR
// feedback event, as the builtin engine sends it; nil for a conflict (no
// failed job).
func (r *TriggerReconciler) prFeedbackLog(ctx context.Context, trg *aiv1alpha1.Trigger, item githubItem) any {
	st := item.GitHubPRState
	if st == nil || st.FailureKind == failureKindConflict {
		return nil
	}
	rec := &aiv1alpha1.PRFeedbackPullRequest{
		Repo: item.Repo, Number: int64(item.Number), FailureKind: st.FailureKind,
		FailureHeadSHA: st.HeadSHA, FailureURL: st.FailureURL,
	}
	for _, c := range st.FailedChecks {
		rec.FailedChecks = append(rec.FailedChecks, c.Name)
	}
	if st.FailureKind == failureKindMergeQueue {
		rec.LastFailure = "mq:" + st.MergeGroupSHA
	}
	gh, err := r.githubClient(ctx, trg)
	if err != nil {
		return fenceUntrusted(sanitizeLog(fmt.Sprintf("(failed jobs unavailable: %v; see %s)\n", err, st.FailureURL)))
	}
	return failureLog(ctx, gh, rec)
}
