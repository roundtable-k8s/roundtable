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
	"sort"
	"strconv"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/chainrun"
	"github.com/roundtable-k8s/roundtable/internal/github"
)

// State selectors of the github source (#528): with checks, mergeable or
// mergeQueue set, the source lists open pull requests and selects those with
// a failure (a merge conflict, failed checks on the head commit, or a
// rejected merge queue attempt). It fires once per failure: the event key
// carries the failure kind and head commit. Each pull request has one run,
// started again for each new failure, up to maxRunsPerItem; then onMaxRuns
// hands the pull request back.

// githubCheckSummaryMax caps a failed check's summary in the event payload.
const githubCheckSummaryMax = 1000

// GitHubPRState is the pull request state a state-selector source adds to
// its event payload. It is exported because githubItem embeds it (JSON).
type GitHubPRState struct {
	// HeadSHA is the pull request's head commit.
	HeadSHA string `json:"headSha"`
	// MergeState is GitHub's mergeable_state ("dirty" for conflicts; empty
	// when the pull request could not be read).
	MergeState string `json:"mergeState"`
	// FailureKind is conflict, ci or merge-queue.
	FailureKind string `json:"failureKind"`
	// FailureURL links the failed check or run, or the pull request.
	FailureURL string `json:"failureUrl"`
	// FailedChecks are the failed checks (ci) or merge queue workflow runs
	// (merge-queue).
	FailedChecks []githubFailedCheck `json:"failedChecks"`
	// MergeGroupSHA is the failed merge group's commit (merge-queue).
	MergeGroupSHA string `json:"mergeGroupSha,omitempty"`
}

// githubFailedCheck is one failed check of a pull request.
type githubFailedCheck struct {
	Name    string `json:"name"`
	URL     string `json:"url"`
	Summary string `json:"summary"`
}

// githubStateSource reports whether src selects pull requests by state.
func githubStateSource(src *aiv1alpha1.TriggerGitHubSource) bool {
	return src != nil && (src.Checks != "" || src.Mergeable != "" || src.MergeQueue != "")
}

// eventKey is the item's event key: its key, plus the failure kind and head
// commit (and merge group commit) for a state-selector item.
func (i githubItem) eventKey() string {
	if i.GitHubPRState == nil {
		return i.key()
	}
	k := i.key() + "@" + i.FailureKind + ":" + i.HeadSHA
	if i.MergeGroupSHA != "" {
		k += ":" + i.MergeGroupSHA
	}
	return k
}

// githubCandidate is an item a poll selected, with the issue form the skip
// rules read.
type githubCandidate struct {
	issue github.Issue
	item  githubItem
}

// hasAllLabels reports whether labels carry every name in want
// (case-insensitive).
func hasAllLabels(labels []github.Label, want []string) bool {
	for _, w := range want {
		if !slices.ContainsFunc(labels, func(l github.Label) bool { return strings.EqualFold(l.Name, w) }) {
			return false
		}
	}
	return true
}

// hasAnyLabel reports whether labels carry one of names (case-insensitive).
func hasAnyLabel(labels []github.Label, names []string) bool {
	for _, n := range names {
		if slices.ContainsFunc(labels, func(l github.Label) bool { return strings.EqualFold(l.Name, n) }) {
			return true
		}
	}
	return false
}

// listGitHubCandidates lists one repository's items matching src. A
// state-selector source reads each pull request's state; problems with one
// pull request are returned in problems and skip it.
func listGitHubCandidates(ctx context.Context, gh *github.Client, src *aiv1alpha1.TriggerGitHubSource, repo string) ([]githubCandidate, github.Rate, []string, error) {
	kind := src.Kind
	if kind == "" {
		kind = aiv1alpha1.TriggerGitHubKindIssue
	}
	if !githubStateSource(src) {
		items, rate, err := gh.ListIssuesByQuery(ctx, repo, github.IssueQuery{
			Labels: src.Query.Labels, State: string(src.Query.State),
			PullRequests: kind == aiv1alpha1.TriggerGitHubKindPullRequest,
		})
		if err != nil {
			return nil, rate, nil, err
		}
		out := make([]githubCandidate, 0, len(items))
		for i := range items {
			if src.Author != "" && !strings.EqualFold(items[i].User.Login, src.Author) {
				continue
			}
			out = append(out, githubCandidate{issue: items[i], item: newGitHubItem(repo, kind, &items[i])})
		}
		return out, rate, nil, nil
	}
	return listGitHubPRFailures(ctx, gh, src, repo)
}

// listGitHubPRFailures lists the open pull requests of repo that match src
// and have a failure it selects: a conflict first (the pull request cannot
// merge until it is rebased, which re-runs CI anyway), then failed checks on
// the head commit, then a rejected merge queue attempt.
func listGitHubPRFailures(ctx context.Context, gh *github.Client, src *aiv1alpha1.TriggerGitHubSource, repo string) ([]githubCandidate, github.Rate, []string, error) {
	var rate github.Rate
	pulls, err := gh.ListOpenPulls(ctx, repo)
	if err != nil {
		return nil, rate, nil, err
	}
	var groups map[int64]*mergeGroup
	if src.MergeQueue != "" {
		runs, err := gh.ListMergeGroupRuns(ctx, repo)
		if err != nil {
			return nil, rate, nil, err
		}
		groups = latestMergeGroups(runs)
	}
	ignore := make(map[string]bool, len(src.IgnoreChecks))
	for _, n := range src.IgnoreChecks {
		ignore[sanitizeCheckName(n)] = true
	}
	var excluded []string
	if src.OnMaxRuns != nil {
		excluded = src.OnMaxRuns.AddLabels
	}

	var out []githubCandidate
	var problems []string
	for i := range pulls {
		pr := &pulls[i]
		if src.Author != "" && !strings.EqualFold(pr.User.Login, src.Author) {
			continue
		}
		if !hasAllLabels(pr.Labels, src.Query.Labels) || hasAnyLabel(pr.Labels, excluded) {
			continue
		}
		state := &GitHubPRState{HeadSHA: pr.Head.SHA, FailedChecks: []githubFailedCheck{}}
		// The list endpoint has no mergeable_state: read the pull request.
		full, err := gh.GetPull(ctx, repo, pr.Number)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s#%d merge state: %v", repo, pr.Number, err))
		} else {
			state.MergeState = full.MergeableState
		}
		switch {
		case src.Mergeable != "" && strings.EqualFold(state.MergeState, ghMergeableDirty):
			state.FailureKind, state.FailureURL = failureKindConflict, pr.HTMLURL
		default:
			if src.Checks != "" {
				checks, err := gh.ListCheckRuns(ctx, repo, pr.Head.SHA)
				if err != nil {
					problems = append(problems, fmt.Sprintf("%s#%d check runs: %v", repo, pr.Number, err))
					continue
				}
				if failed := failedHeadChecks(checks, ignore); len(failed) > 0 {
					state.FailureKind, state.FailureURL, state.FailedChecks = failureKindCI, failed[0].URL, failed
					break
				}
			}
			if mg := groups[int64(pr.Number)]; mg != nil {
				if failed := failedMergeGroupRuns(mg, ignore); len(failed) > 0 {
					state.FailureKind, state.FailureURL, state.FailedChecks = failureKindMergeQueue, failed[0].URL, failed
					state.MergeGroupSHA = mg.sha
				}
			}
		}
		if state.FailureKind == "" {
			continue
		}
		is := github.Issue{
			Number: pr.Number, Title: pr.Title, Body: pr.Body, HTMLURL: pr.HTMLURL, State: pr.State,
			Labels: pr.Labels, User: pr.User, PullRequest: &struct{}{},
		}
		item := newGitHubItem(repo, aiv1alpha1.TriggerGitHubKindPullRequest, &is)
		item.GitHubPRState = state
		out = append(out, githubCandidate{issue: is, item: item})
	}
	return out, rate, problems, nil
}

// failedHeadChecks returns the checks whose latest attempt failed, by name,
// skipping ignored names.
func failedHeadChecks(checks []github.CheckRun, ignore map[string]bool) []githubFailedCheck {
	latest, _ := latestChecks(checks)
	var out []githubFailedCheck
	for name, cr := range latest {
		if cr.Status != ghCompleted || !isFailedConclusion(cr.Conclusion) || ignore[name] {
			continue
		}
		summary := cr.Output.Summary
		if summary == "" {
			summary = cr.Output.Title
		}
		out = append(out, githubFailedCheck{
			Name: name, URL: cr.HTMLURL,
			Summary: truncateUTF8(strings.TrimSpace(sanitizeLog(summary)), githubCheckSummaryMax),
		})
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Name < out[b].Name })
	return out
}

// failedMergeGroupRuns returns the failed workflow runs of a merge group,
// by name, skipping ignored names.
func failedMergeGroupRuns(mg *mergeGroup, ignore map[string]bool) []githubFailedCheck {
	var out []githubFailedCheck
	for _, run := range mg.failed() {
		if name := sanitizeCheckName(run.Name); !ignore[name] {
			out = append(out, githubFailedCheck{Name: name, URL: run.HTMLURL})
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Name < out[b].Name })
	return out
}

// githubItemRunState is what a state-selector item's event does with the
// item's run.
type githubItemRunState int

const (
	// githubItemRunNew: the item has no run; start one.
	githubItemRunNew githubItemRunState = iota
	// githubItemRunAgain: a new failure of an item whose run finished;
	// start the run again.
	githubItemRunAgain
	// githubItemRunDuplicate: the run already handled this failure.
	githubItemRunDuplicate
	// githubItemRunBusy: the item's run is active.
	githubItemRunBusy
	// githubItemRunExhausted: maxRunsPerItem runs were started; hand the
	// item back (onMaxRuns).
	githubItemRunExhausted
)

// githubItemRunName names the run of a state-selector item: one per item,
// "<chain>-<trigger>-<hash of owner/name#number>".
func githubItemRunName(trg *aiv1alpha1.Trigger, tmpl string, item githubItem) string {
	return chainrun.Name(tmpl, trg.Name, dedupeHash(item.key()))
}

// githubItemRuns is the number of runs started for run's item.
func githubItemRuns(run *aiv1alpha1.Chain) int32 {
	n, _ := strconv.ParseInt(run.Annotations[aiv1alpha1.AnnotationTriggerItemRuns], 10, 32)
	return int32(n)
}

// githubItemRun decides what an event with key does with run, the item's
// run (nil when it has none), and returns the runs started for the item
// that count against maxRunsPerItem. A run handed back (onMaxRuns) whose
// item is selected again starts counting over.
func githubItemRun(trg *aiv1alpha1.Trigger, run *aiv1alpha1.Chain, key string) (githubItemRunState, int32) {
	if run == nil {
		return githubItemRunNew, 0
	}
	if chainrun.Active(run) {
		return githubItemRunBusy, githubItemRuns(run)
	}
	handedBack := run.Annotations[aiv1alpha1.AnnotationTriggerHandedBack] != ""
	if handedBack {
		return githubItemRunAgain, 0
	}
	if run.Annotations[aiv1alpha1.AnnotationTriggerKey] == truncate(key, triggerKeyAnnotationMax) {
		return githubItemRunDuplicate, githubItemRuns(run)
	}
	runs := githubItemRuns(run)
	if n := trg.Spec.Source.GitHub.MaxRunsPerItem; n > 0 && runs >= n {
		return githubItemRunExhausted, runs
	}
	return githubItemRunAgain, runs
}

// handBackExhausted applies onMaxRuns to an item whose runs ran out and
// records it on the item's run, so the run count starts over once the item
// is selected again.
func (r *TriggerReconciler) handBackExhausted(ctx context.Context, gh *github.Client, trg *aiv1alpha1.Trigger, ev TriggerEvent, c githubCandidate, run *aiv1alpha1.Chain, key string, runs int32) error {
	hb := trg.Spec.Source.GitHub.OnMaxRuns
	if hb == nil {
		return nil
	}
	data := triggerTemplateData{
		Event: ev, Trigger: triggerRef{Name: trg.Name, Namespace: trg.Namespace},
		Run:    triggerRunRef{Name: run.Name, ID: run.Annotations[aiv1alpha1.AnnotationChainRun], Attempt: runs},
		Reason: fmt.Sprintf("maxRunsPerItem %d reached", runs),
	}
	if err := applyHandBack(ctx, gh, *hb, "onMaxRuns.comment", data, c.item, &c.issue); err != nil {
		return err
	}
	orig := run.DeepCopy()
	if run.Annotations == nil {
		run.Annotations = map[string]string{}
	}
	run.Annotations[aiv1alpha1.AnnotationTriggerHandedBack] = truncate(key, triggerKeyAnnotationMax)
	if err := r.Patch(ctx, run, client.MergeFrom(orig)); err != nil {
		return fmt.Errorf("record hand-back on run %s: %w", run.Name, err)
	}
	return nil
}
