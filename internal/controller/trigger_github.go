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
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/chainrun"
	"github.com/roundtable-k8s/roundtable/internal/github"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

const (
	// triggerGitHubDefaultInterval is a github source's default pollInterval.
	triggerGitHubDefaultInterval = 60 * time.Second
	// triggerGitHubMinInterval is the shortest pollInterval honoured (the
	// API server enforces it too).
	triggerGitHubMinInterval = 30 * time.Second
	// triggerGitHubSecondaryBackoff is how long a poll waits after a
	// secondary rate limit that did not say when to retry.
	triggerGitHubSecondaryBackoff = 5 * time.Minute
	// triggerGitHubMaxBackoff caps how long a rate limit pauses polling.
	triggerGitHubMaxBackoff = time.Hour
)

// githubItem is the payload of a github-source event (.Event.Data).
type githubItem struct {
	Repo   string   `json:"repo"`
	Number int      `json:"number"`
	Title  string   `json:"title"`
	Labels []string `json:"labels"`
	URL    string   `json:"url"`
	Author string   `json:"author"`
	Kind   string   `json:"kind"`
	State  string   `json:"state"`
}

// key is the item's default dedupe key, "<owner/name>#<number>".
func (i githubItem) key() string { return i.Repo + "#" + strconv.Itoa(i.Number) }

func newGitHubItem(repo string, kind aiv1alpha1.TriggerGitHubKind, is *github.Issue) githubItem {
	labels := make([]string, 0, len(is.Labels))
	for _, l := range is.Labels {
		labels = append(labels, l.Name)
	}
	return githubItem{
		Repo: repo, Number: is.Number, Title: is.Title, Labels: labels,
		URL: is.HTMLURL, Author: is.User.Login, Kind: string(kind), State: is.State,
	}
}

// githubPollInterval is the source's pollInterval, defaulted and floored.
func githubPollInterval(src *aiv1alpha1.TriggerGitHubSource) time.Duration {
	if src.PollInterval == nil || src.PollInterval.Duration <= 0 {
		return triggerGitHubDefaultInterval
	}
	return max(src.PollInterval.Duration, triggerGitHubMinInterval)
}

// claimPending reports whether claim still has to be applied to an item
// carrying labels: an addLabel is missing or a removeLabel is present.
// Labels compare case-insensitively, like GitHub.
func claimPending(claim *aiv1alpha1.TriggerGitHubClaim, labels []string) bool {
	if claim == nil {
		return false
	}
	has := func(name string) bool {
		for _, l := range labels {
			if strings.EqualFold(l, name) {
				return true
			}
		}
		return false
	}
	missing := func(name string) bool { return !has(name) }
	return slices.ContainsFunc(claim.AddLabels, missing) || slices.ContainsFunc(claim.RemoveLabels, has)
}

// githubEventItem reads a github-source event's payload back into an item.
func githubEventItem(ev TriggerEvent) (githubItem, error) {
	var item githubItem
	b, err := json.Marshal(ev.Data)
	if err != nil {
		return item, err
	}
	if err := json.Unmarshal(b, &item); err != nil {
		return item, fmt.Errorf("event is not a GitHub item: %w", err)
	}
	if item.Repo == "" || item.Number <= 0 {
		return item, errors.New("event is not a GitHub item: no repo or number")
	}
	return item, nil
}

// githubClient builds a client with the Trigger's RoundTable token.
func (r *TriggerReconciler) githubClient(ctx context.Context, trg *aiv1alpha1.Trigger) (*github.Client, error) {
	rtName := aiv1alpha1.RefName(trg.Spec.RoundTableRef)
	rt := &aiv1alpha1.RoundTable{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: trg.Namespace, Name: rtName}, rt); err != nil {
		return nil, fmt.Errorf("get RoundTable %q: %w", rtName, err)
	}
	ref := github.ResolveTokenRef(github.TableTokenRef(rt))
	if ref == nil {
		return nil, fmt.Errorf("RoundTable %q has no spec.github.tokenSecretRef", rtName)
	}
	token, err := github.ReadToken(ctx, r.Client, trg.Namespace, ref)
	if err != nil {
		return nil, err
	}
	return r.GitHub.New(token), nil
}

// claimGitHub applies the source's claim to the event's item for run: add
// labels, assignees, the comment (once per run, recorded with
// AnnotationTriggerClaimed), then remove labels.
func (r *TriggerReconciler) claimGitHub(ctx context.Context, trg *aiv1alpha1.Trigger, ev TriggerEvent, item githubItem, run *aiv1alpha1.Chain) error {
	claim := trg.Spec.Source.GitHub.Claim
	gh, err := r.githubClient(ctx, trg)
	if err != nil {
		return err
	}
	if len(claim.AddLabels) > 0 {
		if err := gh.AddLabels(ctx, item.Repo, item.Number, claim.AddLabels...); err != nil {
			return fmt.Errorf("add labels %v to %s: %w", claim.AddLabels, item.key(), err)
		}
	}
	if len(claim.Assignees) > 0 {
		if err := gh.AddAssignees(ctx, item.Repo, item.Number, claim.Assignees...); err != nil {
			return fmt.Errorf("assign %v to %s: %w", claim.Assignees, item.key(), err)
		}
	}
	runID := run.Annotations[aiv1alpha1.AnnotationChainRun]
	if !githubClaimed(trg, run) {
		if claim.Comment != "" {
			body, err := renderTriggerTemplate("claim.comment", claim.Comment, triggerTemplateData{
				Event: ev, Trigger: triggerRef{Name: trg.Name, Namespace: trg.Namespace},
				Run: triggerRunRef{Name: run.Name, ID: runID},
			})
			if err != nil {
				return fmt.Errorf("claim.comment template: %w", err)
			}
			if err := gh.Comment(ctx, item.Repo, item.Number, body); err != nil {
				return fmt.Errorf("comment on %s: %w", item.key(), err)
			}
		}
		orig := run.DeepCopy()
		if run.Annotations == nil {
			run.Annotations = map[string]string{}
		}
		run.Annotations[aiv1alpha1.AnnotationTriggerClaimed] = runID
		if pickupTable(trg) != "" {
			// The builtin poller's record, so a switch back to it does not
			// claim (and comment) again.
			run.Annotations[aiv1alpha1.AnnotationPickupClaimed] = runID
		}
		if err := r.Patch(ctx, run, client.MergeFrom(orig)); err != nil {
			return fmt.Errorf("record claim on run %s: %w", run.Name, err)
		}
	}
	for _, l := range claim.RemoveLabels {
		if err := gh.RemoveLabel(ctx, item.Repo, item.Number, l); err != nil {
			return fmt.Errorf("remove label %q from %s: %w", l, item.key(), err)
		}
	}
	return nil
}

// githubRerun reports whether an event for a finished run starts it again:
// the source claims items, the run's claim was applied, and the item is
// selected again with its claim pending (the selecting label was put back).
func githubRerun(trg *aiv1alpha1.Trigger, run *aiv1alpha1.Chain, ev TriggerEvent) bool {
	return !chainrun.Active(run) && pendingGitHubClaim(trg, ev) != nil && githubClaimed(trg, run)
}

// githubClaimed reports whether run's item was claimed for its current run:
// by this Trigger, or (for a Trigger generated by issue pickup) by the
// builtin poller before an engine switch.
func githubClaimed(trg *aiv1alpha1.Trigger, run *aiv1alpha1.Chain) bool {
	id := run.Annotations[aiv1alpha1.AnnotationChainRun]
	if id == "" {
		return false
	}
	return run.Annotations[aiv1alpha1.AnnotationTriggerClaimed] == id ||
		(pickupTable(trg) != "" && run.Annotations[aiv1alpha1.AnnotationPickupClaimed] == id)
}

// githubRunRepo is the repository of the GitHub item a run was started for,
// or "".
func githubRunRepo(c *aiv1alpha1.Chain) string {
	if repo := c.Annotations[aiv1alpha1.AnnotationTriggerGitHubRepo]; repo != "" {
		return repo
	}
	return c.Annotations[aiv1alpha1.AnnotationPickupRepo]
}

// githubActiveIn counts the runs in active started for an item of repo.
func githubActiveIn(active []aiv1alpha1.Chain, repo string) int32 {
	var n int32
	for i := range active {
		if githubRunRepo(&active[i]) == repo {
			n++
		}
	}
	return n
}

// githubMaxPerRepo is source.github.maxActivePerRepository under the Allow
// policy, else 0.
func githubMaxPerRepo(trg *aiv1alpha1.Trigger) int32 {
	src := trg.Spec.Source.GitHub
	if src == nil {
		return 0
	}
	switch trg.Spec.Concurrency.Policy {
	case aiv1alpha1.TriggerConcurrencyForbid, aiv1alpha1.TriggerConcurrencyReplace:
		return 0
	}
	return src.MaxActivePerRepository
}

// triggerRunName names the run for an event. A Trigger generated by a
// RoundTable's issue pickup names it like the builtin poller does
// ("<chain>-<repo name>-<number>"), so an engine switch finds the same run;
// every other Trigger uses "<chain>-<trigger>-<hash of dedupe key>".
func triggerRunName(trg *aiv1alpha1.Trigger, tmpl, key string, item *githubItem) string {
	if item != nil && pickupTable(trg) != "" {
		return pickupChainName(tmpl, item.Repo, item.Number)
	}
	return chainrun.Name(tmpl, trg.Name, dedupeHash(key))
}

// githubRunOptions adds the item to a github-source run: its repository, and
// for a Trigger generated by issue pickup the pickup label and annotations
// the post-run checks look for, with the RoundTable as owner (as the builtin
// poller does), so deleting the Trigger never deletes a run.
func (r *TriggerReconciler) githubRunOptions(ctx context.Context, trg *aiv1alpha1.Trigger, item githubItem, opts *chainrun.StartOptions) error {
	opts.Annotations[aiv1alpha1.AnnotationTriggerGitHubRepo] = item.Repo
	table := pickupTable(trg)
	if table == "" {
		return nil
	}
	rt := &aiv1alpha1.RoundTable{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: trg.Namespace, Name: table}, rt); err != nil {
		return fmt.Errorf("get RoundTable %q: %w", table, err)
	}
	opts.Owner = rt
	opts.Labels[aiv1alpha1.LabelPickupRoundTable] = table
	opts.Annotations[aiv1alpha1.AnnotationPickupRepo] = item.Repo
	opts.Annotations[aiv1alpha1.AnnotationPickupIssue] = strconv.Itoa(item.Number)
	return nil
}

// validateGitHubSource checks what the API server cannot: the comment
// templates parse and the skip patterns compile.
func validateGitHubSource(src *aiv1alpha1.TriggerGitHubSource) error {
	if src == nil {
		return nil
	}
	if src.Claim != nil {
		if _, err := parseTriggerTemplate("claim.comment", src.Claim.Comment); err != nil {
			return fmt.Errorf("claim.comment: %w", err)
		}
	}
	if sk := src.SkipIfBodyMatches; sk != nil {
		if _, err := parseTriggerTemplate("handBack.comment", sk.HandBack.Comment); err != nil {
			return fmt.Errorf("skipIfBodyMatches.handBack.comment: %w", err)
		}
		if _, err := compileSkipPatterns(sk.Patterns); err != nil {
			return err
		}
	}
	return nil
}

func compileSkipPatterns(patterns []string) ([]*regexp.Regexp, error) {
	out := make([]*regexp.Regexp, 0, len(patterns))
	for _, p := range patterns {
		re, err := regexp.Compile(p)
		if err != nil {
			return nil, fmt.Errorf("skipIfBodyMatches.patterns: %w", err)
		}
		out = append(out, re)
	}
	return out, nil
}

// githubSkipReason returns why an item is handed back instead of starting a
// run, or "".
func githubSkipReason(sk *aiv1alpha1.TriggerGitHubSkip, patterns []*regexp.Regexp, is *github.Issue) string {
	if sk == nil {
		return ""
	}
	for _, l := range sk.HandBack.AddLabels {
		if is.HasLabel(l) {
			return "labelled `" + l + "`"
		}
	}
	text := is.Title + "\n" + is.Body
	for _, re := range patterns {
		if m := re.FindString(text); m != "" {
			return fmt.Sprintf("mentions %q, matching `%s`", m, re.String())
		}
	}
	return ""
}

// handBackGitHub applies skipIfBodyMatches.handBack to an item: add labels it
// lacks, comment, then remove labels (last, so a failed comment is retried).
func handBackGitHub(ctx context.Context, gh *github.Client, trg *aiv1alpha1.Trigger, ev TriggerEvent, item githubItem, is *github.Issue, reason string) error {
	hb := trg.Spec.Source.GitHub.SkipIfBodyMatches.HandBack
	var add []string
	for _, l := range hb.AddLabels {
		if !is.HasLabel(l) {
			add = append(add, l)
		}
	}
	if len(add) > 0 {
		if err := gh.AddLabels(ctx, item.Repo, item.Number, add...); err != nil {
			return fmt.Errorf("add labels %v: %w", add, err)
		}
	}
	if hb.Comment != "" {
		body, err := renderTriggerTemplate("handBack.comment", hb.Comment, triggerTemplateData{
			Event: ev, Trigger: triggerRef{Name: trg.Name, Namespace: trg.Namespace}, Reason: reason,
		})
		if err != nil {
			return fmt.Errorf("handBack.comment template: %w", err)
		}
		if err := gh.Comment(ctx, item.Repo, item.Number, body); err != nil {
			return fmt.Errorf("comment: %w", err)
		}
	}
	for _, l := range hb.RemoveLabels {
		if is.HasLabel(l) {
			if err := gh.RemoveLabel(ctx, item.Repo, item.Number, l); err != nil {
				return fmt.Errorf("remove label %q: %w", l, err)
			}
		}
	}
	return nil
}

// pendingGitHubClaim returns the item of a github-source event whose claim
// still has to be applied, or nil.
func pendingGitHubClaim(trg *aiv1alpha1.Trigger, ev TriggerEvent) *githubItem {
	src := trg.Spec.Source.GitHub
	if src == nil || src.Claim == nil {
		return nil
	}
	item, err := githubEventItem(ev)
	if err != nil || !claimPending(src.Claim, item.Labels) {
		return nil
	}
	return &item
}

// pollGitHub lists the source's repositories once and publishes each item
// that needs handling to the Trigger's subject on the events stream: items
// without a run (unless concurrency would hold them back anyway) and items
// whose run exists but whose claim is still pending. It records the outcome
// in the GitHubReady condition and returns when to poll next.
func (r *TriggerReconciler) pollGitHub(ctx context.Context, nn types.NamespacedName) time.Duration {
	log := logf.Log.WithName("trigger-github").WithValues("trigger", nn.String())
	trg := &aiv1alpha1.Trigger{}
	if err := r.reader().Get(ctx, nn, trg); err != nil {
		if !apierrors.IsNotFound(err) {
			log.Error(err, "Failed to read Trigger")
		}
		return triggerGitHubDefaultInterval
	}
	src := trg.Spec.Source.GitHub
	if src == nil || trg.Spec.Suspend || !trg.DeletionTimestamp.IsZero() {
		return triggerGitHubDefaultInterval
	}
	interval := githubPollInterval(src)
	now := r.now()
	setCond := func(status metav1.ConditionStatus, reason, msg string, polled bool) {
		if err := r.setGitHubCondition(ctx, nn, metav1.Condition{
			Type: aiv1alpha1.ConditionTriggerGitHubReady, Status: status, Reason: reason, Message: msg,
		}, polled, now); err != nil && !apierrors.IsNotFound(err) {
			log.Error(err, "Failed to record GitHub poll status")
		}
	}

	gh, err := r.githubClient(ctx, trg)
	if err != nil {
		setCond(metav1.ConditionFalse, aiv1alpha1.ReasonTriggerTokenError, err.Error(), false)
		return interval
	}
	rt := &aiv1alpha1.RoundTable{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: trg.Namespace, Name: aiv1alpha1.RefName(trg.Spec.RoundTableRef)}, rt); err != nil {
		setCond(metav1.ConditionFalse, aiv1alpha1.ReasonTriggerPollError, err.Error(), false)
		return interval
	}
	subject := natspkg.GitHubEventSubject(rt.Spec.NATS.SubjectPrefix, trg.Namespace, trg.Name)
	nc, err := r.natsConn()
	if err != nil {
		setCond(metav1.ConditionFalse, aiv1alpha1.ReasonTriggerPollError, err.Error(), false)
		return interval
	}
	runs, err := r.listRuns(ctx, trg)
	if err != nil {
		setCond(metav1.ConditionFalse, aiv1alpha1.ReasonTriggerPollError, err.Error(), false)
		return interval
	}
	existing := make(map[string]*aiv1alpha1.Chain, len(runs))
	active := 0
	activeByRepo := map[string]int32{}
	for i := range runs {
		existing[runs[i].Name] = &runs[i]
		if chainrun.Active(&runs[i]) {
			active++
			activeByRepo[githubRunRepo(&runs[i])]++
		}
	}
	var skipPatterns []*regexp.Regexp
	if sk := src.SkipIfBodyMatches; sk != nil {
		if skipPatterns, err = compileSkipPatterns(sk.Patterns); err != nil {
			setCond(metav1.ConditionFalse, aiv1alpha1.ReasonTriggerPollError, err.Error(), false)
			return interval
		}
	}
	maxPerRepo := githubMaxPerRepo(trg)

	var (
		rate                                     github.Rate
		matched, published, claims, held, failed int
		handedBack, prHeld                       int
		problems                                 []string
		prHolds                                  = map[string]string{}
	)
	for _, repo := range src.Repositories {
		items, rl, err := gh.ListIssuesByQuery(ctx, repo, github.IssueQuery{
			Labels: src.Query.Labels, State: string(src.Query.State),
			PullRequests: src.Kind == aiv1alpha1.TriggerGitHubKindPullRequest,
		})
		if rl.Limit > 0 {
			rate = rl
		}
		if reset, limited := github.RateLimited(err); limited {
			wait := triggerGitHubSecondaryBackoff
			if !reset.IsZero() {
				wait = reset.Sub(now)
			}
			wait = min(max(wait, interval), triggerGitHubMaxBackoff)
			setCond(metav1.ConditionFalse, aiv1alpha1.ReasonTriggerRateLimited,
				fmt.Sprintf("GitHub rate limit reached listing %s; next poll in %s", repo, wait.Round(time.Second)), false)
			log.Info("GitHub rate limit reached, backing off", "repo", repo, "wait", wait)
			return wait
		}
		if err != nil {
			problems = append(problems, fmt.Sprintf("list %s: %v", repo, err))
			continue
		}
		kind := src.Kind
		if kind == "" {
			kind = aiv1alpha1.TriggerGitHubKindIssue
		}
		for i := range items {
			matched++
			item := newGitHubItem(repo, kind, &items[i])
			ev, err := githubEvent(item, subject, now)
			if err != nil {
				failed++
				problems = append(problems, fmt.Sprintf("%s: %v", item.key(), err))
				continue
			}
			key, err := r.githubDedupeKey(trg, ev, item)
			if err != nil {
				failed++
				problems = append(problems, fmt.Sprintf("%s: %v", item.key(), err))
				continue
			}
			name := triggerRunName(trg, trg.Spec.Target.ChainRef.Name, key, &item)
			run := existing[name]
			if run != nil && pickupTable(trg) != "" {
				// Two repos with the same name in different orgs map to
				// one pickup run name: report it, never run the wrong issue.
				if owner := githubRunRepo(run); owner != "" && owner != item.Repo {
					failed++
					problems = append(problems, fmt.Sprintf("%s: run %s already belongs to %s", item.key(), name, owner))
					continue
				}
			}
			if run != nil && !githubRerun(trg, run, ev) {
				if !claimPending(src.Claim, item.Labels) {
					continue
				}
				claims++
			} else {
				if reason := githubSkipReason(src.SkipIfBodyMatches, skipPatterns, &items[i]); reason != "" {
					if err := handBackGitHub(ctx, gh, trg, ev, item, &items[i], reason); err != nil {
						failed++
						problems = append(problems, fmt.Sprintf("%s: hand back: %v", item.key(), err))
						continue
					}
					handedBack++
					r.event(trg, corev1.EventTypeNormal, "HandedBack", "%s handed back: %s", item.key(), reason)
					continue
				}
				if githubHeldBack(trg, active, published) || (maxPerRepo > 0 && activeByRepo[repo] >= maxPerRepo) {
					held++
					continue
				}
				if label := src.HoldWhilePullRequestLabeled; label != "" {
					if _, ok := prHolds[repo]; !ok {
						hold, err := breakingHold(ctx, gh, repo, label)
						if err != nil {
							problems = append(problems, fmt.Sprintf("list %s pull requests in %s: %v", label, repo, err))
							hold = "could not list " + label + " pull requests"
						}
						prHolds[repo] = hold
						if hold != "" {
							r.event(trg, corev1.EventTypeNormal, "Held", "%s: new items held, open %s pull request(s): %s", repo, label, hold)
						}
					}
					if prHolds[repo] != "" {
						prHeld++
						continue
					}
				}
				published++
				activeByRepo[repo]++
			}
			if err := publishGitHubItem(ctx, nc, subject, item); err != nil {
				setCond(metav1.ConditionFalse, aiv1alpha1.ReasonTriggerPollError, err.Error(), false)
				return interval
			}
		}
	}

	msg := fmt.Sprintf("%d matching %s(s) in %d repositories: %d published, %d claim retries, %d held back by concurrency",
		matched, githubKindNoun(src.Kind), len(src.Repositories), published, claims, held)
	if src.SkipIfBodyMatches != nil {
		msg += fmt.Sprintf(", %d handed back", handedBack)
	}
	if src.HoldWhilePullRequestLabeled != "" {
		msg += fmt.Sprintf(", %d held by open %s pull requests", prHeld, src.HoldWhilePullRequestLabeled)
	}
	if rate.Limit > 0 {
		msg += fmt.Sprintf("; rate limit remaining %d/%d", rate.Remaining, rate.Limit)
	}
	next := interval
	if rate.Limit > 0 && rate.Remaining == 0 && rate.Reset.After(now) {
		// Used up by this poll: wait for the reset instead of failing next time.
		next = min(max(rate.Reset.Sub(now), interval), triggerGitHubMaxBackoff)
	}
	if len(problems) > 0 {
		setCond(metav1.ConditionFalse, aiv1alpha1.ReasonTriggerPollError, msg+"; "+strings.Join(problems, "; "), false)
		return next
	}
	setCond(metav1.ConditionTrue, aiv1alpha1.ReasonTriggerPolling, msg, true)
	return next
}

func githubKindNoun(kind aiv1alpha1.TriggerGitHubKind) string {
	if kind == aiv1alpha1.TriggerGitHubKindPullRequest {
		return "pull request"
	}
	return "issue"
}

// githubEvent is the event the consumer will see for item, so the poller can
// render the templates exactly as fire will.
func githubEvent(item githubItem, subject string, now time.Time) (TriggerEvent, error) {
	payload, err := json.Marshal(item)
	if err != nil {
		return TriggerEvent{}, err
	}
	return TriggerEvent{
		Subject: subject,
		Data:    eventData(payload),
		Headers: map[string]string{natspkg.EventKeyHeader: item.key()},
		Time:    now,
	}, nil
}

// githubDedupeKey renders the Trigger's dedupe key for an item (default
// "<owner/name>#<number>") and checks that its input renders, so an item the
// templates cannot handle is reported in GitHubReady instead of being
// published and rejected on every poll.
func (r *TriggerReconciler) githubDedupeKey(trg *aiv1alpha1.Trigger, ev TriggerEvent, item githubItem) (string, error) {
	data := triggerTemplateData{Event: ev, Trigger: triggerRef{Name: trg.Name, Namespace: trg.Namespace}}
	if _, err := renderTriggerTemplate("input", trg.Spec.Input, data); err != nil {
		return "", fmt.Errorf("input template: %w", err)
	}
	key := item.key()
	if trg.Spec.DedupeKey != "" {
		rendered, err := renderTriggerTemplate("dedupeKey", trg.Spec.DedupeKey, data)
		if err != nil {
			return "", fmt.Errorf("dedupeKey template: %w", err)
		}
		if rendered = strings.TrimSpace(rendered); rendered != "" {
			key = rendered
		}
	}
	return key, nil
}

// githubHeldBack reports whether a new item would be skipped by the
// concurrency policy anyway, given active runs and items already published
// in this poll. fire enforces the policy; this only keeps the poller from
// publishing (and status from recording) a skip per held item per poll.
// Replace starts at most one new item per poll.
func githubHeldBack(trg *aiv1alpha1.Trigger, active, published int) bool {
	switch trg.Spec.Concurrency.Policy {
	case aiv1alpha1.TriggerConcurrencyForbid:
		return active+published > 0
	case aiv1alpha1.TriggerConcurrencyReplace:
		return published > 0
	default:
		n := int(trg.Spec.Concurrency.MaxActive)
		return n > 0 && active+published >= n
	}
}

// publishGitHubItem publishes one item to the Trigger's subject on the events
// stream.
func publishGitHubItem(ctx context.Context, nc *nats.Conn, subject string, item githubItem) error {
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(item)
	if err != nil {
		return err
	}
	msg := nats.NewMsg(subject)
	msg.Data = payload
	msg.Header.Set("Content-Type", "application/json")
	msg.Header.Set(natspkg.EventKeyHeader, item.key())
	if _, err := js.PublishMsg(ctx, msg); err != nil {
		return fmt.Errorf("publish %s to %s: %w", item.key(), subject, err)
	}
	return nil
}

// setGitHubCondition writes the GitHubReady condition (and lastPolledAt
// after a successful poll).
func (r *TriggerReconciler) setGitHubCondition(ctx context.Context, nn types.NamespacedName, cond metav1.Condition, polled bool, now time.Time) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		trg := &aiv1alpha1.Trigger{}
		if err := r.reader().Get(ctx, nn, trg); err != nil {
			return err
		}
		if trg.Spec.Source.GitHub == nil || trg.Spec.Suspend {
			return nil
		}
		cond.ObservedGeneration = trg.Generation
		meta.SetStatusCondition(&trg.Status.Conditions, cond)
		if polled {
			at := metav1.NewTime(now)
			trg.Status.LastPolledAt = &at
		}
		return r.Status().Update(ctx, trg)
	})
}

// githubAfterRun applies a pending claim once the item's run exists (created
// now, or earlier by a firing whose claim failed). It returns the skip reason
// to record, or "" when the claim succeeded or was not needed.
func (r *TriggerReconciler) githubAfterRun(ctx context.Context, trg *aiv1alpha1.Trigger, ev TriggerEvent, run string) string {
	item := pendingGitHubClaim(trg, ev)
	if item == nil {
		return ""
	}
	c := &aiv1alpha1.Chain{}
	err := r.reader().Get(ctx, types.NamespacedName{Namespace: trg.Namespace, Name: run}, c)
	if err == nil {
		err = r.claimGitHub(ctx, trg, ev, *item, c)
	}
	if err != nil {
		r.event(trg, corev1.EventTypeWarning, "ClaimFailed", "Run %s: claim of %s failed, retrying on the next poll: %v", run, item.key(), err)
		return fmt.Sprintf("claim of %s failed, retried on the next poll: %v", item.key(), err)
	}
	r.event(trg, corev1.EventTypeNormal, "Claimed", "Claimed %s for run %s", item.key(), run)
	return ""
}

// triggerPollers runs one GitHub poll loop per github-source Trigger. Loops
// are started from Reconcile, so they only run in the elected leader.
type triggerPollers struct {
	log   logr.Logger
	mu    sync.Mutex
	loops map[string]*triggerPoller
}

type triggerPoller struct {
	generation int64
	cancel     context.CancelFunc
	done       chan struct{}
}

func newTriggerPollers(log logr.Logger) *triggerPollers {
	return &triggerPollers{log: log, loops: map[string]*triggerPoller{}}
}

// ensure starts key's poll loop unless one runs for generation. A new
// generation restarts it, so a spec change polls at once.
func (t *triggerPollers) ensure(key string, generation int64, poll func(ctx context.Context) time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if l, ok := t.loops[key]; ok {
		select {
		case <-l.done:
		default:
			if l.generation == generation {
				return
			}
		}
		l.stop()
		delete(t.loops, key)
	}
	ctx, cancel := context.WithCancel(context.Background())
	l := &triggerPoller{generation: generation, cancel: cancel, done: make(chan struct{})}
	t.loops[key] = l
	log := t.log.WithValues("trigger", key)
	go func() {
		defer close(l.done)
		log.Info("GitHub poller started")
		for {
			wait := poll(ctx)
			select {
			case <-ctx.Done():
				log.Info("GitHub poller stopped")
				return
			case <-time.After(wait):
			}
		}
	}()
}

// running reports whether key has a poll loop.
func (t *triggerPollers) running(key string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.loops[key]
	return ok
}

// stop ends key's poll loop and waits for it.
func (t *triggerPollers) stop(key string) {
	t.mu.Lock()
	l, ok := t.loops[key]
	delete(t.loops, key)
	t.mu.Unlock()
	if ok {
		l.stop()
	}
}

// stopAll ends every poll loop.
func (t *triggerPollers) stopAll() {
	t.mu.Lock()
	loops := t.loops
	t.loops = map[string]*triggerPoller{}
	t.mu.Unlock()
	for _, l := range loops {
		l.stop()
	}
}

func (l *triggerPoller) stop() {
	l.cancel()
	<-l.done
}
