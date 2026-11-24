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
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/github"
)

const (
	defaultPickupInterval  = 300 * time.Second
	minPickupInterval      = 60 * time.Second
	defaultReadyLabel      = "rt:ready"
	defaultInProgressLabel = "rt:in-progress"
	defaultNeedsHumanLabel = "needs-human"
	defaultBreakingLabel   = "breaking"
	pickupLastErrorMax     = 1024
)

// IssuePickupReconciler polls GitHub for issues labelled ready in a
// RoundTable's spec.issuePickup.repos and starts one copy of the configured
// Chain (the PR pipeline) per issue. It only acts on RoundTables that set
// spec.issuePickup.
//
// Claiming is idempotent: the Chain name is derived from the repo and issue
// number, so a poll that fails half-way and is retried finds the existing
// Chain instead of starting a second one, and AnnotationPickupClaimed records
// that the GitHub side (labels, comment) was done for the current run.
type IssuePickupReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	// GitHubAPIURL is the GitHub REST endpoint; "" means api.github.com.
	GitHubAPIURL string

	// Now returns the current time; nil means time.Now (tests override it).
	Now func() time.Time

	// ensuredLabels remembers which repo + label set combinations already
	// have their labels, so polls do not call GitHub for them again. A
	// changed label name in the spec is a new key and is ensured once.
	ensuredLabels sync.Map
}

// +kubebuilder:rbac:groups=ai.roundtable.io,resources=roundtables,verbs=get;list;watch
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=roundtables/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=chains,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

// Reconcile polls GitHub at most once per spec.issuePickup.intervalSeconds.
func (r *IssuePickupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	rt := &aiv1alpha1.RoundTable{}
	if err := r.Get(ctx, req.NamespacedName, rt); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	spec := rt.Spec.IssuePickup
	if spec == nil || spec.Suspend || rt.Spec.Suspend || !rt.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	interval := pickupInterval(spec)
	now := r.now()
	if st := rt.Status.IssuePickup; st != nil && st.LastPollTime != nil {
		if next := st.LastPollTime.Add(interval); now.Before(next) {
			return ctrl.Result{RequeueAfter: next.Sub(now)}, nil
		}
	}

	res := r.poll(ctx, rt, spec, now)

	orig := rt.DeepCopy()
	st := rt.Status.IssuePickup
	if st == nil {
		st = &aiv1alpha1.IssuePickupStatus{}
		rt.Status.IssuePickup = st
	}
	pollTime := metav1.NewTime(now)
	st.LastPollTime = &pollTime
	st.Active = res.active
	st.PickedTotal += res.picked
	st.SkippedTotal += res.skipped
	st.LastError = truncate(strings.Join(res.errs, "; "), pickupLastErrorMax)
	if err := r.Status().Patch(ctx, rt, client.MergeFrom(orig)); err != nil {
		return ctrl.Result{}, err
	}
	if len(res.errs) > 0 {
		r.Recorder.Eventf(rt, corev1.EventTypeWarning, "IssuePickupFailed", "%s", st.LastError)
	}
	// Errors are retried on the next poll, never sooner: a hot retry loop
	// would only burn GitHub rate limit.
	return ctrl.Result{RequeueAfter: interval}, nil
}

type pickupResult struct {
	active          int32
	picked, skipped int64
	errs            []string
}

func (p *pickupResult) fail(format string, args ...any) {
	p.errs = append(p.errs, fmt.Sprintf(format, args...))
}

// poll does one pickup pass: garbage-collects finished Chains, counts the
// active ones, and claims ready issues while there is capacity.
func (r *IssuePickupReconciler) poll(ctx context.Context, rt *aiv1alpha1.RoundTable, spec *aiv1alpha1.IssuePickupSpec, now time.Time) pickupResult {
	log := logf.FromContext(ctx)
	res := pickupResult{}

	chains := &aiv1alpha1.ChainList{}
	if err := r.List(ctx, chains, client.InNamespace(rt.Namespace),
		client.MatchingLabels{aiv1alpha1.LabelPickupRoundTable: rt.Name}); err != nil {
		res.fail("list pickup chains: %v", err)
		return res
	}
	byName := make(map[string]*aiv1alpha1.Chain, len(chains.Items))
	activeByRepo := map[string]int32{}
	for i := range chains.Items {
		c := &chains.Items[i]
		if pickupChainActive(c) {
			res.active++
			activeByRepo[c.Annotations[aiv1alpha1.AnnotationPickupRepo]]++
		} else if pickupChainExpired(c, spec, now) && !pickupNeedsVerify(c) && !pickupNeedsHandBack(c) {
			if err := r.Delete(ctx, c); client.IgnoreNotFound(err) != nil {
				res.fail("delete finished chain %s: %v", c.Name, err)
			} else {
				log.Info("Deleted finished pickup chain", "chain", c.Name)
			}
			continue
		}
		byName[c.Name] = c
	}

	tmpl := &aiv1alpha1.Chain{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: rt.Namespace, Name: spec.ChainRef.Name}, tmpl); err != nil {
		res.fail("get chain template %q: %v", spec.ChainRef.Name, err)
		return res
	}
	patterns, err := compileHumanPatterns(spec.HumanPatterns)
	if err != nil {
		res.fail("%v", err)
		return res
	}
	token, err := r.githubToken(ctx, rt)
	if err != nil {
		res.fail("%v", err)
		return res
	}
	gh := github.NewClient(r.GitHubAPIURL, token)

	labels := pickupLabels(spec)
	for _, c := range byName {
		switch {
		case pickupNeedsVerify(c):
			if err := r.verifyMerge(ctx, gh, rt, c, labels); err != nil {
				res.fail("verify merge for chain %s: %v", c.Name, err)
			}
		case pickupNeedsHandBack(c):
			if err := r.handBackFailed(ctx, gh, rt, c, labels); err != nil {
				res.fail("hand back failed chain %s: %v", c.Name, err)
			}
		}
	}
	maxActive, maxPerRepo := max(spec.MaxActive, 1), max(spec.MaxActivePerRepo, 1)
	for _, repo := range spec.Repos {
		r.ensureLabels(ctx, gh, repo, labels, &res)
		if res.active >= maxActive {
			break
		}
		issues, err := gh.ListOpenIssues(ctx, repo, labels.ready)
		if err != nil {
			res.fail("%s: list issues: %v", repo, err)
			continue
		}
		// Breaking-change protocol (#260): no new work in a repo while a
		// breaking PR is open. If the lookup fails, hold too: the safe side.
		var held string
		if len(issues) > 0 {
			var err error
			if held, err = breakingHold(ctx, gh, repo, breakingLabel(spec)); err != nil {
				res.fail("%s: list %s PRs: %v", repo, breakingLabel(spec), err)
				held = "could not list breaking PRs"
			}
		}
		heldCount := 0
		for i := range issues {
			issue := &issues[i]
			name := pickupChainName(tmpl.Name, repo, issue.Number)
			existing := byName[name]
			if existing != nil && existing.Annotations[aiv1alpha1.AnnotationPickupRepo] != repo {
				res.fail("%s#%d: chain %s already belongs to %s", repo, issue.Number, name,
					existing.Annotations[aiv1alpha1.AnnotationPickupRepo])
				continue
			}

			// Already running for this issue: finish a claim a previous
			// poll left half-done, or drop a ready label re-added mid-run.
			if existing != nil && pickupChainActive(existing) {
				trigger := existing.Annotations[aiv1alpha1.AnnotationChainRun]
				if existing.Annotations[aiv1alpha1.AnnotationPickupClaimed] != trigger {
					if err := r.claim(ctx, gh, rt, existing, repo, issue, labels, spec.Assignees, trigger); err != nil {
						res.fail("%s#%d: claim: %v", repo, issue.Number, err)
					}
				} else if err := gh.RemoveLabel(ctx, repo, issue.Number, labels.ready); err != nil {
					res.fail("%s#%d: remove %s: %v", repo, issue.Number, labels.ready, err)
				}
				continue
			}

			if reason := needsHuman(issue, labels.needsHuman, patterns); reason != "" {
				if err := handBack(ctx, gh, repo, issue, labels, reason); err != nil {
					res.fail("%s#%d: hand back: %v", repo, issue.Number, err)
					continue
				}
				res.skipped++
				r.Recorder.Eventf(rt, corev1.EventTypeNormal, "IssueNeedsHuman", "%s#%d skipped: %s", repo, issue.Number, reason)
				continue
			}

			if res.active >= maxActive || activeByRepo[repo] >= maxPerRepo {
				continue // capacity; it stays ready for a later poll
			}
			if held != "" {
				heldCount++
				continue // it stays ready until the breaking PR is merged or closed
			}

			trigger := strconv.FormatInt(now.Unix(), 10)
			chain, err := r.startChain(ctx, rt, tmpl, existing, name, repo, issue.Number, trigger)
			if err != nil {
				res.fail("%s#%d: start chain %s: %v", repo, issue.Number, name, err)
				continue
			}
			res.active++
			activeByRepo[repo]++
			byName[name] = chain
			if err := r.claim(ctx, gh, rt, chain, repo, issue, labels, spec.Assignees, trigger); err != nil {
				res.fail("%s#%d: claim: %v", repo, issue.Number, err)
				continue
			}
			res.picked++
			log.Info("Picked up issue", "repo", repo, "issue", issue.Number, "chain", name)
			r.Recorder.Eventf(rt, corev1.EventTypeNormal, "IssuePickedUp", "%s#%d started chain %s", repo, issue.Number, name)
		}
		if heldCount > 0 {
			log.Info("Held ready issues behind a breaking PR", "repo", repo, "issues", heldCount, "breaking", held)
			r.Recorder.Eventf(rt, corev1.EventTypeNormal, "IssuePickupHeld", "%s: %d ready issue(s) held, open %s PR(s): %s",
				repo, heldCount, breakingLabel(spec), held)
		}
	}
	return res
}

// pickupLabelDefs are the color and description new pickup labels get.
func pickupLabelDefs(l pickupLabelSet) []struct{ name, color, description string } {
	return []struct{ name, color, description string }{
		{l.ready, "0e8a16", "Round Table: ready to be picked up"},
		{l.inProgress, "fbca04", "Round Table: a chain is working on this"},
		{l.needsHuman, "d93f0b", "Round Table: needs a human"},
	}
}

// ensureLabels creates the pickup labels the repo lacks. Existing labels are
// left alone. Success is cached per repo + label set, so it only calls GitHub
// the first time a repo or label name is seen (and again after a restart);
// failures are reported and retried on the next poll.
func (r *IssuePickupReconciler) ensureLabels(ctx context.Context, gh *github.Client, repo string, labels pickupLabelSet, res *pickupResult) {
	key := repo + "\x00" + labels.ready + "\x00" + labels.inProgress + "\x00" + labels.needsHuman
	if _, ok := r.ensuredLabels.Load(key); ok {
		return
	}
	for _, d := range pickupLabelDefs(labels) {
		created, err := gh.EnsureLabel(ctx, repo, d.name, d.color, d.description)
		if err != nil {
			res.fail("%s: ensure label %s: %v", repo, d.name, err)
			return
		}
		if created {
			logf.FromContext(ctx).Info("Created label", "repo", repo, "label", d.name)
		}
	}
	r.ensuredLabels.Store(key, struct{}{})
}

// startChain creates the issue's Chain from the template, or re-runs a
// finished one (the ready label was put back) with the template's current
// spec.
func (r *IssuePickupReconciler) startChain(ctx context.Context, rt *aiv1alpha1.RoundTable, tmpl, existing *aiv1alpha1.Chain,
	name, repo string, issue int, trigger string) (*aiv1alpha1.Chain, error) {
	input, err := json.Marshal(struct {
		Repo  string `json:"repo"`
		Issue int    `json:"issue"`
	}{repo, issue})
	if err != nil {
		return nil, err
	}
	spec := tmpl.Spec.DeepCopy()
	spec.Input = string(input)
	spec.Schedule = ""
	spec.StartingDeadlineSeconds = nil
	spec.Suspend = false

	if existing != nil {
		orig := existing.DeepCopy()
		existing.Spec = *spec
		if existing.Annotations == nil {
			existing.Annotations = map[string]string{}
		}
		existing.Annotations[aiv1alpha1.AnnotationChainRun] = trigger
		if err := r.Patch(ctx, existing, client.MergeFrom(orig)); err != nil {
			return nil, err
		}
		return existing, nil
	}

	chain := &aiv1alpha1.Chain{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: rt.Namespace,
			Labels:    map[string]string{aiv1alpha1.LabelPickupRoundTable: rt.Name},
			Annotations: map[string]string{
				aiv1alpha1.AnnotationPickupRepo:  repo,
				aiv1alpha1.AnnotationPickupIssue: strconv.Itoa(issue),
				aiv1alpha1.AnnotationChainRun:    trigger,
			},
		},
		Spec: *spec,
	}
	if err := controllerutil.SetControllerReference(rt, chain, r.Scheme); err != nil {
		return nil, err
	}
	if err := r.Create(ctx, chain); err != nil {
		return nil, err
	}
	return chain, nil
}

// claim marks the issue as taken on GitHub (in-progress label, assignees,
// ready label removed, comment) and records it on the Chain. The comment is
// last so a retried claim never posts it twice.
func (r *IssuePickupReconciler) claim(ctx context.Context, gh *github.Client, rt *aiv1alpha1.RoundTable, chain *aiv1alpha1.Chain,
	repo string, issue *github.Issue, labels pickupLabelSet, assignees []string, trigger string) error {
	if err := gh.AddLabels(ctx, repo, issue.Number, labels.inProgress); err != nil {
		return err
	}
	if len(assignees) > 0 {
		if err := gh.AddAssignees(ctx, repo, issue.Number, assignees...); err != nil {
			return err
		}
	}
	if err := gh.RemoveLabel(ctx, repo, issue.Number, labels.ready); err != nil {
		return err
	}
	body := fmt.Sprintf("Picked up by the Round Table (RoundTable `%s/%s`): started Chain `%s` (run `%s`), "+
		"which plans, implements, reviews and merges a PR for this issue or labels it `%s`.\n\n"+
		"Follow it with `kubectl -n %s get chain %s`. Add `%s` again after it finishes to re-run it.",
		rt.Namespace, rt.Name, chain.Name, trigger, labels.needsHuman, rt.Namespace, chain.Name, labels.ready)
	if err := gh.Comment(ctx, repo, issue.Number, body); err != nil {
		return err
	}
	orig := chain.DeepCopy()
	if chain.Annotations == nil {
		chain.Annotations = map[string]string{}
	}
	chain.Annotations[aiv1alpha1.AnnotationPickupClaimed] = trigger
	return r.Patch(ctx, chain, client.MergeFrom(orig))
}

// handBack gives an issue that needs a human back: needs-human label,
// comment, ready label removed (last, so a failed comment is retried).
func handBack(ctx context.Context, gh *github.Client, repo string, issue *github.Issue, labels pickupLabelSet, reason string) error {
	if !issue.HasLabel(labels.needsHuman) {
		if err := gh.AddLabels(ctx, repo, issue.Number, labels.needsHuman); err != nil {
			return err
		}
	}
	body := fmt.Sprintf("Not picked up by the Round Table: this issue needs a human (%s). "+
		"Removed `%s` and labelled `%s`. A knight's bot token cannot push workflow changes, "+
		"and secrets/auth changes need a human; dispatch it by hand if it is safe.",
		reason, labels.ready, labels.needsHuman)
	if err := gh.Comment(ctx, repo, issue.Number, body); err != nil {
		return err
	}
	return gh.RemoveLabel(ctx, repo, issue.Number, labels.ready)
}

// githubToken reads the RoundTable's GitHub token Secret on every poll, so a
// rotated installation token is picked up without a restart.
func (r *IssuePickupReconciler) githubToken(ctx context.Context, rt *aiv1alpha1.RoundTable) (string, error) {
	if rt.Spec.GitHub == nil || rt.Spec.GitHub.TokenSecretRef == nil || rt.Spec.GitHub.TokenSecretRef.Name == "" {
		return "", errors.New("issuePickup needs spec.github.tokenSecretRef")
	}
	ref := rt.Spec.GitHub.TokenSecretRef
	key := ref.Key
	if key == "" {
		key = "token"
	}
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: rt.Namespace, Name: ref.Name}, secret); err != nil {
		return "", fmt.Errorf("read GitHub token secret %q: %w", ref.Name, err)
	}
	token := strings.TrimSpace(string(secret.Data[key]))
	if token == "" {
		return "", fmt.Errorf("GitHub token secret %q has no key %q", ref.Name, key)
	}
	return token, nil
}

func (r *IssuePickupReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

type pickupLabelSet struct{ ready, inProgress, needsHuman string }

func pickupLabels(spec *aiv1alpha1.IssuePickupSpec) pickupLabelSet {
	l := pickupLabelSet{spec.ReadyLabel, spec.InProgressLabel, spec.NeedsHumanLabel}
	if l.ready == "" {
		l.ready = defaultReadyLabel
	}
	if l.inProgress == "" {
		l.inProgress = defaultInProgressLabel
	}
	if l.needsHuman == "" {
		l.needsHuman = defaultNeedsHumanLabel
	}
	return l
}

func breakingLabel(spec *aiv1alpha1.IssuePickupSpec) string {
	if spec.BreakingLabel == "" {
		return defaultBreakingLabel
	}
	return spec.BreakingLabel
}

// breakingHold returns a description of the open breaking PRs in repo
// ("#241 (rt/241-x)"), or "" when there are none.
func breakingHold(ctx context.Context, gh *github.Client, repo, label string) (string, error) {
	prs, err := gh.ListOpenPullRequests(ctx, repo, label)
	if err != nil {
		return "", err
	}
	parts := make([]string, 0, len(prs))
	for _, p := range prs {
		parts = append(parts, fmt.Sprintf("#%d (%s)", p.Number, p.Head.Ref))
	}
	return strings.Join(parts, ", "), nil
}

func pickupInterval(spec *aiv1alpha1.IssuePickupSpec) time.Duration {
	if spec.IntervalSeconds <= 0 {
		return defaultPickupInterval
	}
	return max(time.Duration(spec.IntervalSeconds)*time.Second, minPickupInterval)
}

// pickupChainActive reports whether a Chain is running or has a run pending.
func pickupChainActive(c *aiv1alpha1.Chain) bool {
	return c.Status.Phase == aiv1alpha1.ChainPhaseRunning ||
		c.Annotations[aiv1alpha1.AnnotationChainRun] != c.Status.LastRunTrigger
}

// pickupChainExpired reports whether a finished Chain is past
// ttlSecondsAfterFinished.
func pickupChainExpired(c *aiv1alpha1.Chain, spec *aiv1alpha1.IssuePickupSpec, now time.Time) bool {
	if spec.TTLSecondsAfterFinished == nil || *spec.TTLSecondsAfterFinished <= 0 || c.Status.CompletedAt == nil {
		return false
	}
	return now.Sub(c.Status.CompletedAt.Time) >= time.Duration(*spec.TTLSecondsAfterFinished)*time.Second
}

var nonDNSChars = regexp.MustCompile(`[^a-z0-9-]+`)

// pickupChainName is "<template>-<repo name>-<issue>", e.g.
// pr-pipeline-roundtable-224. The owner is left out to keep names short; two
// configured repos with the same name in different orgs are reported as a
// collision via AnnotationPickupRepo.
func pickupChainName(template, repo string, issue int) string {
	_, name, _ := strings.Cut(repo, "/")
	name = strings.Trim(nonDNSChars.ReplaceAllString(strings.ToLower(name), "-"), "-")
	suffix := "-" + strconv.Itoa(issue)
	prefix := template + "-" + name
	if limit := 253 - len(suffix); len(prefix) > limit {
		prefix = strings.TrimRight(prefix[:limit], "-")
	}
	return prefix + suffix
}

func compileHumanPatterns(patterns []string) ([]*regexp.Regexp, error) {
	out := make([]*regexp.Regexp, 0, len(patterns))
	for _, p := range patterns {
		re, err := regexp.Compile(p)
		if err != nil {
			return nil, fmt.Errorf("humanPatterns: %w", err)
		}
		out = append(out, re)
	}
	return out, nil
}

// needsHuman returns why an issue must not be picked up, or "".
func needsHuman(issue *github.Issue, needsHumanLabel string, patterns []*regexp.Regexp) string {
	if issue.HasLabel(needsHumanLabel) {
		return "labelled `" + needsHumanLabel + "`"
	}
	text := issue.Title + "\n" + issue.Body
	for _, re := range patterns {
		if m := re.FindString(text); m != "" {
			return fmt.Sprintf("mentions %q, matching humanPatterns `%s`", m, re.String())
		}
	}
	return ""
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}

// SetupWithManager registers the controller. Only spec changes trigger a
// reconcile; polling is driven by RequeueAfter.
func (r *IssuePickupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aiv1alpha1.RoundTable{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Named("issuepickup").
		Complete(r)
}
