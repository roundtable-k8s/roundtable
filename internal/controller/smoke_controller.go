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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	knightpkg "github.com/roundtable-k8s/roundtable/internal/knight"
)

// Post-deploy smoke (#228): the operator fingerprints what is deployed (its
// own image and every knight's image). When a new fingerprint has settled it
// runs the smoke Chain; a pass is recorded as known-good (RoundTable status
// and the ConfigMap <roundtable>-smoke), a failure opens an issue with the
// step outputs and holds image automation by labelling its pull request.
// See docs/post-deploy-smoke.md.

const (
	smokeDefaultChain    = "rollout-smoke"
	smokeDefaultInterval = 60
	smokeDefaultLabel    = "needs-human"
	smokeDefaultBranch   = "autodeploy/roundtable"
	smokeDefaultHold     = "hold"
	// smokeStartTimeout is how long a dispatched run may stay unpicked-up by
	// the Chain controller before the watcher forgets it and tries again.
	smokeStartTimeout = 5 * time.Minute
	// smokeOutputLimit caps each step output in a failure issue.
	smokeOutputLimit = 1500
)

// SmokeReconciler runs the post-deploy smoke watcher of every RoundTable that
// enables spec.smoke.
type SmokeReconciler struct {
	client.Client
	Recorder record.EventRecorder
	// GitHubBaseURL overrides the GitHub API URL (tests, GitHub Enterprise).
	GitHubBaseURL string
	// OperatorImage is the operator's own image (OPERATOR_IMAGE), part of
	// the version fingerprint. Empty: only knight images count.
	OperatorImage string
	// DefaultImage is the knight image of knights without spec.image.
	DefaultImage string
	// Now returns the current time (tests).
	Now func() time.Time
}

// +kubebuilder:rbac:groups=ai.roundtable.io,resources=roundtables,verbs=get;list;watch
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=roundtables/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=chains,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=ai.roundtable.io,resources=knights,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

// Reconcile checks the deployed versions of one RoundTable when due.
func (r *SmokeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	rt := &aiv1alpha1.RoundTable{}
	if err := r.Get(ctx, req.NamespacedName, rt); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	cfg := rt.Spec.Smoke
	if cfg == nil || !cfg.Enabled || rt.Spec.Ephemeral {
		return ctrl.Result{}, nil
	}
	interval := time.Duration(smokeInterval(cfg)) * time.Second
	now := r.now()
	if st := rt.Status.Smoke; st != nil && st.LastPollTime != nil {
		if wait := interval - now.Sub(st.LastPollTime.Time); wait > time.Second {
			return ctrl.Result{RequeueAfter: wait}, nil
		}
	}

	orig := rt.DeepCopy()
	st := rt.Status.Smoke
	if st == nil {
		st = &aiv1alpha1.SmokeStatus{}
		rt.Status.Smoke = st
	}
	// save persists the status mid-poll (a copy, so rt keeps our edits): the
	// run is recorded as active before its Chain run is triggered.
	save := func(ctx context.Context) error {
		return r.Status().Patch(ctx, rt.DeepCopy(), client.MergeFrom(orig))
	}
	pollErr := r.poll(ctx, rt, cfg, st, save)
	t := metav1.NewTime(now)
	st.LastPollTime = &t
	st.LastError = ""
	if pollErr != nil {
		st.LastError = truncateUTF8(pollErr.Error(), 1024)
		log.Info("smoke poll had errors", "error", pollErr.Error())
	}
	if err := r.Status().Patch(ctx, rt, client.MergeFrom(orig)); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: interval}, nil
}

func (r *SmokeReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func smokeInterval(cfg *aiv1alpha1.SmokeSpec) int32 {
	if cfg.PollIntervalSeconds > 0 {
		return cfg.PollIntervalSeconds
	}
	return smokeDefaultInterval
}

func smokeChainName(cfg *aiv1alpha1.SmokeSpec) string {
	if cfg.ChainRef != nil && cfg.ChainRef.Name != "" {
		return cfg.ChainRef.Name
	}
	return smokeDefaultChain
}

// smokeVersion fingerprints a deployment: the operator image plus every
// persistent knight's effective image. components lists them as name=image,
// sorted; version is a short hash over them.
func smokeVersion(operatorImage, defaultImage string, knights []aiv1alpha1.Knight) (version string, components []string) {
	if operatorImage != "" {
		components = append(components, "operator="+operatorImage)
	}
	for i := range knights {
		k := &knights[i]
		if _, ok := k.Labels[aiv1alpha1.LabelEphemeral]; ok {
			continue
		}
		if _, ok := k.Labels[aiv1alpha1.LabelWarmPool]; ok {
			continue
		}
		components = append(components, "knight/"+k.Name+"="+knightpkg.EffectiveImage(k, defaultImage))
	}
	sort.Strings(components)
	sum := sha256.Sum256([]byte(strings.Join(components, "\n")))
	return hex.EncodeToString(sum[:])[:12], components
}

// knightsSettled reports whether every non-suspended persistent knight is
// Ready (a rollout in progress leaves some Provisioning or Degraded).
func knightsSettled(knights []aiv1alpha1.Knight) (bool, string) {
	for i := range knights {
		k := &knights[i]
		if _, ok := k.Labels[aiv1alpha1.LabelEphemeral]; ok {
			continue
		}
		if _, ok := k.Labels[aiv1alpha1.LabelWarmPool]; ok {
			continue
		}
		if k.Status.Phase == aiv1alpha1.KnightPhaseSuspended {
			continue
		}
		if k.Status.Phase != aiv1alpha1.KnightPhaseReady {
			return false, fmt.Sprintf("knight %s is %s", k.Name, orDefault(string(k.Status.Phase), "Pending"))
		}
	}
	return true, ""
}

func (r *SmokeReconciler) poll(ctx context.Context, rt *aiv1alpha1.RoundTable, cfg *aiv1alpha1.SmokeSpec, st *aiv1alpha1.SmokeStatus, save func(context.Context) error) error {
	chain := &aiv1alpha1.Chain{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: rt.Namespace, Name: smokeChainName(cfg)}, chain); err != nil {
		return fmt.Errorf("smoke chain %s: %w", smokeChainName(cfg), err)
	}
	knights := &aiv1alpha1.KnightList{}
	if err := r.List(ctx, knights, client.InNamespace(rt.Namespace)); err != nil {
		return fmt.Errorf("list knights: %w", err)
	}
	version, components := smokeVersion(r.OperatorImage, r.DefaultImage, knights.Items)
	settled, why := knightsSettled(knights.Items)
	// A version is tested once two polls saw it and all knights are Ready.
	stable := st.ObservedVersion == version
	st.ObservedVersion = version
	st.Components = components

	var errs []error
	if st.ActiveRun != "" {
		done, err := r.finishActiveRun(ctx, rt, cfg, st, chain)
		if err != nil {
			errs = append(errs, err)
		}
		if !done {
			return errors.Join(errs...)
		}
	}
	if st.Held && st.HoldPullRequest == "" && cfg.Hold != nil {
		if err := r.applyHold(ctx, rt, cfg, st); err != nil {
			errs = append(errs, err)
		}
	}

	switch {
	case st.ActiveRun != "":
	case version == st.LastTestedVersion || version == st.KnownGoodVersion:
	case !settled:
		st.Message = fmt.Sprintf("version %s waiting for the rollout: %s", version, why)
	case !stable:
		st.Message = fmt.Sprintf("version %s seen, waiting for one more poll to see it unchanged", version)
	case chain.Status.Phase == aiv1alpha1.ChainPhaseRunning || chain.Spec.Suspend:
		st.Message = fmt.Sprintf("version %s waiting for the smoke chain to be free", version)
	default:
		if err := r.dispatch(ctx, rt, st, chain, version, components, save); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// dispatch starts a smoke run for version. The run is recorded before it is
// triggered (as the PR feedback watcher does), so a crash in between at worst
// finds the trigger on the Chain or forgets it after smokeStartTimeout.
func (r *SmokeReconciler) dispatch(ctx context.Context, rt *aiv1alpha1.RoundTable, st *aiv1alpha1.SmokeStatus, chain *aiv1alpha1.Chain, version string, components []string, save func(context.Context) error) error {
	input, err := json.Marshal(map[string]any{
		"version":    version,
		"components": components,
		"knownGood":  st.KnownGoodVersion,
	})
	if err != nil {
		return err
	}
	trigger := fmt.Sprintf("smoke-%d", r.now().Unix())
	st.ActiveRun = version + "@" + trigger
	st.Message = fmt.Sprintf("smoke of version %s started on %s", version, chain.Name)
	if err := save(ctx); err != nil {
		st.ActiveRun = ""
		return fmt.Errorf("record smoke run: %w", err)
	}
	patch := client.MergeFrom(chain.DeepCopy())
	chain.Spec.Input = string(input)
	if chain.Annotations == nil {
		chain.Annotations = map[string]string{}
	}
	chain.Annotations[aiv1alpha1.AnnotationChainRun] = trigger
	if err := r.Patch(ctx, chain, patch); err != nil {
		cur := &aiv1alpha1.Chain{}
		if gerr := r.Get(ctx, client.ObjectKeyFromObject(chain), cur); gerr == nil && cur.Annotations[aiv1alpha1.AnnotationChainRun] != trigger {
			st.ActiveRun = ""
		}
		return fmt.Errorf("start smoke chain %s: %w", chain.Name, err)
	}
	r.Recorder.Eventf(rt, corev1.EventTypeNormal, "SmokeStarted", "%s", st.Message)
	return nil
}

// triggerTime parses the unix time out of a smoke-<unix> trigger.
func triggerTime(trigger string) (time.Time, bool) {
	var sec int64
	if _, err := fmt.Sscanf(trigger, "smoke-%d", &sec); err != nil {
		return time.Time{}, false
	}
	return time.Unix(sec, 0), true
}

func isSmokeChainDone(p aiv1alpha1.ChainPhase) bool {
	return p == aiv1alpha1.ChainPhaseSucceeded || p == aiv1alpha1.ChainPhasePartiallySucceeded || p == aiv1alpha1.ChainPhaseFailed
}

// finishActiveRun records the result of the active run once its Chain run has
// finished. done is false while the run is still going or the result could
// not be recorded yet.
func (r *SmokeReconciler) finishActiveRun(ctx context.Context, rt *aiv1alpha1.RoundTable, cfg *aiv1alpha1.SmokeSpec, st *aiv1alpha1.SmokeStatus, chain *aiv1alpha1.Chain) (bool, error) {
	version, trigger, _ := strings.Cut(st.ActiveRun, "@")
	if chain.Annotations[aiv1alpha1.AnnotationChainRun] != trigger {
		// Someone else started a run, or our patch never landed.
		if at, ok := triggerTime(trigger); !ok || r.now().Sub(at) > smokeStartTimeout {
			st.ActiveRun = ""
			st.Message = fmt.Sprintf("smoke run %s was replaced; version %s will be tested again", trigger, version)
			return true, nil
		}
		return false, nil
	}
	if chain.Status.LastRunTrigger != trigger || !isSmokeChainDone(chain.Status.Phase) {
		if at, ok := triggerTime(trigger); ok && chain.Status.LastRunTrigger != trigger && r.now().Sub(at) > smokeStartTimeout {
			st.ActiveRun = ""
			st.Message = fmt.Sprintf("smoke run %s never started; version %s will be tested again", trigger, version)
			return true, nil
		}
		return false, nil
	}

	pass, failures := evaluateSmoke(chain)
	now := metav1.NewTime(r.now())
	if pass {
		st.LastResult = aiv1alpha1.SmokePassed
		st.KnownGoodVersion = version
		st.KnownGoodComponents = append([]string(nil), st.Components...)
		st.Held = false
		st.HoldPullRequest = ""
		st.IssueURL = ""
		st.Message = fmt.Sprintf("smoke of version %s passed; recorded as known-good", version)
		if err := r.writeConfigMap(ctx, rt, st, version); err != nil {
			// Stay active: the next poll records the same result again.
			return false, err
		}
		r.Recorder.Eventf(rt, corev1.EventTypeNormal, "SmokePassed", "%s", st.Message)
	} else {
		var errs []error
		if cfg.IssueRepo != "" && st.IssueURL == "" {
			url, err := r.openIssue(ctx, rt, cfg, st, version, failures)
			if err != nil {
				// Stay active: retry the issue next poll rather than lose it.
				return false, fmt.Errorf("open smoke failure issue: %w", err)
			}
			st.IssueURL = url
		}
		st.LastResult = aiv1alpha1.SmokeFailed
		st.Held = true
		st.Message = fmt.Sprintf("smoke of version %s failed (%d checks); image automation held", version, len(failures))
		if cfg.Hold != nil {
			if err := r.applyHold(ctx, rt, cfg, st); err != nil {
				errs = append(errs, err)
			}
		}
		if err := r.writeConfigMap(ctx, rt, st, version); err != nil {
			errs = append(errs, err)
		}
		r.Recorder.Eventf(rt, corev1.EventTypeWarning, "SmokeFailed", "%s", st.Message)
		if len(errs) > 0 {
			st.LastRunTime = &now
			st.LastTestedVersion = version
			st.ActiveRun = ""
			return true, errors.Join(errs...)
		}
	}
	st.LastRunTime = &now
	st.LastTestedVersion = version
	st.ActiveRun = ""
	return true, nil
}

// smokeResultLine matches the verdict line every smoke step ends with.
var smokeResultLine = regexp.MustCompile(`(?mi)^\W*result:\s*pass\W*$`)

// evaluateSmoke judges a finished smoke Chain run. A step passes when it
// succeeded and its output has a "Result: pass" line of its own (a step that
// ends "Result: fail (<reason>)" or says nothing fails). Skipped steps do not
// count. A run with no step that ran, or a Chain that did not complete, fails.
func evaluateSmoke(chain *aiv1alpha1.Chain) (pass bool, failures []smokeFailure) {
	ran := 0
	for _, s := range chain.Status.StepStatuses {
		switch s.Phase {
		case aiv1alpha1.ChainStepPhaseSkipped:
			continue
		case aiv1alpha1.ChainStepPhaseSucceeded:
			ran++
			if !smokeResultLine.MatchString(s.Output) {
				failures = append(failures, smokeFailure{Step: s.Name, Reason: "no \"Result: pass\" line", Output: s.Output})
			} else if reason := modelMismatch(s); reason != "" {
				failures = append(failures, smokeFailure{Step: s.Name, Reason: reason, Output: s.Output})
			}
		default:
			ran++
			reason := "step " + strings.ToLower(string(s.Phase))
			if s.Error != "" {
				reason = s.Error
			}
			failures = append(failures, smokeFailure{Step: s.Name, Reason: reason, Output: s.Output})
		}
	}
	if ran == 0 {
		failures = append(failures, smokeFailure{Step: "chain", Reason: "no smoke step ran (chain " + string(chain.Status.Phase) + ")"})
	}
	return len(failures) == 0, failures
}

// smokeModelLine matches the "Model: <id>" line of a checks step.
var smokeModelLine = regexp.MustCompile(`(?mi)^\W*model:\s*(\S+?)\W*$`)

// modelMismatch checks the per-task model override: when the step ran with a
// resolved model, the knight must report running exactly that model.
func modelMismatch(s aiv1alpha1.ChainStepStatus) string {
	if s.Model == "" {
		return ""
	}
	m := smokeModelLine.FindStringSubmatch(s.Output)
	if m == nil {
		return fmt.Sprintf("model override %q: the step reported no Model line", s.Model)
	}
	if !strings.EqualFold(m[1], s.Model) {
		return fmt.Sprintf("model override not honoured: asked for %q, ran %q", s.Model, m[1])
	}
	return ""
}

type smokeFailure struct {
	Step, Reason, Output string
}

// smokeSecretToken reads a GitHub token from ref, else from the table's
// spec.github.tokenSecretRef.
func (r *SmokeReconciler) smokeSecretToken(ctx context.Context, rt *aiv1alpha1.RoundTable, ref *corev1.SecretKeySelector) (string, error) {
	if ref == nil && rt.Spec.GitHub != nil {
		ref = rt.Spec.GitHub.TokenSecretRef
	}
	if ref == nil || ref.Name == "" {
		return "", errors.New("no GitHub token: set a tokenSecretRef on spec.smoke (or its hold) or spec.github.tokenSecretRef")
	}
	key := orDefault(ref.Key, "token")
	sec := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: rt.Namespace, Name: ref.Name}, sec); err != nil {
		return "", fmt.Errorf("GitHub token Secret %s: %w", ref.Name, err)
	}
	tok := strings.TrimSpace(string(sec.Data[key]))
	if tok == "" {
		return "", fmt.Errorf("GitHub token Secret %s has no key %q", ref.Name, key)
	}
	return tok, nil
}

// failureBody renders the issue text. Step outputs come from knights that may
// have run attacker-influenced content, so they are sanitized and fenced.
func failureBody(rt *aiv1alpha1.RoundTable, st *aiv1alpha1.SmokeStatus, version string, failures []smokeFailure) string {
	var b strings.Builder
	fmt.Fprintf(&b, "The post-deploy smoke chain failed for version `%s` on RoundTable `%s/%s`.\n\n", version, rt.Namespace, rt.Name)
	b.WriteString("Deployed images:\n")
	for _, c := range st.Components {
		fmt.Fprintf(&b, "- `%s`\n", sanitizeCheckName(c))
	}
	if st.KnownGoodVersion != "" {
		fmt.Fprintf(&b, "\nLast known-good version: `%s`\n", st.KnownGoodVersion)
		for _, c := range st.KnownGoodComponents {
			fmt.Fprintf(&b, "- `%s`\n", sanitizeCheckName(c))
		}
	}
	b.WriteString("\nImage automation is held until a human resolves this: fix or roll back, confirm the smoke passes, then remove the hold label.\n\nFailed checks:\n")
	for _, f := range failures {
		fmt.Fprintf(&b, "\n### `%s`\n%s\n", sanitizeCheckName(f.Step), truncateUTF8(sanitizeCheckName(f.Reason), 300))
		if f.Output != "" {
			out := strings.ReplaceAll(sanitizeLog(truncateUTF8(f.Output, smokeOutputLimit)), "```", "'''")
			fmt.Fprintf(&b, "\n```\n%s\n```\n", out)
		}
	}
	return b.String()
}

func (r *SmokeReconciler) openIssue(ctx context.Context, rt *aiv1alpha1.RoundTable, cfg *aiv1alpha1.SmokeSpec, st *aiv1alpha1.SmokeStatus, version string, failures []smokeFailure) (string, error) {
	token, err := r.smokeSecretToken(ctx, rt, cfg.TokenSecretRef)
	if err != nil {
		return "", err
	}
	gh := github.NewClient(r.GitHubBaseURL, token)
	title := fmt.Sprintf("Post-deploy smoke failed for version %s", version)
	issue, err := gh.CreateIssue(ctx, cfg.IssueRepo, title, failureBody(rt, st, version, failures), orDefault(cfg.NeedsHumanLabel, smokeDefaultLabel))
	if err != nil {
		return "", err
	}
	return issue.HTMLURL, nil
}

// applyHold labels the image automation's open pull request. When none is
// open, or the token cannot reach the repository, st.Held stays set and the
// next poll tries again; a human can also label it (docs/post-deploy-smoke.md).
func (r *SmokeReconciler) applyHold(ctx context.Context, rt *aiv1alpha1.RoundTable, cfg *aiv1alpha1.SmokeSpec, st *aiv1alpha1.SmokeStatus) error {
	h := cfg.Hold
	ref := h.TokenSecretRef
	if ref == nil {
		ref = cfg.TokenSecretRef
	}
	token, err := r.smokeSecretToken(ctx, rt, ref)
	if err != nil {
		return fmt.Errorf("hold %s: %w", h.Repo, err)
	}
	gh := github.NewClient(r.GitHubBaseURL, token)
	pulls, err := gh.ListOpenPulls(ctx, h.Repo)
	if err != nil {
		return fmt.Errorf("hold %s: list pull requests: %w", h.Repo, err)
	}
	branch, label := orDefault(h.Branch, smokeDefaultBranch), orDefault(h.Label, smokeDefaultHold)
	for i := range pulls {
		pr := &pulls[i]
		if pr.Head.Ref != branch {
			continue
		}
		if !pr.HasLabel(label) {
			if err := gh.AddLabels(ctx, h.Repo, pr.Number, label); err != nil {
				return fmt.Errorf("hold %s#%d: label %q: %w", h.Repo, pr.Number, label, err)
			}
		}
		st.HoldPullRequest = fmt.Sprintf("https://github.com/%s/pull/%d", h.Repo, pr.Number)
		r.Recorder.Eventf(rt, corev1.EventTypeWarning, "SmokeHold", "image automation held: %s labelled %q", st.HoldPullRequest, label)
		return nil
	}
	st.Message = fmt.Sprintf("%s; no open pull request from %s in %s to hold yet, will retry", st.Message, branch, h.Repo)
	return nil
}

// writeConfigMap publishes the smoke state as ConfigMap <roundtable>-smoke:
// known-good version and images, last result, and whether automation is held.
func (r *SmokeReconciler) writeConfigMap(ctx context.Context, rt *aiv1alpha1.RoundTable, st *aiv1alpha1.SmokeStatus, tested string) error {
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: rt.Namespace, Name: rt.Name + "-smoke"}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Data = map[string]string{
			"known-good-version":    st.KnownGoodVersion,
			"known-good-components": strings.Join(st.KnownGoodComponents, "\n"),
			"last-tested-version":   tested,
			"last-result":           string(st.LastResult),
			"held":                  fmt.Sprintf("%t", st.Held),
			"updated-at":            r.now().UTC().Format(time.RFC3339),
		}
		return controllerutil.SetControllerReference(rt, cm, r.Scheme())
	})
	if err != nil {
		return fmt.Errorf("write ConfigMap %s-smoke: %w", rt.Name, err)
	}
	return nil
}

// SetupWithManager registers the watcher. Spec changes start a poll; the poll
// interval drives the rest.
func (r *SmokeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&aiv1alpha1.RoundTable{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Named("smoke").
		Complete(r)
}
