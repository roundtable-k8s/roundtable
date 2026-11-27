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

package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/chainrun"
)

const (
	// maxRunInputBytes bounds a run's input.
	maxRunInputBytes = 256 << 10
	// maxRunStepOutput bounds each step's output (and the run output) in a
	// run record.
	maxRunStepOutput = 64 << 10
	// defaultRunTTL is how long a finished run copy is kept.
	defaultRunTTL = 24 * time.Hour

	// annotationRunOf names the template Chain of a gateway run copy (an
	// annotation, not a label: Chain names may exceed 63 characters).
	annotationRunOf = "ai.roundtable.io/run-of"

	// annotationRerunOf names the run a gateway run copy re-ran.
	annotationRerunOf = "ai.roundtable.io/rerun-of"

	// runPhasePending is the phase of a run the chain controller hasn't
	// started yet.
	runPhasePending = "Pending"

	// sourceAll is the list filter value that matches runs of every source.
	sourceAll = "all"

	// labelTrue is the value of an opt-in label.
	labelTrue = "true"
)

// RunOptions configures the chain-run endpoints.
type RunOptions struct {
	// TTL is how long a finished run copy is kept before the janitor
	// deletes it (default 24h).
	TTL time.Duration
	// PollInterval is how often a waiting request re-reads the run
	// (default 2s).
	PollInterval time.Duration
}

func (o *RunOptions) defaults() {
	if o.TTL <= 0 {
		o.TTL = defaultRunTTL
	}
	if o.PollInterval <= 0 {
		o.PollInterval = 2 * time.Second
	}
}

// RunChainRequest is the body of POST /v1/chains/{name}/runs. The caller
// only chooses the input: the steps are always the named Chain's.
type RunChainRequest struct {
	// Input becomes the run's spec.input: a JSON string is used as is, any
	// other JSON value as its JSON text. Omitted or null keeps the Chain's
	// own input.
	Input json.RawMessage `json:"input,omitempty"`
	// Namespace of the Chain; defaults to the gateway's namespace.
	Namespace string `json:"namespace,omitempty"`
	// WaitSeconds waits for the run to finish (capped by the gateway's
	// maximum). 0 returns once the run is created.
	WaitSeconds *int `json:"waitSeconds,omitempty"`
	// IdempotencyKey, from the Idempotency-Key header, names the run after
	// the key so a retry returns the existing run. Not part of the JSON body.
	IdempotencyKey string `json:"-"`
	// rerunOf is the run being re-run (set by RerunRun; not part of the body).
	rerunOf string
}

// RerunRunRequest is the optional body of POST /v1/runs/{namespace}/{run}/rerun.
type RerunRunRequest struct {
	// Input replaces the stored input of the run; omitted or null reuses it.
	Input json.RawMessage `json:"input,omitempty"`
	// WaitSeconds waits for the new run to finish (capped by the gateway's
	// maximum). 0 returns once the run is created.
	WaitSeconds *int `json:"waitSeconds,omitempty"`
	// IdempotencyKey, from the Idempotency-Key header. Not part of the body.
	IdempotencyKey string `json:"-"`
}

// RunRecord is what the gateway returns for a chain run.
type RunRecord struct {
	// Run is the name of the run copy (the Chain resource running it).
	Run       string `json:"run"`
	Chain     string `json:"chain"`
	Namespace string `json:"namespace"`
	RunID     string `json:"runId,omitempty"`
	Phase     string `json:"phase"`
	// Caller is who started the run through the gateway (the
	// ai.roundtable.io/caller annotation).
	Caller string `json:"caller,omitempty"`
	// Done is true once the run reached a terminal phase.
	Done        bool       `json:"done"`
	CreatedAt   time.Time  `json:"createdAt"`
	StartedAt   *time.Time `json:"startedAt,omitempty"`
	CompletedAt *time.Time `json:"completedAt,omitempty"`
	// Steps are the run's steps (get and run only, not list).
	Steps []RunStep `json:"steps,omitempty"`
	// Output is the final step's output: the one step no other step depends
	// on. Omitted when there are several such steps (read steps instead).
	Output          string `json:"output,omitempty"`
	OutputTruncated bool   `json:"outputTruncated,omitempty"`
	// RerunOf is the run this run re-ran, when it was started by a rerun.
	RerunOf string `json:"rerunOf,omitempty"`
	// Replayed marks a record returned for a repeated Idempotency-Key. Not
	// serialized.
	Replayed bool `json:"-"`
	// TraceID is the trace of the run (the Chain's status.traceID); omitted
	// when the run has none or the value is not a valid trace ID.
	TraceID string `json:"traceId,omitempty"`
	// Source is what started the run: gateway, trigger or pickup.
	Source string `json:"source,omitempty"`
	// AgeSeconds is the time since the run was created.
	AgeSeconds *int64 `json:"ageSeconds,omitempty"`
	// RunningSeconds is the time since the run started, while it is Running.
	RunningSeconds *int64 `json:"runningSeconds,omitempty"`
	// Issue is the issue a pickup run belongs to.
	Issue *RunIssue `json:"issue,omitempty"`
}

// RunIssue is the issue an issue-pickup run works on.
type RunIssue struct {
	Repo   string `json:"repo"`
	Number int    `json:"number"`
}

// validTraceID returns id when it is a W3C trace ID (32 lowercase hex digits,
// not all zero), otherwise "".
func validTraceID(id string) string {
	if len(id) != 32 || id == "00000000000000000000000000000000" {
		return ""
	}
	for _, c := range id {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return ""
		}
	}
	return id
}

// RunStep is one step of a run.
type RunStep struct {
	Name            string `json:"name"`
	Phase           string `json:"phase,omitempty"`
	Error           string `json:"error,omitempty"`
	Output          string `json:"output,omitempty"`
	OutputTruncated bool   `json:"outputTruncated,omitempty"`
}

// ChainInfo is one entry of list_chains / GET /v1/chains.
type ChainInfo struct {
	Name        string `json:"name"`
	Namespace   string `json:"namespace"`
	Description string `json:"description,omitempty"`
	Steps       int    `json:"steps"`
	// TimeoutSeconds is the chain's overall timeout.
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`
}

// RunFilter narrows ListRuns.
type RunFilter struct {
	Namespace string `json:"namespace,omitempty" jsonschema:"namespace to list; omit for the default"`
	Chain     string `json:"chain,omitempty" jsonschema:"only runs of this chain"`
	Phase     string `json:"phase,omitempty" jsonschema:"only runs in this phase, e.g. Running, Succeeded or Failed"`
	Limit     int    `json:"limit,omitempty" jsonschema:"maximum number of runs to return (default 20, max 200)"`
	Source    string `json:"source,omitempty" jsonschema:"which runs: gateway (default), trigger, pickup or all"`
	Active    bool   `json:"active,omitempty" jsonschema:"only runs that are Running or Pending"`
	Issue     string `json:"issue,omitempty" jsonschema:"only runs of this picked-up issue, as owner/repo#number"`
}

// RunsEnabled reports whether the chain-run endpoints are served.
func (s *Service) RunsEnabled() bool { return s.Runs != nil }

var errRunsDisabled = fmt.Errorf("%w: chain runs are disabled on this gateway", ErrNotFound)

// invokable reports whether a Chain may be run through the gateway: it opts in
// with the label, and is neither a run copy nor mission-scoped.
func invokable(c *aiv1alpha1.Chain) bool {
	return c.Labels[aiv1alpha1.LabelInvokable] == labelTrue &&
		c.Labels[chainrun.LabelRunSource] == "" &&
		c.Spec.MissionRef == nil
}

// isRun reports whether a Chain is a run copy of any source.
func isRun(c *aiv1alpha1.Chain) bool {
	switch c.Labels[chainrun.LabelRunSource] {
	case chainrun.SourceGateway, chainrun.SourceTrigger, chainrun.SourcePickup:
		return true
	}
	return false
}

// record is runRecord plus the time-relative fields.
func (s *Service) record(c *aiv1alpha1.Chain, withSteps bool) *RunRecord {
	r := runRecord(c, withSteps)
	now := s.now()
	age := max(int64(now.Sub(r.CreatedAt)/time.Second), 0)
	r.AgeSeconds = &age
	if r.Phase == string(aiv1alpha1.ChainPhaseRunning) && r.StartedAt != nil {
		run := max(int64(now.Sub(*r.StartedAt)/time.Second), 0)
		r.RunningSeconds = &run
	}
	return r
}

// parseIssue parses owner/repo#number.
func parseIssue(v string) (repo string, n int, err error) {
	i := strings.LastIndex(v, "#")
	if i > 0 {
		n, err = strconv.Atoi(v[i+1:])
		repo = v[:i]
	}
	if i <= 0 || err != nil || n <= 0 || strings.Count(repo, "/") != 1 {
		return "", 0, fmt.Errorf("%w: issue must be owner/repo#number", ErrInvalid)
	}
	return repo, n, nil
}

// isGatewayRun reports whether a Chain is a run copy this gateway started.
func isGatewayRun(c *aiv1alpha1.Chain) bool {
	return c.Labels[chainrun.LabelRunSource] == chainrun.SourceGateway
}

// runDone reports whether a run copy has finished its (only) run.
func runDone(c *aiv1alpha1.Chain) bool {
	if chainrun.Active(c) {
		return false
	}
	switch c.Status.Phase {
	case aiv1alpha1.ChainPhaseSucceeded, aiv1alpha1.ChainPhaseFailed, aiv1alpha1.ChainPhasePartiallySucceeded:
		return true
	}
	return false
}

// ListChains lists the Chains in a namespace that opted in to gateway runs.
func (s *Service) ListChains(ctx context.Context, namespace string) ([]ChainInfo, error) {
	if !s.RunsEnabled() {
		return nil, errRunsDisabled
	}
	ns, err := s.namespace(namespace)
	if err != nil {
		return nil, err
	}
	var list aiv1alpha1.ChainList
	if err := s.Runs.List(ctx, &list, client.InNamespace(ns),
		client.MatchingLabels{aiv1alpha1.LabelInvokable: labelTrue}); err != nil {
		return nil, k8sError("list chains", err)
	}
	out := make([]ChainInfo, 0, len(list.Items))
	for i := range list.Items {
		c := &list.Items[i]
		if !invokable(c) {
			continue
		}
		out = append(out, ChainInfo{
			Name: c.Name, Namespace: c.Namespace, Description: c.Spec.Description,
			Steps: len(c.Spec.Steps), TimeoutSeconds: c.Spec.TimeoutSeconds,
		})
	}
	slices.SortFunc(out, func(a, b ChainInfo) int { return strings.Compare(a.Name, b.Name) })
	return out, nil
}

// RunChain starts a run of an opted-in Chain with the caller's input and
// optionally waits for it to finish.
func (s *Service) RunChain(ctx context.Context, chainName string, req RunChainRequest) (*RunRecord, error) {
	if !s.RunsEnabled() {
		return nil, errRunsDisabled
	}
	if err := validIdempotencyKey(req.IdempotencyKey); err != nil {
		return nil, err
	}
	ns, err := s.namespace(req.Namespace)
	if err != nil {
		return nil, err
	}
	wait, err := s.waitDuration(req.WaitSeconds)
	if err != nil {
		return nil, err
	}
	input, hasInput, err := runInput(req.Input)
	if err != nil {
		return nil, err
	}
	tmpl, err := s.fetchChain(ctx, ns, chainName)
	if err != nil {
		return nil, err
	}
	setRequestTable(ctx, aiv1alpha1.RefName(tmpl.Spec.RoundTableRef))
	if !invokable(tmpl) {
		return nil, fmt.Errorf("%w: chain %s/%s is not invokable through the gateway (it needs the label %s=true)",
			ErrForbidden, ns, chainName, aiv1alpha1.LabelInvokable)
	}
	if tmpl.Spec.Suspend {
		return nil, fmt.Errorf("%w: chain %s/%s is suspended", ErrUnavailable, ns, chainName)
	}
	if !hasInput {
		input = tmpl.Spec.Input
	}

	suffix := randomHex()
	if req.IdempotencyKey != "" {
		suffix = idempotencyHash(ns, chainName, req.IdempotencyKey)[:8]
	}
	annotations := map[string]string{annotationRunOf: tmpl.Name}
	if c := callerName(ctx); c != "" {
		annotations[aiv1alpha1.AnnotationCaller] = c
	}
	if req.rerunOf != "" {
		annotations[annotationRerunOf] = req.rerunOf
	}
	// A waiting call takes its slot before it creates anything, so a busy
	// gateway answers 429 without leaving a run behind (a replay needs one too).
	release := func() {}
	if wait > 0 {
		if release, err = s.acquireSlot(); err != nil {
			return nil, err
		}
	}
	defer release()
	run, err := chainrun.Start(ctx, s.Runs, s.Runs.Scheme(), tmpl, chainrun.StartOptions{
		Name:        chainrun.Name(tmpl.Name, "gw", suffix),
		Namespace:   ns,
		Input:       input,
		Trigger:     "gateway-" + suffix,
		Source:      chainrun.SourceGateway,
		Labels:      map[string]string{aiv1alpha1.LabelCreatedBy: aiv1alpha1.CreatedByGateway},
		Annotations: annotations,
	})
	if err != nil {
		if req.IdempotencyKey != "" && apierrors.IsAlreadyExists(err) {
			// The run name comes from the key, so the name clash is the dedupe.
			return s.replayRun(ctx, ns, chainrun.Name(tmpl.Name, "gw", suffix), wait)
		}
		return nil, k8sError("create chain run", err)
	}
	s.Log.Info("Started chain run", "chain", tmpl.Name, "run", run.Name, "namespace", ns, "inputBytes", len(input),
		"caller", callerName(ctx))
	if wait > 0 {
		return s.pollRun(ctx, ns, run.Name, wait)
	}
	return s.record(run, true), nil
}

// RerunRun starts a new run of the template of a finished gateway run, with
// the finished run's input unless the request overrides it. All RunChain
// checks apply. Failed steps are not resumed: the whole chain runs again.
func (s *Service) RerunRun(ctx context.Context, namespace, name string, req RerunRunRequest) (*RunRecord, error) {
	if !s.RunsEnabled() {
		return nil, errRunsDisabled
	}
	ns, err := s.namespace(namespace)
	if err != nil {
		return nil, err
	}
	old, err := s.fetchRun(ctx, ns, name)
	if err != nil {
		return nil, err
	}
	if !runDone(old) {
		phase := string(old.Status.Phase)
		if phase == "" {
			phase = runPhasePending
		}
		return nil, &ConflictError{State: phase,
			Reason: fmt.Sprintf("run %s/%s is %s, not finished", ns, name, phase)}
	}
	chainName := old.Annotations[annotationRunOf]
	if chainName == "" {
		return nil, fmt.Errorf("%w: run %s/%s has no template chain", ErrNotFound, ns, name)
	}
	runReq := RunChainRequest{
		Input:          req.Input,
		Namespace:      ns,
		WaitSeconds:    req.WaitSeconds,
		IdempotencyKey: req.IdempotencyKey,
		rerunOf:        name,
	}
	if _, has, err := runInput(req.Input); err != nil {
		return nil, err
	} else if !has {
		stored, err := json.Marshal(old.Spec.Input)
		if err != nil {
			return nil, fmt.Errorf("%w: input: %v", ErrInvalid, err)
		}
		runReq.Input = stored
	}
	return s.RunChain(ctx, chainName, runReq)
}

// replayRun returns the run an earlier request with the same key started.
// A waiting replay runs under the slot RunChain already holds.
func (s *Service) replayRun(ctx context.Context, ns, name string, wait time.Duration) (*RunRecord, error) {
	var rec *RunRecord
	var err error
	if wait > 0 {
		rec, err = s.pollRun(ctx, ns, name, wait)
	} else {
		var c *aiv1alpha1.Chain
		if c, err = s.fetchRun(ctx, ns, name); err == nil {
			rec = s.record(c, true)
		}
	}
	if err != nil {
		return nil, err
	}
	rec.Replayed = true
	return rec, nil
}

// runInput turns the request's input into spec.input. ok is false when the
// caller gave none.
func runInput(raw json.RawMessage) (input string, ok bool, err error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return "", false, nil
	}
	if raw[0] == '"' {
		if err := json.Unmarshal(raw, &input); err != nil {
			return "", false, fmt.Errorf("%w: input: %v", ErrInvalid, err)
		}
	} else {
		var buf bytes.Buffer
		if err := json.Compact(&buf, raw); err != nil {
			return "", false, fmt.Errorf("%w: input: %v", ErrInvalid, err)
		}
		input = buf.String()
	}
	if len(input) > maxRunInputBytes {
		return "", false, fmt.Errorf("%w: input exceeds %d bytes", ErrInvalid, maxRunInputBytes)
	}
	return input, true, nil
}

// GetRun returns a gateway-started run, waiting up to waitSeconds for it to
// finish.
func (s *Service) GetRun(ctx context.Context, namespace, name string, waitSeconds *int) (*RunRecord, error) {
	if !s.RunsEnabled() {
		return nil, errRunsDisabled
	}
	ns, err := s.namespace(namespace)
	if err != nil {
		return nil, err
	}
	wait, err := s.waitDuration(waitSeconds)
	if err != nil {
		return nil, err
	}
	if wait > 0 {
		return s.waitRun(ctx, ns, name, wait)
	}
	c, err := s.fetchRun(ctx, ns, name)
	if err != nil {
		return nil, err
	}
	return s.record(c, true), nil
}

// CancelRunRequest is the optional body of POST /v1/runs/{namespace}/{run}/cancel.
type CancelRunRequest struct {
	// Reason is recorded on the cancelled steps and the run's Cancelled condition.
	Reason string `json:"reason,omitempty"`
}

// cancelWait is how long CancelRun waits for the chain controller to end the
// run before returning its record as it is.
const cancelWait = 10 * time.Second

// CancelRun cancels an unfinished gateway run by annotating it; the chain
// controller fails its unfinished steps and ends the run. Only runs the
// gateway started can be cancelled, and a finished run is a conflict. Tasks
// already published to a knight are not recalled.
func (s *Service) CancelRun(ctx context.Context, namespace, name, reason string) (*RunRecord, error) {
	return s.cancelRun(ctx, namespace, name, reason, cancelRetries)
}

// cancelRetries bounds how often a cancel re-reads the run after a write conflict.
const cancelRetries = 5

func (s *Service) cancelRun(ctx context.Context, namespace, name, reason string, retries int) (*RunRecord, error) {
	if !s.RunsEnabled() {
		return nil, errRunsDisabled
	}
	ns, err := s.namespace(namespace)
	if err != nil {
		return nil, err
	}
	c, err := s.fetchRun(ctx, ns, name)
	if err != nil {
		return nil, err
	}
	if !isGatewayRun(c) {
		return nil, fmt.Errorf("%w: run %s/%s was started by %s, only gateway runs can be cancelled",
			ErrForbidden, ns, name, c.Labels[chainrun.LabelRunSource])
	}
	if runDone(c) {
		return nil, &ConflictError{State: string(c.Status.Phase),
			Reason: fmt.Sprintf("run %s/%s is already %s", ns, name, c.Status.Phase)}
	}
	patch := client.MergeFromWithOptions(c.DeepCopy(), client.MergeFromWithOptimisticLock{})
	if c.Annotations == nil {
		c.Annotations = map[string]string{}
	}
	c.Annotations[aiv1alpha1.AnnotationChainCancel] = reason
	if err := s.Runs.Patch(ctx, c, patch); err != nil {
		if apierrors.IsConflict(err) && retries > 0 {
			// The run moved under us (it may have just finished): re-check.
			return s.cancelRun(ctx, ns, name, reason, retries-1)
		}
		return nil, k8sError("cancel chain run", err)
	}
	s.Log.Info("Cancelling chain run", "run", name, "namespace", ns, "reason", reason)
	return s.waitRun(ctx, ns, name, min(cancelWait, s.Opts.MaxTimeout))
}

// ListRuns lists runs of the filter's source (default gateway) in a namespace, newest first, without
// step details (get one run for those).
func (s *Service) ListRuns(ctx context.Context, f RunFilter) ([]*RunRecord, error) {
	if !s.RunsEnabled() {
		return nil, errRunsDisabled
	}
	ns, err := s.namespace(f.Namespace)
	if err != nil {
		return nil, err
	}
	source := strings.ToLower(f.Source)
	switch source {
	case "":
		source = chainrun.SourceGateway
	case chainrun.SourceGateway, chainrun.SourceTrigger, chainrun.SourcePickup, sourceAll:
	default:
		return nil, fmt.Errorf("%w: source must be gateway, trigger, pickup or all", ErrInvalid)
	}
	var issueRepo string
	var issueNum int
	if f.Issue != "" {
		if issueRepo, issueNum, err = parseIssue(f.Issue); err != nil {
			return nil, err
		}
	}
	var list aiv1alpha1.ChainList
	opts := []client.ListOption{client.InNamespace(ns)}
	if source == sourceAll {
		opts = append(opts, client.HasLabels{chainrun.LabelRunSource})
	} else {
		opts = append(opts, client.MatchingLabels{chainrun.LabelRunSource: source})
	}
	if err := s.Runs.List(ctx, &list, opts...); err != nil {
		return nil, k8sError("list chain runs", err)
	}
	limit := f.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}
	limit = min(limit, maxListLimit)
	items := list.Items
	slices.SortFunc(items, func(a, b aiv1alpha1.Chain) int {
		if c := b.CreationTimestamp.Compare(a.CreationTimestamp.Time); c != 0 {
			return c
		}
		return strings.Compare(a.Name, b.Name)
	})
	out := make([]*RunRecord, 0, min(len(items), limit))
	for i := range items {
		// The label selector keeps unlabelled Chains out; isRun still drops a
		// Chain whose label value is not a known run source.
		if source == sourceAll && !isRun(&items[i]) {
			continue
		}
		r := s.record(&items[i], false)
		if (f.Chain != "" && r.Chain != f.Chain) || (f.Phase != "" && !strings.EqualFold(r.Phase, f.Phase)) {
			continue
		}
		if f.Active && r.Phase != string(aiv1alpha1.ChainPhaseRunning) && r.Phase != runPhasePending {
			continue
		}
		if f.Issue != "" && (r.Issue == nil || r.Issue.Repo != issueRepo || r.Issue.Number != issueNum) {
			continue
		}
		out = append(out, r)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

// waitRun polls the run until it is done or the wait ends, then returns its
// latest record. Waits count against maxInFlight.
func (s *Service) waitRun(ctx context.Context, ns, name string, wait time.Duration) (*RunRecord, error) {
	release, err := s.acquireSlot()
	if err != nil {
		return nil, err
	}
	defer release()
	return s.pollRun(ctx, ns, name, wait)
}

// pollRun is waitRun for a caller that already holds a slot.
func (s *Service) pollRun(ctx context.Context, ns, name string, wait time.Duration) (*RunRecord, error) {
	wctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	if progress := progressFrom(ctx); progress != nil {
		// Report step changes and tool calls to an MCP client that asked for progress.
		defer s.startProgress(wctx, progress, func(ctx context.Context, emit func(StreamEvent) error) error {
			return s.followRun(ctx, ns, name, emit)
		})()
	}
	tick := time.NewTicker(s.Opts.Runs.PollInterval)
	defer tick.Stop()
	for {
		c, err := s.fetchRun(ctx, ns, name)
		if err != nil {
			return nil, err
		}
		if runDone(c) {
			return s.record(c, true), nil
		}
		select {
		case <-wctx.Done():
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return s.record(c, true), nil
		case <-tick.C:
		}
	}
}

// waitDuration validates a caller's waitSeconds and caps it at MaxTimeout.
func (s *Service) waitDuration(seconds *int) (time.Duration, error) {
	if seconds == nil {
		return 0, nil
	}
	if *seconds < 0 {
		return 0, fmt.Errorf("%w: waitSeconds must be >= 0", ErrInvalid)
	}
	return min(time.Duration(*seconds)*time.Second, s.Opts.MaxTimeout), nil
}

// fetchChain reads a Chain named in a request path; a bad name or a miss is
// 404.
func (s *Service) fetchChain(ctx context.Context, ns, name string) (*aiv1alpha1.Chain, error) {
	if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 {
		return nil, fmt.Errorf("%w: chain %s/%s", ErrNotFound, ns, name)
	}
	c := &aiv1alpha1.Chain{}
	if err := s.Runs.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, c); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("%w: chain %s/%s", ErrNotFound, ns, name)
		}
		return nil, k8sError("get chain", err)
	}
	return c, nil
}

// fetchRun reads a run copy of any source; Chains that are not run copies are 404.
func (s *Service) fetchRun(ctx context.Context, ns, name string) (*aiv1alpha1.Chain, error) {
	c, err := s.fetchChain(ctx, ns, name)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, fmt.Errorf("%w: run %s/%s", ErrNotFound, ns, name)
		}
		return nil, err
	}
	if !isRun(c) {
		return nil, fmt.Errorf("%w: run %s/%s", ErrNotFound, ns, name)
	}
	return c, nil
}

// runRecord projects a run copy. withSteps adds the steps and the output.
func runRecord(c *aiv1alpha1.Chain, withSteps bool) *RunRecord {
	st := &c.Status
	r := &RunRecord{
		Run:         c.Name,
		Chain:       runChainName(c),
		Source:      c.Labels[chainrun.LabelRunSource],
		Namespace:   c.Namespace,
		Phase:       string(st.Phase),
		Caller:      c.Annotations[aiv1alpha1.AnnotationCaller],
		RerunOf:     c.Annotations[annotationRerunOf],
		Done:        runDone(c),
		CreatedAt:   c.CreationTimestamp.UTC(),
		StartedAt:   timePtr(st.StartedAt),
		CompletedAt: timePtr(st.CompletedAt),
		TraceID:     validTraceID(st.TraceID),
	}
	if repo := c.Annotations[aiv1alpha1.AnnotationPickupRepo]; repo != "" {
		if n, err := strconv.Atoi(c.Annotations[aiv1alpha1.AnnotationPickupIssue]); err == nil {
			r.Issue = &RunIssue{Repo: repo, Number: n}
		}
	}
	if r.Phase == "" || (chainrun.Active(c) && st.Phase != aiv1alpha1.ChainPhaseRunning) {
		// The run hasn't started: any status is from before it.
		r.Phase = runPhasePending
		r.StartedAt, r.CompletedAt, r.TraceID = nil, nil, ""
	} else {
		r.RunID = st.RunID
	}
	if !withSteps || r.Phase == runPhasePending {
		return r
	}
	byName := make(map[string]*aiv1alpha1.ChainStepStatus, len(st.StepStatuses))
	for i := range st.StepStatuses {
		byName[st.StepStatuses[i].Name] = &st.StepStatuses[i]
	}
	for _, step := range c.Spec.Steps {
		rs := RunStep{Name: step.Name}
		if ss := byName[step.Name]; ss != nil {
			rs.Phase, rs.Error = string(ss.Phase), ss.Error
			rs.Output, rs.OutputTruncated = truncate(ss.Output, maxRunStepOutput)
		}
		r.Steps = append(r.Steps, rs)
	}
	if final := finalStep(c.Spec.Steps); final != "" {
		for _, rs := range r.Steps {
			if rs.Name == final {
				r.Output, r.OutputTruncated = rs.Output, rs.OutputTruncated
			}
		}
	}
	return r
}

// runChainName is the template Chain of a run: the gateway's run-of
// annotation, else the run-template annotation every source sets.
func runChainName(c *aiv1alpha1.Chain) string {
	if v := c.Annotations[annotationRunOf]; v != "" {
		return v
	}
	return c.Annotations[aiv1alpha1.AnnotationRunTemplate]
}

// finalStep returns the one step no other step depends on, or "" if there
// are several.
func finalStep(steps []aiv1alpha1.ChainStep) string {
	needed := map[string]bool{}
	for _, s := range steps {
		for _, d := range s.DependsOn {
			needed[d] = true
		}
	}
	final := ""
	for _, s := range steps {
		if needed[s.Name] {
			continue
		}
		if final != "" {
			return ""
		}
		final = s.Name
	}
	return final
}

// SweepRuns deletes the gateway's run copies that finished more than the run
// TTL ago, in the namespaces it serves. It returns how many it deleted.
func (s *Service) SweepRuns(ctx context.Context) (int, error) {
	if !s.RunsEnabled() {
		return 0, nil
	}
	var namespaces []string
	if slices.Contains(s.Opts.AllowedNamespaces, "*") {
		namespaces = []string{""} // all namespaces
	} else {
		namespaces = append([]string{s.Opts.Namespace}, s.Opts.AllowedNamespaces...)
		slices.Sort(namespaces)
		namespaces = slices.Compact(namespaces)
	}
	ttl := int32(min(s.Opts.Runs.TTL/time.Second, 1<<31-1))
	now := s.now()
	deleted := 0
	var errs []error
	for _, ns := range namespaces {
		var list aiv1alpha1.ChainList
		if err := s.Runs.List(ctx, &list, client.InNamespace(ns),
			client.MatchingLabels{chainrun.LabelRunSource: chainrun.SourceGateway}); err != nil {
			errs = append(errs, k8sError("list chain runs", err))
			continue
		}
		for i := range list.Items {
			c := &list.Items[i]
			if !runDone(c) || !chainrun.Expired(c, &ttl, now) {
				continue
			}
			// Precondition on the UID so a same-named replacement isn't deleted.
			err := s.Runs.Delete(ctx, c, client.Preconditions{UID: &c.UID},
				client.PropagationPolicy(metav1.DeletePropagationBackground))
			if err != nil && !apierrors.IsNotFound(err) {
				errs = append(errs, k8sError("delete chain run", err))
				continue
			}
			if err == nil {
				deleted++
				s.Log.Info("Deleted expired chain run", "run", c.Name, "namespace", c.Namespace)
			}
		}
	}
	return deleted, errors.Join(errs...)
}

// RunJanitor calls SweepRuns every interval until ctx ends.
func (s *Service) RunJanitor(ctx context.Context, interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		if _, err := s.SweepRuns(ctx); err != nil && ctx.Err() == nil {
			s.Log.Error(err, "Failed to clean up expired chain runs")
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func randomHex() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
