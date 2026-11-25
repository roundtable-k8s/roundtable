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

	// runPhasePending is the phase of a run the chain controller hasn't
	// started yet.
	runPhasePending = "Pending"

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
}

// RunRecord is what the gateway returns for a chain run.
type RunRecord struct {
	// Run is the name of the run copy (the Chain resource running it).
	Run       string `json:"run"`
	Chain     string `json:"chain"`
	Namespace string `json:"namespace"`
	RunID     string `json:"runId,omitempty"`
	Phase     string `json:"phase"`
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
	// Replayed marks a record returned for a repeated Idempotency-Key. Not
	// serialized.
	Replayed bool `json:"-"`
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
	run, err := chainrun.Start(ctx, s.Runs, s.Runs.Scheme(), tmpl, chainrun.StartOptions{
		Name:        chainrun.Name(tmpl.Name, "gw", suffix),
		Namespace:   ns,
		Input:       input,
		Trigger:     "gateway-" + suffix,
		Source:      chainrun.SourceGateway,
		Labels:      map[string]string{aiv1alpha1.LabelCreatedBy: aiv1alpha1.CreatedByGateway},
		Annotations: map[string]string{annotationRunOf: tmpl.Name},
	})
	if err != nil {
		if req.IdempotencyKey != "" && apierrors.IsAlreadyExists(err) {
			// The run name comes from the key, so the name clash is the dedupe.
			return s.replayRun(ctx, ns, chainrun.Name(tmpl.Name, "gw", suffix), wait)
		}
		return nil, k8sError("create chain run", err)
	}
	s.Log.Info("Started chain run", "chain", tmpl.Name, "run", run.Name, "namespace", ns, "inputBytes", len(input))
	if wait > 0 {
		return s.waitRun(ctx, ns, run.Name, wait)
	}
	return runRecord(run, true), nil
}

// replayRun returns the run an earlier request with the same key started.
func (s *Service) replayRun(ctx context.Context, ns, name string, wait time.Duration) (*RunRecord, error) {
	var rec *RunRecord
	var err error
	if wait > 0 {
		rec, err = s.waitRun(ctx, ns, name, wait)
	} else {
		var c *aiv1alpha1.Chain
		if c, err = s.fetchRun(ctx, ns, name); err == nil {
			rec = runRecord(c, true)
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
	return runRecord(c, true), nil
}

// ListRuns lists gateway-started runs in a namespace, newest first, without
// step details (get one run for those).
func (s *Service) ListRuns(ctx context.Context, f RunFilter) ([]*RunRecord, error) {
	if !s.RunsEnabled() {
		return nil, errRunsDisabled
	}
	ns, err := s.namespace(f.Namespace)
	if err != nil {
		return nil, err
	}
	var list aiv1alpha1.ChainList
	if err := s.Runs.List(ctx, &list, client.InNamespace(ns),
		client.MatchingLabels{chainrun.LabelRunSource: chainrun.SourceGateway}); err != nil {
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
		r := runRecord(&items[i], false)
		if (f.Chain != "" && r.Chain != f.Chain) || (f.Phase != "" && !strings.EqualFold(r.Phase, f.Phase)) {
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
	if s.inflight != nil {
		select {
		case s.inflight <- struct{}{}:
			defer func() { <-s.inflight }()
		default:
			return nil, ErrBusy
		}
	}
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
			return runRecord(c, true), nil
		}
		select {
		case <-wctx.Done():
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return runRecord(c, true), nil
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

// fetchRun reads a run copy; Chains the gateway didn't start are 404.
func (s *Service) fetchRun(ctx context.Context, ns, name string) (*aiv1alpha1.Chain, error) {
	c, err := s.fetchChain(ctx, ns, name)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, fmt.Errorf("%w: run %s/%s", ErrNotFound, ns, name)
		}
		return nil, err
	}
	if !isGatewayRun(c) {
		return nil, fmt.Errorf("%w: run %s/%s", ErrNotFound, ns, name)
	}
	return c, nil
}

// runRecord projects a run copy. withSteps adds the steps and the output.
func runRecord(c *aiv1alpha1.Chain, withSteps bool) *RunRecord {
	st := &c.Status
	r := &RunRecord{
		Run:         c.Name,
		Chain:       c.Annotations[annotationRunOf],
		Namespace:   c.Namespace,
		Phase:       string(st.Phase),
		Done:        runDone(c),
		CreatedAt:   c.CreationTimestamp.UTC(),
		StartedAt:   timePtr(st.StartedAt),
		CompletedAt: timePtr(st.CompletedAt),
	}
	if r.Phase == "" || (chainrun.Active(c) && st.Phase != aiv1alpha1.ChainPhaseRunning) {
		// The run hasn't started: any status is from before it.
		r.Phase = runPhasePending
		r.StartedAt, r.CompletedAt = nil, nil
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
