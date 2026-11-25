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
	"cmp"
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

const (
	// defaultScheduleRunsLimit and maxScheduleRunsLimit bound the history
	// returned by one request.
	defaultScheduleRunsLimit = 20
	maxScheduleRunsLimit     = 200

	// DefaultScheduleResultRetention is how long the results of a
	// schedule's runs stay readable (chart value
	// gateway.scheduleRuns.resultRetention).
	DefaultScheduleResultRetention = 30 * 24 * time.Hour

	// Outcomes of a schedule run that are not a chain phase.
	scheduleOutcomeSkipped = "skipped"
	scheduleOutcomeRunning = "running"
	scheduleOutcomeUnknown = "unknown"
)

// EffectiveTaskTTL is how long the gateway's task KV bucket keeps records:
// the task TTL, raised to the schedule result retention so the results of
// knight-target schedule runs stay readable that long. The gateway and the
// operator both create the bucket, so both must use this.
func EffectiveTaskTTL(taskTTL, scheduleRetention time.Duration) time.Duration {
	return max(taskTTL, scheduleRetention)
}

var errScheduleRunsDisabled = fmt.Errorf("%w: schedule runs are disabled on this gateway", ErrNotFound)

// ScheduleRunsEnabled reports whether the schedule run-history endpoints are
// served.
func (s *Service) ScheduleRunsEnabled() bool { return s.ScheduleReader != nil }

// ScheduleRun is one firing of a schedule in its run history.
type ScheduleRun struct {
	// Run is the run to fetch the result with: the name of the Chain the
	// firing started, or the task ID of the task it sent. Empty for a
	// skipped firing.
	Run string `json:"run,omitempty"`
	// Time is when the firing was handled.
	Time time.Time `json:"time"`
	// Key is the firing's dedupe key.
	Key string `json:"key,omitempty"`
	// Outcome is succeeded, failed, partiallySucceeded, lost, skipped,
	// pending, running or unknown (the run is gone and its outcome was not
	// recorded).
	Outcome string `json:"outcome"`
	// Reason says why a firing was skipped.
	Reason string `json:"reason,omitempty"`
	// DurationSeconds is how long the run took, once finished.
	DurationSeconds *float64 `json:"durationSeconds,omitempty"`
	// CostUSD is the run's cost, when known.
	CostUSD string `json:"costUSD,omitempty"`
	// ResultAvailable is true while the run's result can still be fetched.
	ResultAvailable bool `json:"resultAvailable"`
}

// ScheduleRunResult is the result of one run of a schedule: the run's record
// (for a chain target, its steps and final output) and the schedule it
// belongs to.
type ScheduleRunResult struct {
	Schedule string `json:"schedule"`
	// Target is the kind of target that ran: chain.
	Target string `json:"target"`
	RunRecord
}

// ScheduleRunsFilter narrows ListScheduleRuns.
type ScheduleRunsFilter struct {
	Namespace string
	Name      string
	Limit     int
}

// lookupScheduleRuns returns the schedule at namespace/name. A Trigger with
// another source is not a schedule and is a 404.
func (s *Service) lookupScheduleRuns(ctx context.Context, namespace, name string) (*aiv1alpha1.Trigger, error) {
	if !s.ScheduleRunsEnabled() {
		return nil, errScheduleRunsDisabled
	}
	ns, err := s.namespace(namespace)
	if err != nil {
		return nil, err
	}
	notFound := fmt.Errorf("%w: schedule %s/%s", ErrNotFound, ns, name)
	if len(validation.IsDNS1123Subdomain(name)) > 0 {
		return nil, notFound
	}
	trg := &aiv1alpha1.Trigger{}
	if err := s.ScheduleReader.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, trg); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, notFound
		}
		return nil, k8sError("get schedule", err)
	}
	if trg.Spec.Source.Schedule == nil {
		return nil, notFound
	}
	return trg, nil
}

// scheduleChains lists the Chains the schedule's firings started, newest
// first, as long as they are kept.
func (s *Service) scheduleChains(ctx context.Context, trg *aiv1alpha1.Trigger) ([]aiv1alpha1.Chain, error) {
	var list aiv1alpha1.ChainList
	if err := s.ScheduleReader.List(ctx, &list, client.InNamespace(trg.Namespace),
		client.MatchingLabels{aiv1alpha1.LabelTrigger: trg.Name}); err != nil {
		return nil, k8sError("list schedule runs", err)
	}
	items := slices.DeleteFunc(list.Items, func(c aiv1alpha1.Chain) bool {
		return !metav1.IsControlledBy(&c, trg) || !c.DeletionTimestamp.IsZero()
	})
	return items, nil
}

// ListScheduleRuns returns a schedule's firings, newest first: the runs it
// started that still exist (so history goes back as far as results are
// retained) merged with status.recentFirings, which adds skipped firings and
// the outcome of runs whose Chain is gone.
func (s *Service) ListScheduleRuns(ctx context.Context, f ScheduleRunsFilter) ([]*ScheduleRun, error) {
	trg, err := s.lookupScheduleRuns(ctx, f.Namespace, f.Name)
	if err != nil {
		return nil, err
	}
	limit := f.Limit
	switch {
	case limit < 0:
		return nil, fmt.Errorf("%w: limit must not be negative", ErrInvalid)
	case limit == 0:
		limit = defaultScheduleRunsLimit
	case limit > maxScheduleRunsLimit:
		limit = maxScheduleRunsLimit
	}
	chains, err := s.scheduleChains(ctx, trg)
	if err != nil {
		return nil, err
	}
	out := make([]*ScheduleRun, 0, len(chains)+len(trg.Status.RecentFirings))
	byRun := make(map[string]*ScheduleRun, len(chains))
	for i := range chains {
		r := chainScheduleRun(&chains[i])
		out = append(out, r)
		byRun[r.Run] = r
	}
	knight := trg.Spec.Target.Knight != nil
	for _, f := range trg.Status.RecentFirings {
		switch {
		case f.SkipReason != "":
			out = append(out, &ScheduleRun{Time: f.Time.UTC(), Key: f.Key, Outcome: scheduleOutcomeSkipped,
				Reason: f.SkipReason})
		case f.Run != "" && byRun[f.Run] != nil:
			// The run itself is richer; keep the firing's time of record.
			if byRun[f.Run].Key == "" {
				byRun[f.Run].Key = f.Key
			}
		default:
			// A knight-target task, or a Chain run already deleted.
			r := &ScheduleRun{Run: f.Run, Time: f.Time.UTC(), Key: f.Key, Outcome: string(f.Outcome)}
			if r.Outcome == "" {
				r.Outcome = scheduleOutcomeUnknown
				if knight {
					r.Outcome = scheduleOutcomeRunning
				}
			}
			// Extension point (#564): a knight-target task's result is
			// readable while its task record is kept.
			out = append(out, r)
		}
	}
	slices.SortStableFunc(out, func(a, b *ScheduleRun) int {
		if c := b.Time.Compare(a.Time); c != 0 {
			return c
		}
		return cmp.Compare(b.Run, a.Run)
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// chainScheduleRun projects a Chain a schedule started.
func chainScheduleRun(c *aiv1alpha1.Chain) *ScheduleRun {
	rec := runRecord(c, false)
	r := &ScheduleRun{
		Run:             c.Name,
		Time:            c.CreationTimestamp.UTC(),
		Key:             c.Annotations[aiv1alpha1.AnnotationTriggerKey],
		Outcome:         scheduleChainOutcome(rec),
		ResultAvailable: true,
	}
	if rec.Done {
		r.CostUSD = c.Status.CostUSD
		if rec.StartedAt != nil && rec.CompletedAt != nil {
			d := rec.CompletedAt.Sub(*rec.StartedAt).Seconds()
			r.DurationSeconds = &d
		}
	}
	return r
}

func scheduleChainOutcome(rec *RunRecord) string {
	switch aiv1alpha1.ChainPhase(rec.Phase) {
	case aiv1alpha1.ChainPhaseSucceeded:
		return "succeeded"
	case aiv1alpha1.ChainPhaseFailed:
		return "failed"
	case aiv1alpha1.ChainPhasePartiallySucceeded:
		return "partiallySucceeded"
	case aiv1alpha1.ChainPhaseRunning:
		return scheduleOutcomeRunning
	}
	if rec.Done {
		return strings.ToLower(rec.Phase)
	}
	return strings.ToLower(runPhasePending)
}

// ScheduleRunResult returns the result of one run of a schedule. A run that
// does not belong to the schedule is a 404.
//
// Extension points, one case per target kind: a knight target (#564) returns
// the task's result from the task record, a mission target (#567) the
// mission's report.
func (s *Service) ScheduleRunResult(ctx context.Context, namespace, name, run string) (*ScheduleRunResult, error) {
	trg, err := s.lookupScheduleRuns(ctx, namespace, name)
	if err != nil {
		return nil, err
	}
	switch {
	case trg.Spec.Target.ChainRef != nil:
		return s.chainScheduleRunResult(ctx, trg, run)
	case trg.Spec.Target.Knight != nil:
		// TODO(#564): read the task record of run (Bus.GetRecord) and check
		// it was sent for this schedule.
		return nil, fmt.Errorf("%w: results of knight-target schedules are not served yet", ErrUnprocessable)
	}
	// TODO(#567): a mission target returns the mission's report.
	return nil, fmt.Errorf("%w: schedule %s/%s has no target whose results are served", ErrUnprocessable, trg.Namespace, trg.Name)
}

func (s *Service) chainScheduleRunResult(ctx context.Context, trg *aiv1alpha1.Trigger, run string) (*ScheduleRunResult, error) {
	notFound := fmt.Errorf("%w: run %s/%s of schedule %s", ErrNotFound, trg.Namespace, run, trg.Name)
	c, err := fetchChainFrom(ctx, s.ScheduleReader, trg.Namespace, run)
	if err != nil {
		return nil, err
	}
	if c == nil || c.Labels[aiv1alpha1.LabelTrigger] != trg.Name || !metav1.IsControlledBy(c, trg) {
		return nil, notFound
	}
	return &ScheduleRunResult{Schedule: trg.Name, Target: "chain", RunRecord: *runRecord(c, true)}, nil
}

// fetchChainFrom reads a Chain through r; a missing one is nil, not an error.
func fetchChainFrom(ctx context.Context, r client.Reader, ns, name string) (*aiv1alpha1.Chain, error) {
	if len(validation.IsDNS1123Subdomain(name)) > 0 {
		return nil, nil //nolint:nilnil // an invalid name is a missing run
	}
	c := &aiv1alpha1.Chain{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, c); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil //nolint:nilnil // a deleted run is a missing run
		}
		return nil, k8sError("get schedule run", err)
	}
	return c, nil
}

func (s *Service) handleScheduleRuns(w http.ResponseWriter, r *http.Request) {
	limit, err := queryInt(r, "limit")
	if err != nil {
		writeError(w, err)
		return
	}
	f := ScheduleRunsFilter{Namespace: r.PathValue("namespace"), Name: r.PathValue("name")}
	if limit != nil {
		f.Limit = *limit
	}
	runs, err := s.ListScheduleRuns(r.Context(), f)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": runs})
}

func (s *Service) handleScheduleRunResult(w http.ResponseWriter, r *http.Request) {
	res, err := s.ScheduleRunResult(r.Context(), r.PathValue("namespace"), r.PathValue("name"), r.PathValue("run"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}
