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
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
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
)

// Limits on a mission request. The objective and criteria are prompts, so
// they get the same kind of bound as a task; the counts mirror the CRD.
const (
	maxObjectiveBytes      = 16 << 10
	maxPlannerContextBytes = 64 << 10
	maxChainInputBytes     = 64 << 10
	maxMissionKnights      = 50
	maxMissionChains       = 20
	// maxRawPlannerOutput bounds the planner's raw output in the projection.
	maxRawPlannerOutput = 4 << 10

	// CRD bounds and defaults of ttlSeconds / timeoutSeconds.
	minMissionSeconds     = 60
	defaultMissionTTL     = 3600
	defaultMissionTimeout = 1800
	crdMaxTTL             = 604800
	crdMaxTimeout         = 86400

	// plannerRoleLabel finds the default planner knight, as the operator does.
	plannerRoleLabel = "ai.roundtable.io/role"
)

// MissionOptions configures the mission endpoints.
type MissionOptions struct {
	// MaxBudgetUSD caps costBudgetUSD (default 10).
	MaxBudgetUSD float64
	// MaxTTLSeconds caps ttlSeconds (default 86400).
	MaxTTLSeconds int32
	// MaxTimeoutSeconds caps timeoutSeconds (default 21600).
	MaxTimeoutSeconds int32
	// AllowSkillGeneration lets callers set planner.allowSkillGeneration.
	AllowSkillGeneration bool
	// PollInterval is how often a waiting request re-reads the Mission
	// (default 2s).
	PollInterval time.Duration
}

func (o *MissionOptions) defaults() {
	if o.MaxBudgetUSD <= 0 {
		o.MaxBudgetUSD = 10
	}
	if o.MaxTTLSeconds <= 0 {
		o.MaxTTLSeconds = 86400
	}
	o.MaxTTLSeconds = min(max(o.MaxTTLSeconds, minMissionSeconds), crdMaxTTL)
	if o.MaxTimeoutSeconds <= 0 {
		o.MaxTimeoutSeconds = 21600
	}
	o.MaxTimeoutSeconds = min(max(o.MaxTimeoutSeconds, minMissionSeconds), crdMaxTimeout)
	if o.PollInterval <= 0 {
		o.PollInterval = 2 * time.Second
	}
}

// StartMissionRequest is the body of POST /v1/missions and the input of the
// start_mission MCP tool. It is deliberately narrow: there is no way to give
// an image, a Secret, a pod or knight spec, or a RoundTable template. Ephemeral
// knights only come from the RoundTable's knightTemplates, chosen by the
// planner. Unknown fields are rejected.
type StartMissionRequest struct {
	Objective       string `json:"objective" jsonschema:"the goal of the mission (max 16 KiB)"`
	SuccessCriteria string `json:"successCriteria,omitempty" jsonschema:"how to tell whether the mission succeeded"`
	// RoundTable is the RoundTable the mission runs under.
	RoundTable string `json:"roundTable" jsonschema:"name of the RoundTable the mission runs under"`
	// CostBudgetUSD is the spending cap; required, > 0 and at most the
	// gateway's maximum.
	CostBudgetUSD float64 `json:"costBudgetUSD" jsonschema:"cost budget in USD (required, greater than 0, capped by the gateway)"`
	TTLSeconds    int32   `json:"ttlSeconds,omitempty" jsonschema:"seconds until the mission is cleaned up (min 60, capped by the gateway; default 3600)"`
	// TimeoutSeconds is how long the mission may run before it fails.
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty" jsonschema:"seconds the mission may run before it fails (min 60, capped by the gateway; default 1800)"`
	// Planner makes this a meta-mission: a planner knight breaks the
	// objective into chains over standing knights and template knights.
	Planner *MissionPlannerRequest `json:"planner,omitempty" jsonschema:"let a planner knight break the objective into chains (a meta-mission)"`
	// RecruitExisting lets the planner use standing knights.
	RecruitExisting bool `json:"recruitExisting,omitempty" jsonschema:"let the mission use standing (non-ephemeral) knights"`
	// Knights are names of existing Knights taking part.
	Knights []string `json:"knights,omitempty" jsonschema:"names of existing knights taking part"`
	// Chains are existing Chains to run as part of the mission.
	Chains []MissionChainRequest `json:"chains,omitempty" jsonschema:"existing chains to run, with optional input"`
	// WaitSeconds waits for the mission to finish before returning. 0 (the
	// default) returns once it is created.
	WaitSeconds *int `json:"waitSeconds,omitempty" jsonschema:"seconds to wait for the mission to finish (default 0 = return at once)"`
	// Namespace of the mission; defaults to the gateway's namespace.
	Namespace string `json:"namespace,omitempty" jsonschema:"namespace of the mission; omit for the default"`
}

// MissionPlannerRequest is the planner part of StartMissionRequest.
type MissionPlannerRequest struct {
	// Knight is an existing Knight to plan with; omitted, the RoundTable's
	// planner is used, else (deprecated) the knight labelled
	// ai.roundtable.io/role=planner in the namespace.
	Knight     string `json:"knight,omitempty" jsonschema:"existing knight to plan with; omit for the table's planner knight"`
	Context    string `json:"context,omitempty" jsonschema:"extra information for the planner (max 64 KiB)"`
	MaxChains  int32  `json:"maxChains,omitempty" jsonschema:"most chains the planner may generate (1-20)"`
	MaxKnights int32  `json:"maxKnights,omitempty" jsonschema:"most ephemeral knights the planner may generate (1-50)"`
	// AllowSkillGeneration is rejected unless the gateway allows it.
	AllowSkillGeneration bool `json:"allowSkillGeneration,omitempty" jsonschema:"let the planner write new skills (only if the gateway allows it)"`
}

// MissionChainRequest names an existing Chain and its input for the mission.
type MissionChainRequest struct {
	Name  string `json:"name" jsonschema:"name of an existing chain"`
	Input string `json:"input,omitempty" jsonschema:"input for this run of the chain (max 64 KiB)"`
}

// MissionView is the fixed projection of a Mission the gateway returns. It
// holds status only: no spec, env, Secret or log fields.
type MissionView struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Phase     string `json:"phase"`
	// Caller is who started the mission through the gateway (the
	// ai.roundtable.io/caller annotation).
	Caller string `json:"caller,omitempty"`
	// Done is true once the mission reached a terminal phase.
	Done bool `json:"done"`
	// CreatedByGateway is true for missions this gateway launched (and may
	// delete).
	CreatedByGateway bool                `json:"createdByGateway"`
	Conditions       []ConditionView     `json:"conditions,omitempty"`
	Planning         *PlanningView       `json:"planning,omitempty"`
	Chains           []MissionChainView  `json:"chains,omitempty"`
	Knights          []MissionKnightView `json:"knights,omitempty"`
	Result           string              `json:"result,omitempty"`
	// TraceID is the mission's trace (status.traceID); omitted when empty or
	// not a valid trace ID.
	TraceID     string     `json:"traceId,omitempty"`
	TotalCost   string     `json:"totalCost,omitempty"`
	CreatedAt   time.Time  `json:"createdAt"`
	StartedAt   *time.Time `json:"startedAt,omitempty"`
	CompletedAt *time.Time `json:"completedAt,omitempty"`
	ExpiresAt   *time.Time `json:"expiresAt,omitempty"`
}

// ConditionView is a Mission condition.
type ConditionView struct {
	Type               string    `json:"type"`
	Status             string    `json:"status"`
	Reason             string    `json:"reason,omitempty"`
	Message            string    `json:"message,omitempty"`
	LastTransitionTime time.Time `json:"lastTransitionTime"`
}

// PlanningView summarises the planning phase of a meta-mission.
type PlanningView struct {
	CompletedAt      *time.Time `json:"completedAt,omitempty"`
	ChainsGenerated  int32      `json:"chainsGenerated"`
	KnightsGenerated int32      `json:"knightsGenerated"`
	SkillsGenerated  int32      `json:"skillsGenerated"`
	Error            string     `json:"error,omitempty"`
	// RawOutput is the start of the planner's raw output.
	RawOutput          string `json:"rawOutput,omitempty"`
	RawOutputTruncated bool   `json:"rawOutputTruncated,omitempty"`
}

// MissionChainView is one chain of a mission.
type MissionChainView struct {
	Name string `json:"name"`
	// Chain is the Chain resource running it.
	Chain string `json:"chain,omitempty"`
	Phase string `json:"phase,omitempty"`
	// Steps are the chain's step phases (get only, not list).
	Steps []MissionStepView `json:"steps,omitempty"`
}

// MissionStepView is one step of a mission chain.
type MissionStepView struct {
	Name  string `json:"name"`
	Phase string `json:"phase,omitempty"`
}

// MissionKnightView is one knight of a mission.
type MissionKnightView struct {
	Name           string `json:"name"`
	Ready          bool   `json:"ready"`
	TasksCompleted int64  `json:"tasksCompleted"`
	Ephemeral      bool   `json:"ephemeral"`
}

// MissionFilter narrows ListMissions.
type MissionFilter struct {
	Namespace string `json:"namespace,omitempty" jsonschema:"namespace to list; omit for the default"`
	Phase     string `json:"phase,omitempty" jsonschema:"only missions in this phase, e.g. Active or Succeeded"`
	Limit     int    `json:"limit,omitempty" jsonschema:"maximum number of missions to return (default 20, max 200)"`
}

// MissionsEnabled reports whether the mission endpoints are served.
func (s *Service) MissionsEnabled() bool { return s.Missions != nil }

func missionDone(p aiv1alpha1.MissionPhase) bool {
	switch p {
	case aiv1alpha1.MissionPhaseSucceeded, aiv1alpha1.MissionPhaseFailed,
		aiv1alpha1.MissionPhaseExpired, aiv1alpha1.MissionPhaseCleaningUp:
		return true
	}
	return false
}

// StartMission validates a restricted mission request, creates the Mission
// and optionally waits for it to finish.
func (s *Service) StartMission(ctx context.Context, req StartMissionRequest) (*MissionView, error) {
	if !s.MissionsEnabled() {
		return nil, fmt.Errorf("%w: missions are disabled on this gateway", ErrNotFound)
	}
	ns, err := s.namespace(req.Namespace)
	if err != nil {
		return nil, err
	}
	wait, err := s.missionWait(req.WaitSeconds)
	if err != nil {
		return nil, err
	}
	m, err := s.buildMission(ctx, ns, req)
	if err != nil {
		return nil, err
	}
	// A waiting call takes its slot before it creates anything, so a busy
	// gateway answers 429 without leaving a Mission behind.
	release := func() {}
	if wait > 0 {
		if release, err = s.acquireSlot(); err != nil {
			return nil, err
		}
	}
	defer release()
	if c := callerName(ctx); c != "" {
		if m.Annotations == nil {
			m.Annotations = map[string]string{}
		}
		m.Annotations[aiv1alpha1.AnnotationCaller] = c
	}
	if err := s.Missions.Create(ctx, m); err != nil {
		return nil, k8sError("create mission", err)
	}
	s.Log.Info("Started mission", "mission", m.Name, "namespace", ns, "roundTable", req.RoundTable,
		"metaMission", m.Spec.MetaMission, "budgetUSD", m.Spec.CostBudgetUSD, "caller", callerName(ctx))
	if wait > 0 {
		return s.pollMission(ctx, ns, m.Name, wait)
	}
	return s.missionView(ctx, m, true), nil
}

// BuildMission validates a mission request and turns it into a Mission
// (not yet created), checking every reference. The operator's Trigger
// mission target uses it too, so both paths validate the same fields; the
// caller sets the Mission's name, labels and owner.
func (s *Service) BuildMission(ctx context.Context, ns string, req StartMissionRequest) (*aiv1alpha1.Mission, error) {
	return s.buildMission(ctx, ns, req)
}

// buildMission turns a request into a Mission, checking every reference.
func (s *Service) buildMission(ctx context.Context, ns string, req StartMissionRequest) (*aiv1alpha1.Mission, error) {
	o := s.Opts.Missions
	switch {
	case strings.TrimSpace(req.Objective) == "":
		return nil, fmt.Errorf("%w: objective is required", ErrInvalid)
	case len(req.Objective) > maxObjectiveBytes:
		return nil, fmt.Errorf("%w: objective exceeds %d bytes", ErrInvalid, maxObjectiveBytes)
	case len(req.SuccessCriteria) > maxObjectiveBytes:
		return nil, fmt.Errorf("%w: successCriteria exceeds %d bytes", ErrInvalid, maxObjectiveBytes)
	case req.RoundTable == "":
		return nil, fmt.Errorf("%w: roundTable is required", ErrInvalid)
	case math.IsNaN(req.CostBudgetUSD) || req.CostBudgetUSD <= 0:
		return nil, fmt.Errorf("%w: costBudgetUSD is required and must be greater than 0", ErrInvalid)
	case req.CostBudgetUSD > o.MaxBudgetUSD:
		return nil, fmt.Errorf("%w: costBudgetUSD %g exceeds the gateway maximum %g", ErrInvalid, req.CostBudgetUSD, o.MaxBudgetUSD)
	case req.Planner == nil && len(req.Chains) == 0:
		return nil, fmt.Errorf("%w: give a planner or at least one chain", ErrInvalid)
	case len(req.Knights) > maxMissionKnights:
		return nil, fmt.Errorf("%w: at most %d knights", ErrInvalid, maxMissionKnights)
	case len(req.Chains) > maxMissionChains:
		return nil, fmt.Errorf("%w: at most %d chains", ErrInvalid, maxMissionChains)
	}
	ttl, err := missionSeconds("ttlSeconds", req.TTLSeconds, defaultMissionTTL, o.MaxTTLSeconds)
	if err != nil {
		return nil, err
	}
	timeout, err := missionSeconds("timeoutSeconds", req.TimeoutSeconds, defaultMissionTimeout, o.MaxTimeoutSeconds)
	if err != nil {
		return nil, err
	}

	rt := &aiv1alpha1.RoundTable{}
	if err := s.getObject(ctx, ns, req.RoundTable, "RoundTable", rt); err != nil {
		return nil, err
	}
	m := &aiv1alpha1.Mission{
		ObjectMeta: metav1.ObjectMeta{
			Name:      newMissionName(),
			Namespace: ns,
			Labels:    map[string]string{aiv1alpha1.LabelCreatedBy: aiv1alpha1.CreatedByGateway},
		},
		Spec: aiv1alpha1.MissionSpec{
			Objective:       req.Objective,
			SuccessCriteria: req.SuccessCriteria,
			RoundTableRef:   aiv1alpha1.LocalRef(req.RoundTable),
			CostBudgetUSD:   strconv.FormatFloat(req.CostBudgetUSD, 'f', -1, 64),
			TTLSeconds:      ttl,
			TimeoutSeconds:  timeout,
			RecruitExisting: req.RecruitExisting,
		},
	}

	seen := map[string]bool{}
	for _, name := range req.Knights {
		if seen[name] {
			return nil, fmt.Errorf("%w: knight %q listed twice", ErrInvalid, name)
		}
		seen[name] = true
		if err := s.getObject(ctx, ns, name, "knight", &aiv1alpha1.Knight{}); err != nil {
			return nil, err
		}
		m.Spec.Knights = append(m.Spec.Knights, aiv1alpha1.MissionKnight{Name: name})
	}
	for _, c := range req.Chains {
		if len(c.Input) > maxChainInputBytes {
			return nil, fmt.Errorf("%w: input of chain %q exceeds %d bytes", ErrInvalid, c.Name, maxChainInputBytes)
		}
		if err := s.getObject(ctx, ns, c.Name, "chain", &aiv1alpha1.Chain{}); err != nil {
			return nil, err
		}
		m.Spec.Chains = append(m.Spec.Chains, aiv1alpha1.MissionChainRef{Name: c.Name, InputOverride: c.Input})
	}

	if p := req.Planner; p != nil {
		switch {
		case len(p.Context) > maxPlannerContextBytes:
			return nil, fmt.Errorf("%w: planner.context exceeds %d bytes", ErrInvalid, maxPlannerContextBytes)
		case p.MaxChains != 0 && (p.MaxChains < 1 || p.MaxChains > maxMissionChains):
			return nil, fmt.Errorf("%w: planner.maxChains must be 1-%d", ErrInvalid, maxMissionChains)
		case p.MaxKnights != 0 && (p.MaxKnights < 1 || p.MaxKnights > maxMissionKnights):
			return nil, fmt.Errorf("%w: planner.maxKnights must be 1-%d", ErrInvalid, maxMissionKnights)
		case p.AllowSkillGeneration && !o.AllowSkillGeneration:
			return nil, fmt.Errorf("%w: planner.allowSkillGeneration is not allowed on this gateway", ErrInvalid)
		}
		plannerKnight := p.Knight
		if plannerKnight == "" {
			if plannerKnight, err = s.defaultPlanner(ctx, ns, rt); err != nil {
				return nil, err
			}
		} else if err := s.getObject(ctx, ns, plannerKnight, "planner knight", &aiv1alpha1.Knight{}); err != nil {
			return nil, err
		}
		m.Spec.MetaMission = true
		m.Spec.Planner = &aiv1alpha1.MissionPlanner{
			KnightRef:            aiv1alpha1.LocalRef(plannerKnight),
			Context:              p.Context,
			MaxChains:            p.MaxChains,
			MaxKnights:           p.MaxKnights,
			AllowSkillGeneration: p.AllowSkillGeneration,
		}
	}
	return m, nil
}

// missionSeconds validates an optional ttl/timeout against the CRD minimum
// and the gateway's cap; 0 means the default (capped).
func missionSeconds(field string, v, def, maxV int32) (int32, error) {
	if v == 0 {
		return min(def, maxV), nil
	}
	if v < minMissionSeconds || v > maxV {
		return 0, fmt.Errorf("%w: %s must be %d-%d", ErrInvalid, field, minMissionSeconds, maxV)
	}
	return v, nil
}

// getObject fetches a named object, mapping a bad name or a miss to 400:
// in a request body, a missing reference is the caller's mistake.
func (s *Service) getObject(ctx context.Context, ns, name, kind string, obj client.Object) error {
	if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 {
		return fmt.Errorf("%w: %s name %q: %s", ErrInvalid, kind, name, strings.Join(errs, "; "))
	}
	if err := s.Resolver.Client.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, obj); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("%w: %s %s/%s not found", ErrInvalid, kind, ns, name)
		}
		return fmt.Errorf("get %s %s/%s: %w", kind, ns, name, err)
	}
	return nil
}

// defaultPlanner is the mission's RoundTable planner (#566), else
// (deprecated) the knight labelled ai.roundtable.io/role=planner that belongs
// to no other table.
func (s *Service) defaultPlanner(ctx context.Context, ns string, rt *aiv1alpha1.RoundTable) (string, error) {
	if name := rt.PlannerKnightName(); name != "" {
		return name, nil
	}
	var list aiv1alpha1.KnightList
	if err := s.Resolver.Client.List(ctx, &list, client.InNamespace(ns),
		client.MatchingLabels{plannerRoleLabel: "planner"}); err != nil {
		return "", fmt.Errorf("list planner knights in %s: %w", ns, err)
	}
	names := make([]string, 0, len(list.Items))
	for _, k := range list.Items {
		if kt := k.Labels[aiv1alpha1.LabelRoundTable]; k.Labels[aiv1alpha1.LabelEphemeral] == labelTrue || (kt != "" && kt != rt.Name) {
			continue
		}
		names = append(names, k.Name)
	}
	if len(names) == 0 {
		return "", fmt.Errorf("%w: RoundTable %s has no planner and no knight is labelled %s=planner in %s; set planner.knight",
			ErrInvalid, rt.Name, plannerRoleLabel, ns)
	}
	slices.Sort(names)
	return names[0], nil
}

func (s *Service) missionWait(seconds *int) (time.Duration, error) {
	if seconds == nil {
		return 0, nil
	}
	if *seconds < 0 {
		return 0, fmt.Errorf("%w: waitSeconds must be >= 0", ErrInvalid)
	}
	return min(time.Duration(*seconds)*time.Second, s.Opts.MaxTimeout), nil
}

// GetMission returns a mission's projection, waiting up to waitSeconds for
// it to reach a terminal phase.
func (s *Service) GetMission(ctx context.Context, namespace, name string, waitSeconds *int) (*MissionView, error) {
	if !s.MissionsEnabled() {
		return nil, fmt.Errorf("%w: missions are disabled on this gateway", ErrNotFound)
	}
	ns, err := s.namespace(namespace)
	if err != nil {
		return nil, err
	}
	wait, err := s.missionWait(waitSeconds)
	if err != nil {
		return nil, err
	}
	if wait > 0 {
		return s.waitMission(ctx, ns, name, wait)
	}
	m, err := s.fetchMission(ctx, ns, name)
	if err != nil {
		return nil, err
	}
	return s.missionView(ctx, m, true), nil
}

func (s *Service) fetchMission(ctx context.Context, ns, name string) (*aiv1alpha1.Mission, error) {
	if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 {
		return nil, fmt.Errorf("%w: mission %s/%s", ErrNotFound, ns, name)
	}
	m := &aiv1alpha1.Mission{}
	if err := s.Missions.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, m); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("%w: mission %s/%s", ErrNotFound, ns, name)
		}
		return nil, k8sError("get mission", err)
	}
	return m, nil
}

// waitMission polls the Mission until it is done or the wait ends, then
// returns its latest projection. Waits count against maxInFlight.
func (s *Service) waitMission(ctx context.Context, ns, name string, wait time.Duration) (*MissionView, error) {
	release, err := s.acquireSlot()
	if err != nil {
		return nil, err
	}
	defer release()
	return s.pollMission(ctx, ns, name, wait)
}

// pollMission is waitMission for a caller that already holds a slot.
func (s *Service) pollMission(ctx context.Context, ns, name string, wait time.Duration) (*MissionView, error) {
	wctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	tick := time.NewTicker(s.Opts.Missions.PollInterval)
	defer tick.Stop()
	for {
		m, err := s.fetchMission(ctx, ns, name)
		if err != nil {
			return nil, err
		}
		if missionDone(m.Status.Phase) {
			return s.missionView(ctx, m, true), nil
		}
		select {
		case <-wctx.Done():
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return s.missionView(ctx, m, true), nil
		case <-tick.C:
		}
	}
}

// ListMissions lists missions in a namespace, newest first, without step
// details (get one mission for those).
func (s *Service) ListMissions(ctx context.Context, f MissionFilter) ([]*MissionView, error) {
	if !s.MissionsEnabled() {
		return nil, fmt.Errorf("%w: missions are disabled on this gateway", ErrNotFound)
	}
	ns, err := s.namespace(f.Namespace)
	if err != nil {
		return nil, err
	}
	var list aiv1alpha1.MissionList
	if err := s.Missions.List(ctx, &list, client.InNamespace(ns)); err != nil {
		return nil, k8sError("list missions", err)
	}
	limit := f.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}
	limit = min(limit, maxListLimit)
	items := list.Items
	slices.SortFunc(items, func(a, b aiv1alpha1.Mission) int {
		if c := b.CreationTimestamp.Compare(a.CreationTimestamp.Time); c != 0 {
			return c
		}
		return strings.Compare(a.Name, b.Name)
	})
	out := make([]*MissionView, 0, min(len(items), limit))
	for i := range items {
		v := s.missionView(ctx, &items[i], false)
		if f.Phase != "" && !strings.EqualFold(v.Phase, f.Phase) {
			continue
		}
		out = append(out, v)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

// DeleteMission deletes a mission this gateway created; others are 403.
func (s *Service) DeleteMission(ctx context.Context, namespace, name string) error {
	if !s.MissionsEnabled() {
		return fmt.Errorf("%w: missions are disabled on this gateway", ErrNotFound)
	}
	ns, err := s.namespace(namespace)
	if err != nil {
		return err
	}
	m, err := s.fetchMission(ctx, ns, name)
	if err != nil {
		return err
	}
	if m.Labels[aiv1alpha1.LabelCreatedBy] != aiv1alpha1.CreatedByGateway {
		return fmt.Errorf("%w: mission %s/%s was not created by the gateway", ErrForbidden, ns, name)
	}
	// Precondition on the UID so a same-named replacement isn't deleted.
	if err := s.Missions.Delete(ctx, m, client.Preconditions{UID: &m.UID},
		client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("%w: mission %s/%s", ErrNotFound, ns, name)
		}
		return k8sError("delete mission", err)
	}
	s.Log.Info("Deleted mission", "mission", name, "namespace", ns)
	return nil
}

// missionView projects a Mission. withSteps also reads each chain's step
// phases from its Chain resource.
func (s *Service) missionView(ctx context.Context, m *aiv1alpha1.Mission, withSteps bool) *MissionView {
	st := &m.Status
	v := &MissionView{
		Name:             m.Name,
		Namespace:        m.Namespace,
		Phase:            string(st.Phase),
		Caller:           m.Annotations[aiv1alpha1.AnnotationCaller],
		Done:             missionDone(st.Phase),
		CreatedByGateway: m.Labels[aiv1alpha1.LabelCreatedBy] == aiv1alpha1.CreatedByGateway,
		Result:           st.Result,
		TraceID:          validTraceID(st.TraceID),
		TotalCost:        st.TotalCost,
		CreatedAt:        m.CreationTimestamp.UTC(),
		StartedAt:        timePtr(st.StartedAt),
		CompletedAt:      timePtr(st.CompletedAt),
		ExpiresAt:        timePtr(st.ExpiresAt),
	}
	if v.Phase == "" {
		v.Phase = string(aiv1alpha1.MissionPhasePending)
	}
	for _, c := range st.Conditions {
		v.Conditions = append(v.Conditions, ConditionView{
			Type: c.Type, Status: string(c.Status), Reason: c.Reason, Message: c.Message,
			LastTransitionTime: c.LastTransitionTime.UTC(),
		})
	}
	if pr := st.PlanningResult; pr != nil {
		raw, cut := truncate(pr.RawOutput, maxRawPlannerOutput)
		v.Planning = &PlanningView{
			CompletedAt:        timePtr(pr.CompletedAt),
			ChainsGenerated:    pr.ChainsGenerated,
			KnightsGenerated:   pr.KnightsGenerated,
			SkillsGenerated:    pr.SkillsGenerated,
			Error:              pr.Error,
			RawOutput:          raw,
			RawOutputTruncated: cut,
		}
	}
	for _, cs := range st.ChainStatuses {
		cv := MissionChainView{Name: cs.Name, Chain: cs.ChainCRName, Phase: string(cs.Phase)}
		if withSteps && cs.ChainCRName != "" {
			ch := &aiv1alpha1.Chain{}
			err := s.Resolver.Client.Get(ctx, types.NamespacedName{Namespace: m.Namespace, Name: cs.ChainCRName}, ch)
			if err == nil {
				for _, ss := range ch.Status.StepStatuses {
					cv.Steps = append(cv.Steps, MissionStepView{Name: ss.Name, Phase: string(ss.Phase)})
				}
			} else if !apierrors.IsNotFound(err) {
				s.Log.Error(err, "Failed to read mission chain", "chain", cs.ChainCRName, "namespace", m.Namespace)
			}
		}
		v.Chains = append(v.Chains, cv)
	}
	for _, ks := range st.KnightStatuses {
		v.Knights = append(v.Knights, MissionKnightView{
			Name: ks.Name, Ready: ks.Ready, TasksCompleted: ks.TasksCompleted, Ephemeral: ks.Ephemeral,
		})
	}
	return v
}

func timePtr(t *metav1.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

// truncate cuts s to at most n bytes on a rune boundary.
func truncate(s string, n int) (string, bool) {
	if len(s) <= n {
		return s, false
	}
	cut := n
	for cut > 0 && !utf8RuneStart(s[cut]) {
		cut--
	}
	return s[:cut], true
}

func utf8RuneStart(b byte) bool { return b&0xC0 != 0x80 }

// k8sError maps Kubernetes API errors: invalid objects are the caller's
// fault (400), the rest are server-side.
func k8sError(op string, err error) error {
	switch {
	case apierrors.IsInvalid(err), apierrors.IsBadRequest(err):
		return fmt.Errorf("%w: %s: %v", ErrInvalid, op, err)
	case apierrors.IsForbidden(err):
		return fmt.Errorf("%w: %s: the gateway's RBAC does not allow it: %v", ErrUnavailable, op, err)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	}
	return fmt.Errorf("%s: %w", op, err)
}

// newMissionName returns "gw-<8 hex>": short, because the operator derives
// knight, stream and RoundTable names from it.
func newMissionName() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return "gw-" + hex.EncodeToString(b[:])
}
