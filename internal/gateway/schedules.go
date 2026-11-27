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
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// ManagedBy says who owns a schedule's Trigger, and so whether the gateway
// may change it.
type ManagedBy string

const (
	// ManagedByGit is a Trigger applied by Flux or Helm: Git is its source of
	// truth, so the API must not replace or delete it.
	ManagedByGit ManagedBy = "git"
	// ManagedByAPI is a Trigger the gateway created.
	ManagedByAPI ManagedBy = "api"
	// ManagedByOther is anything else (kubectl, another tool).
	ManagedByOther ManagedBy = "other"
)

// defaultScheduleTimeZone is the time zone of a schedule that sets none.
const defaultScheduleTimeZone = "UTC"

// Ownership labels and annotations read by ScheduleManagedBy.
const (
	labelManagedBy        = "app.kubernetes.io/managed-by"
	managedByHelm         = "Helm"
	managedByGateway      = "roundtable-gateway"
	fluxKustomizationName = "kustomize.toolkit.fluxcd.io/name"
	fluxHelmReleaseName   = "helm.toolkit.fluxcd.io/name"

	lastAppliedAnnotation = "kubectl.kubernetes.io/last-applied-configuration"

	// ScheduleTargetChain is the target kind of a schedule that starts a
	// Chain run.
	ScheduleTargetChain = "chain"
	// ScheduleTargetKnight is the target kind of a schedule that sends one
	// task to a knight.
	ScheduleTargetKnight = "knight"
)

// ScheduleManagedBy reports who manages a Trigger: git when it carries Flux
// or Helm ownership labels or annotations, api when the gateway created it
// (app.kubernetes.io/managed-by: roundtable-gateway), other otherwise.
func ScheduleManagedBy(t *aiv1alpha1.Trigger) ManagedBy {
	for _, m := range []map[string]string{t.Labels, t.Annotations} {
		if m[fluxKustomizationName] != "" || m[fluxHelmReleaseName] != "" || m[labelManagedBy] == managedByHelm {
			return ManagedByGit
		}
	}
	if t.Labels[labelManagedBy] == managedByGateway {
		return ManagedByAPI
	}
	return ManagedByOther
}

// isSchedule reports whether a Trigger has a schedule source.
func isSchedule(t *aiv1alpha1.Trigger) bool { return t.Spec.Source.Schedule != nil }

// ScheduleView is what the gateway returns for a schedule: a Trigger with a
// schedule source.
type ScheduleView struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	// Table is the RoundTable: the Trigger's roundTableRef, else its target
	// Chain's.
	Table string `json:"table,omitempty"`
	Cron  string `json:"cron"`
	// TimeZone is the IANA zone the cron is evaluated in (UTC when unset).
	TimeZone string `json:"timeZone"`
	// Description is the cron in words, e.g. "at 07:00 on Monday to Friday".
	Description string         `json:"description"`
	Target      ScheduleTarget `json:"target"`
	Suspended   bool           `json:"suspended"`
	// NextRunAt is status.nextScheduledAt.
	NextRunAt *time.Time `json:"nextRunAt,omitempty"`
	// LastRunAt is status.lastFiredAt: when a firing last started a run.
	LastRunAt *time.Time `json:"lastRunAt,omitempty"`
	// LastResult is the newest entry of status.recentFirings.
	LastResult *ScheduleFiring `json:"lastResult,omitempty"`
	ManagedBy  ManagedBy       `json:"managedBy"`
}

// ScheduleTarget is what a schedule starts.
type ScheduleTarget struct {
	// Kind is "chain" or "knight" (a mission comes later).
	Kind string `json:"kind"`
	// Summary is a short description of the target: "chain <name>", or
	// "knight <name>: <first line of the task>" (the line cut to 80 characters).
	Summary string `json:"summary"`
	// Chain is the target Chain (kind chain).
	Chain string `json:"chain,omitempty"`
	// Knight is the target Knight (kind knight).
	Knight string `json:"knight,omitempty"`
}

// ScheduleFiring is one firing of a schedule.
type ScheduleFiring struct {
	Time time.Time `json:"time"`
	// Outcome is "started" when the firing started a run, "skipped" when it
	// did not, and, for knight targets once the task has ended, the final
	// result: "succeeded", "failed" or "lost".
	Outcome    string `json:"outcome"`
	Run        string `json:"run,omitempty"`
	SkipReason string `json:"skipReason,omitempty"`
}

// ScheduleFilter narrows ListSchedules.
type ScheduleFilter struct {
	Namespace string `json:"namespace,omitempty"`
	Table     string `json:"table,omitempty"`
}

// SchedulesEnabled reports whether the schedule endpoints are served.
func (s *Service) SchedulesEnabled() bool { return s.Schedules != nil }

var errSchedulesDisabled = fmt.Errorf("%w: schedules are disabled on this gateway", ErrNotFound)

// ListSchedules lists the schedules in a namespace, by name.
func (s *Service) ListSchedules(ctx context.Context, f ScheduleFilter) ([]*ScheduleView, error) {
	if !s.SchedulesEnabled() {
		return nil, errSchedulesDisabled
	}
	ns, err := s.namespace(f.Namespace)
	if err != nil {
		return nil, err
	}
	var list aiv1alpha1.TriggerList
	if err := s.Schedules.List(ctx, &list, client.InNamespace(ns)); err != nil {
		return nil, k8sError("list triggers", err)
	}
	tables := s.chainTables(ctx, ns)
	knightTable := s.knightTables(ctx, ns)
	out := make([]*ScheduleView, 0, len(list.Items))
	for i := range list.Items {
		t := &list.Items[i]
		if !isSchedule(t) {
			continue
		}
		v := scheduleView(t, tables, knightTable)
		if f.Table != "" && v.Table != f.Table {
			continue
		}
		out = append(out, v)
	}
	slices.SortFunc(out, func(a, b *ScheduleView) int { return strings.Compare(a.Name, b.Name) })
	return out, nil
}

// GetSchedule returns one schedule.
func (s *Service) GetSchedule(ctx context.Context, namespace, name string) (*ScheduleView, error) {
	t, err := s.fetchSchedule(ctx, namespace, name)
	if err != nil {
		return nil, err
	}
	return scheduleView(t, s.chainTables(ctx, t.Namespace), s.knightTables(ctx, t.Namespace)), nil
}

// ScheduleManifest returns a schedule's Trigger as YAML without status or
// server-set metadata, ready to commit to Git.
func (s *Service) ScheduleManifest(ctx context.Context, namespace, name string) ([]byte, error) {
	t, err := s.fetchSchedule(ctx, namespace, name)
	if err != nil {
		return nil, err
	}
	return scheduleManifest(t)
}

// fetchSchedule reads a Trigger with a schedule source; anything else is a
// 404.
func (s *Service) fetchSchedule(ctx context.Context, namespace, name string) (*aiv1alpha1.Trigger, error) {
	if !s.SchedulesEnabled() {
		return nil, errSchedulesDisabled
	}
	ns, err := s.namespace(namespace)
	if err != nil {
		return nil, err
	}
	t := &aiv1alpha1.Trigger{}
	if err := s.Schedules.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, t); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("%w: schedule %s/%s", ErrNotFound, ns, name)
		}
		return nil, k8sError("get trigger", err)
	}
	if !isSchedule(t) {
		return nil, fmt.Errorf("%w: schedule %s/%s", ErrNotFound, ns, name)
	}
	return t, nil
}

// chainTables maps the Chains of a namespace to their RoundTable, for
// schedules without a roundTableRef of their own. Best effort: a failed list
// leaves those tables empty.
func (s *Service) chainTables(ctx context.Context, ns string) map[string]string {
	var list aiv1alpha1.ChainList
	if err := s.Schedules.List(ctx, &list, client.InNamespace(ns)); err != nil {
		s.Log.V(1).Info("Unable to list chains for schedule tables", "namespace", ns, "error", err.Error())
		return nil
	}
	out := make(map[string]string, len(list.Items))
	for i := range list.Items {
		if rt := aiv1alpha1.RefName(list.Items[i].Spec.RoundTableRef); rt != "" {
			out[list.Items[i].Name] = rt
		}
	}
	return out
}

// knightTables returns a memoised lookup of a knight's RoundTable, for
// schedules without a roundTableRef. Best effort: an unknown knight or a
// failed lookup yields "".
func (s *Service) knightTables(ctx context.Context, ns string) func(string) string {
	cache := map[string]string{}
	return func(name string) string {
		if tbl, ok := cache[name]; ok {
			return tbl
		}
		tbl, err := s.Resolver.KnightTable(ctx, ns, name)
		if err != nil {
			s.Log.V(1).Info("Unable to resolve knight table for schedule", "namespace", ns, "knight", name, "error", err.Error())
			tbl = ""
		}
		cache[name] = tbl
		return tbl
	}
}

// knightSummary is "knight <name>: <first non-empty line of the task>", the
// line cut to 80 characters.
func knightSummary(name, task string) string {
	for _, line := range strings.Split(task, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if r := []rune(line); len(r) > 80 {
			line = string(r[:80])
		}
		return "knight " + name + ": " + line
	}
	return "knight " + name
}

// scheduleView renders a Trigger. knightTable (may be nil) resolves a knight
// target's table when the Trigger has no roundTableRef.
func scheduleView(t *aiv1alpha1.Trigger, chainTables map[string]string, knightTable func(string) string) *ScheduleView {
	src := t.Spec.Source.Schedule
	var target ScheduleTarget
	var chain string
	switch {
	case t.Spec.Target.ChainRef != nil:
		chain = t.Spec.Target.ChainRef.Name
		target = ScheduleTarget{Kind: ScheduleTargetChain, Summary: "chain " + chain, Chain: chain}
	case t.Spec.Target.Knight != nil:
		k := t.Spec.Target.Knight.Name
		target = ScheduleTarget{Kind: ScheduleTargetKnight, Summary: knightSummary(k, t.Spec.Target.Knight.Task), Knight: k}
	}
	v := &ScheduleView{
		Name:        t.Name,
		Namespace:   t.Namespace,
		Table:       aiv1alpha1.RefName(t.Spec.RoundTableRef),
		Cron:        src.Cron,
		TimeZone:    src.TimeZone,
		Description: DescribeCron(src.Cron),
		Target:      target,
		Suspended:   t.Spec.Suspend,
		ManagedBy:   ScheduleManagedBy(t),
	}
	if v.Table == "" && chain != "" {
		v.Table = chainTables[chain]
	}
	if v.Table == "" && target.Kind == ScheduleTargetKnight && knightTable != nil {
		v.Table = knightTable(target.Knight)
	}
	if v.TimeZone == "" {
		v.TimeZone = defaultScheduleTimeZone
	}
	if ts := t.Status.NextScheduledAt; ts != nil {
		n := ts.UTC()
		v.NextRunAt = &n
	}
	if ts := t.Status.LastFiredAt; ts != nil {
		l := ts.UTC()
		v.LastRunAt = &l
	}
	if len(t.Status.RecentFirings) > 0 {
		f := t.Status.RecentFirings[0]
		r := &ScheduleFiring{Time: f.Time.UTC(), Run: f.Run, SkipReason: f.SkipReason, Outcome: "started"}
		if f.SkipReason != "" || f.Run == "" {
			r.Outcome = "skipped"
		}
		if f.Outcome != "" {
			r.Outcome = string(f.Outcome)
		}
		v.LastResult = r
	}
	return v
}

// scheduleManifest renders a Trigger as clean YAML: no status, no managed
// fields and none of the metadata the API server sets.
func scheduleManifest(in *aiv1alpha1.Trigger) ([]byte, error) {
	t := in.DeepCopy()
	t.APIVersion = aiv1alpha1.GroupVersion.String()
	t.Kind = "Trigger"
	t.Status = aiv1alpha1.TriggerStatus{}
	t.ManagedFields = nil
	t.ResourceVersion = ""
	t.UID = ""
	t.Generation = 0
	t.CreationTimestamp = metav1.Time{}
	t.Finalizers = nil
	t.OwnerReferences = nil
	delete(t.Annotations, lastAppliedAnnotation)
	if len(t.Annotations) == 0 {
		t.Annotations = nil
	}
	b, err := yaml.Marshal(t)
	if err != nil {
		return nil, fmt.Errorf("render trigger %s/%s: %w", t.Namespace, t.Name, err)
	}
	return b, nil
}

// DescribeCron puts a five-field cron expression (or a @ descriptor) in
// words. Expressions it cannot describe come back as "cron <expr>".
func DescribeCron(expr string) string {
	expr = strings.TrimSpace(expr)
	switch expr {
	case "@yearly", "@annually":
		return "once a year, at 00:00 on 1 January"
	case "@monthly":
		return "at 00:00 on day 1 of every month"
	case "@weekly":
		return "at 00:00 on Sunday"
	case "@daily", "@midnight":
		return "every day at 00:00"
	case "@hourly":
		return "every hour"
	}
	if d, ok := strings.CutPrefix(expr, "@every "); ok {
		return "every " + d
	}
	f := strings.Fields(expr)
	if len(f) != 5 {
		return "cron " + expr
	}
	minute, hour, dom, month, dow := f[0], f[1], f[2], f[3], f[4]
	timePart, ok := describeTime(minute, hour)
	if !ok {
		return "cron " + expr
	}
	var parts []string
	parts = append(parts, timePart)
	if dom != "*" {
		days, ok := describeList(dom, nil)
		if !ok {
			return "cron " + expr
		}
		parts = append(parts, "on day "+days+" of the month")
	}
	if month != "*" {
		months, ok := describeList(month, monthNames)
		if !ok {
			return "cron " + expr
		}
		parts = append(parts, "in "+months)
	}
	if dow != "*" {
		days, ok := describeList(dow, dayNames)
		if !ok {
			return "cron " + expr
		}
		parts = append(parts, "on "+days)
	}
	if dom == "*" && month == "*" && dow == "*" && strings.HasPrefix(timePart, "at ") {
		return "every day " + timePart
	}
	return strings.Join(parts, " ")
}

var (
	dayNames = map[string]string{
		"0": "Sunday", "1": "Monday", "2": "Tuesday", "3": "Wednesday", "4": "Thursday",
		"5": "Friday", "6": "Saturday", "7": "Sunday",
		"SUN": "Sunday", "MON": "Monday", "TUE": "Tuesday", "WED": "Wednesday",
		"THU": "Thursday", "FRI": "Friday", "SAT": "Saturday",
	}
	monthNames = map[string]string{
		"1": "January", "2": "February", "3": "March", "4": "April", "5": "May", "6": "June",
		"7": "July", "8": "August", "9": "September", "10": "October", "11": "November", "12": "December",
		"JAN": "January", "FEB": "February", "MAR": "March", "APR": "April", "MAY": "May", "JUN": "June",
		"JUL": "July", "AUG": "August", "SEP": "September", "OCT": "October", "NOV": "November", "DEC": "December",
	}
)

// describeTime describes the minute and hour fields.
func describeTime(minute, hour string) (string, bool) {
	m, mErr := strconv.Atoi(minute)
	h, hErr := strconv.Atoi(hour)
	mOK := mErr == nil && m >= 0 && m < 60
	hOK := hErr == nil && h >= 0 && h < 24
	switch {
	case mOK && hOK:
		return fmt.Sprintf("at %02d:%02d", h, m), true
	case minute == "*" && hour == "*":
		return "every minute", true
	case strings.HasPrefix(minute, "*/") && hour == "*":
		if n, err := strconv.Atoi(minute[2:]); err == nil && n > 0 {
			return fmt.Sprintf("every %d minutes", n), true
		}
	case mOK && hour == "*":
		return fmt.Sprintf("every hour at minute %d", m), true
	case mOK && strings.HasPrefix(hour, "*/"):
		if n, err := strconv.Atoi(hour[2:]); err == nil && n > 0 {
			return fmt.Sprintf("every %d hours at minute %d", n, m), true
		}
	case mOK:
		if hours, ok := describeList(hour, nil); ok {
			return fmt.Sprintf("at minute %d past hour %s", m, hours), true
		}
	}
	return "", false
}

// describeList describes a list of values and ranges ("1-5", "1,15",
// "MON-FRI"), naming them with names when given.
func describeList(field string, names map[string]string) (string, bool) {
	name := func(v string) (string, bool) {
		if names == nil {
			if _, err := strconv.Atoi(v); err != nil {
				return "", false
			}
			return v, true
		}
		n, ok := names[strings.ToUpper(v)]
		return n, ok
	}
	var out []string
	for item := range strings.SplitSeq(field, ",") {
		lo, hi, isRange := strings.Cut(item, "-")
		a, ok := name(lo)
		if !ok {
			return "", false
		}
		if !isRange {
			out = append(out, a)
			continue
		}
		b, ok := name(hi)
		if !ok {
			return "", false
		}
		out = append(out, a+" to "+b)
	}
	switch len(out) {
	case 1:
		return out[0], true
	default:
		return strings.Join(out[:len(out)-1], ", ") + " and " + out[len(out)-1], true
	}
}
