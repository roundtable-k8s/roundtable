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
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

const (
	// defaultNextCount is how many fire times are returned when count is unset.
	defaultNextCount = 5
	// maxNextCount is the most fire times one request may ask for.
	maxNextCount = 50
)

// ScheduleFireTime is one upcoming cron tick.
type ScheduleFireTime struct {
	// Time is the tick in UTC.
	Time time.Time `json:"time"`
	// Local is the same instant in the schedule's time zone, RFC 3339.
	Local string `json:"local"`
}

// ScheduleNextRuns lists the upcoming ticks of a cron expression. It lists
// cron ticks only: skip rules (concurrency policy, deadlines) are not applied.
type ScheduleNextRuns struct {
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name,omitempty"`
	Cron      string `json:"cron"`
	// TimeZone is the IANA zone the cron is evaluated in (UTC when unset).
	TimeZone string `json:"timeZone"`
	// Suspended is true for a suspended schedule; it still answers.
	Suspended bool               `json:"suspended,omitempty"`
	Times     []ScheduleFireTime `json:"times"`
}

// NextRunsInput is the input of the schedule_next_runs tool.
type NextRunsInput struct {
	Namespace string `json:"namespace,omitempty" jsonschema:"namespace of the schedule; omit for the default"`
	Name      string `json:"name,omitempty" jsonschema:"name of an existing schedule; give this or cron"`
	Cron      string `json:"cron,omitempty" jsonschema:"a cron expression to preview instead of a schedule"`
	TimeZone  string `json:"timeZone,omitempty" jsonschema:"IANA time zone for cron; default UTC"`
	Count     int    `json:"count,omitempty" jsonschema:"how many fire times (default 5, max 50)"`
}

// nextFireTimes computes the next count ticks of expr in the IANA zone tz
// (UTC when empty) strictly after now. A bad expression or zone wraps
// ErrUnprocessable and carries the parser's message; count outside 1..50
// wraps ErrInvalid.
func nextFireTimes(expr, tz string, count int, now time.Time) (*ScheduleNextRuns, error) {
	if count < 1 || count > maxNextCount {
		return nil, fmt.Errorf("%w: count: must be between 1 and %d", ErrInvalid, maxNextCount)
	}
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, fmt.Errorf("%w: cron is required", ErrUnprocessable)
	}
	if strings.Contains(expr, "TZ=") {
		return nil, fmt.Errorf("%w: invalid schedule %q: set the time zone with timeZone instead of a TZ= prefix", ErrUnprocessable, expr)
	}
	if tz == "" {
		tz = defaultScheduleTimeZone
	}
	if tz == "Local" {
		return nil, fmt.Errorf("%w: invalid timeZone %q: must be an IANA time zone name such as \"America/New_York\"", ErrUnprocessable, tz)
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid timeZone %q: %v", ErrUnprocessable, tz, err)
	}
	sched, err := cron.ParseStandard("CRON_TZ=" + tz + " " + expr)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid schedule %q: %v", ErrUnprocessable, expr, err)
	}
	out := &ScheduleNextRuns{Cron: expr, TimeZone: tz, Times: make([]ScheduleFireTime, 0, count)}
	t := now
	for range count {
		t = sched.Next(t)
		if t.IsZero() {
			break // no further tick (e.g. a date that never occurs)
		}
		out.Times = append(out.Times, ScheduleFireTime{Time: t.UTC(), Local: t.In(loc).Format(time.RFC3339)})
	}
	return out, nil
}

// ScheduleNext returns the next count fire times of a stored schedule, from
// its cron and time zone. A suspended schedule still answers.
func (s *Service) ScheduleNext(ctx context.Context, namespace, name string, count int) (*ScheduleNextRuns, error) {
	t, err := s.fetchSchedule(ctx, namespace, name)
	if err != nil {
		return nil, err
	}
	src := t.Spec.Source.Schedule
	out, err := nextFireTimes(src.Cron, src.TimeZone, count, s.now())
	if err != nil {
		return nil, err
	}
	out.Namespace, out.Name, out.Suspended = t.Namespace, t.Name, t.Spec.Suspend
	return out, nil
}

// SchedulePreview returns the next count fire times of an expression that need
// not exist as a schedule yet.
func (s *Service) SchedulePreview(expr, tz string, count int) (*ScheduleNextRuns, error) {
	return nextFireTimes(expr, tz, count, s.now())
}

// queryCount parses the optional count query parameter.
func queryCount(r *http.Request) (int, error) {
	v := r.URL.Query().Get("count")
	if v == "" {
		return defaultNextCount, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%w: count: %v", ErrInvalid, err)
	}
	return n, nil
}

func (s *Service) handleScheduleNext(w http.ResponseWriter, r *http.Request) {
	count, err := queryCount(r)
	if err != nil {
		writeError(w, err)
		return
	}
	out, err := s.ScheduleNext(r.Context(), r.PathValue("namespace"), r.PathValue("name"), count)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Service) handleSchedulePreview(w http.ResponseWriter, r *http.Request) {
	count, err := queryCount(r)
	if err != nil {
		writeError(w, err)
		return
	}
	q := r.URL.Query()
	out, err := s.SchedulePreview(q.Get("cron"), q.Get("timeZone"), count)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}
