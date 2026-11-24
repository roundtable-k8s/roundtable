package controller

import (
	"strings"
	"testing"
	"time"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func scheduleChain(schedule, tz string) *aiv1alpha1.Chain {
	return &aiv1alpha1.Chain{Spec: aiv1alpha1.ChainSpec{Schedule: schedule, TimeZone: tz}}
}

func TestParseScheduleTimeZoneAcrossDST(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	sched, err := parseSchedule(scheduleChain("0 7 * * *", "America/New_York"))
	if err != nil {
		t.Fatal(err)
	}

	// US DST began 2026-03-08 (clocks 02:00 -> 03:00).
	// Fake clock: walk Next() from before the transition to after it.
	now := time.Date(2026, 3, 6, 12, 0, 0, 0, time.UTC)
	wantUTC := []time.Time{
		time.Date(2026, 3, 7, 12, 0, 0, 0, time.UTC), // EST, UTC-5
		time.Date(2026, 3, 8, 11, 0, 0, 0, time.UTC), // EDT, UTC-4
		time.Date(2026, 3, 9, 11, 0, 0, 0, time.UTC),
	}
	for _, want := range wantUTC {
		now = sched.Next(now)
		if !now.Equal(want) {
			t.Fatalf("next = %s, want %s", now.UTC(), want)
		}
		if h := now.In(ny).Hour(); h != 7 {
			t.Errorf("local hour = %d, want 7 (%s)", h, now.In(ny))
		}
	}

	// And the fall-back transition (2026-11-01).
	now = time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	for _, want := range []time.Time{
		time.Date(2026, 10, 31, 11, 0, 0, 0, time.UTC), // EDT
		time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC),  // EST
	} {
		now = sched.Next(now)
		if !now.Equal(want) {
			t.Fatalf("next = %s, want %s", now.UTC(), want)
		}
	}
}

func TestParseScheduleDefaultsToUTC(t *testing.T) {
	sched, err := parseSchedule(scheduleChain("0 7 * * *", ""))
	if err != nil {
		t.Fatal(err)
	}
	got := sched.Next(time.Date(2026, 3, 8, 0, 0, 0, 0, time.UTC))
	if want := time.Date(2026, 3, 8, 7, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("next = %s, want %s", got, want)
	}
}

func TestParseScheduleInvalid(t *testing.T) {
	for _, tc := range []struct{ name, schedule, tz, want string }{
		{"unknown zone", "0 7 * * *", "Mars/Olympus", "invalid timeZone"},
		{"local", "0 7 * * *", "Local", "invalid timeZone"},
		{"bad cron", "nope", "UTC", "invalid schedule"},
		{"inline TZ", "CRON_TZ=UTC 0 7 * * *", "", "spec.timeZone"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseSchedule(scheduleChain(tc.schedule, tc.tz))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestParseScheduleEmpty(t *testing.T) {
	sched, err := parseSchedule(scheduleChain("", ""))
	if err != nil || sched != nil {
		t.Errorf("got (%v, %v), want (nil, nil)", sched, err)
	}
}
