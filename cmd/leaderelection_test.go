package main

import (
	"errors"
	"strings"
	"testing"
	"time"

	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func TestLeaderElectionTimingsValidate(t *testing.T) {
	s := time.Second
	tests := []struct {
		name    string
		t       leaderElectionTimings
		wantErr string
	}{
		{"defaults", leaderElectionTimings{60 * s, 40 * s, 5 * s}, ""},
		{"controller-runtime defaults", leaderElectionTimings{15 * s, 10 * s, 2 * s}, ""},
		{"renew equals lease", leaderElectionTimings{40 * s, 40 * s, 5 * s}, "must be less than"},
		{"renew above lease", leaderElectionTimings{30 * s, 40 * s, 5 * s}, "must be less than"},
		{"retry too close to renew", leaderElectionTimings{60 * s, 10 * s, 9 * s}, "1.2 x"},
		{"zero retry", leaderElectionTimings{60 * s, 40 * s, 0}, "must be positive"},
		{"negative lease", leaderElectionTimings{-1, 40 * s, 5 * s}, "must be positive"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.t.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestIsLeaderElectionLost(t *testing.T) {
	if !isLeaderElectionLost(errors.New("leader election lost")) {
		t.Error("want true for leader election lost")
	}
	if isLeaderElectionLost(errors.New("boom")) || isLeaderElectionLost(nil) {
		t.Error("want false otherwise")
	}
}

func TestManagerOptionsReleaseOnCancel(t *testing.T) {
	lt := leaderElectionTimings{60 * time.Second, 40 * time.Second, 5 * time.Second}
	opts := managerOptions(scheme, metricsserver.Options{}, nil, ":8081", true, lt)
	if !opts.LeaderElectionReleaseOnCancel {
		t.Error("LeaderElectionReleaseOnCancel must be true")
	}
	if !opts.LeaderElection || opts.LeaderElectionID == "" {
		t.Error("leader election not configured")
	}
	if *opts.LeaseDuration != lt.LeaseDuration ||
		*opts.RenewDeadline != lt.RenewDeadline ||
		*opts.RetryPeriod != lt.RetryPeriod {
		t.Error("timings not propagated")
	}
	if shutdownTimeout >= 10*time.Second {
		t.Errorf("shutdownTimeout %s must stay under terminationGracePeriodSeconds (10s)", shutdownTimeout)
	}
}

func TestManagerOptionsGracefulShutdownBelowGracePeriod(t *testing.T) {
	o := managerOptions(scheme, metricsserver.Options{}, nil, ":0", true,
		leaderElectionTimings{LeaseDuration: 15 * time.Second, RenewDeadline: 10 * time.Second, RetryPeriod: 2 * time.Second})
	if o.GracefulShutdownTimeout == nil || *o.GracefulShutdownTimeout != managerShutdownTimeout {
		t.Fatalf("GracefulShutdownTimeout = %v, want %s", o.GracefulShutdownTimeout, managerShutdownTimeout)
	}
	// terminationGracePeriodSeconds is 10 in the chart.
	if managerShutdownTimeout+shutdownTimeout >= 10*time.Second {
		t.Fatalf("shutdown budget %s must stay below the 10s pod grace period", managerShutdownTimeout+shutdownTimeout)
	}
}
