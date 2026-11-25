package main

import (
	"errors"
	"strings"
	"testing"
	"time"
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
