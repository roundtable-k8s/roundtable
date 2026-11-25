/*
Copyright 2026.

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

package main

import (
	"fmt"
	"strings"
	"time"
)

// leaderElectionTimings are the lease timings handed to controller-runtime.
type leaderElectionTimings struct {
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
}

// Validate mirrors client-go's leader election constraints so a bad
// combination fails at start-up with a clear message rather than a late,
// cryptic error from the election loop. client-go requires
// leaseDuration > renewDeadline > retryPeriod * 1.2 (JitterFactor).
func (t leaderElectionTimings) Validate() error {
	if t.LeaseDuration <= 0 || t.RenewDeadline <= 0 || t.RetryPeriod <= 0 {
		return fmt.Errorf("leader election timings must be positive: lease-duration=%s renew-deadline=%s retry-period=%s",
			t.LeaseDuration, t.RenewDeadline, t.RetryPeriod)
	}
	if t.RenewDeadline >= t.LeaseDuration {
		return fmt.Errorf("leader-election-renew-deadline (%s) must be less than leader-election-lease-duration (%s)",
			t.RenewDeadline, t.LeaseDuration)
	}
	if float64(t.RenewDeadline) <= 1.2*float64(t.RetryPeriod) {
		return fmt.Errorf("leader-election-renew-deadline (%s) must be greater than 1.2 x leader-election-retry-period (%s)",
			t.RenewDeadline, t.RetryPeriod)
	}
	return nil
}

// isLeaderElectionLost reports whether a manager.Start error means the lease
// could not be renewed in time (controller-runtime returns "leader election lost").
func isLeaderElectionLost(err error) bool {
	return err != nil && strings.Contains(err.Error(), "leader election lost")
}
