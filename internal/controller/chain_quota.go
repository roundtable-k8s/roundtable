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

package controller

import (
	"regexp"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// Plan-quota refusals (#321). A claude-knight in subscription mode refuses a
// task before spawning Claude Code once a plan window passes the knight
// reserve (claude-knight src/main.ts, src/plan-limits.ts):
//
//	{success: false, usage_limited: true,
//	 result/error: "Task refused: Claude plan five_hour window 91% used — at
//	 the knight reserve of 80% (resets 2026-11-24T18:00:00.000Z)"}
//
// A quota refusal is not a failure, it is "not yet": the chain neither fails
// nor escalates. The refusal result carries the machine-readable
// usage_limited flag, and the reason embeds the plan window's reset time.

// taskRefusedPrefix starts every plan-quota refusal result (result and error
// fields carry the same text).
const taskRefusedPrefix = "Task refused: "

// quotaResetsRe extracts the plan window's reset time from a refusal reason:
// "(resets 2026-11-24T18:00:00.000Z)".
var quotaResetsRe = regexp.MustCompile(`\(resets ([^)]+)\)`)

// quotaDeferJitter is added to a refused plan window's reset time so deferred
// chains requeue slightly after the window actually resets, not the exact
// second it flips (usage may lag the reset by a minute or two).
const quotaDeferJitter = 2 * time.Minute

// quotaDeferFallback is used when a refusal reason carries no parseable reset
// time: requeue in an hour and look again.
const quotaDeferFallback = time.Hour

// stepReasonQuotaRefused marks a step deferred by a plan-quota refusal.
const stepReasonQuotaRefused = "QuotaRefused"

// stepReasonFallbackQuota marks a step re-running on its fallback knight
// after a plan-quota refusal.
const stepReasonFallbackQuota = "FallbackQuota"

// quotaRefusal reports whether a task result is a plan-quota refusal, and
// when the plan window resets. The usage_limited flag is the machine-readable
// signal and always makes a result a refusal; the reset time is parsed from
// the message when present, else the zero time (quotaDeferUntil then uses
// quotaDeferFallback). Without the flag, the "Task refused: " prefix alone is
// not enough: it also needs a parseable reset time. Anything else is an
// ordinary error (the existing retry/failure handling).
func quotaRefusal(res *natspkg.TaskResult) (resetsAt time.Time, ok bool) {
	if res == nil {
		return time.Time{}, false
	}
	msg := res.GetError()
	if res.UsageLimited {
		if m := quotaResetsRe.FindStringSubmatch(msg); m != nil {
			if t, err := time.Parse(time.RFC3339, m[1]); err == nil {
				return t, true
			}
		}
		return time.Time{}, true
	}
	if !strings.HasPrefix(msg, taskRefusedPrefix) {
		return time.Time{}, false
	}
	m := quotaResetsRe.FindStringSubmatch(msg)
	if m == nil {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, m[1])
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// quotaDeferUntil is when a deferred step runs again: the refused plan
// window's reset time plus jitter (or a default when the window is unknown).
func quotaDeferUntil(resetsAt time.Time) time.Time {
	if resetsAt.IsZero() {
		return time.Now().Add(quotaDeferFallback)
	}
	return resetsAt.Add(quotaDeferJitter)
}

// stepFallbackKnight returns the knight a refused ordinary step re-runs on:
// the step's fallbackRef, unless this task already ran there (a refusal on
// the fallback falls through to deferral). Empty means defer.
func stepFallbackKnight(spec *aiv1alpha1.ChainStep, ss *aiv1alpha1.ChainStepStatus) string {
	if spec == nil || spec.FallbackRef.Name == "" {
		return ""
	}
	if ss.Knight == spec.FallbackRef.Name {
		return "" // the fallback refused too: defer until the reset
	}
	return spec.FallbackRef.Name
}

// wakeDeferredSteps moves quota-deferred steps whose plan window has reset
// back to Pending (their primary knight runs them again) and returns the
// earliest future deferUntil: while any step is still deferred, the chain
// holds every other step (nothing fails, nothing escalates, #321).
func wakeDeferredSteps(chain *aiv1alpha1.Chain, now time.Time) *metav1.Time {
	var next *metav1.Time
	for i := range chain.Status.StepStatuses {
		ss := &chain.Status.StepStatuses[i]
		if ss.Phase != aiv1alpha1.ChainStepPhaseDeferred {
			continue
		}
		if ss.DeferUntil == nil || !ss.DeferUntil.After(now) {
			// The refused plan window has reset: run the step again, on its
			// primary knight (deferral exhausted the fallback too).
			keepKnight := ss.Reason == stepReasonProviderOverloaded
			ss.Phase = aiv1alpha1.ChainStepPhasePending
			ss.Reason = ""
			ss.Error = ""
			ss.DeferUntil = nil
			ss.CompletedAt = nil
			if !keepKnight {
				ss.Knight = "" // deferral exhausted the fallback too; retry primary
			}
			ss.Model = ""
			ss.TaskID = ""
			ss.TaskSeq = 0
			ss.StartedAt = nil
			ss.PickedUpAt = nil
			if ss.Loop != nil {
				for j := range ss.Loop.Steps {
					if bs := &ss.Loop.Steps[j]; bs.Phase == aiv1alpha1.ChainStepPhasePending {
						bs.Knight = ""
					}
				}
			}
			continue
		}
		if next == nil || ss.DeferUntil.Before(next) {
			next = ss.DeferUntil
		}
	}
	return next
}

// loopTaskKnight is the knight a loop body task was published to (the body
// step's knightRef, or the loop step's fallback during a quota re-run).
func loopTaskKnight(body *aiv1alpha1.ChainLoopStep, bs *aiv1alpha1.ChainLoopStepStatus) string {
	if bs.Knight != "" {
		return bs.Knight
	}
	return body.KnightRef.Name
}

// loopFallbackKnight returns the knight a quota-refused loop body task
// re-runs on: the loop step's fallbackRef, unless this task already ran
// there (a refusal on the fallback falls through to deferral).
func loopFallbackKnight(spec *aiv1alpha1.ChainStep, bs *aiv1alpha1.ChainLoopStepStatus) string {
	if spec == nil || spec.FallbackRef.Name == "" || bs.Knight == spec.FallbackRef.Name {
		return ""
	}
	return spec.FallbackRef.Name
}

// Transient model provider failures (#481): the provider, not the task, failed
// (HTTP 529 Overloaded and friends). The step is retried after a backoff.

// stepReasonProviderOverloaded marks a step deferred for a provider retry.
const stepReasonProviderOverloaded = "ProviderOverloaded"

// providerFallbackAfter is the number of retries after which the next attempt
// goes to the fallback knight.
const providerFallbackAfter = 2

// providerBackoffBase is the first retry's delay; it doubles each retry.
const providerBackoffBase = time.Minute

// transientStatusRe matches an HTTP status of a transient provider failure.
var transientStatusRe = regexp.MustCompile(`(?i)\b(?:api error|http(?: status)?(?: code)?|status(?: code)?|error)[:= ]+(?:529|503|502|500)\b`)

// Transient failure reasons, as set by knights in transientReason (and
// inferred from the error text for knights that do not set the flag).
const (
	transientOverloaded = "overloaded"
	transientProvider5x = "provider_5xx"
	transientNetwork    = "network"
	transientUnknown    = "unknown"
)

// transientProviderReason reports whether a failed task result is a
// transient model provider failure, and why: the machine-readable transient
// flag first (its transientReason, or "unknown" when unset), else an HTTP
// 529/503/502/500 or "Overloaded"/"overloaded_error" in the error text.
// Plan-quota refusals are not transient (see quotaRefusal).
func transientProviderReason(res *natspkg.TaskResult) (string, bool) {
	if res == nil {
		return "", false
	}
	if _, refused := quotaRefusal(res); refused {
		return "", false
	}
	if res.Transient {
		switch res.TransientReason {
		case transientOverloaded, transientProvider5x, transientNetwork:
			return res.TransientReason, true
		}
		return transientUnknown, true
	}
	msg := res.GetError()
	if msg == "" {
		return "", false
	}
	if strings.Contains(strings.ToLower(msg), "overloaded") {
		return transientOverloaded, true
	}
	if transientStatusRe.MatchString(msg) {
		return transientProvider5x, true
	}
	return "", false
}

// transientProviderFailure reports whether a failed task result is a
// transient model provider failure.
func transientProviderFailure(res *natspkg.TaskResult) bool {
	_, ok := transientProviderReason(res)
	return ok
}

// providerBackoff is the delay before retry number n (1-based): 1m, 2m, 4m...
func providerBackoff(n int32) time.Duration {
	if n < 1 {
		n = 1
	}
	if n > 10 {
		n = 10
	}
	return providerBackoffBase << (n - 1)
}
