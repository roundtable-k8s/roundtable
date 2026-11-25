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
	"encoding/json"
	"net/http"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

func scheduleTrigger(ns, name string) *aiv1alpha1.Trigger {
	return &aiv1alpha1.Trigger{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: aiv1alpha1.TriggerSpec{
			Source:        aiv1alpha1.TriggerSource{Schedule: &aiv1alpha1.TriggerScheduleSource{Cron: "0 2 * * *"}},
			Target:        aiv1alpha1.TriggerTarget{ChainRef: &corev1.LocalObjectReference{Name: "target"}},
			RoundTableRef: &corev1.LocalObjectReference{Name: "dev"},
		},
	}
}

// fireTokens are ServiceAccount tokens the fake TokenReview knows.
type fireTokens struct {
	own, ownOfMissing, sameNSOther, otherTrigger, otherNS string
}

// withFireServiceAccounts turns on ServiceAccount auth with tokens for the
// nightly Trigger's own ServiceAccount and for callers that are not it.
func withFireServiceAccounts(e *hookEnv) fireTokens {
	now := time.Now()
	exp := now.Add(time.Hour)
	tk := fireTokens{
		own:          fakeJWT("own", exp),
		ownOfMissing: fakeJWT("own-of-missing", exp),
		sameNSOther:  fakeJWT("same-ns-other", exp),
		otherTrigger: fakeJWT("other-trigger", exp),
		otherNS:      fakeJWT("other-ns", exp),
	}
	user := func(ns, name string) fakeSAToken {
		return fakeSAToken{user: "system:serviceaccount:" + ns + ":" + name,
			audiences: []string{DefaultServiceAccountAudience}, exp: exp}
	}
	e.auth.ServiceAccounts = &ServiceAccountAuth{Reviewer: &fakeReviewer{now: time.Now, tokens: map[string]fakeSAToken{
		tk.own:          user(testNS, "trigger-nightly"),
		tk.ownOfMissing: user(testNS, "trigger-missing"),
		tk.sameNSOther:  user(testNS, "runner"),
		tk.otherTrigger: user(testNS, "trigger-alerts"),
		tk.otherNS:      user(hookOtherNS, "trigger-nightly"),
	}}}
	return tk
}

func (e *hookEnv) fire(t *testing.T, path, token, body string) (*http.Response, map[string]any) {
	t.Helper()
	h := map[string]string{}
	if token != "" {
		h["Authorization"] = "Bearer " + token
	}
	return e.post(t, path, []byte(body), h)
}

func TestFireTrigger(t *testing.T) {
	e := newHookEnv(t, HookOptions{}, true, hookOtherNS)
	tk := withFireServiceAccounts(e)
	path := "/v1/triggers/" + testNS + "/nightly/fire"
	subject := natspkg.ScheduleEventSubject(prefix, testNS, "nightly")

	// The Trigger's own ServiceAccount, with the CronJob's scheduled time.
	resp, body := e.fire(t, path, tk.own, `{"scheduledTime":"2026-11-25T02:00:00Z"}`)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("own ServiceAccount: %d %v", resp.StatusCode, body)
	}
	if body["key"] != "2026-11-25T02:00:00Z" || body["subject"] != subject || body["stream"] != eventsStream || body["duplicate"] != nil {
		t.Fatalf("result: %v", body)
	}
	st, err := e.js.Stream(context.Background(), eventsStream)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := st.GetLastMsgForSubject(context.Background(), subject)
	if err != nil {
		t.Fatal(err)
	}
	if got := msg.Header.Get(natspkg.EventKeyHeader); got != "2026-11-25T02:00:00Z" {
		t.Errorf("firing key header = %q", got)
	}
	if got := msg.Header.Get(natspkg.ScheduledTimeHeader); got != "2026-11-25T02:00:00Z" {
		t.Errorf("scheduled time header = %q", got)
	}
	var ev ScheduleEvent
	if err := json.Unmarshal(msg.Data, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Trigger != testNS+"/nightly" || ev.Key != "2026-11-25T02:00:00Z" || ev.Caller != testNS+"/trigger-nightly" ||
		!ev.ScheduledTime.Equal(time.Date(2026, 11, 25, 2, 0, 0, 0, time.UTC)) {
		t.Errorf("event = %+v", ev)
	}

	// A second run for the same slot (seconds later, another offset) has the
	// same firing key and is one stream message.
	before := e.msgs(t)
	resp, body = e.fire(t, path, tk.own, `{"scheduledTime":"2026-11-25T03:00:42+01:00"}`)
	if resp.StatusCode != http.StatusAccepted || body["key"] != "2026-11-25T02:00:00Z" || body["duplicate"] != true {
		t.Fatalf("repeat for the same slot: %d %v", resp.StatusCode, body)
	}
	if e.msgs(t) != before {
		t.Errorf("repeat stored a second message")
	}

	// A static token; no scheduled time: now, truncated to the minute.
	e.svc.now = func() time.Time { return time.Date(2026, 11, 25, 4, 5, 59, 999, time.UTC) }
	for i := range 2 {
		resp, body = e.fire(t, path, testToken, "")
		if resp.StatusCode != http.StatusAccepted || body["key"] != "2026-11-25T04:05:00Z" {
			t.Fatalf("static token #%d: %d %v", i, resp.StatusCode, body)
		}
	}

	// The Trigger in another namespace, with its own ServiceAccount.
	resp, body = e.fire(t, "/v1/triggers/"+hookOtherNS+"/nightly/fire", tk.otherNS, "")
	if resp.StatusCode != http.StatusAccepted || body["subject"] != natspkg.ScheduleEventSubject(prefix, hookOtherNS, "nightly") {
		t.Fatalf("other namespace's own ServiceAccount: %d %v", resp.StatusCode, body)
	}

	for _, tc := range []struct {
		name, path, token, body string
		code                    int
	}{
		{"no token", path, "", "", http.StatusUnauthorized},
		{"another ServiceAccount in the namespace", path, tk.sameNSOther, "", http.StatusForbidden},
		{"another Trigger's ServiceAccount", path, tk.otherTrigger, "", http.StatusForbidden},
		{"same-named ServiceAccount in another namespace", path, tk.otherNS, "", http.StatusForbidden},
		{"unknown Trigger, static token", "/v1/triggers/" + testNS + "/missing/fire", testToken, "", http.StatusNotFound},
		{"unknown Trigger, its ServiceAccount", "/v1/triggers/" + testNS + "/missing/fire", tk.ownOfMissing, "", http.StatusNotFound},
		{"namespace not allowed", "/v1/triggers/nowhere/nightly/fire", testToken, "", http.StatusNotFound},
		{"webhook Trigger", "/v1/triggers/" + testNS + "/alerts/fire", testToken, "", http.StatusUnprocessableEntity},
		{"bad scheduled time", path, testToken, `{"scheduledTime":"tomorrow"}`, http.StatusBadRequest},
		{"unknown field", path, testToken, `{"when":"now"}`, http.StatusBadRequest},
	} {
		before := e.msgs(t)
		if resp, body := e.fire(t, tc.path, tc.token, tc.body); resp.StatusCode != tc.code {
			t.Errorf("%s: %d %v, want %d", tc.name, resp.StatusCode, body, tc.code)
		}
		if e.msgs(t) != before {
			t.Errorf("%s: published although refused", tc.name)
		}
	}
}

func TestFireTriggerDisabledWithHooks(t *testing.T) {
	e := newHookEnv(t, HookOptions{}, false)
	if resp, body := e.fire(t, "/v1/triggers/"+testNS+"/nightly/fire", testToken, ""); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("hooks disabled: %d %v", resp.StatusCode, body)
	}
}

func TestScheduleFiringKey(t *testing.T) {
	a := time.Date(2026, 11, 25, 2, 0, 0, 0, time.UTC)
	b := time.Date(2026, 11, 25, 3, 0, 59, 0, time.FixedZone("CET", 3600))
	if ScheduleFiringKey(a) != ScheduleFiringKey(b) || ScheduleFiringKey(a) != "2026-11-25T02:00:00Z" {
		t.Errorf("keys %q, %q", ScheduleFiringKey(a), ScheduleFiringKey(b))
	}
}
