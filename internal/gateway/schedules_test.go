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
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/yaml"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

const otherNS = "other"

func scheduleTrigger(ns, name, cron, chain string) *aiv1alpha1.Trigger {
	return &aiv1alpha1.Trigger{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: aiv1alpha1.TriggerSpec{
			Source: aiv1alpha1.TriggerSource{Schedule: &aiv1alpha1.TriggerScheduleSource{Cron: cron}},
			Target: aiv1alpha1.TriggerTarget{ChainRef: &corev1.LocalObjectReference{Name: chain}},
		},
	}
}

// newScheduleEnv serves the gateway with schedules enabled over a fake API
// holding:
//   - "digest" (Chain under table "fleet-a") and "other-chain" (no table);
//   - schedule "morning" on digest (table from the chain), Git-managed, with
//     status;
//   - schedule "nightly" with its own roundTableRef "fleet-b", API-managed;
//   - "hook", a NATS Trigger (not a schedule);
//   - schedule "elsewhere" in another namespace.
func newScheduleEnv(t *testing.T, enabled bool) *env {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = aiv1alpha1.AddToScheme(scheme)

	digest := templateChain("digest", false)
	digest.Spec.RoundTableRef = aiv1alpha1.LocalRef("fleet-a")

	morning := scheduleTrigger(testNS, "morning", "0 7 * * 1-5", "digest")
	morning.Spec.Source.Schedule.TimeZone = "Europe/Berlin"
	morning.Labels = map[string]string{fluxKustomizationName: "apps"}
	morning.Annotations = map[string]string{lastAppliedAnnotation: `{"kind":"Trigger"}`, "note": "keep"}
	morning.Finalizers = []string{"example.com/keep"}
	next := metav1.NewTime(time.Date(2026, 11, 26, 6, 0, 0, 0, time.UTC))
	last := metav1.NewTime(time.Date(2026, 11, 25, 6, 0, 2, 0, time.UTC))
	morning.Status = aiv1alpha1.TriggerStatus{
		ObservedGeneration: 3,
		NextScheduledAt:    &next,
		LastFiredAt:        &last,
		RecentFirings: []aiv1alpha1.TriggerFiring{
			{Time: last, Key: "k2", Run: "digest-trg-2"},
			{Time: metav1.NewTime(last.Add(-24 * time.Hour)), Key: "k1", SkipReason: "active run"},
		},
	}

	nightly := scheduleTrigger(testNS, "nightly", "30 2 * * *", "other-chain")
	nightly.Spec.RoundTableRef = aiv1alpha1.LocalRef("fleet-b")
	nightly.Spec.Suspend = true
	nightly.Labels = map[string]string{labelManagedBy: managedByGateway}

	hook := &aiv1alpha1.Trigger{
		ObjectMeta: metav1.ObjectMeta{Name: "hook", Namespace: testNS},
		Spec: aiv1alpha1.TriggerSpec{
			Source:        aiv1alpha1.TriggerSource{NATS: &aiv1alpha1.TriggerNATSSource{Subject: "fleet-a.events.x"}},
			Target:        aiv1alpha1.TriggerTarget{ChainRef: &corev1.LocalObjectReference{Name: "digest"}},
			RoundTableRef: aiv1alpha1.LocalRef("fleet-a"),
		},
	}

	objs := []client.Object{digest, templateChain("other-chain", false), morning, nightly, hook,
		scheduleTrigger(otherNS, "elsewhere", "@hourly", "digest")}
	k8s := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
		WithStatusSubresource(&aiv1alpha1.Trigger{}).Build()
	// WithObjects drops status with a status subresource: write it back.
	if err := k8s.Status().Update(t.Context(), morning); err != nil {
		t.Fatal(err)
	}

	svc := NewService(&Resolver{Client: k8s}, nil,
		Options{Namespace: testNS, AllowedNamespaces: []string{otherNS}}, logr.Discard())
	if enabled {
		svc.Schedules = k8s
	}
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(testToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	auth, err := NewTokenAuth(tokenFile, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(Handler(svc, auth))
	t.Cleanup(srv.Close)
	return &env{svc: svc, server: srv}
}

func scheduleNames(body map[string]any) []string {
	list, _ := body["schedules"].([]any)
	out := make([]string, 0, len(list))
	for _, s := range list {
		out = append(out, s.(map[string]any)["name"].(string))
	}
	return out
}

func TestListSchedules(t *testing.T) {
	e := newScheduleEnv(t, true)

	resp, body := e.do(t, "GET", "/v1/schedules", testToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %v", resp.StatusCode, body)
	}
	if got := strings.Join(scheduleNames(body), ","); got != "morning,nightly" {
		t.Fatalf("schedules = %q, want morning,nightly (no NATS Trigger, no other namespace)", got)
	}

	_, body = e.do(t, "GET", "/v1/schedules?table=fleet-a", testToken, nil)
	if got := strings.Join(scheduleNames(body), ","); got != "morning" {
		t.Fatalf("table fleet-a = %q, want morning (table from the target chain)", got)
	}
	_, body = e.do(t, "GET", "/v1/schedules?table=fleet-b", testToken, nil)
	if got := strings.Join(scheduleNames(body), ","); got != "nightly" {
		t.Fatalf("table fleet-b = %q, want nightly", got)
	}
	_, body = e.do(t, "GET", "/v1/schedules?table=none", testToken, nil)
	if got := scheduleNames(body); len(got) != 0 {
		t.Fatalf("table none = %v, want none", got)
	}

	_, body = e.do(t, "GET", "/v1/schedules?namespace="+otherNS, testToken, nil)
	if got := strings.Join(scheduleNames(body), ","); got != "elsewhere" {
		t.Fatalf("namespace other = %q, want elsewhere", got)
	}
	resp, _ = e.do(t, "GET", "/v1/schedules?namespace=forbidden", testToken, nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("disallowed namespace: status %d, want 400", resp.StatusCode)
	}
	resp, _ = e.do(t, "GET", "/v1/schedules", "", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no token: status %d, want 401", resp.StatusCode)
	}
}

func TestGetSchedule(t *testing.T) {
	e := newScheduleEnv(t, true)

	resp, body := e.do(t, "GET", "/v1/schedules/"+testNS+"/morning", testToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %v", resp.StatusCode, body)
	}
	want := map[string]any{
		"name": "morning", "namespace": testNS, "table": "fleet-a", "cron": "0 7 * * 1-5",
		"timeZone": "Europe/Berlin", "description": "at 07:00 on Monday to Friday",
		"suspended": false, "managedBy": "git",
		"nextRunAt": "2026-11-26T06:00:00Z", "lastRunAt": "2026-11-25T06:00:02Z",
	}
	for k, v := range want {
		if body[k] != v {
			t.Errorf("%s = %v, want %v", k, body[k], v)
		}
	}
	target, _ := body["target"].(map[string]any)
	if target["kind"] != "chain" || target["chain"] != "digest" || target["summary"] != "chain digest" {
		t.Errorf("target = %v", target)
	}
	last, _ := body["lastResult"].(map[string]any)
	if last["outcome"] != "started" || last["run"] != "digest-trg-2" {
		t.Errorf("lastResult = %v, want the newest firing", last)
	}

	_, body = e.do(t, "GET", "/v1/schedules/"+testNS+"/nightly", testToken, nil)
	if body["managedBy"] != "api" || body["suspended"] != true || body["timeZone"] != "UTC" ||
		body["table"] != "fleet-b" || body["lastResult"] != nil {
		t.Errorf("nightly = %v", body)
	}

	for _, path := range []string{"/v1/schedules/" + testNS + "/missing", "/v1/schedules/" + testNS + "/hook",
		"/v1/schedules/" + testNS + "/missing/manifest", "/v1/schedules/" + testNS + "/hook/manifest"} {
		if resp, _ := e.do(t, "GET", path, testToken, nil); resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s: status %d, want 404", path, resp.StatusCode)
		}
	}
}

func TestSchedulesDisabled(t *testing.T) {
	e := newScheduleEnv(t, false)
	if resp, _ := e.do(t, "GET", "/v1/schedules", testToken, nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status %d, want 404 when schedules are disabled", resp.StatusCode)
	}
}

func TestScheduleManifest(t *testing.T) {
	e := newScheduleEnv(t, true)
	req, _ := http.NewRequest("GET", e.server.URL+"/v1/schedules/"+testNS+"/morning/manifest", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %s", resp.StatusCode, raw)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/yaml" {
		t.Errorf("Content-Type = %q", ct)
	}
	for _, banned := range []string{"status:", "managedFields", "resourceVersion", "uid:", "generation",
		"creationTimestamp", "last-applied-configuration", "finalizers", "recentFirings"} {
		if strings.Contains(string(raw), banned) {
			t.Errorf("manifest contains %q:\n%s", banned, raw)
		}
	}

	var got aiv1alpha1.Trigger
	if err := yaml.UnmarshalStrict(raw, &got); err != nil {
		t.Fatalf("manifest does not parse back into a Trigger: %v\n%s", err, raw)
	}
	if got.APIVersion != aiv1alpha1.GroupVersion.String() || got.Kind != "Trigger" {
		t.Errorf("apiVersion/kind = %s/%s", got.APIVersion, got.Kind)
	}
	if got.Name != "morning" || got.Namespace != testNS || got.Spec.Source.Schedule == nil ||
		got.Spec.Source.Schedule.Cron != "0 7 * * 1-5" || got.Spec.Target.ChainRef.Name != "digest" {
		t.Errorf("spec/metadata lost: %+v", got)
	}
	if got.Labels[fluxKustomizationName] != "apps" || got.Annotations["note"] != "keep" {
		t.Errorf("labels/annotations lost: %v %v", got.Labels, got.Annotations)
	}
	if got.Status.ObservedGeneration != 0 || got.Status.NextScheduledAt != nil || len(got.Status.RecentFirings) != 0 ||
		len(got.ManagedFields) != 0 || got.ResourceVersion != "" || got.UID != "" || got.Generation != 0 ||
		!got.CreationTimestamp.IsZero() {
		t.Errorf("server-set fields kept: %+v", got)
	}
}

func TestScheduleManagedBy(t *testing.T) {
	cases := []struct {
		name        string
		labels      map[string]string
		annotations map[string]string
		want        ManagedBy
	}{
		{"flux kustomization label", map[string]string{fluxKustomizationName: "apps"}, nil, ManagedByGit},
		{"flux helmrelease label", map[string]string{fluxHelmReleaseName: "rt"}, nil, ManagedByGit},
		{"flux annotation", nil, map[string]string{fluxKustomizationName: "apps"}, ManagedByGit},
		{"helm", map[string]string{labelManagedBy: "Helm"}, nil, ManagedByGit},
		{"git wins over api", map[string]string{labelManagedBy: managedByGateway, fluxKustomizationName: "apps"}, nil, ManagedByGit},
		{"api", map[string]string{labelManagedBy: managedByGateway}, nil, ManagedByAPI},
		{"other tool", map[string]string{labelManagedBy: "kustomize"}, nil, ManagedByOther},
		{"none", nil, nil, ManagedByOther},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := scheduleTrigger(testNS, "s", "@daily", "digest")
			tr.Labels, tr.Annotations = tc.labels, tc.annotations
			if got := ScheduleManagedBy(tr); got != tc.want {
				t.Fatalf("ScheduleManagedBy = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDescribeCron(t *testing.T) {
	cases := map[string]string{
		"0 7 * * 1-5":     "at 07:00 on Monday to Friday",
		"30 2 * * *":      "every day at 02:30",
		"*/15 * * * *":    "every 15 minutes",
		"* * * * *":       "every minute",
		"5 * * * *":       "every hour at minute 5",
		"0 */6 * * *":     "every 6 hours at minute 0",
		"0 9 1 * *":       "at 09:00 on day 1 of the month",
		"0 9 1 * 1":       "cron 0 9 1 * 1",
		"0 9 * 1,7 MON":   "at 09:00 in January and July on Monday",
		"0 8,20 * * *":    "every day at minute 0 past hour 8 and 20",
		"@hourly":         "every hour",
		"@daily":          "every day at 00:00",
		"@every 1h30m":    "every 1h30m",
		"0 7 L * *":       "cron 0 7 L * *",
		"1-5/2 * * * *":   "cron 1-5/2 * * * *",
		"0 0 * * * extra": "cron 0 0 * * * extra",
	}
	for expr, want := range cases {
		if got := DescribeCron(expr); got != want {
			t.Errorf("DescribeCron(%q) = %q, want %q", expr, got, want)
		}
	}
}

func TestScheduleViewKnightTarget(t *testing.T) {
	tr := scheduleTrigger(testNS, "ping", "*/5 * * * *", "")
	tr.Spec.Target = aiv1alpha1.TriggerTarget{Knight: &aiv1alpha1.TriggerKnightTarget{Name: "galahad", Task: "ping"}}
	v := scheduleView(tr, map[string]string{"": "fleet-a"}, nil)
	want := ScheduleTarget{Kind: ScheduleTargetKnight, Summary: "knight galahad: ping", Knight: "galahad"}
	if v.Target != want {
		t.Errorf("target = %+v, want %+v", v.Target, want)
	}
	if v.Table != "" {
		t.Errorf("table = %q, want empty for a knight target without roundTableRef", v.Table)
	}
}

func TestScheduleViewKnightFiringOutcome(t *testing.T) {
	for _, o := range []aiv1alpha1.TriggerFiringOutcome{
		aiv1alpha1.TriggerFiringSucceeded, aiv1alpha1.TriggerFiringFailed, aiv1alpha1.TriggerFiringLost,
	} {
		tr := scheduleTrigger(testNS, "ping", "*/5 * * * *", "")
		tr.Spec.Target = aiv1alpha1.TriggerTarget{Knight: &aiv1alpha1.TriggerKnightTarget{Name: "galahad", Task: "ping"}}
		tr.Status.RecentFirings = []aiv1alpha1.TriggerFiring{{Time: metav1.Now(), Run: "task-1", Outcome: o}}
		v := scheduleView(tr, nil, nil)
		if v.LastResult == nil || v.LastResult.Outcome != string(o) {
			t.Errorf("lastResult = %+v, want outcome %q", v.LastResult, o)
		}
	}
}

func TestKnightSummary(t *testing.T) {
	long := strings.Repeat("x", 100)
	cases := []struct{ task, want string }{
		{"ping", "knight k: ping"},
		{"\n  \nfirst line\nsecond", "knight k: first line"},
		{long, "knight k: " + long[:80]},
		{"  \n", "knight k"},
	}
	for _, tc := range cases {
		if got := knightSummary("k", tc.task); got != tc.want {
			t.Errorf("knightSummary(%q) = %q, want %q", tc.task, got, tc.want)
		}
	}
}

func TestScheduleViewKnightTargetTable(t *testing.T) {
	tr := scheduleTrigger(testNS, "ping", "*/5 * * * *", "")
	tr.Spec.Target = aiv1alpha1.TriggerTarget{Knight: &aiv1alpha1.TriggerKnightTarget{Name: "galahad", Task: "ping"}}
	v := scheduleView(tr, nil, func(n string) string { return "table-of-" + n })
	if v.Table != "table-of-galahad" {
		t.Errorf("table = %q, want the knight's table", v.Table)
	}
}

func TestScheduleViewConcurrencyAndSuspension(t *testing.T) {
	// Defaults when unset.
	v := scheduleView(scheduleTrigger(testNS, "a", "* * * * *", "digest"), nil, nil)
	if v.Concurrency.Policy != "Allow" || v.Concurrency.MaxActive != 0 || v.StartingDeadlineSeconds != nil {
		t.Errorf("defaults: concurrency %+v deadline %v", v.Concurrency, v.StartingDeadlineSeconds)
	}
	if v.Suspended || v.SuspendedBy != "" {
		t.Errorf("not suspended: got %v %q", v.Suspended, v.SuspendedBy)
	}

	// Forbid, deadline and annotation suspension come through.
	tr := scheduleTrigger(testNS, "b", "* * * * *", "digest")
	tr.Spec.Concurrency = aiv1alpha1.TriggerConcurrency{Policy: aiv1alpha1.TriggerConcurrencyForbid, MaxActive: 2}
	d := int64(300)
	tr.Spec.Source.Schedule.StartingDeadlineSeconds = &d
	tr.Annotations = map[string]string{aiv1alpha1.AnnotationTriggerSuspended: "true"}
	v = scheduleView(tr, nil, nil)
	if v.Concurrency.Policy != "Forbid" || v.Concurrency.MaxActive != 2 {
		t.Errorf("concurrency = %+v", v.Concurrency)
	}
	if v.StartingDeadlineSeconds == nil || *v.StartingDeadlineSeconds != 300 {
		t.Errorf("startingDeadlineSeconds = %v", v.StartingDeadlineSeconds)
	}
	if !v.Suspended || v.SuspendedBy != "annotation" {
		t.Errorf("suspended = %v by %q, want true by annotation", v.Suspended, v.SuspendedBy)
	}

	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	if m["suspended"] != true || m["suspendedBy"] != "annotation" || m["startingDeadlineSeconds"] != float64(300) {
		t.Errorf("json = %s", b)
	}

	// spec.suspend alone, and with the annotation.
	tr = scheduleTrigger(testNS, "c", "* * * * *", "digest")
	tr.Spec.Suspend = true
	if v = scheduleView(tr, nil, nil); !v.Suspended || v.SuspendedBy != "spec" {
		t.Errorf("spec: suspended = %v by %q", v.Suspended, v.SuspendedBy)
	}
	tr.Annotations = map[string]string{aiv1alpha1.AnnotationTriggerSuspended: "true"}
	if v = scheduleView(tr, nil, nil); !v.Suspended || v.SuspendedBy != "spec,annotation" {
		t.Errorf("spec+annotation: suspended = %v by %q", v.Suspended, v.SuspendedBy)
	}
}
