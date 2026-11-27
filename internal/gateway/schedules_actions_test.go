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
	"errors"
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
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	"github.com/roundtable-k8s/roundtable/internal/chainrun"
)

// actionTrigger is a schedule Trigger targeting the Chain "digest".
func actionTrigger(name string, labels map[string]string) *aiv1alpha1.Trigger {
	return &aiv1alpha1.Trigger{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, Labels: labels},
		Spec: aiv1alpha1.TriggerSpec{
			Source: aiv1alpha1.TriggerSource{Schedule: &aiv1alpha1.TriggerScheduleSource{Cron: "0 7 * * *"}},
			Target: aiv1alpha1.TriggerTarget{ChainRef: &corev1.LocalObjectReference{Name: "digest"}},
		},
	}
}

type actionEnv struct {
	env
	k8s client.Client
}

// newActionEnv serves the gateway over a fake API holding the Chain
// "digest", a Git-managed schedule "git-digest" (Flux Kustomization labels), a
// schedule "suspended" with spec.suspend, a schedule "orphan" whose Chain is
// missing and a NATS Trigger "events".
func newActionEnv(t *testing.T, enabled bool) *actionEnv {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = aiv1alpha1.AddToScheme(scheme)
	suspended := actionTrigger("suspended", nil)
	suspended.Spec.Suspend = true
	orphan := actionTrigger("orphan", nil)
	orphan.Spec.Target.ChainRef.Name = "gone"
	knight := actionTrigger("knight-digest", nil)
	knight.Spec.Target = aiv1alpha1.TriggerTarget{Knight: &aiv1alpha1.TriggerKnightTarget{Name: "galahad", Task: "brief me"}}
	mission := actionTrigger("mission-digest", nil)
	mission.Spec.Target = aiv1alpha1.TriggerTarget{Mission: &aiv1alpha1.TriggerMissionTarget{Objective: "review"}}
	invoke := actionTrigger("invoke-digest", nil)
	events := actionTrigger("events", nil)
	events.Spec.Source = aiv1alpha1.TriggerSource{NATS: &aiv1alpha1.TriggerNATSSource{Subject: "x.events.y"}}
	k8s := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		templateChain("digest", false),
		actionTrigger("git-digest", map[string]string{
			"kustomize.toolkit.fluxcd.io/name":      "apps",
			"kustomize.toolkit.fluxcd.io/namespace": "flux-system",
		}),
		suspended, orphan, events, knight, mission, invoke,
	).WithStatusSubresource(&aiv1alpha1.Trigger{}, &aiv1alpha1.Chain{}).Build()

	svc := NewService(&Resolver{Client: k8s}, nil, Options{Namespace: testNS, MaxTimeout: time.Minute,
		Runs: RunOptions{PollInterval: 10 * time.Millisecond}}, logr.Discard())
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
	return &actionEnv{env: env{svc: svc, server: srv}, k8s: k8s}
}

func (e *actionEnv) trigger(t *testing.T, name string) *aiv1alpha1.Trigger {
	t.Helper()
	trg := &aiv1alpha1.Trigger{}
	if err := e.k8s.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, trg); err != nil {
		t.Fatal(err)
	}
	return trg
}

// playTrigger stands in for the trigger controller: it handles the run-now
// annotation of name once, recording a firing that started the run (or was
// skipped with skipReason), and removes the annotation.
func (e *actionEnv) playTrigger(t *testing.T, name, skipReason string) {
	ctx := context.Background()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		trg := &aiv1alpha1.Trigger{}
		if err := e.k8s.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, trg); err != nil {
			t.Error(err)
			return
		}
		id := trg.Annotations[aiv1alpha1.AnnotationTriggerRunNow]
		if id == "" {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		key := aiv1alpha1.TriggerRunNowKey(id)
		firing := aiv1alpha1.TriggerFiring{Time: metav1.Now(), Key: key, SkipReason: skipReason}
		if skipReason == "" {
			if trg.Spec.Target.ChainRef != nil {
				firing.Run = chainrun.TriggerRunName(trg.Spec.Target.ChainRef.Name, trg.Name, key)
			} else if trg.Spec.Target.Mission != nil {
				firing.Run = "invoke-mission-1a2b3c4d"
			} else {
				firing.Run = TaskIDPrefix + "galahad-abc123"
			}
		}
		trg.Status.RecentFirings = append([]aiv1alpha1.TriggerFiring{firing}, trg.Status.RecentFirings...)
		if err := e.k8s.Status().Update(ctx, trg); err != nil {
			t.Error(err)
			return
		}
		patch := client.MergeFrom(trg.DeepCopy())
		delete(trg.Annotations, aiv1alpha1.AnnotationTriggerRunNow)
		delete(trg.Annotations, aiv1alpha1.AnnotationTriggerRunNowCaller)
		if err := e.k8s.Patch(ctx, trg, patch); err != nil {
			t.Error(err)
		}
		return
	}
	t.Error("no run-now request seen")
}

func TestSuspendResumeGitManagedSchedule(t *testing.T) {
	e := newActionEnv(t, true)
	base := "/v1/schedules/" + testNS + "/git-digest"
	before := e.trigger(t, "git-digest")

	resp, out := e.do(t, http.MethodPost, base+"/suspend", testToken, nil)
	if resp.StatusCode != http.StatusOK || out["suspended"] != true || out["suspendedBy"] != "annotation" {
		t.Fatalf("suspend: %d %v", resp.StatusCode, out)
	}
	trg := e.trigger(t, "git-digest")
	if trg.Annotations[aiv1alpha1.AnnotationTriggerSuspended] != "token" {
		t.Errorf("annotation: %v", trg.Annotations)
	}
	if trg.Spec.Suspend || trg.Labels["kustomize.toolkit.fluxcd.io/name"] != "apps" {
		t.Errorf("spec or labels changed: suspend=%v labels=%v", trg.Spec.Suspend, trg.Labels)
	}
	// Suspending again is a no-op.
	if resp, out = e.do(t, http.MethodPost, base+"/suspend", testToken, nil); resp.StatusCode != http.StatusOK || out["suspended"] != true {
		t.Fatalf("suspend again: %d %v", resp.StatusCode, out)
	}

	resp, out = e.do(t, http.MethodPost, base+"/resume", testToken, nil)
	if resp.StatusCode != http.StatusOK || out["suspended"] != false || out["message"] != nil {
		t.Fatalf("resume: %d %v", resp.StatusCode, out)
	}
	trg = e.trigger(t, "git-digest")
	if _, ok := trg.Annotations[aiv1alpha1.AnnotationTriggerSuspended]; ok {
		t.Errorf("annotation not removed: %v", trg.Annotations)
	}
	if trg.Generation != before.Generation {
		t.Errorf("spec generation changed: %d -> %d", before.Generation, trg.Generation)
	}
}

func TestResumeKeepsSpecSuspend(t *testing.T) {
	e := newActionEnv(t, true)
	base := "/v1/schedules/" + testNS + "/suspended"
	if resp, out := e.do(t, http.MethodPost, base+"/suspend", testToken, nil); resp.StatusCode != http.StatusOK ||
		out["suspendedBy"] != "spec,annotation" {
		t.Fatalf("suspend: %d %v", resp.StatusCode, out)
	}
	resp, out := e.do(t, http.MethodPost, base+"/resume", testToken, nil)
	msg, _ := out["message"].(string)
	if resp.StatusCode != http.StatusOK || out["suspended"] != true || out["suspendedBy"] != "spec" || msg == "" {
		t.Fatalf("resume: %d %v", resp.StatusCode, out)
	}
	if !e.trigger(t, "suspended").Spec.Suspend {
		t.Error("resume cleared spec.suspend")
	}
}

func TestRunGitManagedSchedule(t *testing.T) {
	e := newActionEnv(t, true)
	go e.playTrigger(t, "git-digest", "")
	resp, out := e.do(t, http.MethodPost, "/v1/schedules/"+testNS+"/git-digest/run", testToken, nil)
	if resp.StatusCode != http.StatusCreated || out["state"] != ScheduleRunStarted {
		t.Fatalf("run: %d %v", resp.StatusCode, out)
	}
	key, _ := out["key"].(string)
	want := chainrun.TriggerRunName("digest", "git-digest", key)
	if out["run"] != want || out["namespace"] != testNS || out["schedule"] != "git-digest" || out["caller"] != "token" {
		t.Errorf("run reference: %v, want run %s", out, want)
	}
	if loc := resp.Header.Get("Location"); loc != "/v1/schedules/"+testNS+"/git-digest/runs/"+want {
		t.Errorf("Location: %q", loc)
	}
	if e.trigger(t, "git-digest").Spec.Suspend {
		t.Error("spec changed")
	}
}

func TestRunScheduleRecordsCallerAndPending(t *testing.T) {
	old := scheduleRunWait
	scheduleRunWait = 50 * time.Millisecond
	t.Cleanup(func() { scheduleRunWait = old })
	e := newActionEnv(t, true)
	path := "/v1/schedules/" + testNS + "/suspended/run"

	// Nobody handles it: 202 with the run it will start.
	resp, out := e.do(t, http.MethodPost, path, testToken, nil)
	if resp.StatusCode != http.StatusAccepted || out["state"] != ScheduleRunPending || out["run"] == "" {
		t.Fatalf("run: %d %v", resp.StatusCode, out)
	}
	if resp.Header.Get("Location") == "" {
		t.Error("no Location")
	}
	trg := e.trigger(t, "suspended")
	if aiv1alpha1.TriggerRunNowKey(trg.Annotations[aiv1alpha1.AnnotationTriggerRunNow]) != out["key"] ||
		trg.Annotations[aiv1alpha1.AnnotationTriggerRunNowCaller] != "token" {
		t.Errorf("annotations: %v", trg.Annotations)
	}

	// A second request while the first waits is a conflict.
	resp, out = e.do(t, http.MethodPost, path, testToken, nil)
	if resp.StatusCode != http.StatusConflict || out["state"] != ScheduleRunPending {
		t.Fatalf("second run: %d %v", resp.StatusCode, out)
	}
}

func TestRunKnightSchedule(t *testing.T) {
	old := scheduleRunWait
	t.Cleanup(func() { scheduleRunWait = old })
	e := newActionEnv(t, true)
	path := "/v1/schedules/" + testNS + "/knight-digest/run"

	// Handled: 201 with the task ID.
	go e.playTrigger(t, "knight-digest", "")
	resp, out := e.do(t, http.MethodPost, path, testToken, nil)
	if resp.StatusCode != http.StatusCreated || out["state"] != ScheduleRunStarted || out["run"] != TaskIDPrefix+"galahad-abc123" || out["caller"] != "token" {
		t.Fatalf("run: %d %v", resp.StatusCode, out)
	}
	if loc := resp.Header.Get("Location"); loc != "/v1/tasks/"+TaskIDPrefix+"galahad-abc123" {
		t.Errorf("Location: %q", loc)
	}

	// Not handled yet: 202 with the key, no run and no Location.
	scheduleRunWait = 50 * time.Millisecond
	resp, out = e.do(t, http.MethodPost, path, testToken, nil)
	if resp.StatusCode != http.StatusAccepted || out["state"] != ScheduleRunPending || out["key"] == "" {
		t.Fatalf("pending run: %d %v", resp.StatusCode, out)
	}
	if run, ok := out["run"]; ok && run != "" {
		t.Errorf("pending run named: %v", out)
	}
	if loc := resp.Header.Get("Location"); loc != "" {
		t.Errorf("pending Location: %q", loc)
	}
	trg := e.trigger(t, "knight-digest")
	if trg.Annotations[aiv1alpha1.AnnotationTriggerRunNowCaller] != "token" {
		t.Errorf("annotations: %v", trg.Annotations)
	}
}

func TestRunMissionSchedule(t *testing.T) {
	e := newActionEnv(t, true)
	go e.playTrigger(t, "mission-digest", "")
	resp, out := e.do(t, http.MethodPost, "/v1/schedules/"+testNS+"/mission-digest/run", testToken, nil)
	if resp.StatusCode != http.StatusCreated || out["state"] != ScheduleRunStarted || out["target"] != "mission" || out["run"] != "invoke-mission-1a2b3c4d" {
		t.Fatalf("run: %d %v", resp.StatusCode, out)
	}
	if loc := resp.Header.Get("Location"); loc != "/v1/missions/"+testNS+"/invoke-mission-1a2b3c4d" {
		t.Errorf("Location: %q", loc)
	}
}

// A Chain run of a schedule named "invoke-..." has a name starting with the
// task ID prefix; its Location must still be the run route.
func TestRunChainScheduleLocationIgnoresNamePrefix(t *testing.T) {
	e := newActionEnv(t, true)
	go e.playTrigger(t, "invoke-digest", "")
	resp, out := e.do(t, http.MethodPost, "/v1/schedules/"+testNS+"/invoke-digest/run", testToken, nil)
	if resp.StatusCode != http.StatusCreated || out["target"] != "chain" {
		t.Fatalf("run: %d %v", resp.StatusCode, out)
	}
	run, _ := out["run"].(string)
	if loc := resp.Header.Get("Location"); loc != "/v1/schedules/"+testNS+"/invoke-digest/runs/"+run {
		t.Errorf("Location: %q", loc)
	}
}

func TestRunScheduleSkipped(t *testing.T) {
	e := newActionEnv(t, true)
	go e.playTrigger(t, "git-digest", "concurrency Forbid: run x is active")
	resp, out := e.do(t, http.MethodPost, "/v1/schedules/"+testNS+"/git-digest/run", testToken, nil)
	if resp.StatusCode != http.StatusConflict || out["state"] != ScheduleRunSkipped {
		t.Fatalf("run: %d %v", resp.StatusCode, out)
	}
	run, _ := out["run"].(map[string]any)
	if run["skipReason"] != "concurrency Forbid: run x is active" {
		t.Errorf("skip reason: %v", out)
	}
}

func TestScheduleActionsNotFound(t *testing.T) {
	e := newActionEnv(t, true)
	for _, name := range []string{"nope", "events"} {
		for _, action := range []string{"suspend", "resume", "run"} {
			resp, _ := e.do(t, http.MethodPost, "/v1/schedules/"+testNS+"/"+name+"/"+action, testToken, nil)
			if resp.StatusCode != http.StatusNotFound {
				t.Errorf("%s %s: %d, want 404", action, name, resp.StatusCode)
			}
		}
	}
	// A namespace callers may not target.
	if resp, _ := e.do(t, http.MethodPost, "/v1/schedules/other/git-digest/suspend", testToken, nil); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("other namespace: %d", resp.StatusCode)
	}
	// A schedule whose Chain is missing can't run.
	if resp, out := e.do(t, http.MethodPost, "/v1/schedules/"+testNS+"/orphan/run", testToken, nil); resp.StatusCode != http.StatusConflict {
		t.Errorf("orphan: %d %v", resp.StatusCode, out)
	}
	if _, ok := e.trigger(t, "orphan").Annotations[aiv1alpha1.AnnotationTriggerRunNow]; ok {
		t.Error("orphan annotated")
	}
	// Without a token: 401.
	if resp, _ := e.do(t, http.MethodPost, "/v1/schedules/"+testNS+"/git-digest/suspend", "", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("unauthenticated: %d", resp.StatusCode)
	}
}

func TestScheduleActionsDisabled(t *testing.T) {
	e := newActionEnv(t, false)
	for _, action := range []string{"suspend", "resume", "run"} {
		resp, _ := e.do(t, http.MethodPost, "/v1/schedules/"+testNS+"/git-digest/"+action, testToken, nil)
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s disabled: %d, want 404", action, resp.StatusCode)
		}
	}
}

// tableTriggers adds schedules to the fake API: two on the table "knights"
// (one already suspended by annotation) and one on "other".
func (e *actionEnv) addTableTriggers(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	for _, c := range []struct{ name, table string }{{"t-a", "knights"}, {"t-b", "knights"}, {"t-c", "other"}} {
		trg := actionTrigger(c.name, nil)
		trg.Spec.RoundTableRef = &corev1.LocalObjectReference{Name: c.table}
		if c.name == "t-b" {
			trg.Annotations = map[string]string{aiv1alpha1.AnnotationTriggerSuspended: "someone"}
		}
		if err := e.k8s.Create(ctx, trg); err != nil {
			t.Fatal(err)
		}
	}
}

func names(v any) []string {
	items := v.([]any)
	out := make([]string, 0, len(items))
	for _, x := range items {
		out = append(out, x.(map[string]any)["name"].(string))
	}
	return out
}

func TestSuspendResumeTableSchedules(t *testing.T) {
	e := newActionEnv(t, true)
	e.addTableTriggers(t)
	body := map[string]any{"table": "knights"}

	resp, out := e.do(t, http.MethodPost, "/v1/schedules/suspend", testToken, body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("suspend: %d %v", resp.StatusCode, out)
	}
	if got := names(out["changed"]); len(got) != 1 || got[0] != "t-a" {
		t.Errorf("changed = %v", got)
	}
	if got := names(out["unchanged"]); len(got) != 1 || got[0] != "t-b" {
		t.Errorf("unchanged = %v", got)
	}
	if f, _ := out["failed"].([]any); len(f) != 0 {
		t.Errorf("failed = %v", f)
	}
	if !e.trigger(t, "t-a").SuspendedByAnnotation() || !e.trigger(t, "t-b").SuspendedByAnnotation() {
		t.Error("table schedules not suspended")
	}
	if e.trigger(t, "t-c").Suspended() {
		t.Error("schedule of another table was suspended")
	}

	resp, out = e.do(t, http.MethodPost, "/v1/schedules/resume", testToken, body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("resume: %d %v", resp.StatusCode, out)
	}
	if got := names(out["changed"]); len(got) != 2 {
		t.Errorf("changed = %v", got)
	}
	if e.trigger(t, "t-a").Suspended() || e.trigger(t, "t-b").Suspended() {
		t.Error("table schedules still suspended")
	}
}

func TestResumeTableSchedulesKeepsSpecSuspend(t *testing.T) {
	e := newActionEnv(t, true)
	e.addTableTriggers(t)
	ctx := context.Background()
	trg := e.trigger(t, "t-a")
	trg.Spec.Suspend = true
	if err := e.k8s.Update(ctx, trg); err != nil {
		t.Fatal(err)
	}
	body := map[string]any{"table": "knights"}
	e.do(t, http.MethodPost, "/v1/schedules/suspend", testToken, body)
	_, out := e.do(t, http.MethodPost, "/v1/schedules/resume", testToken, body)
	if got := names(out["changed"]); len(got) != 2 {
		t.Fatalf("changed = %v", got)
	}
	got := e.trigger(t, "t-a")
	if !got.Spec.Suspend || got.SuspendedByAnnotation() {
		t.Errorf("spec.suspend=%v annotation=%v", got.Spec.Suspend, got.SuspendedByAnnotation())
	}
	for _, c := range out["changed"].([]any) {
		m := c.(map[string]any)
		if m["name"] == "t-a" && (m["suspended"] != true || m["message"] == nil) {
			t.Errorf("t-a state: %v", m)
		}
	}
}

func TestTableSchedulesRequiresTable(t *testing.T) {
	e := newActionEnv(t, true)
	resp, out := e.do(t, http.MethodPost, "/v1/schedules/suspend", testToken, map[string]any{})
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status %d %v", resp.StatusCode, out)
	}
}

// countingClient counts Get calls and fails Patch for the names in failPatch.
type countingClient struct {
	client.Client
	gets      int
	failPatch map[string]bool
}

func (c *countingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	c.gets++
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *countingClient) Patch(ctx context.Context, obj client.Object, p client.Patch, opts ...client.PatchOption) error {
	if c.failPatch[obj.GetName()] {
		return errors.New("boom")
	}
	return c.Client.Patch(ctx, obj, p, opts...)
}

func TestTableSuspendReadsEachScheduleOnce(t *testing.T) {
	e := newActionEnv(t, true)
	e.addTableTriggers(t)
	cc := &countingClient{Client: e.k8s}
	e.svc.Schedules = cc
	if _, err := e.svc.SuspendSchedules(context.Background(), TableSchedulesRequest{Table: "knights", Namespace: testNS}); err != nil {
		t.Fatal(err)
	}
	// The table is listed with List (not counted); each schedule (t-a, t-b) is
	// fetched once, and no separate read detects the change.
	if cc.gets != 2 {
		t.Errorf("suspend Gets = %d, want 2", cc.gets)
	}
	cc.gets = 0
	if _, err := e.svc.ResumeSchedules(context.Background(), TableSchedulesRequest{Table: "knights", Namespace: testNS}); err != nil {
		t.Fatal(err)
	}
	if cc.gets != 2 {
		t.Errorf("resume Gets = %d, want 2", cc.gets)
	}
}

func TestTableSchedulesFailedPath(t *testing.T) {
	for _, suspend := range []bool{true, false} {
		e := newActionEnv(t, true)
		e.addTableTriggers(t)
		if !suspend {
			// t-a must carry the annotation for resume to patch it.
			if _, err := e.svc.SuspendSchedules(context.Background(), TableSchedulesRequest{Table: "knights", Namespace: testNS}); err != nil {
				t.Fatal(err)
			}
		}
		e.svc.Schedules = &countingClient{Client: e.k8s, failPatch: map[string]bool{"t-a": true}}
		var res *TableSchedulesResult
		var err error
		if suspend {
			res, err = e.svc.SuspendSchedules(context.Background(), TableSchedulesRequest{Table: "knights", Namespace: testNS})
		} else {
			res, err = e.svc.ResumeSchedules(context.Background(), TableSchedulesRequest{Table: "knights", Namespace: testNS})
		}
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Failed) != 1 || res.Failed[0].Name != "t-a" || !strings.Contains(res.Failed[0].Error, "patch schedule") {
			t.Errorf("suspend=%v failed = %+v", suspend, res.Failed)
		}
		// The other schedule is still handled.
		if len(res.Changed)+len(res.Unchanged) != 1 {
			t.Errorf("suspend=%v changed=%d unchanged=%d, want 1 handled", suspend, len(res.Changed), len(res.Unchanged))
		}
	}
}
