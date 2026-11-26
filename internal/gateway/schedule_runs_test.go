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
	"net/http/httptest"
	"os"
	"path/filepath"
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
)

var scheduleT0 = time.Date(2026, 11, 20, 7, 0, 0, 0, time.UTC)

func runScheduleTrigger(name string, mutate func(*aiv1alpha1.Trigger)) *aiv1alpha1.Trigger {
	t := &aiv1alpha1.Trigger{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, UID: types.UID("uid-" + name)},
		Spec: aiv1alpha1.TriggerSpec{
			Source: aiv1alpha1.TriggerSource{Schedule: &aiv1alpha1.TriggerScheduleSource{Cron: "0 7 * * *"}},
			Target: aiv1alpha1.TriggerTarget{ChainRef: &corev1.LocalObjectReference{Name: "digest"}},
		},
	}
	if mutate != nil {
		mutate(t)
	}
	return t
}

// scheduleRun is a Chain run of trigger, created at scheduleT0 + day days.
func scheduleRun(trg *aiv1alpha1.Trigger, name string, day int, phase aiv1alpha1.ChainPhase) *aiv1alpha1.Chain {
	created := scheduleT0.AddDate(0, 0, day)
	started := metav1.NewTime(created.Add(time.Second))
	done := metav1.NewTime(created.Add(61 * time.Second))
	c := &aiv1alpha1.Chain{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: trg.Namespace, CreationTimestamp: metav1.NewTime(created),
			Labels:      map[string]string{aiv1alpha1.LabelTrigger: trg.Name},
			Annotations: map[string]string{aiv1alpha1.AnnotationTriggerKey: created.Format(time.RFC3339)},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: aiv1alpha1.GroupVersion.String(), Kind: "Trigger",
				Name: trg.Name, UID: trg.UID, Controller: new(true)}},
		},
		Spec: aiv1alpha1.ChainSpec{Steps: []aiv1alpha1.ChainStep{step("fetch"), step("summarise", "fetch")}},
		Status: aiv1alpha1.ChainStatus{Phase: phase, StartedAt: &started, CompletedAt: &done, RunID: "r-" + name,
			CostUSD: "0.42", StepStatuses: []aiv1alpha1.ChainStepStatus{
				{Name: "fetch", Phase: aiv1alpha1.ChainStepPhase(aiv1alpha1.ChainPhaseSucceeded), Output: "raw"},
				{Name: "summarise", Phase: aiv1alpha1.ChainStepPhase(phase), Output: "digest of " + name},
			}},
	}
	return c
}

func newRunsEnv(t *testing.T, objs ...client.Object) *env {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = aiv1alpha1.AddToScheme(scheme)
	k8s := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	svc := NewService(&Resolver{Client: k8s}, nil, Options{Namespace: testNS, MaxTimeout: time.Minute}, logr.Discard())
	svc.ScheduleReader = k8s
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
	return &env{svc: svc, auth: auth, server: srv}
}

func runsOf(body map[string]any) []map[string]any {
	list, _ := body["runs"].([]any)
	out := make([]map[string]any, 0, len(list))
	for _, r := range list {
		out = append(out, r.(map[string]any))
	}
	return out
}

func TestScheduleRunsKnightFiringReason(t *testing.T) {
	trg := runScheduleTrigger("digest-daily", func(t *aiv1alpha1.Trigger) {
		t.Status.RecentFirings = []aiv1alpha1.TriggerFiring{
			{Time: metav1.NewTime(scheduleT0), Key: "k", Run: "invoke-x-1",
				Outcome: aiv1alpha1.TriggerFiringFailed, Reason: "boom"},
		}
	})
	e := newRunsEnv(t, trg)
	resp, body := e.do(t, "GET", "/v1/schedules/"+testNS+"/digest-daily/runs", testToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %v", resp.StatusCode, body)
	}
	runs := runsOf(body)
	if len(runs) != 1 || runs[0]["outcome"] != "failed" || runs[0]["reason"] != "boom" {
		t.Errorf("runs = %v", runs)
	}
}

func TestScheduleRunsHistory(t *testing.T) {
	trg := runScheduleTrigger("digest-daily", func(t *aiv1alpha1.Trigger) {
		t.Status.RecentFirings = []aiv1alpha1.TriggerFiring{
			{Time: metav1.NewTime(scheduleT0.AddDate(0, 0, 3)), Key: "k3", SkipReason: "missed schedule time by more than startingDeadlineSeconds"},
			{Time: metav1.NewTime(scheduleT0.AddDate(0, 0, 2)), Key: "k2", Run: "digest-daily-2"},
			// Its Chain is gone: only the firing remains.
			{Time: metav1.NewTime(scheduleT0.AddDate(0, 0, -5)), Key: "old", Run: "digest-daily-old"},
		}
	})
	other := runScheduleTrigger("other", nil)
	e := newRunsEnv(t, trg, other,
		scheduleRun(trg, "digest-daily-0", 0, aiv1alpha1.ChainPhaseSucceeded),
		scheduleRun(trg, "digest-daily-1", 1, aiv1alpha1.ChainPhaseFailed),
		scheduleRun(trg, "digest-daily-2", 2, aiv1alpha1.ChainPhaseSucceeded),
		scheduleRun(other, "other-0", 4, aiv1alpha1.ChainPhaseSucceeded))

	resp, body := e.do(t, "GET", "/v1/schedules/"+testNS+"/digest-daily/runs", testToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %v", resp.StatusCode, body)
	}
	runs := runsOf(body)
	got := make([]string, 0, len(runs))
	for _, r := range runs {
		got = append(got, fmt.Sprintf("%v:%v", r["run"], r["outcome"]))
	}
	want := []string{"<nil>:skipped", "digest-daily-2:succeeded", "digest-daily-1:failed", "digest-daily-0:succeeded", "digest-daily-old:unknown"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("runs (newest first) = %v, want %v", got, want)
	}
	if runs[0]["reason"] != "missed schedule time by more than startingDeadlineSeconds" || runs[0]["resultAvailable"] != false {
		t.Errorf("skipped firing = %v", runs[0])
	}
	if runs[1]["durationSeconds"] != float64(60) || runs[1]["costUSD"] != "0.42" || runs[1]["key"] != scheduleT0.AddDate(0, 0, 2).Format(time.RFC3339) {
		t.Errorf("succeeded run = %v", runs[1])
	}

	_, body = e.do(t, "GET", "/v1/schedules/"+testNS+"/digest-daily/runs?limit=2", testToken, nil)
	if n := len(runsOf(body)); n != 2 {
		t.Errorf("limit=2 returned %d runs", n)
	}
	if resp, _ := e.do(t, "GET", "/v1/schedules/"+testNS+"/digest-daily/runs?limit=-1", testToken, nil); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("negative limit status = %d, want 400", resp.StatusCode)
	}
}

func TestScheduleRunsOnlyForSchedules(t *testing.T) {
	hook := runScheduleTrigger("hook", func(t *aiv1alpha1.Trigger) {
		t.Spec.Source = aiv1alpha1.TriggerSource{Webhook: &aiv1alpha1.TriggerWebhookSource{}}
	})
	e := newRunsEnv(t, hook)
	for _, path := range []string{"/hook/runs", "/missing/runs", "/hook/runs/x"} {
		if resp, _ := e.do(t, "GET", "/v1/schedules/"+testNS+path, testToken, nil); resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s status = %d, want 404", path, resp.StatusCode)
		}
	}
	if resp, _ := e.do(t, "GET", "/v1/schedules/"+testNS+"/hook/runs", "", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("no token status = %d, want 401", resp.StatusCode)
	}
}

func TestScheduleRunResult(t *testing.T) {
	trg := runScheduleTrigger("digest-daily", nil)
	other := runScheduleTrigger("other", nil)
	e := newRunsEnv(t, trg, other,
		scheduleRun(trg, "digest-daily-0", 0, aiv1alpha1.ChainPhaseSucceeded),
		scheduleRun(other, "other-0", 1, aiv1alpha1.ChainPhaseSucceeded))
	base := "/v1/schedules/" + testNS + "/digest-daily/runs/"

	resp, body := e.do(t, "GET", base+"digest-daily-0", testToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %v", resp.StatusCode, body)
	}
	if body["output"] != "digest of digest-daily-0" || body["schedule"] != "digest-daily" ||
		body["target"] != "chain" || body["phase"] != "Succeeded" || body["done"] != true {
		t.Errorf("result = %v", body)
	}
	// A run of another schedule, a plain Chain and a missing run are 404.
	for _, run := range []string{"other-0", "digest-daily-gone", "Not_A_Name"} {
		if resp, _ := e.do(t, "GET", base+run, testToken, nil); resp.StatusCode != http.StatusNotFound {
			t.Errorf("run %q status = %d, want 404", run, resp.StatusCode)
		}
	}
}

// stubBus serves task records from memory; every other Bus method is nil.
type stubBus struct {
	Bus
	recs map[string]*TaskRecord
}

func (b *stubBus) GetRecord(_ context.Context, id string) (*TaskRecord, error) {
	if r, ok := b.recs[id]; ok {
		return r, nil
	}
	return nil, ErrNotFound
}

func knightScheduleTrigger(name string, firings ...aiv1alpha1.TriggerFiring) *aiv1alpha1.Trigger {
	trg := runScheduleTrigger(name, func(t *aiv1alpha1.Trigger) {
		t.Spec.Target = aiv1alpha1.TriggerTarget{Knight: &aiv1alpha1.TriggerKnightTarget{Name: "operator", Task: "go"}}
	})
	trg.Status.RecentFirings = firings
	return trg
}

func TestScheduleRunResultKnightTarget(t *testing.T) {
	done := scheduleT0.Add(time.Minute)
	trg := knightScheduleTrigger("k",
		aiv1alpha1.TriggerFiring{Run: "task-1", Time: metav1.NewTime(scheduleT0), Key: "a"},
		aiv1alpha1.TriggerFiring{Run: "task-expired", Time: metav1.NewTime(scheduleT0.Add(time.Hour)), Key: "b"})
	e := newRunsEnv(t, trg)
	e.svc.Bus = &stubBus{recs: map[string]*TaskRecord{
		"task-1": {TaskID: "task-1", Knight: "operator", Namespace: testNS, Status: TaskSucceeded, Output: "all done",
			Model: "opus", Verdict: "ship", CreatedAt: scheduleT0, CompletedAt: &done},
		// Sent by something else, not by this schedule.
		"task-foreign": {TaskID: "task-foreign", Knight: "operator", Namespace: testNS, Status: TaskSucceeded, Output: "x"},
	}}
	base := "/v1/schedules/" + testNS + "/k/runs"

	resp, body := e.do(t, "GET", base+"/task-1", testToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %v", resp.StatusCode, body)
	}
	task, _ := body["task"].(map[string]any)
	if body["target"] != "knight" || body["schedule"] != "k" || task["output"] != "all done" ||
		task["status"] != "succeeded" || task["model"] != "opus" || task["run"] != "task-1" {
		t.Errorf("result = %v", body)
	}
	for _, run := range []string{"task-foreign", "task-expired", "task-gone", "Not a name"} {
		if resp, _ := e.do(t, "GET", base+"/"+run, testToken, nil); resp.StatusCode != http.StatusNotFound {
			t.Errorf("run %q status = %d, want 404", run, resp.StatusCode)
		}
	}

	resp, body = e.do(t, "GET", base, testToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list status %d: %v", resp.StatusCode, body)
	}
	for _, r := range runsOf(body) {
		want := r["run"] == "task-1"
		if r["resultAvailable"] != want {
			t.Errorf("run %v resultAvailable = %v, want %v", r["run"], r["resultAvailable"], want)
		}
	}
}

func TestScheduleRunsKnightUsage(t *testing.T) {
	calls := 4
	trg := knightScheduleTrigger("u",
		aiv1alpha1.TriggerFiring{Run: "invoke-op-1-a", Time: metav1.NewTime(scheduleT0.Add(2 * time.Hour)), Key: "a"},
		aiv1alpha1.TriggerFiring{Run: "invoke-op-2-expired", Time: metav1.NewTime(scheduleT0.Add(time.Hour)), Key: "b"},
		aiv1alpha1.TriggerFiring{Run: "other-task", Time: metav1.NewTime(scheduleT0), Key: "c"})
	e := newRunsEnv(t, trg)
	e.svc.Bus = &stubBus{recs: map[string]*TaskRecord{
		"invoke-op-1-a": {TaskID: "invoke-op-1-a", Namespace: testNS, Status: TaskSucceeded, Output: "x",
			DurationMs: 94000, Cost: 0.5, Turns: 6, ToolCalls: &calls},
		"other-task": {TaskID: "other-task", Namespace: testNS, Status: TaskSucceeded, Output: "x", DurationMs: 1000, Cost: 1},
	}}
	_, body := e.do(t, "GET", "/v1/schedules/"+testNS+"/u/runs", testToken, nil)
	for _, r := range runsOf(body) {
		switch r["run"] {
		case "invoke-op-1-a":
			if r["durationSeconds"] != float64(94) || r["costUSD"] != "0.5" || r["turns"] != float64(6) || r["toolCalls"] != float64(4) {
				t.Errorf("usage row = %v", r)
			}
		default:
			for _, k := range []string{"durationSeconds", "costUSD", "turns", "toolCalls"} {
				if _, ok := r[k]; ok {
					t.Errorf("run %v has %s: %v", r["run"], k, r)
				}
			}
		}
	}
}

func missionScheduleRun(trg *aiv1alpha1.Trigger, name, result string) *aiv1alpha1.Mission {
	return &aiv1alpha1.Mission{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: trg.Namespace,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: aiv1alpha1.GroupVersion.String(), Kind: "Trigger",
				Name: trg.Name, UID: trg.UID, Controller: new(true)}}},
		Status: aiv1alpha1.MissionStatus{Phase: aiv1alpha1.MissionPhaseSucceeded, Result: result},
	}
}

func TestScheduleRunResultMissionTarget(t *testing.T) {
	missionTarget := func(t *aiv1alpha1.Trigger) {
		t.Spec.Target = aiv1alpha1.TriggerTarget{Mission: &aiv1alpha1.TriggerMissionTarget{}}
	}
	trg := runScheduleTrigger("m", missionTarget)
	other := runScheduleTrigger("other", missionTarget)
	trg.Status.RecentFirings = []aiv1alpha1.TriggerFiring{{Run: "m-1", Time: metav1.NewTime(scheduleT0), Key: "a"}}
	e := newRunsEnv(t, trg, other, missionScheduleRun(trg, "m-1", "the report"), missionScheduleRun(other, "other-1", "x"))
	base := "/v1/schedules/" + testNS + "/m/runs"

	resp, body := e.do(t, "GET", base+"/m-1", testToken, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %v", resp.StatusCode, body)
	}
	mission, _ := body["mission"].(map[string]any)
	if body["target"] != "mission" || mission["phase"] != "Succeeded" || mission["result"] != "the report" {
		t.Errorf("result = %v", body)
	}
	for _, run := range []string{"other-1", "m-gone"} {
		if resp, _ := e.do(t, "GET", base+"/"+run, testToken, nil); resp.StatusCode != http.StatusNotFound {
			t.Errorf("run %q status = %d, want 404", run, resp.StatusCode)
		}
	}
	_, body = e.do(t, "GET", base, testToken, nil)
	if runs := runsOf(body); len(runs) != 1 || runs[0]["resultAvailable"] != true {
		t.Errorf("runs = %v, want one with resultAvailable", runs)
	}
}

func TestScheduleRunsDisabled(t *testing.T) {
	e := newEnv(t, Options{})
	if resp, _ := e.do(t, "GET", "/v1/schedules/"+testNS+"/x/runs", testToken, nil); resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404 with schedule runs disabled", resp.StatusCode)
	}
}

func TestEffectiveTaskTTL(t *testing.T) {
	if got := EffectiveTaskTTL(7*24*time.Hour, DefaultScheduleResultRetention); got != DefaultScheduleResultRetention {
		t.Errorf("got %v, want the retention", got)
	}
	if got := EffectiveTaskTTL(60*24*time.Hour, DefaultScheduleResultRetention); got != 60*24*time.Hour {
		t.Errorf("got %v, want the task TTL", got)
	}
}
