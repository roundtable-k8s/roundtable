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

func TestScheduleRunResultKnightTargetNotServedYet(t *testing.T) {
	trg := runScheduleTrigger("k", func(t *aiv1alpha1.Trigger) {
		t.Spec.Target = aiv1alpha1.TriggerTarget{Knight: &aiv1alpha1.TriggerKnightTarget{Name: "operator", Task: "go"}}
	})
	e := newRunsEnv(t, trg)
	if resp, _ := e.do(t, "GET", "/v1/schedules/"+testNS+"/k/runs/task-1", testToken, nil); resp.StatusCode != http.StatusUnprocessableEntity {
		t.Errorf("status = %d, want 422", resp.StatusCode)
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
