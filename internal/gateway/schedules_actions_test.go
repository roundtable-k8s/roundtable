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
	"github.com/roundtable-k8s/roundtable/internal/chainrun"
)

// scheduleTrigger is a schedule Trigger targeting the Chain "digest".
func scheduleTrigger(name string, labels map[string]string) *aiv1alpha1.Trigger {
	return &aiv1alpha1.Trigger{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, Labels: labels},
		Spec: aiv1alpha1.TriggerSpec{
			Source: aiv1alpha1.TriggerSource{Schedule: &aiv1alpha1.TriggerScheduleSource{Cron: "0 7 * * *"}},
			Target: aiv1alpha1.TriggerTarget{ChainRef: &corev1.LocalObjectReference{Name: "digest"}},
		},
	}
}

type scheduleEnv struct {
	env
	k8s client.Client
}

// newScheduleEnv serves the gateway over a fake API holding the Chain
// "digest", a Git-managed schedule "git-digest" (Flux Kustomization labels), a
// schedule "suspended" with spec.suspend, a schedule "orphan" whose Chain is
// missing and a NATS Trigger "events".
func newScheduleEnv(t *testing.T, enabled bool) *scheduleEnv {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = aiv1alpha1.AddToScheme(scheme)
	suspended := scheduleTrigger("suspended", nil)
	suspended.Spec.Suspend = true
	orphan := scheduleTrigger("orphan", nil)
	orphan.Spec.Target.ChainRef.Name = "gone"
	events := scheduleTrigger("events", nil)
	events.Spec.Source = aiv1alpha1.TriggerSource{NATS: &aiv1alpha1.TriggerNATSSource{Subject: "x.events.y"}}
	k8s := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		templateChain("digest", false),
		scheduleTrigger("git-digest", map[string]string{
			"kustomize.toolkit.fluxcd.io/name":      "apps",
			"kustomize.toolkit.fluxcd.io/namespace": "flux-system",
		}),
		suspended, orphan, events,
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
	return &scheduleEnv{env: env{svc: svc, server: srv}, k8s: k8s}
}

func (e *scheduleEnv) trigger(t *testing.T, name string) *aiv1alpha1.Trigger {
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
func (e *scheduleEnv) playTrigger(t *testing.T, name, skipReason string) {
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
			firing.Run = chainrun.TriggerRunName(trg.Spec.Target.ChainRef.Name, trg.Name, key)
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
	e := newScheduleEnv(t, true)
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
	e := newScheduleEnv(t, true)
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
	e := newScheduleEnv(t, true)
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
	e := newScheduleEnv(t, true)
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

func TestRunScheduleSkipped(t *testing.T) {
	e := newScheduleEnv(t, true)
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
	e := newScheduleEnv(t, true)
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
	e := newScheduleEnv(t, false)
	for _, action := range []string{"suspend", "resume", "run"} {
		resp, _ := e.do(t, http.MethodPost, "/v1/schedules/"+testNS+"/git-digest/"+action, testToken, nil)
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s disabled: %d, want 404", action, resp.StatusCode)
		}
	}
}
