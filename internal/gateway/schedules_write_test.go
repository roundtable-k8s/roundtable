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
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

type scheduleEnv struct {
	env
	k8s client.Client
}

// newScheduleWriteEnv serves the gateway with schedule writes enabled over a fake
// Kubernetes API holding a RoundTable and a Chain.
func newScheduleWriteEnv(t *testing.T, enabled bool, funcs *interceptor.Funcs, extra ...client.Object) *scheduleEnv {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = aiv1alpha1.AddToScheme(scheme)
	objs := append([]client.Object{
		&aiv1alpha1.RoundTable{ObjectMeta: metav1.ObjectMeta{Name: "dev", Namespace: testNS}},
		&aiv1alpha1.Chain{ObjectMeta: metav1.ObjectMeta{Name: "digest", Namespace: testNS}},
		&aiv1alpha1.Chain{ObjectMeta: metav1.ObjectMeta{Name: "report", Namespace: testNS}},
	}, extra...)
	b := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
		WithStatusSubresource(&aiv1alpha1.Trigger{})
	if funcs != nil {
		b = b.WithInterceptorFuncs(*funcs)
	}
	k8s := b.Build()
	svc := NewService(&Resolver{Client: k8s}, nil, Options{Namespace: testNS}, logr.Discard())
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

func scheduleSpec(cron, tz, chain string) map[string]any {
	sch := map[string]any{"cron": cron}
	if tz != "" {
		sch["timeZone"] = tz
	}
	return map[string]any{
		"source": map[string]any{"schedule": sch},
		"target": map[string]any{"chainRef": map[string]any{"name": chain}},
	}
}

func writeTestTrigger(name string, labels, annotations map[string]string) *aiv1alpha1.Trigger {
	return &aiv1alpha1.Trigger{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, Labels: labels, Annotations: annotations},
		Spec: aiv1alpha1.TriggerSpec{
			Source: aiv1alpha1.TriggerSource{Schedule: &aiv1alpha1.TriggerScheduleSource{Cron: "0 7 * * *"}},
			Target: aiv1alpha1.TriggerTarget{ChainRef: &corev1.LocalObjectReference{Name: "digest"}},
		},
	}
}

func TestCreateSchedule(t *testing.T) {
	e := newScheduleWriteEnv(t, true, nil)
	body := map[string]any{"name": "morning-digest", "spec": scheduleSpec("0 7 * * 1-5", "Europe/Berlin", "digest")}
	body["spec"].(map[string]any)["roundTableRef"] = map[string]any{"name": "dev"}

	resp, out := e.do(t, http.MethodPost, "/v1/schedules", testToken, body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status %d: %v", resp.StatusCode, out)
	}
	if loc := resp.Header.Get("Location"); loc != "/v1/schedules/"+testNS+"/morning-digest" {
		t.Errorf("Location = %q", loc)
	}
	if out["managedBy"] != string(ManagedByAPI) || out["caller"] != defaultTokenName || out["namespace"] != testNS {
		t.Errorf("response = %v", out)
	}

	trg := e.trigger(t, "morning-digest")
	if trg.Labels[labelManagedBy] != managedByGateway {
		t.Errorf("labels = %v", trg.Labels)
	}
	if trg.Annotations[aiv1alpha1.AnnotationCaller] != defaultTokenName {
		t.Errorf("annotations = %v", trg.Annotations)
	}
	if s := trg.Spec.Source.Schedule; s == nil || s.Cron != "0 7 * * 1-5" || s.TimeZone != "Europe/Berlin" {
		t.Errorf("schedule = %+v", s)
	}
	if trg.Spec.Target.ChainRef.Name != "digest" || trg.Spec.RoundTableRef.Name != "dev" {
		t.Errorf("spec = %+v", trg.Spec)
	}

	// The same name again is a 409.
	resp, out = e.do(t, http.MethodPost, "/v1/schedules", testToken, body)
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("duplicate: status %d: %v", resp.StatusCode, out)
	}
}

func TestCreateScheduleValidation(t *testing.T) {
	e := newScheduleWriteEnv(t, true, nil)
	spec := func(mut func(map[string]any)) map[string]any {
		s := scheduleSpec("0 7 * * *", "", "digest")
		mut(s)
		return s
	}
	sched := func(s map[string]any) map[string]any {
		return s["source"].(map[string]any)["schedule"].(map[string]any)
	}
	cases := []struct {
		name  string
		body  map[string]any
		field string
	}{
		{"missing name", map[string]any{"spec": spec(func(map[string]any) {})}, "name"},
		{"long name", map[string]any{"name": strings.Repeat("a", 64), "spec": spec(func(map[string]any) {})}, "name"},
		{"bad name", map[string]any{"name": "Bad_Name", "spec": spec(func(map[string]any) {})}, "name"},
		{"namespace not allowed", map[string]any{"name": "x", "namespace": "other", "spec": spec(func(map[string]any) {})}, "namespace"},
		{"bad cron", map[string]any{"name": "x", "spec": spec(func(s map[string]any) { sched(s)["cron"] = "every day" })}, "spec.source.schedule.cron"},
		{"six-field cron", map[string]any{"name": "x", "spec": spec(func(s map[string]any) { sched(s)["cron"] = "0 0 7 * * *" })}, "spec.source.schedule.cron"},
		{"empty cron", map[string]any{"name": "x", "spec": spec(func(s map[string]any) { sched(s)["cron"] = "" })}, "spec.source.schedule.cron"},
		{"TZ prefix", map[string]any{"name": "x", "spec": spec(func(s map[string]any) { sched(s)["cron"] = "TZ=UTC 0 7 * * *" })}, "spec.source.schedule.cron"},
		{"bad time zone", map[string]any{"name": "x", "spec": spec(func(s map[string]any) { sched(s)["timeZone"] = "Mars/Olympus" })}, "spec.source.schedule.timeZone"},
		{"Local time zone", map[string]any{"name": "x", "spec": spec(func(s map[string]any) { sched(s)["timeZone"] = "Local" })}, "spec.source.schedule.timeZone"},
		{"bad deadline", map[string]any{"name": "x", "spec": spec(func(s map[string]any) { sched(s)["startingDeadlineSeconds"] = 0 })}, "spec.source.schedule.startingDeadlineSeconds"},
		{"no schedule", map[string]any{"name": "x", "spec": spec(func(s map[string]any) { s["source"] = map[string]any{} })}, "spec.source.schedule"},
		{"nats source", map[string]any{"name": "x", "spec": spec(func(s map[string]any) {
			s["source"].(map[string]any)["nats"] = map[string]any{"subject": "dev.events.>"}
		})}, "spec.source"},
		{"no target", map[string]any{"name": "x", "spec": spec(func(s map[string]any) { delete(s, "target") })}, "spec.target"},
		{"missing chain", map[string]any{"name": "x", "spec": spec(func(s map[string]any) {
			s["target"] = map[string]any{"chainRef": map[string]any{"name": "nope"}}
		})}, "spec.target.chainRef.name"},
		{"missing table", map[string]any{"name": "x", "spec": spec(func(s map[string]any) {
			s["roundTableRef"] = map[string]any{"name": "nope"}
		})}, "spec.roundTableRef.name"},
		{"bad policy", map[string]any{"name": "x", "spec": spec(func(s map[string]any) {
			s["concurrency"] = map[string]any{"policy": "Sometimes"}
		})}, "spec.concurrency.policy"},
		{"negative maxActive", map[string]any{"name": "x", "spec": spec(func(s map[string]any) {
			s["concurrency"] = map[string]any{"maxActive": -1}
		})}, "spec.concurrency.maxActive"},
		{"bad input template", map[string]any{"name": "x", "spec": spec(func(s map[string]any) { s["input"] = "{{ .Oops" })}, "spec.input"},
		{"bad dedupeKey template", map[string]any{"name": "x", "spec": spec(func(s map[string]any) { s["dedupeKey"] = "{{ .Oops" })}, "spec.dedupeKey"},
		{"negative ttl", map[string]any{"name": "x", "spec": spec(func(s map[string]any) { s["ttlSecondsAfterFinished"] = -1 })}, "spec.ttlSecondsAfterFinished"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, out := e.do(t, http.MethodPost, "/v1/schedules", testToken, tc.body)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status %d: %v", resp.StatusCode, out)
			}
			if out["field"] != tc.field {
				t.Errorf("field = %v, want %s (%v)", out["field"], tc.field, out["error"])
			}
			if msg, _ := out["error"].(string); !strings.Contains(msg, tc.field) {
				t.Errorf("error %q does not name %s", msg, tc.field)
			}
		})
	}

	// Labels, annotations or status in the body are unknown fields.
	resp, out := e.do(t, http.MethodPost, "/v1/schedules", testToken, map[string]any{
		"name": "x", "labels": map[string]string{"a": "b"}, "spec": spec(func(map[string]any) {}),
	})
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("unknown field: status %d: %v", resp.StatusCode, out)
	}
}

func knightSpec(knight, task string) map[string]any {
	s := scheduleSpec("0 7 * * *", "", "digest")
	s["target"] = map[string]any{"knight": map[string]any{"name": knight, "task": task}}
	return s
}

func testKnight(name, table string) *aiv1alpha1.Knight {
	k := &aiv1alpha1.Knight{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS}}
	if table != "" {
		k.Labels = map[string]string{aiv1alpha1.LabelRoundTable: table}
	}
	k.Spec.NATS.Subjects = []string{"fleet.tasks." + name + ".>"}
	return k
}

func TestCreateScheduleKnightTarget(t *testing.T) {
	e := newScheduleWriteEnv(t, true, nil, testKnight("galahad", "dev"), testKnight("loner", ""))
	resp, out := e.do(t, http.MethodPost, "/v1/schedules", testToken, map[string]any{
		"name": "nightly", "spec": knightSpec("galahad", "Run {{ .Trigger.Name }}"),
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status %d: %v", resp.StatusCode, out)
	}
	trg := e.trigger(t, "nightly")
	if trg.Spec.Target.Knight == nil || trg.Spec.Target.Knight.Name != "galahad" || trg.Spec.Target.ChainRef != nil {
		t.Errorf("target = %+v", trg.Spec.Target)
	}

	// PUT keeps a knight target.
	resp, out = e.do(t, http.MethodPut, "/v1/schedules/"+testNS+"/nightly", testToken, map[string]any{
		"spec": knightSpec("galahad", "Run again"),
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("replace: status %d: %v", resp.StatusCode, out)
	}

	bad := func(mut func(map[string]any)) map[string]any {
		s := knightSpec("galahad", "do it")
		mut(s)
		return map[string]any{"name": "x", "spec": s}
	}
	cases := []struct {
		name  string
		body  map[string]any
		field string
	}{
		{"both", bad(func(s map[string]any) {
			s["target"].(map[string]any)["chainRef"] = map[string]any{"name": "digest"}
		}), "spec.target"},
		{"neither", bad(func(s map[string]any) { s["target"] = map[string]any{} }), "spec.target"},
		{"missing knight", map[string]any{"name": "x", "spec": knightSpec("nope", "t")}, "spec.target.knight.name"},
		{"empty task", map[string]any{"name": "x", "spec": knightSpec("galahad", "")}, "spec.target.knight.task"},
		{"bad task template", map[string]any{"name": "x", "spec": knightSpec("galahad", "{{ .Oops")}, "spec.target.knight.task"},
		{"input set", bad(func(s map[string]any) { s["input"] = "hi" }), "spec.input"},
		{"replace policy", bad(func(s map[string]any) { s["concurrency"] = map[string]any{"policy": "Replace"} }), "spec.concurrency.policy"},
		{"wrong table", bad(func(s map[string]any) {
			s["target"].(map[string]any)["knight"].(map[string]any)["name"] = "loner"
			s["roundTableRef"] = map[string]any{"name": "dev"}
		}), "spec.target.knight.name"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, out := e.do(t, http.MethodPost, "/v1/schedules", testToken, tc.body)
			if resp.StatusCode != http.StatusBadRequest || out["field"] != tc.field {
				t.Fatalf("status %d, field %v, want 400 %s: %v", resp.StatusCode, out["field"], tc.field, out)
			}
		})
	}
}

func missionSpec(objective string) map[string]any {
	s := scheduleSpec("0 7 * * *", "", "digest")
	s["target"] = map[string]any{"mission": map[string]any{
		"objective": objective, "successCriteria": "Report for {{ .Trigger.Name }} posted", "costBudgetUSD": "2.50",
	}}
	s["roundTableRef"] = map[string]any{"name": "dev"}
	return s
}

func TestCreateScheduleMissionTarget(t *testing.T) {
	e := newScheduleWriteEnv(t, true, nil)
	resp, out := e.do(t, http.MethodPost, "/v1/schedules", testToken, map[string]any{
		"name": "weekly", "spec": missionSpec("Review {{ .Trigger.Name }}"),
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status %d: %v", resp.StatusCode, out)
	}
	trg := e.trigger(t, "weekly")
	if m := trg.Spec.Target.Mission; m == nil || m.Objective != "Review {{ .Trigger.Name }}" || trg.Spec.Target.ChainRef != nil {
		t.Errorf("target = %+v", trg.Spec.Target)
	}

	// PUT keeps a mission target.
	resp, out = e.do(t, http.MethodPut, "/v1/schedules/"+testNS+"/weekly", testToken, map[string]any{
		"spec": missionSpec("Review again"),
	})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("replace: status %d: %v", resp.StatusCode, out)
	}
	if trg := e.trigger(t, "weekly"); trg.Spec.Target.Mission == nil || trg.Spec.Target.Mission.Objective != "Review again" {
		t.Errorf("replaced target = %+v", trg.Spec.Target)
	}

	bad := func(mut func(map[string]any)) map[string]any {
		s := missionSpec("do it")
		mut(s)
		return map[string]any{"name": "x", "spec": s}
	}
	mission := func(s map[string]any) map[string]any {
		return s["target"].(map[string]any)["mission"].(map[string]any)
	}
	cases := []struct {
		name  string
		body  map[string]any
		field string
	}{
		{"with chainRef", bad(func(s map[string]any) {
			s["target"].(map[string]any)["chainRef"] = map[string]any{"name": "digest"}
		}), "spec.target"},
		{"with knight", bad(func(s map[string]any) {
			s["target"].(map[string]any)["knight"] = map[string]any{"name": "galahad", "task": "t"}
		}), "spec.target"},
		{"no roundTableRef", bad(func(s map[string]any) { delete(s, "roundTableRef") }), "spec.roundTableRef.name"},
		{"missing table", bad(func(s map[string]any) { s["roundTableRef"] = map[string]any{"name": "nope"} }), "spec.roundTableRef.name"},
		{"input set", bad(func(s map[string]any) { s["input"] = "hi" }), "spec.input"},
		{"replace policy", bad(func(s map[string]any) { s["concurrency"] = map[string]any{"policy": "Replace"} }), "spec.concurrency.policy"},
		{"empty objective", bad(func(s map[string]any) { mission(s)["objective"] = "" }), "spec.target.mission.objective"},
		{"bad objective template", bad(func(s map[string]any) { mission(s)["objective"] = "{{ .Oops" }), "spec.target.mission.objective"},
		{"bad successCriteria template", bad(func(s map[string]any) { mission(s)["successCriteria"] = "{{ .Oops" }), "spec.target.mission.successCriteria"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, out := e.do(t, http.MethodPost, "/v1/schedules", testToken, tc.body)
			if resp.StatusCode != http.StatusBadRequest || out["field"] != tc.field {
				t.Fatalf("status %d, field %v, want 400 %s: %v", resp.StatusCode, out["field"], tc.field, out)
			}
		})
	}
}

func TestCreateScheduleAPIServerValidation(t *testing.T) {
	funcs := &interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			return apierrors.NewInvalid(schema.GroupKind{Group: "ai.roundtable.io", Kind: "Trigger"}, obj.GetName(),
				field.ErrorList{field.Invalid(field.NewPath("spec", "source", "schedule", "timeZone"), "x", "should match pattern")})
		},
	}
	e := newScheduleWriteEnv(t, true, funcs)
	resp, out := e.do(t, http.MethodPost, "/v1/schedules", testToken,
		map[string]any{"name": "x", "spec": scheduleSpec("0 7 * * *", "", "digest")})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d: %v", resp.StatusCode, out)
	}
	if out["field"] != "spec.source.schedule.timeZone" {
		t.Errorf("field = %v (%v)", out["field"], out["error"])
	}
}

func TestReplaceSchedule(t *testing.T) {
	existing := writeTestTrigger("nightly", map[string]string{labelManagedBy: managedByGateway, "team": "a"}, nil)
	e := newScheduleWriteEnv(t, true, nil, existing)

	resp, out := e.do(t, http.MethodPut, "/v1/schedules/"+testNS+"/nightly", testToken,
		map[string]any{"spec": scheduleSpec("30 2 * * *", "America/New_York", "report")})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %v", resp.StatusCode, out)
	}
	trg := e.trigger(t, "nightly")
	if s := trg.Spec.Source.Schedule; s.Cron != "30 2 * * *" || s.TimeZone != "America/New_York" {
		t.Errorf("schedule = %+v", s)
	}
	if trg.Spec.Target.ChainRef.Name != "report" {
		t.Errorf("target = %+v", trg.Spec.Target)
	}
	if trg.Labels["team"] != "a" || trg.Labels[labelManagedBy] != managedByGateway {
		t.Errorf("labels not kept: %v", trg.Labels)
	}
	if trg.Annotations[aiv1alpha1.AnnotationCaller] != defaultTokenName {
		t.Errorf("caller not recorded: %v", trg.Annotations)
	}

	// A body naming another object, a missing schedule and a non-schedule
	// Trigger.
	resp, out = e.do(t, http.MethodPut, "/v1/schedules/"+testNS+"/nightly", testToken,
		map[string]any{"name": "other", "spec": scheduleSpec("0 7 * * *", "", "digest")})
	if resp.StatusCode != http.StatusBadRequest || out["field"] != "name" {
		t.Errorf("name mismatch: status %d: %v", resp.StatusCode, out)
	}
	resp, _ = e.do(t, http.MethodPut, "/v1/schedules/"+testNS+"/missing", testToken,
		map[string]any{"spec": scheduleSpec("0 7 * * *", "", "digest")})
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("missing: status %d", resp.StatusCode)
	}
	resp, out = e.do(t, http.MethodPut, "/v1/schedules/"+testNS+"/nightly", testToken,
		map[string]any{"spec": scheduleSpec("not cron", "", "digest")})
	if resp.StatusCode != http.StatusBadRequest || out["field"] != "spec.source.schedule.cron" {
		t.Errorf("bad cron: status %d: %v", resp.StatusCode, out)
	}
}

func TestReplaceScheduleNonSchedule(t *testing.T) {
	nats := &aiv1alpha1.Trigger{
		ObjectMeta: metav1.ObjectMeta{Name: "on-deploy", Namespace: testNS},
		Spec: aiv1alpha1.TriggerSpec{
			Source:        aiv1alpha1.TriggerSource{NATS: &aiv1alpha1.TriggerNATSSource{Subject: "dev.events.>"}},
			Target:        aiv1alpha1.TriggerTarget{ChainRef: &corev1.LocalObjectReference{Name: "digest"}},
			RoundTableRef: &corev1.LocalObjectReference{Name: "dev"},
		},
	}
	e := newScheduleWriteEnv(t, true, nil, nats)
	resp, _ := e.do(t, http.MethodPut, "/v1/schedules/"+testNS+"/on-deploy", testToken,
		map[string]any{"spec": scheduleSpec("0 7 * * *", "", "digest")})
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("PUT non-schedule: status %d", resp.StatusCode)
	}
	resp, _ = e.do(t, http.MethodDelete, "/v1/schedules/"+testNS+"/on-deploy", testToken, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("DELETE non-schedule: status %d", resp.StatusCode)
	}
	if e.trigger(t, "on-deploy").Spec.Source.NATS == nil {
		t.Error("non-schedule Trigger changed")
	}
}

func TestDeleteSchedule(t *testing.T) {
	e := newScheduleWriteEnv(t, true, nil,
		writeTestTrigger("nightly", map[string]string{labelManagedBy: managedByGateway}, nil),
		writeTestTrigger("by-hand", nil, nil))
	for _, name := range []string{"nightly", "by-hand"} {
		resp, out := e.do(t, http.MethodDelete, "/v1/schedules/"+testNS+"/"+name, testToken, nil)
		if resp.StatusCode != http.StatusOK || out["deleted"] != testNS+"/"+name {
			t.Fatalf("delete %s: status %d: %v", name, resp.StatusCode, out)
		}
		err := e.k8s.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: name}, &aiv1alpha1.Trigger{})
		if !apierrors.IsNotFound(err) {
			t.Errorf("%s still exists: %v", name, err)
		}
	}
	resp, _ := e.do(t, http.MethodDelete, "/v1/schedules/"+testNS+"/nightly", testToken, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("second delete: status %d", resp.StatusCode)
	}
}

func TestGitManagedScheduleIsReadOnly(t *testing.T) {
	cases := []struct {
		name        string
		labels      map[string]string
		annotations map[string]string
		where       string
	}{
		{"kustomization", map[string]string{
			fluxKustomizationName: "apps", fluxKustomizationNS: "flux-system",
		}, nil, "Flux Kustomization flux-system/apps"},
		{"helmrelease", map[string]string{
			fluxHelmReleaseName: "roundtable", fluxHelmReleaseNS: "flux-system", labelManagedBy: "Helm",
		}, nil, "Flux HelmRelease flux-system/roundtable"},
		{"helm", map[string]string{labelManagedBy: "Helm"}, map[string]string{
			helmReleaseName: "rt", helmReleaseNS: "roundtable",
		}, "Helm release roundtable/rt"},
		{"helm-unnamed", map[string]string{labelManagedBy: "Helm"}, nil, "in Git"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newScheduleWriteEnv(t, true, nil, writeTestTrigger("from-git", tc.labels, tc.annotations))
			path := "/v1/schedules/" + testNS + "/from-git"
			resp, out := e.do(t, http.MethodPut, path, testToken,
				map[string]any{"spec": scheduleSpec("0 9 * * *", "", "report")})
			if resp.StatusCode != http.StatusConflict || out["managedBy"] != string(ManagedByGit) {
				t.Fatalf("PUT: status %d: %v", resp.StatusCode, out)
			}
			if msg, _ := out["error"].(string); !strings.Contains(msg, tc.where) || !strings.Contains(msg, "Git") {
				t.Errorf("PUT message %q does not say %q", msg, tc.where)
			}
			resp, out = e.do(t, http.MethodDelete, path, testToken, nil)
			if resp.StatusCode != http.StatusConflict || out["managedBy"] != string(ManagedByGit) {
				t.Fatalf("DELETE: status %d: %v", resp.StatusCode, out)
			}
			trg := e.trigger(t, "from-git")
			if trg.Spec.Source.Schedule.Cron != "0 7 * * *" || trg.Spec.Target.ChainRef.Name != "digest" {
				t.Errorf("Git-managed Trigger changed: %+v", trg.Spec)
			}
		})
	}
}

func TestManagedBy(t *testing.T) {
	cases := []struct {
		labels, annotations map[string]string
		want                ManagedBy
	}{
		{map[string]string{fluxKustomizationName: "apps"}, nil, ManagedByGit},
		{nil, map[string]string{fluxKustomizationName: "apps"}, ManagedByGit},
		{map[string]string{fluxHelmReleaseName: "rt"}, nil, ManagedByGit},
		{map[string]string{labelManagedBy: "Helm"}, nil, ManagedByGit},
		{map[string]string{labelManagedBy: managedByGateway}, nil, ManagedByAPI},
		{map[string]string{labelManagedBy: "kubectl"}, nil, ManagedByOther},
		{nil, nil, ManagedByOther},
	}
	for _, tc := range cases {
		obj := &aiv1alpha1.Trigger{ObjectMeta: metav1.ObjectMeta{Labels: tc.labels, Annotations: tc.annotations}}
		if got := ScheduleManagedBy(obj); got != tc.want {
			t.Errorf("ManagedBy(%v, %v) = %s, want %s", tc.labels, tc.annotations, got, tc.want)
		}
	}
}

func TestScheduleWritesDisabled(t *testing.T) {
	e := newScheduleWriteEnv(t, false, nil, writeTestTrigger("nightly", nil, nil))
	resp, _ := e.do(t, http.MethodPost, "/v1/schedules", testToken,
		map[string]any{"name": "x", "spec": scheduleSpec("0 7 * * *", "", "digest")})
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("POST: status %d", resp.StatusCode)
	}
	resp, _ = e.do(t, http.MethodDelete, "/v1/schedules/"+testNS+"/nightly", testToken, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("DELETE: status %d", resp.StatusCode)
	}
	resp, _ = e.do(t, http.MethodPost, "/v1/schedules", "", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("no token: status %d", resp.StatusCode)
	}
}
