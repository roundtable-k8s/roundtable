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
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func TestEvalTriggerWhen(t *testing.T) {
	trg := &aiv1alpha1.Trigger{ObjectMeta: metav1.ObjectMeta{Name: "alerts", Namespace: "ns"}}
	ev := TriggerEvent{
		Subject: "rt.events.x",
		Data:    map[string]any{"status": "firing", "labels": map[string]any{"env": "prod"}},
		Headers: map[string]string{"X-GitHub-Event": "push"},
		Time:    time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
	}
	cases := []struct {
		name    string
		expr    string
		want    bool
		wantErr string
	}{
		{name: "data true", expr: `event.data.status == "firing"`, want: true},
		{name: "data false", expr: `event.data.status == "resolved"`},
		{name: "nested", expr: `event.data.labels.env == "prod"`, want: true},
		{name: "header", expr: `event.headers["X-GitHub-Event"] == "push"`, want: true},
		{name: "subject and trigger", expr: `event.subject.startsWith("rt.") && trigger.name == "alerts" && trigger.namespace == "ns"`, want: true},
		{name: "time", expr: `event.time == "2026-01-02T03:04:05Z"`, want: true},
		{name: "guard", expr: `has(event.data.service) && event.data.service == "api"`},
		{name: "missing key", expr: `event.data.service == "api"`, wantErr: "no such key"},
		{name: "non-bool", expr: `event.data.status`, wantErr: "bool"},
		{name: "syntax", expr: `event.data.status ==`, wantErr: "Syntax error"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			trg := trg.DeepCopy()
			trg.Spec.When = c.expr
			got, err := evalTriggerWhen(trg, ev)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != c.want {
				t.Errorf("got %v, want %v", got, c.want)
			}
		})
	}
}

func TestEvalTriggerWhenStringPayload(t *testing.T) {
	trg := &aiv1alpha1.Trigger{Spec: aiv1alpha1.TriggerSpec{When: `event.data.contains("boom")`}}
	got, err := evalTriggerWhen(trg, TriggerEvent{Data: "a boom happened"})
	if err != nil || !got {
		t.Fatalf("got %v, %v", got, err)
	}
}

func TestCompileTriggerWhenCaches(t *testing.T) {
	a, err := compileTriggerWhen(`event.subject == "a"`)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := compileTriggerWhen(`event.subject == "a"`)
	if a != b {
		t.Error("program not cached")
	}
	if _, err := compileTriggerWhen(`nope == 1`); err == nil {
		t.Error("undeclared variable accepted")
	}
}
