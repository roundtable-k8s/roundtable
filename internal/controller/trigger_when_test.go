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
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

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

func whenTrigger(uid, expr string, gen int64) *aiv1alpha1.Trigger {
	return &aiv1alpha1.Trigger{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "t", UID: types.UID(uid), Generation: gen},
		Spec:       aiv1alpha1.TriggerSpec{When: expr},
	}
}

func whenCacheLen() int {
	triggerWhenMu.Lock()
	defer triggerWhenMu.Unlock()
	return len(triggerWhenPrograms)
}

func TestCompileTriggerWhenCaches(t *testing.T) {
	trg := whenTrigger("uid-cache", `event.subject == "a"`, 1)
	t.Cleanup(func() { forgetTriggerWhen(client.ObjectKeyFromObject(trg)) })
	a, err := compileTriggerWhen(trg)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := compileTriggerWhen(trg)
	if a != b {
		t.Error("program not cached")
	}
	if _, err := compileTriggerWhen(whenTrigger("uid-bad", `nope == 1`, 1)); err == nil {
		t.Error("undeclared variable accepted")
	}
}

func TestCompileTriggerWhenEvicts(t *testing.T) {
	base := whenCacheLen()
	trg := whenTrigger("uid-evict", `event.subject == "a"`, 1)
	t.Cleanup(func() { forgetTriggerWhen(client.ObjectKeyFromObject(trg)) })
	if _, err := compileTriggerWhen(trg); err != nil {
		t.Fatal(err)
	}
	// A changed expression replaces the entry rather than adding one.
	trg.Generation, trg.Spec.When = 2, `event.subject == "b"`
	if _, err := compileTriggerWhen(trg); err != nil {
		t.Fatal(err)
	}
	if got := whenCacheLen(); got != base+1 {
		t.Fatalf("cache has %d entries, want %d", got, base+1)
	}
	got, err := evalTriggerWhen(trg, TriggerEvent{Subject: "b"})
	if err != nil || !got {
		t.Fatalf("changed expression not recompiled: %v, %v", got, err)
	}
	// Deleting the Trigger drops its entry.
	forgetTriggerWhen(client.ObjectKeyFromObject(trg))
	if got := whenCacheLen(); got != base {
		t.Fatalf("cache has %d entries after delete, want %d", got, base)
	}
}
