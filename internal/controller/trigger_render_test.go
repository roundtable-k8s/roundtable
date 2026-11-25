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

	"github.com/nats-io/nats.go"
)

func TestRenderTriggerTemplate(t *testing.T) {
	at := time.Date(2026, 11, 24, 7, 0, 0, 0, time.UTC)
	data := triggerTemplateData{
		Event: TriggerEvent{
			Subject: "fleet-a.events.deploys.prod",
			Data:    eventData([]byte(`{"service":"api","version":"1.2.3","tags":["a","b"]}`)),
			Headers: eventHeaders(nats.Header{"Nats-Msg-Id": {"m1"}, "X-Multi": {"a", "b"}}),
			Time:    at,
		},
		Trigger: triggerRef{Name: "on-deploy", Namespace: "roundtable"},
	}
	cases := []struct {
		name, tmpl, want, wantErr string
	}{
		{name: "empty", tmpl: "", want: ""},
		{name: "json field", tmpl: `{"service": "{{ .Event.Data.service }}"}`, want: `{"service": "api"}`},
		{name: "subject and time", tmpl: `{{ .Event.Subject }} {{ .Event.Time.Format "2006-01-02" }}`, want: "fleet-a.events.deploys.prod 2026-11-24"},
		{name: "headers", tmpl: `{{ index .Event.Headers "Nats-Msg-Id" }}/{{ index .Event.Headers "X-Multi" }}`, want: "m1/a, b"},
		{name: "toJson", tmpl: `{"tags": {{ toJson .Event.Data.tags }}, "who": {{ toJson .Trigger.Name }}}`, want: `{"tags": ["a","b"], "who": "on-deploy"}`},
		{name: "missing key", tmpl: `{{ .Event.Data.missing }}`, wantErr: "missing"},
		{name: "parse error", tmpl: `{{ .Event.Data.service `, wantErr: "unclosed action"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := renderTriggerTemplate("input", tc.tmpl, data)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestEventData(t *testing.T) {
	if got, ok := eventData([]byte(`{"a":1}`)).(map[string]any); !ok || got["a"] != float64(1) {
		t.Fatalf("JSON object = %#v", got)
	}
	if got := eventData([]byte("plain text")); got != "plain text" {
		t.Fatalf("raw = %#v", got)
	}
	if got := eventData(nil); got != "" {
		t.Fatalf("empty = %#v", got)
	}
	// A raw string payload renders as itself.
	out, err := renderTriggerTemplate("input", "{{ .Event.Data }}", triggerTemplateData{Event: TriggerEvent{Data: eventData([]byte("hi"))}})
	if err != nil || out != "hi" {
		t.Fatalf("render raw = %q, %v", out, err)
	}
}

func TestDedupeHash(t *testing.T) {
	a, b := dedupeHash("m1"), dedupeHash("m2")
	if a == b || len(a) != 12 || a != dedupeHash("m1") {
		t.Fatalf("dedupeHash: %q %q", a, b)
	}
}
