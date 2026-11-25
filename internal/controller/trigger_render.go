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
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"text/template"
	"time"

	"github.com/nats-io/nats.go"
)

// TriggerEvent is what a Trigger's input and dedupeKey templates see as
// .Event.
type TriggerEvent struct {
	// Subject is the NATS subject (empty for schedule).
	Subject string
	// Data is the payload parsed as JSON, or the raw string when it is not
	// JSON (nil for schedule).
	Data any
	// Headers maps a header name to its values joined with ", ".
	Headers map[string]string
	// Time is when the event was published, or the scheduled time.
	Time time.Time
}

// triggerTemplateData is the root object of a Trigger's templates.
type triggerTemplateData struct {
	Event   TriggerEvent
	Trigger triggerRef
}

type triggerRef struct {
	Name      string
	Namespace string
}

var triggerFuncs = template.FuncMap{
	"toJson": func(v any) (string, error) {
		b, err := json.Marshal(v)
		return string(b), err
	},
}

// parseTriggerTemplate parses a Trigger template. A missing map key is an
// error, so an event without a field the template needs fails to render
// instead of rendering "<no value>".
func parseTriggerTemplate(name, text string) (*template.Template, error) {
	return template.New(name).Option("missingkey=error").Funcs(triggerFuncs).Parse(text)
}

// renderTriggerTemplate renders text with the event. Empty text renders "".
func renderTriggerTemplate(name, text string, data triggerTemplateData) (string, error) {
	if text == "" {
		return "", nil
	}
	tmpl, err := parseTriggerTemplate(name, text)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// eventData parses a payload as JSON, falling back to the raw string.
func eventData(payload []byte) any {
	if len(bytes.TrimSpace(payload)) == 0 {
		return string(payload)
	}
	var v any
	if err := json.Unmarshal(payload, &v); err != nil {
		return string(payload)
	}
	return v
}

// eventHeaders flattens NATS headers into name -> values joined by ", ".
func eventHeaders(h nats.Header) map[string]string {
	out := make(map[string]string, len(h))
	for k, v := range h {
		out[k] = strings.Join(v, ", ")
	}
	return out
}

// dedupeHash is the short, name-safe hash of a dedupe key that names a run.
func dedupeHash(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])[:12]
}
