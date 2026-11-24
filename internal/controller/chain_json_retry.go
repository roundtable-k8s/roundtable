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
	"fmt"
	"regexp"
	"strings"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// A step whose output later steps read as JSON (json(steps.<name>.output) in a
// when / until / resumeRunId expression, or forEach.from: steps.<name>) must
// reply with a JSON document. A reply without one (#314: prose or a broken
// fence around the plan) would fail every dependent step, so the step is
// re-run once with a short reminder; a second unparseable reply fails the
// step itself, with the parse error.

// stepReasonJSONRetry marks a step re-dispatched because its output had no
// parseable JSON document. It stays set while the retry is pending/running.
const stepReasonJSONRetry = "JSONRetry"

// jsonRetryReminder is appended to the task of the retried step.
const jsonRetryReminder = "\n\nYour previous reply to this task had no parseable JSON block%s. " +
	"Do the task again if needed, then reply with ONLY the JSON block the task asks for: " +
	"one fenced ```json block, no other text."

// stepOutputJSONConsumed reports whether a later step reads step name's
// output as JSON.
func stepOutputJSONConsumed(chain *aiv1alpha1.Chain, name string) bool {
	q := regexp.QuoteMeta(name)
	ref := regexp.MustCompile(`json\(\s*steps\s*(?:\.\s*` + q + `|\[\s*["']` + q + `["']\s*\])\s*\.\s*output\s*\)`)
	for i := range chain.Spec.Steps {
		s := &chain.Spec.Steps[i]
		if s.Name == name {
			continue
		}
		if s.ForEach != nil && s.ForEach.From == "steps."+name {
			return true
		}
		exprs := []string{s.When, s.ResumeRunID}
		if s.Loop != nil {
			exprs = append(exprs, s.Loop.Until)
		}
		for _, e := range exprs {
			if e != "" && ref.MatchString(e) {
				return true
			}
		}
	}
	return false
}

// checkJSONOutput handles a successful result of a step whose output must be
// JSON. It returns retry=true when the step must be re-dispatched once, or a
// non-empty failMsg when the retried step still replied without JSON.
func checkJSONOutput(chain *aiv1alpha1.Chain, ss *aiv1alpha1.ChainStepStatus, output string) (retry bool, failMsg string) {
	if !stepOutputJSONConsumed(chain, ss.Name) {
		return false, ""
	}
	_, err := parseJSONDocument(output)
	if err == nil {
		return false, ""
	}
	if ss.Reason != stepReasonJSONRetry {
		return true, ""
	}
	return false, fmt.Sprintf("output has no parseable JSON block after one retry: %v", err)
}

// jsonRetryTask appends the reminder to a retried step's task.
func jsonRetryTask(task, parseErr string) string {
	if parseErr != "" {
		parseErr = " (" + truncateUTF8(parseErr, 200) + ")"
	}
	return strings.TrimRight(task, "\n") + fmt.Sprintf(jsonRetryReminder, parseErr)
}
