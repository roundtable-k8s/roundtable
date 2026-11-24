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

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

func TestStepOutputJSONConsumed(t *testing.T) {
	chain := &aiv1alpha1.Chain{Spec: aiv1alpha1.ChainSpec{Steps: []aiv1alpha1.ChainStep{
		{Name: "plan"},
		{Name: "planner"},
		{Name: "a", When: `json( steps.plan.output ).split`},
		{Name: "b", ForEach: &aiv1alpha1.ChainForEach{From: "steps.fanin"}},
		{Name: "c", ResumeRunID: `json(steps['resume'].output).runId`},
		{Name: "d", Loop: &aiv1alpha1.ChainLoop{Until: `json(steps.until.output).done`}},
		{Name: "e", When: `steps.prose.output.contains("x")`},
	}}}
	for name, want := range map[string]bool{
		"plan": true, "fanin": true, "resume": true, "until": true,
		"planner": false, "prose": false, "a": false,
	} {
		if got := stepOutputJSONConsumed(chain, name); got != want {
			t.Errorf("stepOutputJSONConsumed(%q) = %v, want %v", name, got, want)
		}
	}
}

func TestPlannerOutputJSONConsumedInPRPipeline(t *testing.T) {
	if !stepOutputJSONConsumed(loadPRPipelineExample(t), "plan") {
		t.Fatal("the PR pipeline's plan output must be detected as JSON-consumed")
	}
}

func TestCheckJSONOutput(t *testing.T) {
	chain := &aiv1alpha1.Chain{Spec: aiv1alpha1.ChainSpec{Steps: []aiv1alpha1.ChainStep{
		{Name: "plan"}, {Name: "impl", When: `json(steps.plan.output).ok`},
	}}}
	ss := &aiv1alpha1.ChainStepStatus{Name: "plan"}
	if retry, msg := checkJSONOutput(chain, ss, "```json\n{\"ok\": true}\n```"); retry || msg != "" {
		t.Fatalf("valid fenced JSON: retry=%v msg=%q", retry, msg)
	}
	if retry, msg := checkJSONOutput(chain, ss, "prose"); !retry || msg != "" {
		t.Fatalf("first bad output: retry=%v msg=%q", retry, msg)
	}
	ss.Reason = stepReasonJSONRetry
	if retry, msg := checkJSONOutput(chain, ss, "prose"); retry || !strings.Contains(msg, "after one retry") {
		t.Fatalf("second bad output: retry=%v msg=%q", retry, msg)
	}
	if got := jsonRetryTask("plan it\n", ""); !strings.HasPrefix(got, "plan it\n\nYour previous reply to this task had no parseable JSON block. ") {
		t.Fatalf("jsonRetryTask without error = %q", got)
	}
}
