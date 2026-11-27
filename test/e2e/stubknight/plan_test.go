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

package main

import (
	"encoding/json"
	"testing"

	"github.com/roundtable-k8s/roundtable/internal/mission"
)

const samplePlan = `{"planVersion":"v1alpha1","metadata":{"objective":"Summarise the release"},` +
	`"chains":[{"name":"summarise","steps":[{"name":"gather","knightRef":{"name":"stub"},"task":"Gather facts."}]}]}`

func TestPlannerReply(t *testing.T) {
	planningTask := plannerPrompt +
		" Your task is to generate a detailed execution plan.\n\n**Mission Objective:**\nSummarise the release"
	cases := []struct {
		name     string
		task     string
		planJSON string
		wantOK   bool
	}{
		{"planning prompt with plan", planningTask, samplePlan, true},
		{"leading whitespace is trimmed", "\n  " + planningTask, samplePlan, true},
		{"normal task", "Gather facts about the release.", samplePlan, false},
		{"planner words not at the start", "Please: " + planningTask, samplePlan, false},
		{"planning prompt without plan", planningTask, "", false},
		{"empty task", "", samplePlan, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := plannerReply(c.task, c.planJSON)
			if ok != c.wantOK {
				t.Fatalf("plannerReply ok = %t, want %t", ok, c.wantOK)
			}
			if ok && got != c.planJSON {
				t.Errorf("plannerReply = %q, want the plan verbatim %q", got, c.planJSON)
			}
		})
	}
}

func TestSamplePlanDecodesAsPlannerOutput(t *testing.T) {
	var po mission.PlannerOutput
	if err := json.Unmarshal([]byte(samplePlan), &po); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if po.PlanVersion != "v1alpha1" {
		t.Errorf("PlanVersion = %q, want v1alpha1", po.PlanVersion)
	}
	if po.Metadata.Objective != "Summarise the release" {
		t.Errorf("Objective = %q", po.Metadata.Objective)
	}
	if len(po.Chains) != 1 || len(po.Chains[0].Steps) != 1 {
		t.Fatalf("chains = %+v, want one chain with one step", po.Chains)
	}
	if got := po.Chains[0].Steps[0].KnightRef.Name; got != "stub" {
		t.Errorf("step knightRef = %q, want stub", got)
	}
}
