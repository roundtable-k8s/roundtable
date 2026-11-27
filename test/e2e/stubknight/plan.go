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

import "strings"

// plannerPrompt opens the mission planning prompt the operator sends to a
// planner knight (mission.PlanningPromptPreamble, used by buildPlanningPrompt).
// It is a copy because this main package cannot import the operator's
// internal/mission; plan_test.go guards it against the exported constant.
const plannerPrompt = "You are a mission planner for the Round Table AI agent orchestration system."

// plannerReply returns the canned plan for a mission planning task: ok only
// when the trimmed task starts with plannerPrompt and planJSON is non-empty.
// The plan is returned verbatim.
func plannerReply(task, planJSON string) (string, bool) {
	if planJSON == "" || !strings.HasPrefix(strings.TrimSpace(task), plannerPrompt) {
		return "", false
	}
	return planJSON, true
}
