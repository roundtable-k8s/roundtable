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

	missionpkg "github.com/roundtable-k8s/roundtable/internal/mission"
)

func TestMissionSetupWithManagerRequiresServices(t *testing.T) {
	full := func() *MissionReconciler {
		return &MissionReconciler{
			Planner:     &missionpkg.Planner{},
			Assembler:   &missionpkg.KnightAssembler{},
			Cleaner:     &missionpkg.Cleaner{},
			CostTracker: &missionpkg.CostTracker{},
			Briefer:     &missionpkg.Briefer{},
			Chains:      &missionpkg.ChainManager{},
		}
	}
	cases := map[string]func(*MissionReconciler){
		"Planner":     func(r *MissionReconciler) { r.Planner = nil },
		"Assembler":   func(r *MissionReconciler) { r.Assembler = nil },
		"Cleaner":     func(r *MissionReconciler) { r.Cleaner = nil },
		"CostTracker": func(r *MissionReconciler) { r.CostTracker = nil },
		"Briefer":     func(r *MissionReconciler) { r.Briefer = nil },
		"Chains":      func(r *MissionReconciler) { r.Chains = nil },
	}
	for name, unset := range cases {
		r := full()
		unset(r)
		err := r.SetupWithManager(nil)
		if err == nil || !strings.Contains(err.Error(), name) {
			t.Errorf("%s unset: got error %v", name, err)
		}
	}
}
