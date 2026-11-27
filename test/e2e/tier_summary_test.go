//go:build e2e

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

package e2e

import (
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2/types"
)

func specReport(state types.SpecState, runTime time.Duration, labels ...string) types.SpecReport {
	return types.SpecReport{
		LeafNodeType:             types.NodeTypeIt,
		State:                    state,
		RunTime:                  runTime,
		ContainerHierarchyLabels: [][]string{labels},
	}
}

func TestSummarizeTiers(t *testing.T) {
	tests := []struct {
		name    string
		reports []types.SpecReport
		want    []tierRow
	}{
		{
			name:    "empty report has no rows",
			reports: nil,
			want:    nil,
		},
		{
			name: "mixed pass fail and skip in one tier",
			reports: []types.SpecReport{
				specReport(types.SpecStatePassed, 2*time.Second, tierGateway),
				specReport(types.SpecStateFailed, 3*time.Second, tierGateway),
				specReport(types.SpecStateSkipped, 0, tierGateway),
				specReport(types.SpecStatePending, 0, tierGateway),
				specReport(types.SpecStatePanicked, time.Second, tierGateway),
			},
			want: []tierRow{
				{Tier: tierGateway, Passed: 1, Failed: 2, Skipped: 2, Total: 6 * time.Second},
			},
		},
		{
			name: "unlabelled spec counts under unlabelled and rows follow tier order",
			reports: []types.SpecReport{
				specReport(types.SpecStatePassed, time.Second),
				specReport(types.SpecStatePassed, time.Second, tierCore),
				specReport(types.SpecStateFailed, time.Second, tierCore),
			},
			want: []tierRow{
				{Tier: tierCore, Passed: 1, Failed: 1, Total: 2 * time.Second},
				{Tier: unlabelledTier, Passed: 1, Total: time.Second},
			},
		},
		{
			name: "first tier label wins and non-tier labels are ignored",
			reports: []types.SpecReport{
				specReport(types.SpecStatePassed, time.Second, "slow", tierChain, tierGitHub),
			},
			want: []tierRow{
				{Tier: tierChain, Passed: 1, Total: time.Second},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := summarizeTiers(tc.reports)
			if len(got) != len(tc.want) {
				t.Fatalf("summarizeTiers() = %+v, want %+v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("row %d = %+v, want %+v", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestFormatTierTable(t *testing.T) {
	out := formatTierTable([]tierRow{
		{Tier: tierCore, Passed: 3, Failed: 1, Skipped: 2, Total: 65 * time.Second},
	})
	want := "TIER  PASSED  FAILED  SKIPPED  TOTAL  RUN TIME\n" +
		"core  3       1       2        6      1m5s\n"
	if out != want {
		t.Errorf("formatTierTable() =\n%s\nwant\n%s", out, want)
	}
}

func TestSelectedReports(t *testing.T) {
	suiteSetup := types.SpecReport{
		LeafNodeType: types.NodeTypeBeforeSuite,
		State:        types.SpecStatePassed,
		RunTime:      time.Minute,
	}
	reports := []types.SpecReport{
		suiteSetup,
		specReport(types.SpecStatePassed, time.Second, tierCore),
		specReport(types.SpecStatePassed, time.Second, tierTrace),
		specReport(types.SpecStatePassed, time.Second),
	}

	t.Run("single tier keeps only its scenarios", func(t *testing.T) {
		t.Setenv("TIER", tierCore)
		got := selectedReports(reports)
		if len(got) != 1 || specTier(got[0]) != tierCore {
			t.Fatalf("selectedReports() kept %d reports, want only the core one", len(got))
		}
	})

	t.Run("all tiers drops suite setup", func(t *testing.T) {
		t.Setenv("TIER", "all")
		got := selectedReports(reports)
		if len(got) != 3 {
			t.Fatalf("selectedReports() kept %d reports, want 3 scenarios", len(got))
		}
		for _, r := range got {
			if r.LeafNodeType != types.NodeTypeIt {
				t.Errorf("kept non-scenario report of type %s", r.LeafNodeType)
			}
		}
		rows := summarizeTiers(got)
		for _, row := range rows {
			if row.Tier == unlabelledTier && row.Passed != 1 {
				t.Errorf("unlabelled passed = %d, want 1 (BeforeSuite must not count)", row.Passed)
			}
		}
	})
}
