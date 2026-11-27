//go:build e2e
// +build e2e

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
	"fmt"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/onsi/ginkgo/v2/types"
)

// unlabelledTier is the row name of scenarios that carry no tier label.
const unlabelledTier = "unlabelled"

// allTiers lists the tier labels in run order; the summary prints rows in this order.
var allTiers = []string{
	tierCore, tierTrace, tierTrigger, tierLost, tierRestart, tierGateway, tierChain, tierGitHub,
}

// tierRow is one line of the end-of-suite summary.
type tierRow struct {
	Tier    string
	Passed  int
	Failed  int
	Skipped int
	Total   time.Duration
}

// specTier returns the first label of the spec that is a tier, or unlabelled.
func specTier(r types.SpecReport) string {
	for _, l := range r.Labels() {
		for _, t := range allTiers {
			if l == t {
				return t
			}
		}
	}
	return unlabelledTier
}

// selectedReports keeps the reports of the tiers that ran, so a tier that was
// not selected with TIER does not show up in the summary.
func selectedReports(reports []types.SpecReport) []types.SpecReport {
	var out []types.SpecReport
	for _, r := range reports {
		if tierSelected(specTier(r)) {
			out = append(out, r)
		}
	}
	return out
}

// summarizeTiers counts the outcome of every spec per tier. Rows follow allTiers,
// then unlabelled; a tier with no specs in reports has no row.
func summarizeTiers(reports []types.SpecReport) []tierRow {
	byTier := map[string]*tierRow{}
	for _, r := range reports {
		name := specTier(r)
		row, ok := byTier[name]
		if !ok {
			row = &tierRow{Tier: name}
			byTier[name] = row
		}
		switch r.State {
		case types.SpecStatePassed:
			row.Passed++
		case types.SpecStateSkipped, types.SpecStatePending:
			row.Skipped++
		default:
			row.Failed++
		}
		row.Total += r.RunTime
	}

	var rows []tierRow
	for _, name := range append(append([]string{}, allTiers...), unlabelledTier) {
		if row, ok := byTier[name]; ok {
			rows = append(rows, *row)
		}
	}
	return rows
}

// formatTierTable renders rows as an aligned table with a header line.
func formatTierTable(rows []tierRow) string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "TIER\tPASSED\tFAILED\tSKIPPED\tTOTAL\tRUN TIME")
	for _, r := range rows {
		_, _ = fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\t%s\n",
			r.Tier, r.Passed, r.Failed, r.Skipped, r.Passed+r.Failed+r.Skipped, r.Total.Round(time.Second))
	}
	_ = w.Flush()
	return sb.String()
}
