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

package nats

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/roundtable-k8s/roundtable/pkg/nats/contract"
)

// Review verdicts a knight may report in TaskResult.Verdict.
const (
	VerdictShipIt       = string(contract.TaskResultVerdictShipIt)
	VerdictNeedsChanges = string(contract.TaskResultVerdictNeedsChanges)
)

// PR actions a knight may report in PRRef.Action.
const (
	PRActionOpened  = string(contract.TaskResultPrsElemActionOpened)
	PRActionUpdated = string(contract.TaskResultPrsElemActionUpdated)
	// PRActionQueued and PRActionMerged are the PR pipeline merge step's
	// success shape (#591): a PR in the merge queue (auto-merge on) is done.
	// Operator-only: read from the merge step's prose, not in the contract.
	PRActionQueued = "queued"
	PRActionMerged = "merged"
)

// PRRef is a pull request a task opened or updated (contract.TaskResultPrsElem).
type PRRef struct {
	Repo   string `json:"repo,omitempty"`
	Number int    `json:"number,omitempty"`
	URL    string `json:"url,omitempty"`
	Action string `json:"action,omitempty"`
}

// Structured is the machine-readable part of a task result.
type Structured struct {
	PRs     []PRRef `json:"prs,omitempty"`
	Verdict string  `json:"verdict,omitempty"`
}

// Structured returns the result's PRs and verdict. Fields the knight reported
// explicitly win; anything missing is recovered from the prose output
// (ParseStructured), so knights that predate the fields keep working.
func (r *TaskResult) Structured() Structured {
	s := Structured{PRs: r.PRs, Verdict: NormalizeVerdict(r.Verdict)}
	if s.Verdict == "" || len(s.PRs) == 0 {
		p := ParseStructured(r.GetOutput())
		if s.Verdict == "" {
			s.Verdict = p.Verdict
		}
		if len(s.PRs) == 0 {
			s.PRs = p.PRs
		}
	}
	return s
}

// NormalizeVerdict maps spellings such as "SHIP IT" or "Needs-Changes" to the
// contract values. Anything else (including "null") yields "".
func NormalizeVerdict(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	v = strings.NewReplacer(" ", "_", "-", "_").Replace(v)
	switch v {
	case VerdictShipIt, VerdictNeedsChanges:
		return v
	}
	return ""
}

var (
	verdictLineRe = regexp.MustCompile(
		`(?im)^[\s>*_#-]*verdict[\s*_]*[:=-][\s*_` + "`" + `]*(ship[ _-]?it|needs[ _-]?changes)\b`)
	prURLRe     = regexp.MustCompile(`https?://github\.com/([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)/pull/(\d+)`)
	prRepoRefRe = regexp.MustCompile(`\b([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)#(\d+)\b`)
	// prBareRefRe assumes a bare #N in the output is a PR. GitHub shares the
	// number space, so an issue reference in prose can match too; it is only
	// used when no URL or owner/repo#N reference was found.
	prBareRefRe = regexp.MustCompile(`(?:^|[^\w/#&])#(\d+)\b`)
	// prStateLineRe matches the merge step's per-PR line, "<url> queued" or
	// "<url> merged" (#591); the word after the URL is the PR's action.
	prStateLineRe = regexp.MustCompile(`(?im)(https?://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/pull/\d+)` +
		`[\s*_` + "`" + `:-]+(queued|merged)\b`)
)

// PRsLanded reports whether every PR is queued or merged: the merge step's
// success shape. A list with no PRs has not landed.
func PRsLanded(prs []PRRef) bool {
	if len(prs) == 0 {
		return false
	}
	for _, p := range prs {
		if p.Action != PRActionQueued && p.Action != PRActionMerged {
			return false
		}
	}
	return true
}

// ParseStructured is the prose fallback: it extracts a verdict (from a
// "Verdict: SHIP IT" line) and PR references (GitHub PR URLs, owner/repo#N,
// or bare #N) from free text. Bare #N carries no repo or URL. Order of first
// appearance is kept and duplicates are dropped. A PR URL followed by
// "queued" or "merged" (the merge step's reply lines) gets that action.
func ParseStructured(output string) Structured {
	var s Structured
	if ms := verdictLineRe.FindAllStringSubmatch(output, -1); len(ms) > 0 {
		s.Verdict = NormalizeVerdict(ms[len(ms)-1][1])
	}
	seen := map[string]bool{}
	add := func(repo string, n int, url string) {
		key := repo + "#" + strconv.Itoa(n)
		if repo == "" {
			key = "#" + strconv.Itoa(n)
		}
		if seen[key] {
			return
		}
		seen[key] = true
		s.PRs = append(s.PRs, PRRef{Repo: repo, Number: n, URL: url})
	}
	for _, m := range prURLRe.FindAllStringSubmatch(output, -1) {
		n, _ := strconv.Atoi(m[2])
		add(m[1], n, m[0])
	}
	actions := map[string]string{}
	for _, m := range prStateLineRe.FindAllStringSubmatch(output, -1) {
		actions[m[1]] = strings.ToLower(m[2])
	}
	for i := range s.PRs {
		if a, ok := actions[s.PRs[i].URL]; ok {
			s.PRs[i].Action = a
		}
	}
	for _, m := range prRepoRefRe.FindAllStringSubmatch(output, -1) {
		n, _ := strconv.Atoi(m[2])
		add(m[1], n, "")
	}
	// Bare #N is only trusted when nothing more specific was found.
	if len(s.PRs) == 0 {
		for _, m := range prBareRefRe.FindAllStringSubmatch(output, -1) {
			n, _ := strconv.Atoi(m[1])
			add("", n, "")
		}
	}
	return s
}
