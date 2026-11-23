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
)

// Review verdicts a knight may report in TaskResult.Verdict.
const (
	VerdictShipIt       = "ship_it"
	VerdictNeedsChanges = "needs_changes"
)

// PR actions a knight may report in PRRef.Action.
const (
	PRActionOpened  = "opened"
	PRActionUpdated = "updated"
)

// PRRef is a pull request a task opened or updated.
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
	verdictLineRe = regexp.MustCompile(`(?im)^[\s>*_#-]*verdict[\s*_]*[:=-][\s*_` + "`" + `]*(ship[ _-]?it|needs[ _-]?changes)\b`)
	prURLRe       = regexp.MustCompile(`https?://github\.com/([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)/pull/(\d+)`)
	prRepoRefRe   = regexp.MustCompile(`\b([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)#(\d+)\b`)
	prBareRefRe   = regexp.MustCompile(`(?:^|[^\w/#&])#(\d+)\b`)
)

// ParseStructured is the prose fallback: it extracts a verdict (from a
// "Verdict: SHIP IT" line) and PR references (GitHub PR URLs, owner/repo#N,
// or bare #N) from free text. Bare #N carries no repo or URL. Order of first
// appearance is kept and duplicates are dropped.
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
