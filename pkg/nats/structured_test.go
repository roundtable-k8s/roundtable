package nats

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestNormalizeVerdict(t *testing.T) {
	for in, want := range map[string]string{
		"ship_it": VerdictShipIt, "SHIP IT": VerdictShipIt, "Needs-Changes": VerdictNeedsChanges,
		"needs_changes": VerdictNeedsChanges, "": "", "null": "", "lgtm": "",
	} {
		if got := NormalizeVerdict(in); got != want {
			t.Errorf("NormalizeVerdict(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestStructuredExplicitFields(t *testing.T) {
	var r TaskResult
	data := `{"taskId":"t","output":"done, see #1","verdict":"needs_changes",
		"prs":[{"repo":"o/r","number":7,"url":"https://github.com/o/r/pull/7","action":"opened"}]}`
	if err := json.Unmarshal([]byte(data), &r); err != nil {
		t.Fatal(err)
	}
	s := r.Structured()
	if s.Verdict != VerdictNeedsChanges || len(s.PRs) != 1 || s.PRs[0].Number != 7 || s.PRs[0].Action != PRActionOpened {
		t.Fatalf("explicit fields must win over prose: %+v", s)
	}
}

func TestStructuredNullVerdict(t *testing.T) {
	var r TaskResult
	if err := json.Unmarshal([]byte(`{"output":"fine","verdict":null}`), &r); err != nil {
		t.Fatal(err)
	}
	if s := r.Structured(); s.Verdict != "" || len(s.PRs) != 0 {
		t.Fatalf("got %+v", s)
	}
}

func TestParseStructuredProse(t *testing.T) {
	cases := map[string]struct {
		in   string
		want Structured
	}{
		"url": {"Opened https://github.com/roundtable-k8s/roundtable/pull/245 for review",
			Structured{PRs: []PRRef{{Repo: "roundtable-k8s/roundtable", Number: 245, URL: "https://github.com/roundtable-k8s/roundtable/pull/245"}}}},
		"repo ref":              {"see roundtable-k8s/arsenal#12", Structured{PRs: []PRRef{{Repo: "roundtable-k8s/arsenal", Number: 12}}}},
		"bare ref":              {"PR #245 is up", Structured{PRs: []PRRef{{Number: 245}}}},
		"url wins over bare":    {"PR #9: https://github.com/o/r/pull/9", Structured{PRs: []PRRef{{Repo: "o/r", Number: 9, URL: "https://github.com/o/r/pull/9"}}}},
		"verdict":               {"Looks good.\n**Verdict: SHIP IT**", Structured{Verdict: VerdictShipIt}},
		"verdict nc":            {"Verdict: needs changes\n- fix x", Structured{Verdict: VerdictNeedsChanges}},
		"last verdict wins":     {"Verdict: needs_changes\n...\nVerdict: ship_it", Structured{Verdict: VerdictShipIt}},
		"no verdict in passing": {"we should not ship it yet", Structured{}},
		"nothing":               {"hello", Structured{}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := ParseStructured(tc.in); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}
