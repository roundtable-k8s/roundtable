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
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/roundtable-k8s/roundtable/pkg/nats/contract"
	"github.com/roundtable-k8s/roundtable/pkg/nats/contract/contracttest"
)

// Contract tests: the Go types in this package against the knight task
// contract vendored in contract/ (schemas, golden fixtures, generated types).

const contractDir = "contract"

// operatorIgnoredTaskFields are task schema properties the operator neither
// sends nor reads: snake_case and legacy spellings used by knight-to-knight
// nats_request payloads, and dispatcher metadata knights set for each other.
// A new schema property is not ignored until it is added here on purpose.
var operatorIgnoredTaskFields = map[string]bool{
	"task_id": true, "description": true, "message": true,
	"chain_name": true, "chain": true, "step_name": true, "step": true,
	"run_id": true, "domain": true, "from": true, "dispatched_by": true,
	"dispatchedBy": true, "timestamp": true, "metadata": true, "payload": true,
}

// contractPair maps a generated contract type onto the pkg/nats type that
// carries it, with the schema properties the operator ignores.
type contractPair struct {
	name      string
	generated reflect.Type
	goType    reflect.Type
	ignored   map[string]bool
}

var contractPairs = []contractPair{
	{"TaskPayload", reflect.TypeFor[contract.TaskPayload](), reflect.TypeFor[TaskPayload](), operatorIgnoredTaskFields},
	{"TaskResult", reflect.TypeFor[contract.TaskResult](), reflect.TypeFor[TaskResult](), nil},
	{"TaskResult.tokens", reflect.TypeFor[contract.TaskResultTokens](), reflect.TypeFor[TokenUsage](), nil},
	{"TaskResult.prs[]", reflect.TypeFor[contract.TaskResultPrsElem](), reflect.TypeFor[PRRef](), nil},
	// result.task is kept raw on TaskResult and decoded as a TaskPayload.
	{"TaskResult.task", reflect.TypeFor[contract.TaskResultTask](), reflect.TypeFor[TaskPayload](), nil},
	{"ActivityEvent", reflect.TypeFor[contract.TaskActivityEvent](), reflect.TypeFor[ActivityEvent](), nil},
}

// jsonFields returns a struct type's fields by JSON name.
func jsonFields(t reflect.Type) map[string]reflect.StructField {
	out := map[string]reflect.StructField{}
	for f := range t.Fields() {
		tag := f.Tag.Get("json")
		name, _, _ := strings.Cut(tag, ",")
		if name == "-" || !f.IsExported() {
			continue
		}
		if name == "" {
			if f.Anonymous || tag == "" {
				continue
			}
			name = f.Name
		}
		out[name] = f
	}
	return out
}

// jsonKind is the JSON kind a Go type encodes to: string, number, boolean,
// array, object or any (interface or raw JSON, which accept everything).
func jsonKind(t reflect.Type) string {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch {
	case t == reflect.TypeFor[json.RawMessage](), t.Kind() == reflect.Interface:
		return "any"
	case t == reflect.TypeFor[time.Time]():
		return "string"
	}
	switch t.Kind() {
	case reflect.String:
		return "string"
	case reflect.Bool:
		return "boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return "number"
	case reflect.Slice, reflect.Array:
		return "array"
	case reflect.Struct, reflect.Map:
		return "object"
	}
	return t.Kind().String()
}

// coverageGaps lists the generated type's fields (schema properties) that
// the Go type lacks or carries as a different JSON kind.
func coverageGaps(p contractPair) []string {
	have := jsonFields(p.goType)
	var gaps []string
	for name, gf := range jsonFields(p.generated) {
		if p.ignored[name] {
			continue
		}
		f, ok := have[name]
		if !ok {
			gaps = append(gaps, fmt.Sprintf("%s: schema property %q has no field in pkg/nats.%s", p.name, name, p.goType.Name()))
			continue
		}
		gk, k := jsonKind(gf.Type), jsonKind(f.Type)
		if gk != k && gk != "any" && k != "any" {
			gaps = append(gaps, fmt.Sprintf("%s: %q is %s in the schema but %s in pkg/nats.%s",
				p.name, name, gk, k, p.goType.Name()))
		}
	}
	sort.Strings(gaps)
	return gaps
}

// TestContractGoTypesCoverSchema fails when a knights schema property has no
// field in the pkg/nats type that carries it (or a different JSON kind).
func TestContractGoTypesCoverSchema(t *testing.T) {
	for _, p := range contractPairs {
		for _, g := range coverageGaps(p) {
			t.Error(g)
		}
	}
}

func TestContractCoverageGapsDetectsMissingField(t *testing.T) {
	type narrowResult struct {
		TaskID string `json:"task_id"`
	}
	gaps := coverageGaps(contractPair{
		"TaskResult", reflect.TypeFor[contract.TaskResult](), reflect.TypeFor[narrowResult](), nil,
	})
	if len(gaps) == 0 || !strings.Contains(strings.Join(gaps, "\n"), `"knight"`) {
		t.Fatalf("expected a gap for knight, got %v", gaps)
	}
}

// fixture is one golden payload from contract/testdata.
type fixture struct {
	name   string
	data   []byte
	schema string
	decode func([]byte) (any, error)
	ignore map[string]bool
}

// strictDecode decodes data into a new T, failing on a JSON key T has no
// field for (other than ignored ones).
func strictDecode[T any](ignored map[string]bool) func([]byte) (any, error) {
	return func(data []byte) (any, error) {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, err
		}
		for k := range ignored {
			delete(m, k)
		}
		trimmed, err := json.Marshal(m)
		if err != nil {
			return nil, err
		}
		dec := json.NewDecoder(bytes.NewReader(trimmed))
		dec.DisallowUnknownFields()
		v := new(T)
		if err := dec.Decode(v); err != nil {
			return nil, err
		}
		return v, nil
	}
}

func loadFixtures(t *testing.T) []fixture {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(contractDir, "testdata", "*.json"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("no contract fixtures in %s/testdata: %v", contractDir, err)
	}
	out := make([]fixture, 0, len(paths))
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		f := fixture{name: filepath.Base(p), data: data}
		switch {
		case strings.HasPrefix(f.name, "task."):
			f.schema, f.ignore = contract.TaskPayloadSchema, operatorIgnoredTaskFields
			f.decode = strictDecode[TaskPayload](operatorIgnoredTaskFields)
		case strings.HasPrefix(f.name, "result."):
			f.schema, f.decode = contract.TaskResultSchema, strictDecode[TaskResult](nil)
		case strings.HasPrefix(f.name, "activity."):
			f.schema, f.decode = contract.ActivityEventSchema, strictDecode[ActivityEvent](nil)
		default:
			t.Fatalf("fixture %s: unknown kind (want a task., result. or activity. prefix)", f.name)
		}
		out = append(out, f)
	}
	return out
}

// dropZero removes keys whose value is JSON's zero (false, 0, "", null, {} or
// []) recursively: the Go types omit zero-valued optional fields, so a fixture
// key holding a zero value is equivalent to an absent one.
func dropZero(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			e = dropZero(e)
			if isZeroJSON(e) {
				delete(x, k)
				continue
			}
			x[k] = e
		}
		return x
	case []any:
		for i := range x {
			x[i] = dropZero(x[i])
		}
		return x
	}
	return v
}

func isZeroJSON(v any) bool {
	switch x := v.(type) {
	case nil:
		return true
	case bool:
		return !x
	case float64:
		return x == 0
	case string:
		return x == ""
	case map[string]any:
		return len(x) == 0
	case []any:
		return len(x) == 0
	}
	return false
}

// roundTripLoss decodes data into the fixture's Go type, re-encodes it and
// reports any non-zero field that did not survive (or changed).
func roundTripLoss(f fixture) error {
	v, err := f.decode(f.data)
	if err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	out, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	var want, got map[string]any
	if err := json.Unmarshal(f.data, &want); err != nil {
		return err
	}
	if err := json.Unmarshal(out, &got); err != nil {
		return err
	}
	for k := range f.ignore {
		delete(want, k)
	}
	want, got = dropZero(want).(map[string]any), dropZero(got).(map[string]any)
	if !reflect.DeepEqual(want, got) {
		w, _ := json.Marshal(want)
		g, _ := json.Marshal(got)
		return fmt.Errorf("re-encoded payload differs:\n want %s\n  got %s", w, g)
	}
	return nil
}

// TestContractFixtures checks every golden fixture: it validates against its
// schema, decodes into the pkg/nats type with no unknown field, and
// re-encodes without losing or changing a field.
func TestContractFixtures(t *testing.T) {
	for _, f := range loadFixtures(t) {
		t.Run(f.name, func(t *testing.T) {
			if err := contracttest.Validate(f.schema, f.data); err != nil {
				t.Fatalf("fixture does not validate: %v", err)
			}
			if err := roundTripLoss(f); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestContractFixtureWithUnknownFieldFails is the acceptance check: a fixture
// carrying a field the Go types lack fails the contract test.
func TestContractFixtureWithUnknownFieldFails(t *testing.T) {
	for _, f := range loadFixtures(t) {
		var m map[string]any
		if err := json.Unmarshal(f.data, &m); err != nil {
			t.Fatal(err)
		}
		m["brandNewField"] = "x"
		f.data, _ = json.Marshal(m)
		if err := roundTripLoss(f); err == nil {
			t.Errorf("%s: a field missing from the Go types was not detected", f.name)
		}
	}
}

// TestContractFixturesDecodeLikeTheOperator checks what the operator reads
// from the result fixtures through the compatibility accessors.
func TestContractFixturesDecodeLikeTheOperator(t *testing.T) {
	read := func(name string) *TaskResult {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(contractDir, "testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		var r TaskResult
		if err := json.Unmarshal(data, &r); err != nil {
			t.Fatal(err)
		}
		return &r
	}
	ok := read("result.success.json")
	if ok.GetTaskID() != "t-1" || ok.GetOutput() != "Done." || ok.GetError() != "" || ok.QuotaRefused() {
		t.Errorf("success: %+v", ok)
	}
	if r := read("result.failure.json"); r.GetError() == "" {
		t.Error("failure: GetError is empty")
	}
	if r := read("result.refused-quota.json"); !r.QuotaRefused() || r.Refused != RefusedQuota {
		t.Errorf("refused-quota: QuotaRefused=%v Refused=%q", r.QuotaRefused(), r.Refused)
	}
	if r := read("result.refused-credits.json"); r.QuotaRefused() || r.Refused != RefusedCredits || r.GetError() == "" {
		t.Errorf("refused-credits: QuotaRefused=%v Refused=%q", r.QuotaRefused(), r.Refused)
	}
	if r := read("result.transient.json"); !r.Transient || r.TransientReason != TransientOverloaded {
		t.Errorf("transient: %+v", r)
	}
	if r := read("result.lost.json"); !r.Lost || r.GetError() == "" {
		t.Errorf("lost: %+v", r)
	}
	s := read("result.structured.json").Structured()
	if s.Verdict != VerdictShipIt || len(s.PRs) != 1 || s.PRs[0].Action != PRActionOpened || s.PRs[0].Number != 60 {
		t.Errorf("structured: %+v", s)
	}
}

func TestQuotaRefused(t *testing.T) {
	for _, tc := range []struct {
		r    TaskResult
		want bool
	}{
		{TaskResult{}, false},
		{TaskResult{UsageLimited: true}, true},
		{TaskResult{Refused: RefusedQuota}, true},
		{TaskResult{Refused: RefusedQuota, UsageLimited: true}, true},
		{TaskResult{Refused: RefusedCredits}, false},
	} {
		if got := tc.r.QuotaRefused(); got != tc.want {
			t.Errorf("%+v: QuotaRefused() = %v, want %v", tc.r, got, tc.want)
		}
	}
}

// TestContractOperatorPayloadsValidate checks the shapes of the task payloads
// the operator and gateway publish (chain step, fan-out item, loop body,
// gateway invoke, manual retry) against the task schema. The publishers'
// own tests check the payloads they actually build the same way.
func TestContractOperatorPayloadsValidate(t *testing.T) {
	for name, p := range map[string]TaskPayload{
		"chain step": {
			TaskID: "chain-dev-implement.r1-1", ChainName: "dev", StepName: "implement", RunID: "r1",
			Task: "Implement it.", Model: "opus", Effort: "high",
		},
		"fan-out item": {
			TaskID: "chain-dev-review-0.r1-0-1", ChainName: "dev", StepName: "review", RunID: "r1-0",
			Task: "Review item 0.",
		},
		"loop body": {
			TaskID: "chain-dev-fix-code-r1.r1-1", ChainName: "dev", StepName: "fix.code", RunID: "r1",
			Task: "Fix it.",
		},
		"gateway":    {TaskID: "gw-1", RunID: "conv-1", Task: "Hello", Model: "sonnet"},
		"retry":      {TaskID: "t-2", Task: "Again", Attempt: 2, RetryOf: "t-1", PreviousFailure: "timed out"},
		"empty task": {TaskID: "t-3"},
	} {
		data, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		if err := contracttest.ValidateTask(data); err != nil {
			t.Errorf("%s: %v\n%s", name, err, data)
		}
	}
}

// TestContractGeneratedCodeUpToDate regenerates the contract types into a
// temp dir with the //go:generate command in contract/doc.go and fails when
// the checked-in zz_generated.go differs (run make generate-contract).
func TestContractGeneratedCodeUpToDate(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the go-jsonschema tool")
	}
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go not in PATH")
	}
	doc, err := os.ReadFile(filepath.Join(contractDir, "doc.go"))
	if err != nil {
		t.Fatal(err)
	}
	var args []string
	for line := range strings.Lines(string(doc)) {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), "//go:generate go "); ok {
			args = strings.Fields(rest)
		}
	}
	if len(args) == 0 {
		t.Fatal("no //go:generate go ... line in contract/doc.go")
	}
	out := filepath.Join(t.TempDir(), "zz_generated.go")
	for i := range args {
		if args[i] == "-o" && i+1 < len(args) {
			args[i+1] = out
		}
	}
	if !strings.Contains(strings.Join(args, " "), out) {
		t.Fatal("the //go:generate line has no -o <file>")
	}
	cmd := exec.Command(goBin, args...)
	cmd.Dir = contractDir
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go %s: %v\n%s", strings.Join(args, " "), err, b)
	}
	want, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(contractDir, "zz_generated.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(want, got) {
		t.Fatal("contract/zz_generated.go is stale: run make generate-contract and commit the result")
	}
}
