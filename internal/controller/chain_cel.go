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
	"encoding/json"
	"fmt"
	"sync"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/ext"
)

// Chain conditions (step `when`, loop `until`) are CEL expressions. CEL is
// side-effect free, always terminates and is the language already used for
// CRD validation; the cost limit below bounds pathological expressions
// (e.g. huge comprehensions) on top of that.
const (
	// celCostLimit caps the runtime cost of one evaluation.
	celCostLimit = 1_000_000
	// celOutputLimit caps how much of a step output an expression sees.
	celOutputLimit = 256 * 1024
)

var (
	celEnvOnce sync.Once
	celEnv     *cel.Env
	errCELEnv  error
)

// chainCELEnv returns the shared CEL environment for chain conditions.
func chainCELEnv() (*cel.Env, error) {
	celEnvOnce.Do(func() {
		celEnv, errCELEnv = cel.NewEnv(
			cel.Variable("input", cel.StringType),
			cel.Variable("steps", cel.MapType(cel.StringType, cel.DynType)),
			cel.Variable("last", cel.DynType),
			// "loop" is a reserved word in CEL, so loop state is flat.
			cel.Variable("iteration", cel.IntType),
			cel.Variable("maxIterations", cel.IntType),
			cel.Variable("body", cel.MapType(cel.StringType, cel.DynType)),
			ext.Strings(),
			cel.Function("json",
				cel.Overload("json_string_dyn", []*cel.Type{cel.StringType}, cel.DynType,
					cel.UnaryBinding(celParseJSON))),
		)
	})
	return celEnv, errCELEnv
}

// celParseJSON implements json(string): it parses a JSON document (tolerating
// prose around a fenced ```json block, like forEach) into CEL values.
func celParseJSON(v ref.Val) ref.Val {
	s, ok := v.Value().(string)
	if !ok {
		return types.NewErr("json() expects a string")
	}
	doc, err := parseJSONDocument(s)
	if err != nil {
		return types.NewErr("json(): %v", err)
	}
	return types.DefaultTypeAdapter.NativeToValue(normalizeJSONNumbers(doc))
}

// normalizeJSONNumbers turns json.Number values into int64 (when integral)
// or float64, so CEL sees ordinary numbers.
func normalizeJSONNumbers(v any) any {
	switch t := v.(type) {
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}
		f, _ := t.Float64()
		return f
	case []any:
		for i := range t {
			t[i] = normalizeJSONNumbers(t[i])
		}
		return t
	case map[string]any:
		for k := range t {
			t[k] = normalizeJSONNumbers(t[k])
		}
		return t
	}
	return v
}

// compileCondition parses and type-checks a condition expression; it must
// yield a bool.
func compileCondition(expr string) (cel.Program, error) {
	env, err := chainCELEnv()
	if err != nil {
		return nil, err
	}
	ast, iss := env.Compile(expr)
	if iss != nil && iss.Err() != nil {
		return nil, iss.Err()
	}
	if t := ast.OutputType(); !t.IsAssignableType(cel.BoolType) {
		return nil, fmt.Errorf("expression must evaluate to a bool, not %s", t)
	}
	return env.Program(ast, cel.CostLimit(celCostLimit))
}

// evalCondition compiles and evaluates expr against vars.
func evalCondition(expr string, vars map[string]any) (bool, error) {
	prg, err := compileCondition(expr)
	if err != nil {
		return false, err
	}
	defaults := map[string]any{
		"input": "", "steps": map[string]any{}, "last": map[string]any{},
		"iteration": int64(0), "maxIterations": int64(0), "body": map[string]any{},
	}
	for k, v := range defaults {
		if _, ok := vars[k]; !ok {
			vars[k] = v
		}
	}
	out, _, err := prg.Eval(vars)
	if err != nil {
		return false, err
	}
	b, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("expression yielded %v (%T), not a bool", out.Value(), out.Value())
	}
	return b, nil
}

// clipForCEL bounds an output handed to an expression.
func clipForCEL(s string) string {
	if len(s) <= celOutputLimit {
		return s
	}
	return truncateUTF8(s, celOutputLimit)
}
