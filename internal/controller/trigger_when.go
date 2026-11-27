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
	"fmt"
	"sync"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/ext"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

var (
	triggerWhenEnvOnce sync.Once
	triggerWhenEnv     *cel.Env
	errTriggerWhenEnv  error

	// triggerWhenPrograms caches compiled programs by expression: a program
	// is stateless and safe for concurrent use.
	triggerWhenPrograms sync.Map // string -> cel.Program
)

// triggerWhenCELEnv is the CEL environment of spec.when: event and trigger
// variables, with the libraries and cost limit of chain conditions.
func triggerWhenCELEnv() (*cel.Env, error) {
	triggerWhenEnvOnce.Do(func() {
		triggerWhenEnv, errTriggerWhenEnv = cel.NewEnv(
			cel.Variable("event", cel.MapType(cel.StringType, cel.DynType)),
			cel.Variable("trigger", cel.MapType(cel.StringType, cel.StringType)),
			ext.Strings(),
			celJSONFunction(),
		)
	})
	return triggerWhenEnv, errTriggerWhenEnv
}

// compileTriggerWhen compiles (and caches) a spec.when expression; it must
// yield a bool.
func compileTriggerWhen(expr string) (cel.Program, error) {
	if p, ok := triggerWhenPrograms.Load(expr); ok {
		return p.(cel.Program), nil
	}
	env, err := triggerWhenCELEnv()
	if err != nil {
		return nil, err
	}
	ast, iss := env.Compile(expr)
	if iss != nil && iss.Err() != nil {
		return nil, iss.Err()
	}
	if t := ast.OutputType(); !t.IsAssignableType(cel.BoolType) && t != cel.DynType {
		return nil, fmt.Errorf("expression must evaluate to a bool, not %s", t)
	}
	prg, err := env.Program(ast, cel.CostLimit(celCostLimit))
	if err != nil {
		return nil, err
	}
	triggerWhenPrograms.Store(expr, prg)
	return prg, nil
}

// evalTriggerWhen evaluates spec.when against the event.
func evalTriggerWhen(trg *aiv1alpha1.Trigger, ev TriggerEvent) (bool, error) {
	prg, err := compileTriggerWhen(trg.Spec.When)
	if err != nil {
		return false, err
	}
	headers := make(map[string]any, len(ev.Headers))
	for k, v := range ev.Headers {
		headers[k] = v
	}
	out, _, err := prg.Eval(map[string]any{
		"event": map[string]any{
			"subject": ev.Subject,
			"data":    ev.Data,
			"headers": headers,
			"time":    ev.Time.UTC().Format(time.RFC3339),
		},
		"trigger": map[string]string{"name": trg.Name, "namespace": trg.Namespace},
	})
	if err != nil {
		return false, err
	}
	b, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("expression yielded %v (%T), not a bool", out.Value(), out.Value())
	}
	return b, nil
}
