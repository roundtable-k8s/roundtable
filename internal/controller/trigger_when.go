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
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

var (
	triggerWhenEnvOnce sync.Once
	triggerWhenEnv     *cel.Env
	errTriggerWhenEnv  error

	// triggerWhenPrograms caches the compiled spec.when of each Trigger, one
	// entry per Trigger UID, replaced when its generation changes and dropped
	// when the Trigger is deleted. A program is stateless and safe for
	// concurrent use.
	triggerWhenMu       sync.Mutex
	triggerWhenPrograms = map[types.UID]triggerWhenEntry{}
)

// triggerWhenEntry is a compiled spec.when and the generation it came from.
type triggerWhenEntry struct {
	nn         types.NamespacedName
	generation int64
	expr       string
	prg        cel.Program
}

// forgetTriggerWhen drops the cached spec.when program of a deleted Trigger.
func forgetTriggerWhen(nn types.NamespacedName) {
	triggerWhenMu.Lock()
	defer triggerWhenMu.Unlock()
	for uid, e := range triggerWhenPrograms {
		if e.nn == nn {
			delete(triggerWhenPrograms, uid)
		}
	}
}

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

// compileTriggerWhen compiles a Trigger's spec.when expression, caching the
// program by the Trigger's UID and generation; it must yield a bool.
func compileTriggerWhen(trg *aiv1alpha1.Trigger) (cel.Program, error) {
	expr := trg.Spec.When
	triggerWhenMu.Lock()
	e, ok := triggerWhenPrograms[trg.UID]
	triggerWhenMu.Unlock()
	if ok && e.generation == trg.Generation && e.expr == expr {
		return e.prg, nil
	}
	env, err := triggerWhenCELEnv()
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
	prg, err := env.Program(ast, cel.CostLimit(celCostLimit))
	if err != nil {
		return nil, err
	}
	triggerWhenMu.Lock()
	triggerWhenPrograms[trg.UID] = triggerWhenEntry{nn: client.ObjectKeyFromObject(trg), generation: trg.Generation, expr: expr, prg: prg}
	triggerWhenMu.Unlock()
	return prg, nil
}

// evalTriggerWhen evaluates spec.when against the event.
func evalTriggerWhen(trg *aiv1alpha1.Trigger, ev TriggerEvent) (bool, error) {
	prg, err := compileTriggerWhen(trg)
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
