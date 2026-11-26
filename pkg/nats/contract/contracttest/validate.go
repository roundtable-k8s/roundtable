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

// Package contracttest validates JSON payloads against the vendored knight
// task contract schemas. It is meant for tests: the operator and the gateway
// use it to check that every task payload they build is one a knight accepts.
package contracttest

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/roundtable-k8s/roundtable/pkg/nats/contract"
)

var (
	once     sync.Once
	schemas  map[string]*jsonschema.Schema
	compErr  error
	allNames = []string{contract.TaskPayloadSchema, contract.TaskResultSchema, contract.ActivityEventSchema}
)

func compile() {
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	for _, name := range allNames {
		b, err := contract.Schemas.ReadFile(name)
		if err != nil {
			compErr = err
			return
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
		if err != nil {
			compErr = fmt.Errorf("%s: %w", name, err)
			return
		}
		if err := c.AddResource(name, doc); err != nil {
			compErr = fmt.Errorf("%s: %w", name, err)
			return
		}
	}
	schemas = map[string]*jsonschema.Schema{}
	for _, name := range allNames {
		s, err := c.Compile(name)
		if err != nil {
			compErr = fmt.Errorf("%s: %w", name, err)
			return
		}
		schemas[name] = s
	}
}

// Validate checks data (one JSON document) against the named schema, one of
// contract.TaskPayloadSchema, contract.TaskResultSchema or
// contract.ActivityEventSchema.
func Validate(schema string, data []byte) error {
	once.Do(compile)
	if compErr != nil {
		return fmt.Errorf("compile contract schemas: %w", compErr)
	}
	s, ok := schemas[schema]
	if !ok {
		return fmt.Errorf("unknown contract schema %q", schema)
	}
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("decode %s instance: %w", schema, err)
	}
	if err := s.Validate(inst); err != nil {
		return fmt.Errorf("%s: %w", schema, err)
	}
	return nil
}

// ValidateTask checks a task payload against the task schema.
func ValidateTask(data []byte) error { return Validate(contract.TaskPayloadSchema, data) }

// ValidateResult checks a task result against the result schema.
func ValidateResult(data []byte) error { return Validate(contract.TaskResultSchema, data) }

// ValidateActivity checks an activity event against the activity schema.
func ValidateActivity(data []byte) error { return Validate(contract.ActivityEventSchema, data) }
