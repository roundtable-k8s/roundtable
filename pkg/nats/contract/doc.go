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

// Package contract holds the knight task contract: the JSON Schemas and
// golden fixtures copied from roundtable-k8s/knights (packages/core/contract,
// at the commit in VERSION) and the Go types generated from those schemas.
//
// The schemas in the knights repo are the source of truth. Do not edit
// schema/, testdata/ or zz_generated.go by hand: run
//
//	make contract-sync KNIGHTS_REF=<knights commit>
//
// which copies the files and regenerates the types (make generate-contract).
// pkg/nats builds its TaskPayload, TaskResult, PRRef and ActivityEvent on
// these types; its contract tests fail when the two drift apart.
package contract

//go:generate go tool go-jsonschema --only-models --tags json --struct-name-from-title --capitalization ID --capitalization URL -p contract -o zz_generated.go schema/task-payload.schema.json schema/task-result.schema.json schema/task-activity-events.schema.json
