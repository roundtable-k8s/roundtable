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

package contract

import "embed"

// Schemas holds the vendored JSON Schemas (schema/*.schema.json), for
// validating payloads in tests (see contracttest).
//
//go:embed schema/*.schema.json
var Schemas embed.FS

// Schema file names in Schemas.
const (
	TaskPayloadSchema   = "schema/task-payload.schema.json"
	TaskResultSchema    = "schema/task-result.schema.json"
	ActivityEventSchema = "schema/task-activity-events.schema.json"
)
