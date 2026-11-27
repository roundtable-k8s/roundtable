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

package gateway

import (
	"fmt"
	"maps"
	"net/http"
	"strings"
)

// The OpenAPI 3.1 description of the HTTP API. It is hand-written: the route
// table (Service.routes) decides which operations are included, and
// openAPIOperations holds one entry per route pattern. openapi_test.go fails
// when the two drift apart.

type obj = map[string]any

func (s *Service) handleOpenAPI(w http.ResponseWriter, _ *http.Request) {
	doc, err := s.openAPIDocument()
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, doc)
}

// openAPIDocument builds the document for the routes the Service serves.
func (s *Service) openAPIDocument() (obj, error) {
	paths := obj{}
	for _, rt := range s.routes() {
		op, ok := openAPIOperations[rt.pattern]
		if !ok {
			return nil, fmt.Errorf("route %q has no OpenAPI description", rt.pattern)
		}
		method, path, _ := strings.Cut(rt.pattern, " ")
		item, _ := paths[path].(obj)
		if item == nil {
			item = obj{}
			paths[path] = item
		}
		o := maps.Clone(op)
		if resp, ok := o["responses"].(obj); ok && path != "/healthz" {
			resp = maps.Clone(resp)
			resp["500"] = ref("responses", "Error500")
			o["responses"] = resp
		}
		if rt.public {
			o["security"] = []any{}
		}
		item[strings.ToLower(method)] = o
	}
	version := s.Opts.Version
	if version == "" {
		version = DefaultAPIVersion
	}
	return obj{
		"openapi": "3.1.0",
		"info": obj{
			"title":       "Round Table invoke gateway",
			"version":     version,
			"description": "Synchronous HTTP API in front of the knights. The /mcp endpoint (MCP over streamable HTTP) is not described here.",
		},
		"security": []any{obj{"bearerAuth": []any{}}},
		"paths":    paths,
		"components": obj{
			"securitySchemes": obj{"bearerAuth": obj{"type": "http", "scheme": "bearer"}},
			"responses":       openAPIResponses,
			"schemas":         openAPISchemas,
		},
	}, nil
}

func ref(kind, name string) obj { return obj{"$ref": "#/components/" + kind + "/" + name} }

func schemaRef(name string) obj { return ref("schemas", name) }

func str(desc string) obj { return obj{"type": "string", "description": desc} }

func integer(desc string) obj { return obj{"type": "integer", "description": desc} }

func boolean(desc string) obj { return obj{"type": "boolean", "description": desc} }

func array(items obj) obj { return obj{"type": "array", "items": items} }

func object(required []string, props obj) obj {
	o := obj{"type": "object", "properties": props, "additionalProperties": false}
	if len(required) > 0 {
		o["required"] = required
	}
	return o
}

func pathParam(name, desc string) obj {
	return obj{"name": name, "in": "path", "required": true, "description": desc, "schema": obj{"type": "string"}}
}

func queryParam(name, desc string) obj {
	return obj{"name": name, "in": "query", "description": desc, "schema": obj{"type": "string"}}
}

func queryInteger(name, desc string) obj {
	return obj{"name": name, "in": "query", "description": desc, "schema": obj{"type": "integer", "minimum": 0}}
}

func jsonBody(schema obj, required bool) obj {
	return obj{"required": required, "content": obj{"application/json": obj{"schema": schema}}}
}

func jsonResp(desc string, schema obj) obj {
	return obj{"description": desc, "content": obj{"application/json": obj{"schema": schema}}}
}

// errs adds the shared error responses to an operation's responses.
func errs(resp obj, codes ...string) obj {
	for _, c := range codes {
		resp[c] = ref("responses", "Error"+c)
	}
	return resp
}

var idempotencyKeyParam = obj{"name": "Idempotency-Key", "in": "header",
	"description": "Makes a retry return the original result instead of starting a new one. The reply to a repeated key carries the Idempotent-Replay: true header.",
	"schema":      obj{"type": "string"}}

var waitSecondsParam = queryInteger("waitSeconds", "Long-poll up to this many seconds for the result to be final (capped by the gateway).")

var eventStreamResp = obj{
	"description": "Server-sent event stream; ends with a result event.",
	"content":     obj{"text/event-stream": obj{"schema": obj{"type": "string"}}},
}

// listOp builds a GET collection operation returning {key: [schema]}.
func listOp(summary, tag, key, item string, params ...obj) obj {
	ps := make([]any, len(params))
	for i, p := range params {
		ps[i] = p
	}
	return obj{
		"summary": summary, "tags": []any{tag}, "parameters": ps,
		"responses": errs(obj{"200": jsonResp("OK", object([]string{key}, obj{key: array(schemaRef(item))}))},
			"400", "401", "503"),
	}
}

// withListTotal adds the matching-entries count to a list operation's 200 response.
func withListTotal(op obj) obj {
	schema := op["responses"].(obj)["200"].(obj)["content"].(obj)["application/json"].(obj)["schema"].(obj)
	schema["properties"].(obj)["total"] = integer("Number of entries matching the filters, counted before the limit.")
	schema["required"] = append(schema["required"].([]string), "total")
	return op
}

// DefaultAPIVersion is the version reported when none is configured.
const DefaultAPIVersion = "dev"

var openAPIResponses = obj{
	"Error400": jsonResp("Invalid request.", schemaRef("Error")),
	"Error401": jsonResp("Missing or wrong bearer token.", schemaRef("Error")),
	"Error403": jsonResp("The caller may not target that namespace or resource.", schemaRef("Error")),
	"Error404": jsonResp("Not found.", schemaRef("Error")),
	"Error409": jsonResp("Conflict with the resource's current state.", schemaRef("ConflictError")),
	"Error422": jsonResp("The request is well-formed but cannot be processed.", schemaRef("Error")),
	"Error429": jsonResp("Too many in-flight invocations (or webhook rate limit; see Retry-After).", schemaRef("Error")),
	"Error500": jsonResp("Unexpected server error.", schemaRef("Error")),
	"Error503": jsonResp("A dependency (NATS, the Kubernetes API) is unavailable.", schemaRef("Error")),
}

var openAPIOperations = map[string]obj{
	"GET /healthz": {
		"summary": "Liveness probe", "tags": []any{"health"},
		"responses": obj{"200": obj{"description": "ok", "content": obj{"text/plain": obj{"schema": obj{"type": "string"}}}}},
	},
	"GET /openapi.json": {
		"summary": "This OpenAPI document", "tags": []any{"meta"},
		"responses": errs(obj{"200": jsonResp("OpenAPI 3.1 document.", obj{"type": "object"})}, "401"),
	},
	"GET /v1/knights": listOp("List knights", "knights", "knights", "KnightInfo",
		queryParam("namespace", "Namespace to list; omit for the gateway's default."),
		queryParam("load", "Set to true to include each knight's queue (queued and in-flight task counts).")),
	"GET /v1/knights/{name}/snapshots": {
		"summary": "List a knight's workspace snapshots", "tags": []any{"knights"},
		"description": "status.workspace.snapshots (oldest first), the WorkspaceSnapshots condition and the last handled snapshot request.",
		"parameters":  []any{pathParam("name", "Knight name."), queryParam("namespace", "Knight namespace; omit for the gateway's default.")},
		"responses":   errs(obj{"200": jsonResp("OK", schemaRef("KnightSnapshots"))}, "400", "401", "404", "503"),
	},
	"POST /v1/knights/{name}/snapshot": {
		"summary": "Take a workspace snapshot of a knight", "tags": []any{"knights"},
		"description": "Sets the ai.roundtable.io/snapshot annotation to a fresh value; the operator takes a VolumeSnapshot named <knight>-ws-<unix seconds>. 409 for a second request within the same second, 422 for an ephemeral workspace. A suspended knight answers 202 with a note: the snapshot runs on resume. With waitSeconds the call waits (capped) until the snapshot is readyToUse.",
		"parameters": []any{pathParam("name", "Knight name."), queryParam("namespace", "Knight namespace; omit for the gateway's default."),
			queryInteger("waitSeconds", "Seconds to wait for the snapshot to be readyToUse (default 0).")},
		"responses": errs(obj{"202": jsonResp("Snapshot requested.", schemaRef("SnapshotRequest"))},
			"400", "401", "404", "409", "422", "503"),
	},
	"POST /v1/knights/{name}/invoke": {
		"summary": "Invoke a knight and wait for the answer", "tags": []any{"tasks"},
		"parameters":  []any{pathParam("name", "Knight name."), idempotencyKeyParam},
		"requestBody": jsonBody(schemaRef("InvokeRequest"), true),
		"responses": errs(obj{
			"200": jsonResp("The task finished.", schemaRef("Task")),
			"202": obj{"description": "Still running; poll Location (GET /v1/tasks/{taskId}).",
				"headers": obj{"Location": obj{"schema": obj{"type": "string"}}},
				"content": obj{"application/json": obj{"schema": schemaRef("Task")}}},
		}, "400", "401", "403", "404", "409", "429", "503"),
	},
	"GET /v1/tasks": listOp("List tasks started through the gateway", "tasks", "tasks", "Task",
		queryParam("knight", "Only tasks of this knight."),
		queryParam("runId", "Only tasks of this runId."),
		obj{"name": "status", "in": "query", "description": "Only tasks in this status.",
			"schema": obj{"type": "string", "enum": []any{"pending", "succeeded", "failed"}}},
		queryInteger("limit", "Maximum number of tasks to return.")),
	"GET /v1/tasks/{taskId}": {
		"summary": "Get a task", "tags": []any{"tasks"},
		"parameters": []any{pathParam("taskId", "Task ID.")},
		"responses":  errs(obj{"200": jsonResp("The task.", schemaRef("Task"))}, "401", "404", "503"),
	},
	"GET /v1/tasks/{taskId}/events": {
		"summary": "Stream a task's progress as server-sent events", "tags": []any{"tasks"},
		"parameters": []any{pathParam("taskId", "Task ID."),
			obj{"name": "Last-Event-ID", "in": "header", "schema": obj{"type": "string"},
				"description": "Set by EventSource on reconnect; 204 means the client already has the result."}},
		"responses": errs(obj{"200": eventStreamResp, "204": obj{"description": "The client already has the result."}},
			"401", "404", "429", "503"),
	},
	"GET /v1/dead-letters": withListTotal(listOp("List dead-lettered tasks", "dead-letters", "deadLetters", "DeadLetter",
		queryParam("knight", "Only dead letters of this knight."),
		queryParam("reason", "lost, failed, timeout, usage_limited or refused."),
		queryParam("state", "open, retried or resolved."),
		queryInteger("limit", "Maximum number of entries (default and max 100)."))),
	"GET /v1/dead-letters/{taskId}": {
		"summary": "Get a dead letter", "tags": []any{"dead-letters"},
		"parameters": []any{pathParam("taskId", "Task ID."),
			queryParam("table", "RoundTable of the entry; needed only if the task is dead-lettered in several.")},
		"responses": errs(obj{"200": jsonResp("The dead letter.", schemaRef("DeadLetter"))}, "400", "401", "404", "503"),
	},
	"POST /v1/dead-letters/{taskId}/retry": {
		"summary": "Retry a dead-lettered task", "tags": []any{"dead-letters"},
		"parameters": []any{pathParam("taskId", "Task ID."),
			queryParam("table", "RoundTable of the entry; overrides the body's table.")},
		"requestBody": jsonBody(schemaRef("RetryRequest"), false),
		"responses": errs(obj{"202": obj{"description": "Retry published; Location is the new task.",
			"headers": obj{"Location": obj{"schema": obj{"type": "string"}}},
			"content": obj{"application/json": obj{"schema": schemaRef("RetryResult")}}}},
			"400", "401", "403", "404", "409", "422", "429", "503"),
	},
	"POST /v1/missions": {
		"summary": "Start a mission", "tags": []any{"missions"},
		"requestBody": jsonBody(schemaRef("StartMissionRequest"), true),
		"responses": errs(obj{
			"201": obj{"description": "Mission created (or finished within waitSeconds).",
				"headers": obj{"Location": obj{"schema": obj{"type": "string"}}},
				"content": obj{"application/json": obj{"schema": schemaRef("Mission")}}},
			"202": obj{"description": "Created and still running after waitSeconds.",
				"headers": obj{"Location": obj{"schema": obj{"type": "string"}}},
				"content": obj{"application/json": obj{"schema": schemaRef("Mission")}}},
		}, "400", "401", "403", "404", "422", "503"),
	},
	"GET /v1/missions": listOp("List missions", "missions", "missions", "Mission",
		queryParam("namespace", "Namespace to list; omit for the default."),
		queryParam("phase", "Only missions in this phase."),
		queryInteger("limit", "Maximum number of missions (default 20, max 200).")),
	"GET /v1/missions/{namespace}/{name}": {
		"summary": "Get a mission", "tags": []any{"missions"},
		"parameters": []any{pathParam("namespace", "Mission namespace."), pathParam("name", "Mission name."), waitSecondsParam},
		"responses":  errs(obj{"200": jsonResp("The mission.", schemaRef("Mission"))}, "400", "401", "403", "404", "503"),
	},
	"DELETE /v1/missions/{namespace}/{name}": {
		"summary": "Delete a gateway-created mission", "tags": []any{"missions"},
		"parameters": []any{pathParam("namespace", "Mission namespace."), pathParam("name", "Mission name.")},
		"responses": errs(obj{"200": jsonResp("Deleted.",
			object([]string{"deleted"}, obj{"deleted": str("namespace/name of the deleted mission")}))},
			"401", "403", "404", "503"),
	},
	"GET /v1/chains": listOp("List chains that may be run", "chains", "chains", "ChainInfo",
		queryParam("namespace", "Namespace to list; omit for the default.")),
	"POST /v1/chains/{name}/runs": {
		"summary": "Run a chain", "tags": []any{"runs"},
		"parameters":  []any{pathParam("name", "Chain name."), idempotencyKeyParam},
		"requestBody": jsonBody(schemaRef("RunChainRequest"), false),
		"responses": errs(obj{
			"200": jsonResp("The run finished.", schemaRef("Run")),
			"202": obj{"description": "Still running; poll Location (GET /v1/runs/{namespace}/{run}).",
				"headers": obj{"Location": obj{"schema": obj{"type": "string"}}},
				"content": obj{"application/json": obj{"schema": schemaRef("Run")}}},
		}, "400", "401", "403", "404", "409", "429", "503"),
	},
	"GET /v1/runs": listOp("List chain runs", "runs", "runs", "Run",
		queryParam("namespace", "Namespace to list; omit for the default."),
		queryParam("chain", "Only runs of this chain."),
		queryParam("phase", "Only runs in this phase, e.g. Running, Succeeded or Failed."),
		queryParam("source", "Which runs: gateway (default), trigger, pickup or all."),
		queryParam("active", "true: only runs that are Running or Pending."),
		queryParam("issue", "Only runs of this picked-up issue, as owner/repo#number."),
		queryInteger("limit", "Maximum number of runs (default 20, max 200).")),
	"GET /v1/runs/{namespace}/{run}": {
		"summary": "Get a chain run", "tags": []any{"runs"},
		"parameters": []any{pathParam("namespace", "Run namespace."), pathParam("run", "Run name."), waitSecondsParam},
		"responses": errs(obj{
			"200": jsonResp("The run finished.", schemaRef("Run")),
			"202": obj{"description": "Still running.", "headers": obj{"Location": obj{"schema": obj{"type": "string"}}},
				"content": obj{"application/json": obj{"schema": schemaRef("Run")}}},
		}, "400", "401", "403", "404", "503"),
	},
	"GET /v1/runs/{namespace}/{run}/events": {
		"summary": "Stream a chain run's progress as server-sent events", "tags": []any{"runs"},
		"parameters": []any{pathParam("namespace", "Run namespace."), pathParam("run", "Run name."),
			obj{"name": "Last-Event-ID", "in": "header", "schema": obj{"type": "string"},
				"description": "Set by EventSource on reconnect; 204 means the client already has the result."}},
		"responses": errs(obj{"200": eventStreamResp, "204": obj{"description": "The client already has the result."}},
			"401", "403", "404", "429", "503"),
	},
	"POST /v1/runs/{namespace}/{run}/cancel": {
		"summary": "Cancel an unfinished gateway-started chain run", "tags": []any{"runs"},
		"description": "Unfinished steps fail and the run ends; tasks already published to a knight are not recalled. A finished run is a conflict.",
		"parameters":  []any{pathParam("namespace", "Run namespace."), pathParam("run", "Run name.")},
		"requestBody": jsonBody(schemaRef("CancelRunRequest"), false),
		"responses": errs(obj{
			"200": jsonResp("The run ended.", schemaRef("Run")),
			"202": obj{"description": "Cancellation requested; the run is still ending.",
				"headers": obj{"Location": obj{"schema": obj{"type": "string"}}},
				"content": obj{"application/json": obj{"schema": schemaRef("Run")}}},
		}, "400", "401", "403", "404", "409", "503"),
	},
	"GET /v1/schedules": {
		"summary": "List schedules", "tags": []any{"schedules"},
		"description": "The schedules (cron Triggers) of a namespace, by name.",
		"parameters": []any{queryParam("namespace", "Namespace; default the gateway's."),
			queryParam("table", "Only the schedules of this table (RoundTable).")},
		"responses": errs(obj{"200": jsonResp("OK", object([]string{"schedules"}, obj{"schedules": array(schemaRef("ScheduleView"))}))}, "400", "401", "403", "503"),
	},
	"POST /v1/schedules": {
		"summary": "Create a schedule", "tags": []any{"schedules"},
		"description": "Creates a schedule Trigger labelled as the gateway's own. spec.source.schedule is required.",
		"requestBody": jsonBody(schemaRef("ScheduleRequest"), true),
		"responses": errs(obj{"201": obj{"description": "Created.",
			"headers": obj{"Location": obj{"schema": obj{"type": "string"}}},
			"content": obj{"application/json": obj{"schema": schemaRef("ScheduleRecord")}}}}, "400", "401", "403", "404", "409", "422", "503"),
	},
	"GET /v1/schedules/{namespace}/{name}": {
		"summary": "Get a schedule", "tags": []any{"schedules"},
		"parameters": []any{pathParam("namespace", "Schedule namespace."), pathParam("name", "Schedule name.")},
		"responses":  errs(obj{"200": jsonResp("OK", schemaRef("ScheduleView"))}, "400", "401", "403", "404", "503"),
	},
	"PUT /v1/schedules/{namespace}/{name}": {
		"summary": "Replace a schedule's spec", "tags": []any{"schedules"},
		"description": "Replaces the spec of a schedule the gateway manages; a schedule applied from Git is a 409.",
		"parameters":  []any{pathParam("namespace", "Schedule namespace."), pathParam("name", "Schedule name.")},
		"requestBody": jsonBody(schemaRef("ScheduleRequest"), true),
		"responses":   errs(obj{"200": jsonResp("Replaced.", schemaRef("ScheduleRecord"))}, "400", "401", "403", "404", "409", "422", "503"),
	},
	"DELETE /v1/schedules/{namespace}/{name}": {
		"summary": "Delete a schedule", "tags": []any{"schedules"},
		"description": "Deletes a schedule the gateway manages; a schedule applied from Git is a 409.",
		"parameters":  []any{pathParam("namespace", "Schedule namespace."), pathParam("name", "Schedule name.")},
		"responses": errs(obj{"200": jsonResp("Deleted.", object([]string{"deleted"}, obj{"deleted": str("namespace/name")}))},
			"400", "401", "403", "404", "409", "503"),
	},
	"GET /v1/schedules/{namespace}/{name}/manifest": {
		"summary": "A schedule's Trigger as YAML", "tags": []any{"schedules"},
		"description": "The Trigger without status or server-set metadata, ready to commit to Git.",
		"parameters":  []any{pathParam("namespace", "Schedule namespace."), pathParam("name", "Schedule name.")},
		"responses": errs(obj{"200": obj{"description": "The manifest.",
			"content": obj{"application/yaml": obj{"schema": obj{"type": "string"}}}}}, "400", "401", "403", "404", "503"),
	},
	"POST /v1/schedules/{namespace}/{name}/suspend": scheduleActionOp("Suspend a schedule",
		"Sets the suspended annotation (its value is the caller); spec.suspend and Git are untouched."),
	"POST /v1/schedules/{namespace}/{name}/resume": scheduleActionOp("Resume a schedule",
		"Removes the suspended annotation. A schedule suspended in its spec stays suspended (the state carries a message)."),
	"POST /v1/schedules/{namespace}/{name}/run": {
		"summary": "Run a schedule now", "tags": []any{"schedules"},
		"description": "Asks the trigger controller to fire the schedule once, even while suspended, and waits briefly for it. 201 when the run started, 202 while still pending, 409 when the firing was skipped.",
		"parameters":  []any{pathParam("namespace", "Schedule namespace."), pathParam("name", "Schedule name.")},
		"responses": errs(obj{
			"201": obj{"description": "The run started.", "headers": obj{"Location": obj{"schema": obj{"type": "string"}}},
				"content": obj{"application/json": obj{"schema": schemaRef("ScheduleRunNow")}}},
			"202": obj{"description": "The controller has not handled the request yet.", "headers": obj{"Location": obj{"schema": obj{"type": "string"}}},
				"content": obj{"application/json": obj{"schema": schemaRef("ScheduleRunNow")}}},
		}, "400", "401", "403", "404", "409", "503"),
	},
	"GET /v1/schedules/{namespace}/{name}/next": {
		"summary": "Next fire times of a schedule", "tags": []any{"schedules"},
		"description": "The next `count` cron ticks of the stored cron in the schedule's time zone. A suspended schedule still answers, with `suspended: true`. Lists cron ticks only: skip rules (concurrency policy, deadlines) are not applied.",
		"parameters": []any{pathParam("namespace", "Schedule namespace."), pathParam("name", "Schedule name."),
			obj{"name": "count", "in": "query", "description": "How many fire times (default 5, max 50).", "schema": obj{"type": "integer", "minimum": 1, "maximum": 50}}},
		"responses": errs(obj{"200": jsonResp("OK", schemaRef("ScheduleNextRuns"))}, "400", "401", "403", "404", "503"),
	},
	"GET /v1/schedules/preview": {
		"summary": "Preview the fire times of a cron expression", "tags": []any{"schedules"},
		"description": "The next `count` ticks of an expression that does not exist as a schedule yet. An invalid cron or time zone is a 422 with the parser's message. Lists cron ticks only.",
		"parameters": []any{queryParam("cron", "Five-field cron expression or @ descriptor."),
			queryParam("timeZone", "IANA time zone; default UTC."),
			obj{"name": "count", "in": "query", "description": "How many fire times (default 5, max 50).", "schema": obj{"type": "integer", "minimum": 1, "maximum": 50}}},
		"responses": errs(obj{"200": jsonResp("OK", schemaRef("ScheduleNextRuns"))}, "400", "401", "403", "422", "503"),
	},
	"GET /v1/schedules/{namespace}/{name}/runs": {
		"summary": "List a schedule's runs, newest first", "tags": []any{"schedules"},
		"description": "Firings with their time, outcome (skips with their reason), duration and cost when known, and the run to fetch the result with. History goes back as far as results are retained (gateway.scheduleRuns.resultRetention).",
		"parameters": []any{pathParam("namespace", "Schedule namespace."), pathParam("name", "Schedule name."),
			queryInteger("limit", "Maximum number of runs (default 20, max 200).")},
		"responses": errs(obj{"200": jsonResp("OK", object([]string{"runs"}, obj{"runs": array(schemaRef("ScheduleRun"))}))},
			"400", "401", "404", "503"),
	},
	"GET /v1/schedules/{namespace}/{name}/runs/{run}": {
		"summary": "Get the result of one run of a schedule", "tags": []any{"schedules"},
		"description": "For a chain target, the run's steps and final output; for a knight target, the task's result (`task`), readable while its task record is kept; for a mission target, the Mission (`mission`) with its phase and result, readable while it exists. A run that does not belong to the schedule, or whose record or Mission is gone, is a 404.",
		"parameters": []any{pathParam("namespace", "Schedule namespace."), pathParam("name", "Schedule name."),
			pathParam("run", "Run from the run history.")},
		"responses": errs(obj{"200": jsonResp("The run's result.", schemaRef("ScheduleRunResult"))},
			"400", "401", "404", "422", "503"),
	},
	"POST /v1/schedules/suspend": tableSchedulesOp("Suspend every schedule of a table",
		"Sets the suspended annotation on each schedule of the table; spec.suspend and Git are untouched. One failing schedule does not stop the rest."),
	"POST /v1/schedules/resume": tableSchedulesOp("Resume every schedule of a table",
		"Removes the suspended annotation from each schedule of the table. A schedule suspended in its spec stays suspended (its state carries a message)."),
	"POST /v1/hooks/{namespace}/{trigger}": {
		"summary": "Deliver a webhook to a Trigger", "tags": []any{"hooks"},
		"description": "Not behind the API bearer token: authenticated by the Trigger's own bearer token or HMAC signature (X-Hub-Signature-256 or X-Signature).",
		"parameters":  []any{pathParam("namespace", "Trigger namespace."), pathParam("trigger", "Trigger name.")},
		"requestBody": obj{"required": true, "content": obj{"*/*": obj{"schema": obj{}}}},
		"responses": errs(obj{
			"202": jsonResp("Delivery published.", schemaRef("HookResult")),
			"413": jsonResp("Body too large.", schemaRef("Error")),
		}, "400", "401", "404", "429", "503"),
	},
	"POST /v1/triggers/{namespace}/{name}/fire": {
		"summary": "Fire a schedule Trigger", "tags": []any{"triggers"},
		"description": "Called by the Trigger's CronJob. Allowed for the Trigger's own ServiceAccount (<namespace>/trigger-<name>) and static tokens. " +
			"Publishes a schedule event keyed by the scheduled time (truncated to the minute) to the Trigger's events subject; the trigger controller dedupes and records the firing.",
		"parameters":  []any{pathParam("namespace", "Trigger namespace."), pathParam("name", "Trigger name.")},
		"requestBody": jsonBody(schemaRef("FireRequest"), false),
		"responses": errs(obj{
			"202": jsonResp("Firing published.", schemaRef("FireResult")),
		}, "400", "401", "403", "404", "422", "503"),
	},
}

var openAPISchemas = obj{
	"Error": object([]string{"error"}, obj{"error": str("What went wrong.")}),
	"ConflictError": object([]string{"error"}, obj{
		"error": str("What went wrong."), "state": str("The resource's current state.")}),
	"InvokeRequest": object([]string{"task"}, obj{
		"task":           str("The prompt to send to the knight."),
		"runId":          str("runId of an earlier invocation to continue that conversation; omit to start a new one."),
		"model":          str("Model ID or RoundTable alias for this task only; omit to use the knight's model."),
		"timeoutSeconds": integer("Seconds to wait for the result before returning a pending task (0 = do not wait)."),
		"namespace":      str("Namespace of the knight; omit for the default."),
	}),
	"PRRef": object(nil, obj{"repo": str(""), "number": integer(""), "url": str(""), "action": str("")}),
	"Task": object([]string{"taskId", "runId", "knight", "namespace", "status", "createdAt", "resultSubject"}, obj{
		"taskId":         str("Task ID."),
		"runId":          str("Conversation ID; pass it back to continue the conversation."),
		"knight":         str("Knight name."),
		"namespace":      str("Knight namespace."),
		"roundTable":     str("RoundTable the knight belongs to."),
		"status":         obj{"type": "string", "enum": []any{"pending", "succeeded", "failed"}},
		"output":         str("The knight's answer, once finished."),
		"error":          str("The knight's error (status failed)."),
		"usageLimited":   boolean("The failure is a plan-quota refusal: back off rather than retry."),
		"traceId":        str("Trace ID of the gateway's span for the call; the knight's invoke_agent span is in the same trace. Omitted when tracing is off."),
		"requestedModel": str("Resolved per-task model override."),
		"model":          str("Model the knight reports having used."),
		"cost":           obj{"type": "number", "description": "Cost in USD the knight reported (notional for subscription billing); omitted when not reported."},
		"billing":        str("How the model is billed: api, subscription or local; omitted when not reported."),
		"durationMs":     obj{"type": "number", "description": "Wall-clock run time in ms the knight reported."},
		"tokens":         obj{"type": "object", "description": "Token use the knight reported: input, output, cacheRead, cacheWrite."},
		"turns":          obj{"type": "integer", "description": "Agent turns the knight reported."},
		"toolCalls":      obj{"type": "integer", "description": "Tool calls the knight reported; omitted (not 0) when the knight does not report them."},
		"prs":            array(schemaRef("PRRef")),
		"verdict":        str("Structured verdict reported by the knight."),
		"createdAt":      obj{"type": "string", "format": "date-time"},
		"completedAt":    obj{"type": "string", "format": "date-time"},
		"resultSubject":  str("NATS subject holding the result."),
		"resultsStream":  str("NATS stream holding the result."),
	}),
	"KnightSnapshots": object([]string{"knight", "namespace", "snapshots"}, obj{
		"knight": str(""), "namespace": str(""),
		"lastSnapshotRequest": str("Last handled value of the ai.roundtable.io/snapshot annotation."),
		"snapshots": array(object([]string{"name", "readyToUse"}, obj{
			"name": str("VolumeSnapshot name."), "createdAt": obj{"type": "string", "format": "date-time"}, "readyToUse": boolean(""),
		})),
		"condition": object([]string{"status", "reason"}, obj{"status": str("True or False."), "reason": str(""), "message": str("")}),
	}),
	"SnapshotRequest": object([]string{"knight", "namespace", "request", "expectedName"}, obj{
		"knight": str(""), "namespace": str(""), "request": str("Value set on the snapshot annotation (unix nanoseconds)."),
		"expectedName": str("Name the VolumeSnapshot will get: <knight>-ws-<unix seconds>."),
		"caller":       str(""), "note": str("Set for a suspended knight: the snapshot runs on resume."),
		"ready":     boolean("True once the snapshot is readyToUse (only with waitSeconds)."),
		"snapshot":  object([]string{"name", "readyToUse"}, obj{"name": str(""), "createdAt": obj{"type": "string", "format": "date-time"}, "readyToUse": boolean("")}),
		"condition": object([]string{"status", "reason"}, obj{"status": str(""), "reason": str(""), "message": str("")}),
	}),
	"KnightInfo": object([]string{"name", "namespace", "domain", "ready"}, obj{
		"name": str(""), "namespace": str(""), "domain": str(""), "model": str(""), "runtime": str(""),
		"phase": str(""), "ready": boolean(""), "suspended": boolean(""), "roundTable": str(""),
		"queue": object([]string{"pending", "inFlight"}, obj{
			"pending":  integer("Tasks queued for the knight."),
			"inFlight": integer("Tasks delivered to the knight and not yet acknowledged."),
		}),
	}),
	"DeadLetter": object([]string{"taskId", "knight", "state", "reason", "attempt", "updatedAt"}, obj{
		"taskId": str(""), "table": str("RoundTable of the entry."), "knight": str(""),
		"state":  obj{"type": "string", "description": "open, retried or resolved."},
		"reason": obj{"type": "string", "description": "lost, failed, timeout, usage_limited or refused."},
		"error":  str(""), "attempt": integer(""), "retryOf": str(""), "chainName": str(""), "stepName": str(""),
		"runId": str(""), "traceId": str("Trace ID (32 hex) of the original task, when it carried a valid traceparent."), "deadLetteredAt": obj{"type": "string", "format": "date-time"},
		"updatedAt": obj{"type": "string", "format": "date-time"}, "retriedAs": str(""),
		"task": obj{"type": "object", "description": "The original task payload."},
	}),
	"RetryRequest": object(nil, obj{
		"taskId":    str("Ignored: the path's taskId is used."),
		"table":     str("RoundTable of the entry; only needed if the taskId is dead-lettered in several."),
		"force":     boolean("Retry a chain step on its own anyway (the chain does not continue)."),
		"namespace": str("Namespace of the knight; omit for the default."),
	}),
	"RetryResult": object([]string{"taskId", "retryOf", "attempt", "knight"}, obj{
		"taskId": str("The new task."), "retryOf": str("The dead-lettered task."), "attempt": integer(""), "knight": str(""),
	}),
	"StartMissionRequest": object([]string{"objective", "roundTable", "costBudgetUSD"}, obj{
		"objective":       str("The goal of the mission (max 16 KiB)."),
		"successCriteria": str("How to tell whether the mission succeeded."),
		"roundTable":      str("Name of the RoundTable the mission runs under."),
		"costBudgetUSD":   obj{"type": "number", "exclusiveMinimum": 0, "description": "Cost budget in USD (capped by the gateway)."},
		"ttlSeconds":      integer("Seconds until the mission is cleaned up (min 60, capped; default 3600)."),
		"timeoutSeconds":  integer("Seconds the mission may run before it fails (min 60, capped; default 1800)."),
		"planner": object(nil, obj{
			"knight":               str("Existing knight to plan with; omit for the table's planner knight."),
			"context":              str("Extra information for the planner (max 64 KiB)."),
			"maxChains":            integer("Most chains the planner may generate (1-20)."),
			"maxKnights":           integer("Most ephemeral knights the planner may generate (1-50)."),
			"allowSkillGeneration": boolean("Let the planner write new skills (only if the gateway allows it)."),
		}),
		"recruitExisting": boolean("Let the mission use standing (non-ephemeral) knights."),
		"knights":         array(obj{"type": "string"}),
		"chains": array(object([]string{"name"}, obj{
			"name": str("Name of an existing chain."), "input": str("Input for this run of the chain (max 64 KiB)."),
		})),
		"waitSeconds": integer("Seconds to wait for the mission to finish (default 0 = return at once)."),
		"namespace":   str("Namespace of the mission; omit for the default."),
	}),
	"Mission": object([]string{"name", "namespace", "phase", "done", "createdByGateway", "createdAt"}, obj{
		"name": str(""), "namespace": str(""), "phase": str(""), "done": boolean(""),
		"createdByGateway": boolean(""),
		"conditions": array(object([]string{"type", "status", "lastTransitionTime"}, obj{
			"type": str(""), "status": str(""), "reason": str(""), "message": str(""),
			"lastTransitionTime": obj{"type": "string", "format": "date-time"},
		})),
		"planning": object(nil, obj{
			"completedAt":     obj{"type": "string", "format": "date-time"},
			"chainsGenerated": integer(""), "knightsGenerated": integer(""), "skillsGenerated": integer(""),
			"error": str(""), "rawOutput": str(""), "rawOutputTruncated": boolean(""),
		}),
		"chains": array(object([]string{"name"}, obj{
			"name": str(""), "chain": str(""), "phase": str(""),
			"steps": array(object([]string{"name"}, obj{"name": str(""), "phase": str("")})),
		})),
		"knights": array(object([]string{"name", "ready", "tasksCompleted", "ephemeral"}, obj{
			"name": str(""), "ready": boolean(""), "tasksCompleted": integer(""), "ephemeral": boolean(""),
		})),
		"result": str(""), "totalCost": str(""),
		"traceId":     str("Trace ID (32 hex) from status.traceID of the Mission; omitted when empty or invalid."),
		"createdAt":   obj{"type": "string", "format": "date-time"},
		"startedAt":   obj{"type": "string", "format": "date-time"},
		"completedAt": obj{"type": "string", "format": "date-time"},
		"expiresAt":   obj{"type": "string", "format": "date-time"},
	}),
	"ChainInfo": object([]string{"name", "namespace", "steps"}, obj{
		"name": str(""), "namespace": str(""), "description": str(""), "steps": integer(""), "timeoutSeconds": integer(""),
	}),
	"RunChainRequest": object(nil, obj{
		"input":       obj{"description": "The run's input: a JSON string is used as is, any other JSON value as its JSON text. Omit to keep the Chain's own input."},
		"namespace":   str("Namespace of the Chain; omit for the default."),
		"waitSeconds": integer("Wait for the run to finish (capped by the gateway). 0 returns once the run is created."),
	}),
	"TableSchedulesRequest": object([]string{"table"}, obj{
		"table": str("Table (RoundTable) whose schedules to change."), "namespace": str("Namespace of the table; omit for the default."),
	}),
	"TableSchedulesResult": object([]string{"changed", "unchanged", "failed"}, obj{
		"changed":   array(object(nil, scheduleStateProps)),
		"unchanged": array(object(nil, scheduleStateProps)),
		"failed":    array(object([]string{"name", "error"}, obj{"name": str(""), "error": str("")})),
	}),
	"CancelRunRequest": object(nil, obj{
		"reason": str("Recorded on the cancelled steps and the run's Cancelled condition."),
	}),
	"Run": object([]string{"run", "chain", "namespace", "phase", "done", "createdAt"}, obj{
		"run": str("Name of the run copy."), "chain": str(""), "namespace": str(""), "runId": str(""),
		"traceId": str("Trace ID (32 hex) from status.traceID of the run's Chain; omitted when empty or invalid."),
		"phase":   str(""), "done": boolean(""),
		"createdAt":   obj{"type": "string", "format": "date-time"},
		"startedAt":   obj{"type": "string", "format": "date-time"},
		"completedAt": obj{"type": "string", "format": "date-time"},
		"steps": array(object([]string{"name"}, obj{
			"name": str(""), "phase": str(""), "error": str(""), "output": str(""), "outputTruncated": boolean(""),
		})),
		"output": str(""), "outputTruncated": boolean(""),
		"source":         str("What started the run: gateway, trigger or pickup."),
		"ageSeconds":     obj{"type": "integer", "description": "Seconds since the run was created."},
		"runningSeconds": obj{"type": "integer", "description": "Seconds since the run started, while Running."},
		"issue":          object([]string{"repo", "number"}, obj{"repo": str("owner/name"), "number": obj{"type": "integer"}}),
	}),
	"ScheduleView": object([]string{"name", "namespace", "cron", "timeZone", "description", "target", "suspended", "concurrency", "managedBy"}, obj{
		"name": str(""), "namespace": str(""), "table": str("The RoundTable: the Trigger's roundTableRef, else its target Chain's."),
		"cron": str(""), "timeZone": str("IANA zone the cron is evaluated in (UTC when unset)."),
		"description": str("The cron in words."),
		"target": object([]string{"kind", "summary"}, obj{
			"kind": str("chain, knight or mission."), "summary": str("Short description of the target."),
			"chain": str("Target Chain (kind chain)."), "knight": str("Target Knight (kind knight)."),
		}),
		"suspended":   boolean("Effective suspension: spec.suspend or the suspended annotation."),
		"suspendedBy": str("spec, annotation or spec,annotation; omitted when not suspended."),
		"concurrency": object([]string{"policy", "maxActive"}, obj{
			"policy": str("Allow, Forbid or Replace."), "maxActive": integer("Cap on active runs under Allow; 0 means no cap."),
		}),
		"startingDeadlineSeconds": integer("How late a missed tick may still run."),
		"nextRunAt":               obj{"type": "string", "format": "date-time"},
		"lastRunAt":               obj{"type": "string", "format": "date-time"},
		"lastResult": object([]string{"time", "outcome"}, obj{
			"time": obj{"type": "string", "format": "date-time"}, "outcome": str("started, skipped, succeeded, failed or lost."),
			"run": str(""), "skipReason": str(""),
		}),
		"managedBy": obj{"type": "string", "enum": []any{"git", "api", "other"}, "description": "Who owns the Trigger."},
	}),
	"ScheduleRequest": object([]string{specKey}, obj{
		"name": str("Trigger name; required on POST."), "namespace": str("Defaults to the gateway's namespace on POST; must match the path on PUT."),
		specKey: obj{"type": "object", "description": "The Trigger's spec; spec.source.schedule is required and no other source may be set."},
	}),
	"ScheduleRecord": object([]string{"name", "namespace", "managedBy", "createdAt", specKey}, obj{
		"name": str(""), "namespace": str(""), "managedBy": str("git, api or other."), "caller": str(""),
		"createdAt": obj{"type": "string", "format": "date-time"}, specKey: obj{"type": "object"},
	}),
	"ScheduleState": object([]string{"name", "namespace", "suspended"}, scheduleStateProps),
	"ScheduleRunNow": object([]string{"schedule", "namespace", "target", "key", "state"}, obj{
		"schedule": str(""), "namespace": str(""), "target": str("chain, knight or mission."),
		"run": str("The run, task ID or Mission; empty while pending."), "key": str("The firing's dedupe key, manual:<id>."),
		"state": str("started, pending or skipped."), "skipReason": str(""), "caller": str(""),
	}),
	"ScheduleNextRuns": object([]string{"cron", "timeZone", "times"}, obj{
		"namespace": str("Schedule namespace (omitted by the preview)."), "name": str("Schedule name (omitted by the preview)."),
		"cron": str(""), "timeZone": str("IANA zone the cron is evaluated in."),
		"suspended": boolean("The schedule is suspended; it still answers."),
		"times": array(object([]string{"time", "local"}, obj{
			"time":  obj{"type": "string", "format": "date-time", "description": "The tick in UTC."},
			"local": str("The same instant in the time zone, RFC 3339."),
		})),
	}),
	"ScheduleRun": object([]string{"time", "outcome", "resultAvailable"}, obj{
		"run":  str("Run to fetch the result with; empty for a skipped firing."),
		"time": obj{"type": "string", "format": "date-time"}, "key": str("The firing's dedupe key."),
		"outcome": str("succeeded, failed, partiallySucceeded, lost, skipped, pending, running or unknown."),
		"reason":  str("Why a firing was skipped."), "durationSeconds": obj{"type": "number"},
		"turns":     obj{"type": "integer", "description": "Agent turns of a knight-target run."},
		"toolCalls": obj{"type": "integer", "description": "Tool calls of a knight-target run; omitted when not reported."},
		"costUSD":   str("The run's cost, when known."), "resultAvailable": boolean("The result can still be fetched."),
		"traceId": str("Trace ID of the run: the Chain's or Mission's status.traceID, or the task record's traceId; omitted when unknown or the run is gone."),
	}),
	"ScheduleRunResult": object([]string{"schedule", "target"}, obj{
		"schedule": str(""), "target": str("Kind of target that ran: chain, knight or mission. A chain target's record is flattened into this object; a knight target sets task, a mission target sets mission."),
		"task": object([]string{"run", "knight", "status", "createdAt"}, obj{
			"run": str("Task ID."), "knight": str(""), "status": str("pending, succeeded or failed."),
			"output": str(""), "error": str(""), "model": str(""), "verdict": str(""),
			"traceId":     str("Trace ID from the task record; omitted when empty or invalid."),
			"prs":         array(obj{"type": "object"}),
			"createdAt":   obj{"type": "string", "format": "date-time"},
			"completedAt": obj{"type": "string", "format": "date-time"},
		}),
		"mission": obj{"type": "object", "description": "The Mission, as GET /v1/missions/{namespace}/{name} returns it."},
		"run":     str("Name of the run."), "chain": str(""), "namespace": str(""), "runId": str(""),
		"traceId": str("Trace ID (32 hex) from status.traceID of the run's Chain; omitted when empty or invalid."),
		"phase":   str(""), "done": boolean(""),
		"createdAt":   obj{"type": "string", "format": "date-time"},
		"startedAt":   obj{"type": "string", "format": "date-time"},
		"completedAt": obj{"type": "string", "format": "date-time"},
		"steps": array(object([]string{"name"}, obj{
			"name": str(""), "phase": str(""), "error": str(""), "output": str(""), "outputTruncated": boolean(""),
		})),
		"output": str("The run's final output."), "outputTruncated": boolean(""),
	}),
	"FireRequest": object(nil, obj{
		"scheduledTime": obj{"type": "string", "format": "date-time",
			"description": "The schedule time this firing is for; omitted: now. Truncated to the minute."},
	}),
	"FireResult": object([]string{"subject", "stream", "seq", "key", "scheduledTime"}, obj{
		"subject": str("NATS subject the firing was published on."), "stream": str("JetStream stream."),
		"seq":           integer("Stream sequence of the firing."),
		"duplicate":     boolean("JetStream dropped the firing as a repeat of one for the same scheduled time."),
		"key":           str("Firing key: the scheduled time, RFC 3339 in UTC."),
		"scheduledTime": obj{"type": "string", "format": "date-time"},
	}),
	"HookResult": object([]string{"subject", "stream", "seq"}, obj{
		"subject": str("NATS subject the delivery was published on."), "stream": str("JetStream stream."),
		"seq":       integer("Stream sequence of the delivery."),
		"duplicate": boolean("JetStream dropped the delivery as a repeat of an earlier one."),
	}),
}

var scheduleStateProps = obj{
	"name": str(""), "namespace": str(""), "suspended": boolean("Effective suspension after the change."),
	"suspendedBy": str("spec, annotation or spec,annotation."), "message": str("Set when a resume left the schedule suspended."),
}

// specKey is the Trigger spec property of the schedule schemas.
const specKey = "spec"

func scheduleActionOp(summary, description string) obj {
	return obj{
		"summary": summary, "tags": []any{"schedules"}, "description": description,
		"parameters": []any{pathParam("namespace", "Schedule namespace."), pathParam("name", "Schedule name.")},
		"responses":  errs(obj{"200": jsonResp("The schedule's state after the change.", schemaRef("ScheduleState"))}, "400", "401", "403", "404", "503"),
	}
}

func tableSchedulesOp(summary, description string) obj {
	return obj{
		"summary": summary, "tags": []any{"schedules"}, "description": description,
		"requestBody": jsonBody(schemaRef("TableSchedulesRequest"), true),
		"responses":   errs(obj{"200": jsonResp("Per-schedule outcome.", schemaRef("TableSchedulesResult"))}, "400", "401", "403", "503"),
	}
}
