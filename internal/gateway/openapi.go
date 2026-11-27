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
		if resp, ok := o["responses"].(obj); ok && method != "" && path != "/healthz" {
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
		queryParam("namespace", "Namespace to list; omit for the gateway's default.")),
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
	"KnightInfo": object([]string{"name", "namespace", "domain", "ready"}, obj{
		"name": str(""), "namespace": str(""), "domain": str(""), "model": str(""), "runtime": str(""),
		"phase": str(""), "ready": boolean(""), "suspended": boolean(""), "roundTable": str(""),
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
	"CancelRunRequest": object(nil, obj{
		"reason": str("Recorded on the cancelled steps and the run's Cancelled condition."),
	}),
	"Run": object([]string{"run", "chain", "namespace", "phase", "done", "createdAt"}, obj{
		"run": str("Name of the run copy."), "chain": str(""), "namespace": str(""), "runId": str(""),
		"phase": str(""), "done": boolean(""),
		"createdAt":   obj{"type": "string", "format": "date-time"},
		"startedAt":   obj{"type": "string", "format": "date-time"},
		"completedAt": obj{"type": "string", "format": "date-time"},
		"steps": array(object([]string{"name"}, obj{
			"name": str(""), "phase": str(""), "error": str(""), "output": str(""), "outputTruncated": boolean(""),
		})),
		"output": str(""), "outputTruncated": boolean(""),
	}),
	"ScheduleRun": object([]string{"time", "outcome", "resultAvailable"}, obj{
		"run":  str("Run to fetch the result with; empty for a skipped firing."),
		"time": obj{"type": "string", "format": "date-time"}, "key": str("The firing's dedupe key."),
		"outcome": str("succeeded, failed, partiallySucceeded, lost, skipped, pending, running or unknown."),
		"reason":  str("Why a firing was skipped."), "durationSeconds": obj{"type": "number"},
		"turns":     obj{"type": "integer", "description": "Agent turns of a knight-target run."},
		"toolCalls": obj{"type": "integer", "description": "Tool calls of a knight-target run; omitted when not reported."},
		"costUSD":   str("The run's cost, when known."), "resultAvailable": boolean("The result can still be fetched."),
	}),
	"ScheduleRunResult": object([]string{"schedule", "target"}, obj{
		"schedule": str(""), "target": str("Kind of target that ran: chain, knight or mission. A chain target's record is flattened into this object; a knight target sets task, a mission target sets mission."),
		"task": object([]string{"run", "knight", "status", "createdAt"}, obj{
			"run": str("Task ID."), "knight": str(""), "status": str("pending, succeeded or failed."),
			"output": str(""), "error": str(""), "model": str(""), "verdict": str(""),
			"prs":         array(obj{"type": "object"}),
			"createdAt":   obj{"type": "string", "format": "date-time"},
			"completedAt": obj{"type": "string", "format": "date-time"},
		}),
		"mission": obj{"type": "object", "description": "The Mission, as GET /v1/missions/{namespace}/{name} returns it."},
		"run":     str("Name of the run."), "chain": str(""), "namespace": str(""), "runId": str(""),
		"phase": str(""), "done": boolean(""),
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
