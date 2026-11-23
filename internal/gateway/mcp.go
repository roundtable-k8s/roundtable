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
	"context"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// serverVersion is reported in the MCP initialize handshake.
const serverVersion = "v1alpha1"

// ListKnightsInput is the input of the list_knights tool.
type ListKnightsInput struct {
	Namespace string `json:"namespace,omitempty" jsonschema:"namespace to list; omit for the default"`
}

// ListKnightsOutput is the output of the list_knights tool.
type ListKnightsOutput struct {
	Knights []KnightInfo `json:"knights"`
}

// InvokeKnightInput is the input of the invoke_knight tool.
type InvokeKnightInput struct {
	Knight string `json:"knight" jsonschema:"name of the knight to invoke"`
	InvokeRequest
}

// GetTaskInput is the input of the get_task tool.
type GetTaskInput struct {
	TaskID string `json:"taskId" jsonschema:"taskId returned by invoke_knight"`
}

// ListTasksOutput is the output of the list_tasks tool.
type ListTasksOutput struct {
	Tasks []*TaskRecord `json:"tasks"`
}

// NewMCPServer builds the MCP server exposing the gateway's operations.
func NewMCPServer(svc *Service) *mcp.Server {
	s := mcp.NewServer(&mcp.Implementation{Name: "roundtable-gateway", Title: "Round Table knights", Version: serverVersion}, nil)
	readOnly := &mcp.ToolAnnotations{ReadOnlyHint: true}

	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_knights",
		Description: "List the Round Table knights (agents) that can be invoked, with their domain, model and readiness.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in ListKnightsInput) (*mcp.CallToolResult, ListKnightsOutput, error) {
		knights, err := svc.ListKnights(ctx, in.Namespace)
		return nil, ListKnightsOutput{Knights: knights}, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name: "invoke_knight",
		Description: "Send a task to a knight and wait for its answer. If the answer isn't ready within the timeout, " +
			"the task is returned with status \"pending\": poll get_task with its taskId. " +
			"Pass the returned runId on a later call to continue the same conversation.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in InvokeKnightInput) (*mcp.CallToolResult, *TaskRecord, error) {
		rec, err := svc.Invoke(ctx, in.Knight, in.InvokeRequest)
		if err != nil {
			return nil, nil, err
		}
		return taskResult(rec), rec, nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "get_task",
		Description: "Get a task started by invoke_knight: its status and, once finished, the knight's output or error.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in GetTaskInput) (*mcp.CallToolResult, *TaskRecord, error) {
		rec, err := svc.GetTask(ctx, in.TaskID)
		if err != nil {
			return nil, nil, err
		}
		return taskResult(rec), rec, nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_tasks",
		Description: "List recent tasks started through this gateway, newest first.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in TaskFilter) (*mcp.CallToolResult, ListTasksOutput, error) {
		tasks, err := svc.ListTasks(ctx, in)
		return nil, ListTasksOutput{Tasks: tasks}, err
	})
	return s
}

// taskResult puts a finished task's output first in the text content (what a
// model reads) and marks failed tasks as tool errors. The full record is the
// structured content either way.
func taskResult(rec *TaskRecord) *mcp.CallToolResult {
	switch rec.Status {
	case TaskSucceeded:
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: rec.Output}}}
	case TaskFailed:
		return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{
			Text: "knight " + rec.Knight + " failed task " + rec.TaskID + ": " + rec.Error}}}
	default:
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{
			Text: "Task " + rec.TaskID + " is still running on knight " + rec.Knight +
				". Call get_task with this taskId later. runId: " + rec.RunID}}}
	}
}

// MCPHandler serves the MCP streamable HTTP transport. It is stateless (no
// Mcp-Session-Id), so any gateway replica can serve any request.
func MCPHandler(svc *Service) http.Handler {
	server := NewMCPServer(svc)
	return mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server },
		&mcp.StreamableHTTPOptions{Stateless: true})
}
