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
	"fmt"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
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

// ListDeadLettersOutput is the output of the list_dead_letters tool.
type ListDeadLettersOutput struct {
	DeadLetters []*DeadLetterView `json:"deadLetters"`
}

// GetMissionInput is the input of the get_mission tool.
type GetMissionInput struct {
	Name        string `json:"name" jsonschema:"name of the mission (returned by start_mission)"`
	Namespace   string `json:"namespace,omitempty" jsonschema:"namespace of the mission; omit for the default"`
	WaitSeconds *int   `json:"waitSeconds,omitempty" jsonschema:"seconds to wait for the mission to finish before returning (default 0)"`
}

// ListMissionsOutput is the output of the list_missions tool.
type ListMissionsOutput struct {
	Missions []*MissionView `json:"missions"`
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

	mcp.AddTool(s, &mcp.Tool{
		Name: "list_dead_letters",
		Description: "List dead-lettered tasks (lost or failed knight tasks), newest first, with their state: open, retried or resolved. " +
			"Retry an open one with retry_task.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in DeadLetterFilter) (*mcp.CallToolResult, ListDeadLettersOutput, error) {
		out, err := svc.ListDeadLetters(ctx, in)
		return nil, ListDeadLettersOutput{DeadLetters: out}, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name: "retry_task",
		Description: "Retry an open dead-lettered task: republish it to the same knight as a new task with attempt number + 1, " +
			"so the knight checks for partial work. Poll the returned taskId with get_task. A task already retried or resolved, " +
			"or one owned by a chain (unless force is set), is refused.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in RetryRequest) (*mcp.CallToolResult, *RetryResult, error) {
		res, err := svc.RetryDeadLetter(ctx, in)
		if err != nil {
			return nil, nil, err
		}
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{
			Text: fmt.Sprintf("Retrying %s as %s (attempt %d) on knight %s. Call get_task with taskId %s.",
				res.RetryOf, res.TaskID, res.Attempt, res.Knight, res.TaskID)}}}, res, nil
	})
	if svc.MissionsEnabled() {
		addMissionTools(s, svc)
	}
	return s
}

func addMissionTools(s *mcp.Server, svc *Service) {
	readOnly := &mcp.ToolAnnotations{ReadOnlyHint: true}
	mcp.AddTool(s, &mcp.Tool{
		Name: "start_mission",
		Description: "Launch a mission: an objective, a RoundTable and a cost budget. With a planner, a planner knight " +
			"breaks the objective into chains over standing knights and the RoundTable's knight templates; otherwise " +
			"the listed existing chains run. Returns the mission; poll get_mission with its name until done is true.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in StartMissionRequest) (*mcp.CallToolResult, *MissionView, error) {
		v, err := svc.StartMission(ctx, in)
		if err != nil {
			return nil, nil, err
		}
		return missionResult(v), v, nil
	})
	mcp.AddTool(s, &mcp.Tool{
		Name:        "get_mission",
		Description: "Get a mission's phase, planning result, chain and knight progress, cost and, once done, its result.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in GetMissionInput) (*mcp.CallToolResult, *MissionView, error) {
		v, err := svc.GetMission(ctx, in.Namespace, in.Name, in.WaitSeconds)
		if err != nil {
			return nil, nil, err
		}
		return missionResult(v), v, nil
	})
	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_missions",
		Description: "List missions, newest first, optionally only those in one phase.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in MissionFilter) (*mcp.CallToolResult, ListMissionsOutput, error) {
		missions, err := svc.ListMissions(ctx, in)
		return nil, ListMissionsOutput{Missions: missions}, err
	})
}

// missionResult puts a finished mission's result first in the text content
// and marks failed or expired missions as tool errors.
func missionResult(v *MissionView) *mcp.CallToolResult {
	id := v.Namespace + "/" + v.Name
	if !v.Done {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{
			Text: "Mission " + id + " is " + v.Phase + ". Call get_mission with name " + v.Name + " later."}}}
	}
	failed := v.Phase == string(aiv1alpha1.MissionPhaseFailed) || v.Phase == string(aiv1alpha1.MissionPhaseExpired)
	for _, c := range v.Conditions {
		if c.Type == aiv1alpha1.ConditionMissionComplete && c.Reason != "" && c.Reason != aiv1alpha1.ReasonMissionSucceeded {
			failed = true
		}
	}
	text := v.Result
	if text == "" {
		text = "Mission " + id + " finished: " + v.Phase
	}
	return &mcp.CallToolResult{IsError: failed, Content: []mcp.Content{&mcp.TextContent{Text: text}}}
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
