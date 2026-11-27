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
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
)

// serverVersion is reported in the MCP initialize handshake.
const serverVersion = "v1alpha1"

// ListKnightsInput is the input of the list_knights tool.
type ListKnightsInput struct {
	Namespace string `json:"namespace,omitempty" jsonschema:"namespace to list; omit for the default"`
	Load      bool   `json:"load,omitempty" jsonschema:"also report each knight's queued (pending) and in-flight task counts"`
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

// GetDeadLetterInput is the input of the get_dead_letter tool.
type GetDeadLetterInput struct {
	TaskID string `json:"taskId" jsonschema:"taskId of the dead-lettered task (from list_dead_letters)"`
	Table  string `json:"table,omitempty" jsonschema:"RoundTable of the entry; needed only when the task is dead-lettered in several"`
}

// ListDeadLettersOutput is the output of the list_dead_letters tool.
type ListDeadLettersOutput struct {
	DeadLetters []*DeadLetterView `json:"deadLetters"`
	// Total is the number of entries matching the filters, before the limit.
	Total int `json:"total"`
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

// ListChainsInput is the input of the list_chains tool.
type ListChainsInput struct {
	Namespace string `json:"namespace,omitempty" jsonschema:"namespace to list; omit for the default"`
}

// ListChainsOutput is the output of the list_chains tool.
type ListChainsOutput struct {
	Chains []ChainInfo `json:"chains"`
}

// RunChainInput is the input of the run_chain tool.
type RunChainInput struct {
	Chain string `json:"chain" jsonschema:"name of the chain to run (see list_chains)"`
	// Input is any JSON value: a string is passed as is, anything else as
	// its JSON text.
	Input       any    `json:"input,omitempty" jsonschema:"input for this run (a string or any JSON value); omit to use the chain's own input"`
	WaitSeconds *int   `json:"waitSeconds,omitempty" jsonschema:"seconds to wait for the run to finish before returning (default 0 = return at once)"`
	Namespace   string `json:"namespace,omitempty" jsonschema:"namespace of the chain; omit for the default"`
}

// GetRunInput is the input of the get_run tool.
type GetRunInput struct {
	Run         string `json:"run" jsonschema:"name of the run (returned by run_chain)"`
	Namespace   string `json:"namespace,omitempty" jsonschema:"namespace of the run; omit for the default"`
	WaitSeconds *int   `json:"waitSeconds,omitempty" jsonschema:"seconds to wait for the run to finish before returning (default 0)"`
}

// CancelRunInput is the input of the cancel_run tool.
type CancelRunInput struct {
	Run       string `json:"run" jsonschema:"name of the run to cancel (returned by run_chain)"`
	Namespace string `json:"namespace,omitempty" jsonschema:"namespace of the run; omit for the default"`
	Reason    string `json:"reason,omitempty" jsonschema:"why the run is cancelled; recorded on its unfinished steps"`
}

// RerunRunInput is the input of the rerun_run tool.
type RerunRunInput struct {
	Run         string `json:"run" jsonschema:"name of the finished run to re-run"`
	Namespace   string `json:"namespace,omitempty" jsonschema:"namespace of the run; omit for the default"`
	Input       any    `json:"input,omitempty" jsonschema:"new input (a string or any JSON value); omit to reuse the run's input"`
	WaitSeconds *int   `json:"waitSeconds,omitempty" jsonschema:"seconds to wait for the new run to finish before returning (default 0)"`
}

// ListRunsOutput is the output of the list_runs tool.
type ListRunsOutput struct {
	Runs []*RunRecord `json:"runs"`
}

// NewMCPServer builds the MCP server exposing the gateway's operations.
func NewMCPServer(svc *Service) *mcp.Server {
	s := mcp.NewServer(&mcp.Implementation{Name: "roundtable-gateway", Title: "Round Table knights", Version: serverVersion}, nil)
	readOnly := &mcp.ToolAnnotations{ReadOnlyHint: true}

	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_knights",
		Description: "List the Round Table knights (agents) that can be invoked, with their domain, model and readiness. Set load to include queued and in-flight task counts per knight.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in ListKnightsInput) (*mcp.CallToolResult, ListKnightsOutput, error) {
		knights, err := svc.ListKnights(ctx, in.Namespace, in.Load)
		return nil, ListKnightsOutput{Knights: knights}, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name: "invoke_knight",
		Description: "Send a task to a knight and wait for its answer. If the answer isn't ready within the timeout, " +
			"the task is returned with status \"pending\": poll get_task with its taskId. " +
			"Pass the returned runId on a later call to continue the same conversation.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, in InvokeKnightInput) (*mcp.CallToolResult, *TaskRecord, error) {
		rec, err := svc.Invoke(withMCPProgress(ctx, req), in.Knight, in.InvokeRequest)
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
		out, total, err := svc.ListDeadLetters(ctx, in)
		return nil, ListDeadLettersOutput{DeadLetters: out, Total: total}, err
	})

	mcp.AddTool(s, &mcp.Tool{
		Name: "get_dead_letter",
		Description: "Get one dead-lettered task with its state, failure reason and the original task payload (what was asked). " +
			"Use it to inspect a dead letter from list_dead_letters before retrying it.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in GetDeadLetterInput) (*mcp.CallToolResult, *DeadLetterView, error) {
		v, err := svc.GetDeadLetter(ctx, in.TaskID, in.Table)
		if err != nil {
			return nil, nil, err
		}
		return nil, v, nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name: "retry_task",
		Description: "Retry an open dead-lettered task: republish it to the same knight as a new task with attempt number + 1, " +
			"so the knight checks for partial work. Inspect first with get_dead_letter. Poll the returned taskId with get_task. " +
			"A task already retried or resolved, or one owned by a chain (unless force is set), is refused.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in RetryRequest) (*mcp.CallToolResult, *RetryResult, error) {
		res, err := svc.RetryDeadLetter(ctx, in)
		if err != nil {
			return nil, nil, err
		}
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{
			Text: fmt.Sprintf("Retrying %s as %s (attempt %d) on knight %s. Call get_task with taskId %s.",
				res.RetryOf, res.TaskID, res.Attempt, res.Knight, res.TaskID)}}}, res, nil
	})
	addSnapshotTools(s, svc)
	if svc.MissionsEnabled() {
		addMissionTools(s, svc)
	}
	if svc.RunsEnabled() {
		addRunTools(s, svc)
	}
	if svc.SchedulesEnabled() {
		addScheduleTools(s, svc)
	}
	if svc.ScheduleRunsEnabled() {
		addScheduleRunTools(s, svc)
	}
	return s
}

// ScheduleRefInput names one schedule.
type ScheduleRefInput struct {
	Namespace string `json:"namespace,omitempty" jsonschema:"namespace of the schedule; omit for the default"`
	Name      string `json:"name" jsonschema:"name of the schedule"`
}

// ListSchedulesOutput is the output of the list_schedules tool.
type ListSchedulesOutput struct {
	Schedules []*ScheduleView `json:"schedules"`
}

// ListScheduleRunsInput is the input of the list_schedule_runs tool.
type ListScheduleRunsInput struct {
	ScheduleRefInput
	Limit int `json:"limit,omitempty" jsonschema:"maximum number of runs to return"`
}

// ListScheduleRunsOutput is the output of the list_schedule_runs tool.
type ListScheduleRunsOutput struct {
	Runs []*ScheduleRun `json:"runs"`
}

// GetScheduleRunInput is the input of the get_schedule_run tool.
type GetScheduleRunInput struct {
	ScheduleRefInput
	Run string `json:"run" jsonschema:"run name from list_schedule_runs"`
}

func addScheduleTools(s *mcp.Server, svc *Service) {
	readOnly := &mcp.ToolAnnotations{ReadOnlyHint: true}
	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_schedules",
		Description: "List the schedules (cron Triggers) of a table with their target, next fire time, suspension and how they are managed (git or api).",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in ScheduleFilter) (*mcp.CallToolResult, ListSchedulesOutput, error) {
		out, err := svc.ListSchedules(ctx, in)
		return nil, ListSchedulesOutput{Schedules: out}, err
	})
	mcp.AddTool(s, &mcp.Tool{
		Name:        "get_schedule",
		Description: "Get one schedule: cron, target, suspension, recent firings and whether it is Git-managed.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in ScheduleRefInput) (*mcp.CallToolResult, *ScheduleView, error) {
		v, err := svc.GetSchedule(ctx, in.Namespace, in.Name)
		return nil, v, err
	})
	mcp.AddTool(s, &mcp.Tool{
		Name: "schedule_next_runs",
		Description: "List the next fire times of a schedule ({namespace, name}) or of a cron expression that does not exist yet " +
			"({cron, timeZone}), in UTC and in the time zone. Cron ticks only: skip rules are not applied. Count defaults to 5, max 50.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in NextRunsInput) (*mcp.CallToolResult, *ScheduleNextRuns, error) {
		count := in.Count
		if count == 0 {
			count = defaultNextCount
		}
		switch {
		case in.Name != "" && in.Cron != "":
			return nil, nil, fmt.Errorf("%w: give either name or cron, not both", ErrInvalid)
		case in.Name != "":
			out, err := svc.ScheduleNext(ctx, in.Namespace, in.Name, count)
			return nil, out, err
		case in.Cron != "":
			out, err := svc.SchedulePreview(in.Cron, in.TimeZone, count)
			return nil, out, err
		}
		return nil, nil, fmt.Errorf("%w: give name (and namespace) or cron (and timeZone)", ErrInvalid)
	})
	mcp.AddTool(s, &mcp.Tool{
		Name: "suspend_schedule",
		Description: "Suspend a schedule so it stops firing. Allowed on Git-managed schedules too: it only sets an annotation, " +
			"not the spec. Undo with resume_schedule.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in ScheduleRefInput) (*mcp.CallToolResult, *ScheduleState, error) {
		st, err := svc.SuspendSchedule(ctx, in.Namespace, in.Name)
		return nil, st, err
	})
	mcp.AddTool(s, &mcp.Tool{
		Name: "resume_schedule",
		Description: "Resume a schedule suspended with suspend_schedule. A schedule suspended in its spec (Git) stays suspended; " +
			"the result says so.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in ScheduleRefInput) (*mcp.CallToolResult, *ScheduleState, error) {
		st, err := svc.ResumeSchedule(ctx, in.Namespace, in.Name)
		return nil, st, err
	})
	mcp.AddTool(s, &mcp.Tool{
		Name: "suspend_table_schedules",
		Description: "Suspend every schedule of a table in one call (maintenance, incident). Annotation only: Trigger manifests in Git " +
			"are untouched. Returns changed, unchanged and failed schedules. Undo with resume_table_schedules.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in TableSchedulesRequest) (*mcp.CallToolResult, *TableSchedulesResult, error) {
		res, err := svc.SuspendSchedules(ctx, in)
		return nil, res, err
	})
	mcp.AddTool(s, &mcp.Tool{
		Name: "resume_table_schedules",
		Description: "Resume every schedule of a table suspended with suspend_table_schedules. Schedules suspended in their spec (Git) " +
			"stay suspended; their state says so.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in TableSchedulesRequest) (*mcp.CallToolResult, *TableSchedulesResult, error) {
		res, err := svc.ResumeSchedules(ctx, in)
		return nil, res, err
	})
	mcp.AddTool(s, &mcp.Tool{
		Name: "run_schedule",
		Description: "Fire a schedule once now, even while suspended and also for Git-managed schedules. Waits briefly for the operator " +
			"to start the run; state is started, pending or skipped. Fetch the result with get_schedule_run.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in ScheduleRefInput) (*mcp.CallToolResult, *ScheduleRunNow, error) {
		run, err := svc.RunSchedule(ctx, in.Namespace, in.Name)
		if err != nil {
			return nil, nil, err
		}
		return scheduleRunNowResult(run), run, nil
	})
}

func addScheduleRunTools(s *mcp.Server, svc *Service) {
	readOnly := &mcp.ToolAnnotations{ReadOnlyHint: true}
	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_schedule_runs",
		Description: "List a schedule's runs (firings), newest first, with outcome, duration and cost.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in ListScheduleRunsInput) (*mcp.CallToolResult, ListScheduleRunsOutput, error) {
		runs, err := svc.ListScheduleRuns(ctx, ScheduleRunsFilter{Namespace: in.Namespace, Name: in.Name, Limit: in.Limit})
		return nil, ListScheduleRunsOutput{Runs: runs}, err
	})
	mcp.AddTool(s, &mcp.Tool{
		Name:        "get_schedule_run",
		Description: "Get the result of one run of a schedule: the output of its chain, knight task or mission. A failed run is a tool error.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in GetScheduleRunInput) (*mcp.CallToolResult, any, error) {
		res, err := svc.ScheduleRunResult(ctx, in.Namespace, in.Name, in.Run)
		if err != nil {
			return nil, nil, err
		}
		// The result's shape depends on the target (the embedded run record is
		// empty for knight and mission runs), so no output schema is derived
		// from the type; the structured content keeps the REST shape.
		out := scheduleRunResult(res)
		out.StructuredContent = res
		return out, nil, nil
	})
}

// scheduleRunNowResult describes a run-now outcome; a skipped firing is a
// tool error (nothing started, as the REST route answers 409).
func scheduleRunNowResult(r *ScheduleRunNow) *mcp.CallToolResult {
	id := r.Namespace + "/" + r.Schedule
	switch r.State {
	case ScheduleRunSkipped:
		return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{
			Text: "Schedule " + id + " run was skipped: " + r.SkipReason}}}
	case ScheduleRunPending:
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{
			Text: "Schedule " + id + " run requested; the operator has not started it yet. Check list_schedule_runs shortly."}}}
	}
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{
		Text: "Schedule " + id + " started " + r.Target + " run " + r.Run + ". Fetch it with get_schedule_run."}}}
}

// scheduleRunResult puts a run's output first in the text content and marks
// failed runs as tool errors.
func scheduleRunResult(r *ScheduleRunResult) *mcp.CallToolResult {
	switch {
	case r.Task != nil:
		switch r.Task.Status {
		case string(TaskSucceeded):
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: r.Task.Output}}}
		case string(TaskFailed):
			return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{
				Text: "knight " + r.Task.Knight + " failed task " + r.Task.Run + ": " + r.Task.Error}}}
		}
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{
			Text: "Task " + r.Task.Run + " of schedule " + r.Schedule + " is still running."}}}
	case r.Mission != nil:
		return missionResult(r.Mission)
	case r.RunRecord != nil:
		return runResult(r.RunRecord)
	}
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "Run of schedule " + r.Schedule + " has no result."}}}
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

func addRunTools(s *mcp.Server, svc *Service) {
	readOnly := &mcp.ToolAnnotations{ReadOnlyHint: true}
	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_chains",
		Description: "List the chains (multi-knight pipelines) that can be run with run_chain, with their description.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in ListChainsInput) (*mcp.CallToolResult, ListChainsOutput, error) {
		chains, err := svc.ListChains(ctx, in.Namespace)
		return nil, ListChainsOutput{Chains: chains}, err
	})
	mcp.AddTool(s, &mcp.Tool{
		Name: "run_chain",
		Description: "Run a chain with your input and wait for its final output. If it isn't finished within " +
			"waitSeconds, the run is returned unfinished: call get_run with its name (waitSeconds lets get_run wait too).",
	}, func(ctx context.Context, mreq *mcp.CallToolRequest, in RunChainInput) (*mcp.CallToolResult, *RunRecord, error) {
		ctx = withMCPProgress(ctx, mreq)
		req := RunChainRequest{Namespace: in.Namespace, WaitSeconds: in.WaitSeconds}
		if in.Input != nil {
			raw, err := json.Marshal(in.Input)
			if err != nil {
				return nil, nil, fmt.Errorf("%w: input: %v", ErrInvalid, err)
			}
			req.Input = raw
		}
		rec, err := svc.RunChain(ctx, in.Chain, req)
		if err != nil {
			return nil, nil, err
		}
		return runResult(rec), rec, nil
	})
	mcp.AddTool(s, &mcp.Tool{
		Name:        "get_run",
		Description: "Get a chain run started by run_chain: its phase, steps and, once finished, the final output.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in GetRunInput) (*mcp.CallToolResult, *RunRecord, error) {
		rec, err := svc.GetRun(ctx, in.Namespace, in.Run, in.WaitSeconds)
		if err != nil {
			return nil, nil, err
		}
		return runResult(rec), rec, nil
	})
	mcp.AddTool(s, &mcp.Tool{
		Name: "cancel_run",
		Description: "Cancel an unfinished chain run started by run_chain: its unfinished steps are marked failed with the reason and no new steps start. " +
			"Tasks a knight already picked up are not recalled. A finished run is an error.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in CancelRunInput) (*mcp.CallToolResult, *RunRecord, error) {
		rec, err := svc.CancelRun(ctx, in.Namespace, in.Run, in.Reason)
		if err != nil {
			return nil, nil, err
		}
		return cancelResult(rec), rec, nil
	})
	mcp.AddTool(s, &mcp.Tool{
		Name: "rerun_run",
		Description: "Re-run a finished chain run started by run_chain, with the same input unless you give a new one. " +
			"The whole chain runs again (failed steps are not resumed). A run that is not finished is an error.",
	}, func(ctx context.Context, mreq *mcp.CallToolRequest, in RerunRunInput) (*mcp.CallToolResult, *RunRecord, error) {
		ctx = withMCPProgress(ctx, mreq)
		req := RerunRunRequest{WaitSeconds: in.WaitSeconds}
		if in.Input != nil {
			raw, err := json.Marshal(in.Input)
			if err != nil {
				return nil, nil, fmt.Errorf("%w: input: %v", ErrInvalid, err)
			}
			req.Input = raw
		}
		rec, err := svc.RerunRun(ctx, in.Namespace, in.Run, req)
		if err != nil {
			return nil, nil, err
		}
		return runResult(rec), rec, nil
	})
	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_runs",
		Description: "List chain runs, newest first: those started through this gateway by default, or set source (trigger, pickup, all), active and issue to see what the pipelines are doing. Records carry source, ageSeconds and, for pickup runs, the issue.",
		Annotations: readOnly,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in RunFilter) (*mcp.CallToolResult, ListRunsOutput, error) {
		runs, err := svc.ListRuns(ctx, in)
		return nil, ListRunsOutput{Runs: runs}, err
	})
}

// runResult puts a finished run's output first in the text content and marks
// failed runs as tool errors.
func runResult(rec *RunRecord) *mcp.CallToolResult {
	id := rec.Namespace + "/" + rec.Run
	if !rec.Done {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{
			Text: "Run " + id + " of chain " + rec.Chain + " is " + rec.Phase + ". Call get_run with run " + rec.Run + " later."}}}
	}
	if rec.Phase == string(aiv1alpha1.ChainPhaseFailed) {
		var errs []string
		for _, st := range rec.Steps {
			if st.Error != "" {
				errs = append(errs, st.Name+": "+st.Error)
			}
		}
		text := "Run " + id + " of chain " + rec.Chain + " failed"
		if len(errs) > 0 {
			text += ": " + strings.Join(errs, "; ")
		}
		return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: text}}}
	}
	text := rec.Output
	if text == "" {
		text = "Run " + id + " of chain " + rec.Chain + " finished: " + rec.Phase + ". See steps for each step's output."
	}
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: text}}}
}

// cancelResult reports a cancel_run outcome. A cancelled run ends Failed, so
// unlike runResult it is a normal result, not a tool error; a run that wasn't
// ended yet, or that finished some other way first, is described as usual.
func cancelResult(rec *RunRecord) *mcp.CallToolResult {
	if !rec.Done || rec.Phase != string(aiv1alpha1.ChainPhaseFailed) {
		return runResult(rec)
	}
	for _, st := range rec.Steps {
		if r, ok := strings.CutPrefix(st.Error, "cancelled:"); ok {
			text := "Run " + rec.Namespace + "/" + rec.Run + " of chain " + rec.Chain + " was cancelled"
			if r = strings.TrimSpace(r); r != "" {
				text += ": " + r
			} else {
				text += "."
			}
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: text}}}
		}
	}
	// Failed for another reason while a cancel was in flight.
	return runResult(rec)
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
		prefix := ""
		if rec.UsageLimited {
			prefix = "usage limited: "
		}
		return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{
			Text: prefix + "knight " + rec.Knight + " failed task " + rec.TaskID + ": " + rec.Error}}}
	default:
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{
			Text: "Task " + rec.TaskID + " is still running on knight " + rec.Knight +
				". Call get_task with this taskId later. runId: " + rec.RunID}}}
	}
}

// mcpProgressInterval is the minimum gap between (unforced) progress
// notifications of one call.
const mcpProgressInterval = 500 * time.Millisecond

// withMCPProgress makes ctx report progress to the caller as MCP progress
// notifications when the request carries a progress token.
func withMCPProgress(ctx context.Context, req *mcp.CallToolRequest) context.Context {
	if req == nil || req.Session == nil || req.Params == nil {
		return ctx
	}
	token := req.Params.GetProgressToken()
	if token == nil {
		return ctx
	}
	var (
		mu    sync.Mutex
		count float64
		last  time.Time
	)
	return withProgress(ctx, func(message string, force bool) {
		mu.Lock()
		defer mu.Unlock()
		if !force && time.Since(last) < mcpProgressInterval {
			return
		}
		last = time.Now()
		count++
		_ = req.Session.NotifyProgress(ctx, &mcp.ProgressNotificationParams{
			ProgressToken: token, Progress: count, Message: message})
	})
}

// MCPHandler serves the MCP streamable HTTP transport. It is stateless (no
// Mcp-Session-Id), so any gateway replica can serve any request.
func MCPHandler(svc *Service) http.Handler {
	server := NewMCPServer(svc)
	return mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server },
		&mcp.StreamableHTTPOptions{Stateless: true})
}
