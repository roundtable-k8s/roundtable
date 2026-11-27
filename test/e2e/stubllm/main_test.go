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

package main

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(newHandler())
	t.Cleanup(srv.Close)
	return srv
}

func postChat(t *testing.T, srv *httptest.Server, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/chat/completions: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(b)
}

func TestChatCompletionScriptedReply(t *testing.T) {
	srv := newTestServer(t)
	tests := []struct {
		name     string
		messages string
		want     string
	}{
		{
			name:     "tag selects reply",
			messages: `[{"role":"user","content":"please STUB:pong now"}]`,
			want:     "pong",
		},
		{
			name:     "multi-word reply",
			messages: `[{"role":"user","content":"STUB:greeting"}]`,
			want:     "Hello from the stub LLM.",
		},
		{
			name:     "no tag gets default reply",
			messages: `[{"role":"user","content":"hello there"}]`,
			want:     defaultReply,
		},
		{
			name: "tag in an earlier user message is ignored",
			messages: `[{"role":"user","content":"STUB:pong"},` +
				`{"role":"assistant","content":"pong"},{"role":"user","content":"again?"}]`,
			want: defaultReply,
		},
		{
			name:     "array content parts are read",
			messages: `[{"role":"user","content":[{"type":"text","text":"STUB:summary"}]}]`,
			want:     replies["summary"],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := postChat(t, srv, `{"model":"stubprov/e2e","messages":`+tt.messages+`}`)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}
			var got completionResponse
			if err := json.Unmarshal([]byte(readBody(t, resp)), &got); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if got.Object != "chat.completion" {
				t.Errorf("object = %q, want chat.completion", got.Object)
			}
			if got.Model != "stubprov/e2e" {
				t.Errorf("model = %q, want the request model echoed", got.Model)
			}
			if len(got.Choices) != 1 {
				t.Fatalf("choices = %d, want 1", len(got.Choices))
			}
			c := got.Choices[0]
			if c.Message.Role != "assistant" || c.Message.Content != tt.want {
				t.Errorf("message = %+v, want assistant %q", c.Message, tt.want)
			}
			if c.FinishReason != finishStop {
				t.Errorf("finish_reason = %q, want stop", c.FinishReason)
			}
			if got.Usage.PromptTokens < 1 || got.Usage.CompletionTokens < 1 {
				t.Errorf("usage = %+v, want positive token counts", got.Usage)
			}
			if got.Usage.TotalTokens != got.Usage.PromptTokens+got.Usage.CompletionTokens {
				t.Errorf("usage total = %d, want prompt+completion", got.Usage.TotalTokens)
			}
		})
	}
}

func TestChatCompletionUnknownTagIsBadRequest(t *testing.T) {
	srv := newTestServer(t)
	resp := postChat(t, srv, `{"model":"m","messages":[{"role":"user","content":"STUB:nope"}]}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	if body := readBody(t, resp); !strings.Contains(body, `unknown STUB tag \"nope\"`) {
		t.Errorf("body = %s, want the unknown tag named", body)
	}
}

func TestChatCompletionInvalidJSONIsBadRequest(t *testing.T) {
	srv := newTestServer(t)
	resp := postChat(t, srv, `{not json`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestChatCompletionRejectsGET(t *testing.T) {
	srv := newTestServer(t)
	resp, err := http.Get(srv.URL + "/v1/chat/completions")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", resp.StatusCode)
	}
}

// sseData returns the payload of each `data:` line in order.
func sseData(t *testing.T, body string) []string {
	t.Helper()
	var out []string
	sc := bufio.NewScanner(strings.NewReader(body))
	for sc.Scan() {
		if line, ok := strings.CutPrefix(sc.Text(), "data: "); ok {
			out = append(out, line)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan SSE: %v", err)
	}
	return out
}

func TestChatCompletionStreamingWithUsage(t *testing.T) {
	srv := newTestServer(t)
	resp := postChat(t, srv, `{"model":"m","stream":true,"stream_options":{"include_usage":true},`+
		`"messages":[{"role":"user","content":"STUB:greeting"}]}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}
	events := sseData(t, readBody(t, resp))
	if len(events) < 4 {
		t.Fatalf("got %d events, want at least role, content, finish, usage, [DONE]", len(events))
	}
	if events[len(events)-1] != "[DONE]" {
		t.Fatalf("last event = %q, want [DONE]", events[len(events)-1])
	}

	var text strings.Builder
	finishes := 0
	var role string
	var usageChunk *chunkResponse
	for i, ev := range events[:len(events)-1] {
		var c chunkResponse
		if err := json.Unmarshal([]byte(ev), &c); err != nil {
			t.Fatalf("event %d: decode: %v", i, err)
		}
		if c.Object != "chat.completion.chunk" {
			t.Errorf("event %d: object = %q", i, c.Object)
		}
		if c.Usage != nil {
			usageChunk = &c
			if len(c.Choices) != 0 {
				t.Errorf("usage chunk has %d choices, want 0", len(c.Choices))
			}
			continue
		}
		if len(c.Choices) != 1 {
			t.Fatalf("event %d: %d choices, want 1", i, len(c.Choices))
		}
		ch := c.Choices[0]
		if ch.Delta.Role != "" && role == "" {
			role = ch.Delta.Role
		}
		text.WriteString(ch.Delta.Content)
		if ch.FinishReason != nil {
			finishes++
			if *ch.FinishReason != finishStop {
				t.Errorf("finish_reason = %q, want stop", *ch.FinishReason)
			}
		}
	}
	if role != "assistant" {
		t.Errorf("role = %q, want assistant", role)
	}
	if want := "Hello from the stub LLM."; text.String() != want {
		t.Errorf("streamed text = %q, want %q", text.String(), want)
	}
	if finishes != 1 {
		t.Errorf("finish chunks = %d, want 1", finishes)
	}
	if usageChunk == nil {
		t.Fatal("no usage chunk despite include_usage")
	}
	if usageChunk.Usage.TotalTokens != usageChunk.Usage.PromptTokens+usageChunk.Usage.CompletionTokens {
		t.Errorf("usage = %+v, want total = prompt+completion", usageChunk.Usage)
	}
}

func TestChatCompletionStreamingWithoutUsage(t *testing.T) {
	srv := newTestServer(t)
	resp := postChat(t, srv, `{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	events := sseData(t, readBody(t, resp))
	if events[len(events)-1] != "[DONE]" {
		t.Fatalf("last event = %q, want [DONE]", events[len(events)-1])
	}
	var text strings.Builder
	for _, ev := range events[:len(events)-1] {
		var c chunkResponse
		if err := json.Unmarshal([]byte(ev), &c); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if c.Usage != nil {
			t.Errorf("usage chunk sent without include_usage: %s", ev)
		}
		if len(c.Choices) != 1 {
			t.Fatalf("%d choices in %s, want 1", len(c.Choices), ev)
		}
		text.WriteString(c.Choices[0].Delta.Content)
	}
	if text.String() != defaultReply {
		t.Errorf("streamed text = %q, want %q", text.String(), defaultReply)
	}
}

func postMessages(t *testing.T, srv *httptest.Server, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(srv.URL+"/v1/messages", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/messages: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestMessagesScriptedReply(t *testing.T) {
	srv := newTestServer(t)
	tests := []struct {
		name     string
		messages string
		want     string
	}{
		{
			name:     "string content",
			messages: `[{"role":"user","content":"STUB:pong"}]`,
			want:     "pong",
		},
		{
			name:     "block content",
			messages: `[{"role":"user","content":[{"type":"text","text":"STUB:summary"}]}]`,
			want:     replies["summary"],
		},
		{
			name:     "no tag gets default reply",
			messages: `[{"role":"user","content":"hello there"}]`,
			want:     defaultReply,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := postMessages(t, srv,
				`{"model":"claude-stub","max_tokens":64,"system":"be brief","messages":`+tt.messages+`}`)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}
			var got anthropicMessage
			if err := json.Unmarshal([]byte(readBody(t, resp)), &got); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if !strings.HasPrefix(got.ID, "msg_stub_") {
				t.Errorf("id = %q, want msg_stub_ prefix", got.ID)
			}
			if got.Type != "message" || got.Role != "assistant" {
				t.Errorf("type/role = %q/%q, want message/assistant", got.Type, got.Role)
			}
			if got.Model != "claude-stub" {
				t.Errorf("model = %q, want the request model echoed", got.Model)
			}
			if len(got.Content) != 1 || got.Content[0].Type != "text" || got.Content[0].Text != tt.want {
				t.Errorf("content = %+v, want one text block %q", got.Content, tt.want)
			}
			if got.StopReason == nil || *got.StopReason != "end_turn" {
				t.Errorf("stop_reason = %v, want end_turn", got.StopReason)
			}
			if got.StopSequence != nil {
				t.Errorf("stop_sequence = %v, want null", *got.StopSequence)
			}
			if got.Usage.InputTokens < 1 || got.Usage.OutputTokens != approxTokens(tt.want) {
				t.Errorf("usage = %+v, want input >= 1 and output = %d", got.Usage, approxTokens(tt.want))
			}
		})
	}
}

func TestMessagesUnknownTagIsBadRequest(t *testing.T) {
	srv := newTestServer(t)
	resp := postMessages(t, srv, `{"model":"m","max_tokens":8,"messages":[{"role":"user","content":"STUB:nope"}]}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	var got struct {
		Type  string `json:"type"`
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(readBody(t, resp)), &got); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if got.Type != "error" || got.Error.Type != "invalid_request_error" {
		t.Errorf("error envelope = %+v, want type error / invalid_request_error", got)
	}
	if !strings.Contains(got.Error.Message, `unknown STUB tag "nope"`) {
		t.Errorf("message = %q, want the unknown tag named", got.Error.Message)
	}
}

func TestMessagesInvalidJSONIsBadRequest(t *testing.T) {
	srv := newTestServer(t)
	resp := postMessages(t, srv, `{not json`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

type sseEvent struct {
	name string
	data string
}

// sseEvents returns each event as its `event:` name and `data:` payload, in order.
func sseEvents(t *testing.T, body string) []sseEvent {
	t.Helper()
	var out []sseEvent
	var name string
	sc := bufio.NewScanner(strings.NewReader(body))
	for sc.Scan() {
		line := sc.Text()
		if n, ok := strings.CutPrefix(line, "event: "); ok {
			name = n
		} else if d, ok := strings.CutPrefix(line, "data: "); ok {
			out = append(out, sseEvent{name: name, data: d})
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan SSE: %v", err)
	}
	return out
}

func TestMessagesStreamEventOrder(t *testing.T) {
	srv := newTestServer(t)
	resp := postMessages(t, srv, `{"model":"claude-stub","max_tokens":64,"stream":true,`+
		`"messages":[{"role":"user","content":[{"type":"text","text":"STUB:greeting"}]}]}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}
	events := sseEvents(t, readBody(t, resp))

	// Collect the names in order, folding the run of deltas into one entry.
	var names []string
	for _, ev := range events {
		if ev.name == "content_block_delta" && len(names) > 0 && names[len(names)-1] == ev.name {
			continue
		}
		names = append(names, ev.name)
	}
	wantNames := []string{"message_start", "content_block_start", "content_block_delta",
		"content_block_stop", "message_delta", "message_stop"}
	if strings.Join(names, ",") != strings.Join(wantNames, ",") {
		t.Fatalf("event order = %v, want %v", names, wantNames)
	}

	var text strings.Builder
	for _, ev := range events {
		switch ev.name {
		case "message_start":
			var m struct {
				Message anthropicMessage `json:"message"`
			}
			if err := json.Unmarshal([]byte(ev.data), &m); err != nil {
				t.Fatalf("message_start: %v", err)
			}
			if m.Message.Role != "assistant" || m.Message.Model != "claude-stub" || len(m.Message.Content) != 0 {
				t.Errorf("message_start message = %+v", m.Message)
			}
		case "content_block_start":
			if !strings.Contains(ev.data, `"type":"text"`) {
				t.Errorf("content_block_start = %s, want a text block", ev.data)
			}
		case "content_block_delta":
			var d struct {
				Delta struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"delta"`
			}
			if err := json.Unmarshal([]byte(ev.data), &d); err != nil {
				t.Fatalf("content_block_delta: %v", err)
			}
			if d.Delta.Type != "text_delta" {
				t.Errorf("delta type = %q, want text_delta", d.Delta.Type)
			}
			text.WriteString(d.Delta.Text)
		case "message_delta":
			var d struct {
				Delta struct {
					StopReason *string `json:"stop_reason"`
				} `json:"delta"`
				Usage anthropicUsage `json:"usage"`
			}
			if err := json.Unmarshal([]byte(ev.data), &d); err != nil {
				t.Fatalf("message_delta: %v", err)
			}
			if d.Delta.StopReason == nil || *d.Delta.StopReason != "end_turn" {
				t.Errorf("stop_reason = %v, want end_turn", d.Delta.StopReason)
			}
			if want := approxTokens(replies["greeting"]); d.Usage.OutputTokens != want {
				t.Errorf("output_tokens = %d, want %d", d.Usage.OutputTokens, want)
			}
		}
	}
	if want := replies["greeting"]; text.String() != want {
		t.Errorf("streamed text = %q, want %q", text.String(), want)
	}
}

func TestListModels(t *testing.T) {
	srv := newTestServer(t)
	resp, err := http.Get(srv.URL + "/v1/models")
	if err != nil {
		t.Fatalf("GET /v1/models: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var got struct {
		Object string `json:"object"`
		Data   []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Object != "list" || len(got.Data) != 1 || got.Data[0].ID != modelID {
		t.Errorf("models = %+v, want one model %q", got, modelID)
	}
}
