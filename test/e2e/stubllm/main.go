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

// Command stubllm is an OpenAI-compatible chat completions server, plus an
// Anthropic Messages endpoint, for the e2e tests. It is stdlib only and
// key-free, so a knight pointed at it with OPENAI_BASE_URL or ANTHROPIC_BASE_URL
// gets deterministic replies: the reply is chosen by a `STUB:<name>` tag in the
// last user message, and a prompt without a tag gets the fixed default reply.
// See README.md in this directory.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

// modelID is the only model listed by GET /v1/models. Chat requests accept any
// model string and echo it back.
const modelID = "stubllm"

// defaultReply answers a prompt that carries no STUB tag.
const defaultReply = "done"

const finishStop = "stop"

// stopEndTurn is the Anthropic stop_reason for a complete text reply.
const stopEndTurn = "end_turn"

// replies is the scripted table a `STUB:<name>` tag selects from.
var replies = map[string]string{
	"pong":     "pong",
	"greeting": "Hello from the stub LLM.",
	"summary":  "Summary: this is a canned answer from the stub LLM.",
}

var stubTag = regexp.MustCompile(`STUB:([A-Za-z0-9_-]+)`)

// completionSeq makes response ids unique and deterministic per process.
var completionSeq atomic.Int64

type chatRequest struct {
	Model         string         `json:"model"`
	Messages      []chatMessage  `json:"messages"`
	Stream        bool           `json:"stream"`
	StreamOptions *streamOptions `json:"stream_options,omitempty"`
}

type streamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type chatMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

type usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type completionResponse struct {
	ID      string             `json:"id"`
	Object  string             `json:"object"`
	Created int64              `json:"created"`
	Model   string             `json:"model"`
	Choices []completionChoice `json:"choices"`
	Usage   usage              `json:"usage"`
}

type completionChoice struct {
	Index        int        `json:"index"`
	Message      chunkDelta `json:"message"`
	FinishReason string     `json:"finish_reason"`
}

type chunkResponse struct {
	ID      string        `json:"id"`
	Object  string        `json:"object"`
	Created int64         `json:"created"`
	Model   string        `json:"model"`
	Choices []chunkChoice `json:"choices"`
	Usage   *usage        `json:"usage,omitempty"`
}

type chunkChoice struct {
	Index        int        `json:"index"`
	Delta        chunkDelta `json:"delta"`
	FinishReason *string    `json:"finish_reason"`
}

type chunkDelta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

// anthropicRequest is the subset of POST /v1/messages the stub reads. System is
// accepted but never scripted: only the last user message picks the reply.
type anthropicRequest struct {
	Model     string          `json:"model"`
	System    json.RawMessage `json:"system,omitempty"`
	Messages  []chatMessage   `json:"messages"`
	MaxTokens int             `json:"max_tokens"` // accepted, deliberately unused: replies are scripted
	Stream    bool            `json:"stream"`
}

type anthropicBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type anthropicUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

type anthropicMessage struct {
	ID           string           `json:"id"`
	Type         string           `json:"type"`
	Role         string           `json:"role"`
	Model        string           `json:"model"`
	Content      []anthropicBlock `json:"content"`
	StopReason   *string          `json:"stop_reason"`
	StopSequence *string          `json:"stop_sequence"`
	Usage        anthropicUsage   `json:"usage"`
}

// newHandler returns the server's routes.
func newHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", handleChat)
	mux.HandleFunc("POST /v1/messages", handleMessages)
	mux.HandleFunc("GET /v1/models", handleModels)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

func handleChat(w http.ResponseWriter, r *http.Request) {
	var req chatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	reply, err := scriptedReply(lastUserText(req.Messages))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	u := usageFor(req.Messages, reply)
	id := fmt.Sprintf("chatcmpl-stub-%d", completionSeq.Add(1))
	created := time.Now().Unix()

	if req.Stream {
		streamReply(w, req, id, created, reply, u)
		return
	}
	writeJSON(w, http.StatusOK, completionResponse{
		ID:      id,
		Object:  "chat.completion",
		Created: created,
		Model:   req.Model,
		Choices: []completionChoice{{
			Index:        0,
			Message:      chunkDelta{Role: "assistant", Content: reply},
			FinishReason: finishStop,
		}},
		Usage: u,
	})
}

// handleMessages answers the Anthropic Messages API: a JSON message, or the
// SSE event sequence when the request sets stream.
func handleMessages(w http.ResponseWriter, r *http.Request) {
	var req anthropicRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAnthropicError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	reply, err := scriptedReply(lastUserText(req.Messages))
	if err != nil {
		writeAnthropicError(w, http.StatusBadRequest, err.Error())
		return
	}
	u := usageFor(req.Messages, reply)
	msg := anthropicMessage{
		ID:    fmt.Sprintf("msg_stub_%d", completionSeq.Add(1)),
		Type:  "message",
		Role:  "assistant",
		Model: req.Model,
		Usage: anthropicUsage{InputTokens: u.PromptTokens, OutputTokens: u.CompletionTokens},
	}
	if req.Stream {
		streamAnthropic(w, msg, reply)
		return
	}
	stop := stopEndTurn
	msg.Content = []anthropicBlock{{Type: "text", Text: reply}}
	msg.StopReason = &stop
	writeJSON(w, http.StatusOK, msg)
}

// streamAnthropic writes the reply as Anthropic SSE: message_start,
// content_block_start, one content_block_delta per word, content_block_stop,
// message_delta carrying the stop reason and output usage, then message_stop.
// Each event is written as an `event:` line followed by a `data:` line.
func streamAnthropic(w http.ResponseWriter, msg anthropicMessage, reply string) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)

	emit := func(event string, v any) {
		b, err := json.Marshal(v)
		if err != nil {
			return
		}
		_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, b)
		if flusher != nil {
			flusher.Flush()
		}
	}

	start := msg
	start.Content = []anthropicBlock{}
	start.Usage.OutputTokens = 0
	emit("message_start", map[string]any{"type": "message_start", "message": start})
	emit("content_block_start", map[string]any{
		"type":          "content_block_start",
		"index":         0,
		"content_block": anthropicBlock{Type: "text", Text: ""},
	})
	for _, piece := range wordPieces(reply) {
		emit("content_block_delta", map[string]any{
			"type":  "content_block_delta",
			"index": 0,
			"delta": map[string]string{"type": "text_delta", "text": piece},
		})
	}
	emit("content_block_stop", map[string]any{"type": "content_block_stop", "index": 0})
	emit("message_delta", map[string]any{
		"type":  "message_delta",
		"delta": map[string]any{"stop_reason": stopEndTurn, "stop_sequence": nil},
		"usage": map[string]int{"output_tokens": msg.Usage.OutputTokens},
	})
	emit("message_stop", map[string]string{"type": "message_stop"})
}

func handleModels(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"object": "list",
		"data": []map[string]any{{
			"id":       modelID,
			"object":   "model",
			"created":  0,
			"owned_by": "stubllm",
		}},
	})
}

// streamReply writes the reply as OpenAI-style SSE: a role chunk, one chunk per
// word, a finish chunk, an optional usage chunk when the client asked for it,
// and the [DONE] sentinel.
func streamReply(w http.ResponseWriter, req chatRequest, id string, created int64, reply string, u usage) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)

	emit := func(c chunkResponse) {
		b, err := json.Marshal(c)
		if err != nil {
			return
		}
		_, _ = fmt.Fprintf(w, "data: %s\n\n", b)
		if flusher != nil {
			flusher.Flush()
		}
	}
	base := func(choices []chunkChoice) chunkResponse {
		return chunkResponse{ID: id, Object: "chat.completion.chunk", Created: created, Model: req.Model, Choices: choices}
	}

	emit(base([]chunkChoice{{Index: 0, Delta: chunkDelta{Role: "assistant"}}}))
	for _, piece := range wordPieces(reply) {
		emit(base([]chunkChoice{{Index: 0, Delta: chunkDelta{Content: piece}}}))
	}
	stop := finishStop
	emit(base([]chunkChoice{{Index: 0, Delta: chunkDelta{}, FinishReason: &stop}}))
	if req.StreamOptions != nil && req.StreamOptions.IncludeUsage {
		final := base([]chunkChoice{})
		final.Usage = &u
		emit(final)
	}
	_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
	if flusher != nil {
		flusher.Flush()
	}
}

// scriptedReply returns the table entry named by the first STUB tag in prompt,
// the default reply when there is no tag, and an error for an unknown name so a
// typo in a test fails loudly instead of passing with the wrong reply.
func scriptedReply(prompt string) (string, error) {
	m := stubTag.FindStringSubmatch(prompt)
	if m == nil {
		return defaultReply, nil
	}
	reply, ok := replies[m[1]]
	if !ok {
		return "", fmt.Errorf("unknown STUB tag %q", m[1])
	}
	return reply, nil
}

// lastUserText returns the text of the most recent user message, or "" when
// there is none. Content may be a plain string or an array of text parts.
func lastUserText(msgs []chatMessage) string {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == "user" {
			return contentText(msgs[i].Content)
		}
	}
	return ""
}

func contentText(raw json.RawMessage) string {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &parts); err != nil {
		return ""
	}
	var b strings.Builder
	for _, p := range parts {
		if p.Type == "text" {
			b.WriteString(p.Text)
		}
	}
	return b.String()
}

// usageFor estimates token counts at roughly four characters per token. The
// numbers are deterministic, not a tokenizer's, which is all the e2e checks need.
func usageFor(msgs []chatMessage, reply string) usage {
	prompt := 0
	for _, m := range msgs {
		prompt += approxTokens(contentText(m.Content))
	}
	completion := approxTokens(reply)
	return usage{PromptTokens: prompt, CompletionTokens: completion, TotalTokens: prompt + completion}
}

func approxTokens(s string) int {
	return max(1, (len(s)+3)/4)
}

// wordPieces splits a reply into the chunks streamed one at a time, each word
// keeping its trailing space.
func wordPieces(reply string) []string {
	var out []string
	for _, piece := range strings.SplitAfter(reply, " ") {
		if piece != "" {
			out = append(out, piece)
		}
	}
	return out
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{"message": msg, "type": "invalid_request_error"},
	})
}

// writeAnthropicError uses the Anthropic error envelope.
func writeAnthropicError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{
		"type":  "error",
		"error": map[string]string{"type": "invalid_request_error", "message": msg},
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	srv := &http.Server{
		Addr:              *addr,
		Handler:           newHandler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("stubllm listening on %s", *addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
