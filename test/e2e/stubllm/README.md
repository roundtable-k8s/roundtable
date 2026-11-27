# stubllm: OpenAI- and Anthropic-compatible stub LLM for e2e

A stdlib-only HTTP server that answers `POST /v1/chat/completions` (streaming and
non-streaming), `POST /v1/messages` (Anthropic Messages API, streaming and
non-streaming) and `GET /v1/models`, with scripted replies so e2e runs are
deterministic and need no API key.

## Replies

The last `user` message is scanned for a `STUB:<name>` tag. The reply is the
entry named `<name>` in the table in `main.go`:

| Tag | Reply |
|-----|-------|
| `STUB:pong` | `pong` |
| `STUB:greeting` | `Hello from the stub LLM.` |
| `STUB:summary` | `Summary: this is a canned answer from the stub LLM.` |

A message with no tag gets `done`. An unknown name returns HTTP 400 so a typo
fails loudly instead of passing with the wrong reply. Only the last user message
counts; tags in earlier turns are ignored.

## Pointing a knight at it

pi-knight routes any model string that is not in its catalog to the base URL in
`OPENAI_BASE_URL` (or `OPENAI_API_BASE`), using the `openai-completions` API. Set
the base URL to the stub's `/v1` and use any `provider/model` string:

```yaml
env:
  - name: OPENAI_BASE_URL
    value: http://stubllm:8080/v1
model: stubllm/e2e
```

If `KNIGHT_MODEL_BASE_URL` is also set it takes precedence over `OPENAI_BASE_URL`
and `OPENAI_API_BASE` (`localBaseUrlFromEnv` in `packages/pi-knight/src/model.ts`
in roundtable-k8s/knights). No API key is needed: with no auth configured
pi-knight sets a dummy runtime key itself.

## Anthropic Messages API

`POST /v1/messages` takes the same tags. Both `content` as a string and as an
array of `{"type":"text","text":...}` blocks are read; `system` is accepted and
ignored. Without `stream` it returns a Messages object; with `"stream": true` it
returns SSE events in this order: `message_start`, `content_block_start`, one
`content_block_delta` (`text_delta`) per word, `content_block_stop`,
`message_delta` (with `stop_reason` and `usage.output_tokens`), `message_stop`.
An unknown tag returns HTTP 400 with an Anthropic error body
(`{"type":"error","error":{"type":"invalid_request_error","message":...}}`).

Point a client that honours `ANTHROPIC_BASE_URL` at the stub root, not at `/v1`:

```bash
curl -s localhost:8080/v1/messages -H 'Content-Type: application/json' \
  -d '{"model":"claude-stub","max_tokens":64,"messages":[{"role":"user","content":"STUB:pong"}]}'
# {"id":"msg_stub_1","type":"message","role":"assistant","model":"claude-stub",
#  "content":[{"type":"text","text":"pong"}],"stop_reason":"end_turn",
#  "stop_sequence":null,"usage":{"input_tokens":...,"output_tokens":1}}

curl -sN localhost:8080/v1/messages -H 'Content-Type: application/json' \
  -d '{"model":"claude-stub","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"STUB:pong"}]}'
```

## Build and run locally

```bash
make stubllm-image                     # builds stubllm:e2e
docker run --rm -p 8080:8080 stubllm:e2e
curl -s localhost:8080/v1/chat/completions -H 'Content-Type: application/json' \
  -d '{"model":"stubllm/e2e","messages":[{"role":"user","content":"STUB:pong"}]}'
```

## Tests

```bash
go test ./test/e2e/stubllm/...
```

`make test` runs it; only the Ginkgo suite package `./test/e2e` is excluded.
