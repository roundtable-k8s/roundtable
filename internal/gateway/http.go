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
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
)

// maxBodyBytes bounds request bodies (the task plus a little JSON).
const maxBodyBytes = maxTaskBytes + 16<<10

// minTokenLength rejects trivially guessable bearer tokens.
const minTokenLength = 16

// Handler returns the gateway's HTTP handler: the REST API under /v1, the MCP
// endpoint at /mcp (both behind bearer-token auth), an unauthenticated
// /healthz and, when hooks are enabled, POST /v1/hooks/{namespace}/{trigger},
// which accepts only each Trigger's own credential (never the API tokens).
func Handler(svc *Service, auth *TokenAuth) http.Handler {
	api := http.NewServeMux()
	mux := http.NewServeMux()
	for _, rt := range svc.routes() {
		if rt.public {
			mux.HandleFunc(rt.pattern, rt.handler)
		} else {
			api.HandleFunc(rt.pattern, rt.handler)
		}
	}
	if svc.SchedulesEnabled() {
		api.HandleFunc("GET /v1/schedules", svc.handleListSchedules)
		api.HandleFunc("GET /v1/schedules/{namespace}/{name}", svc.handleGetSchedule)
		api.HandleFunc("GET /v1/schedules/{namespace}/{name}/manifest", svc.handleScheduleManifest)
		api.HandleFunc("POST /v1/schedules/{namespace}/{name}/suspend", svc.handleSuspendSchedule)
		api.HandleFunc("POST /v1/schedules/{namespace}/{name}/resume", svc.handleResumeSchedule)
		api.HandleFunc("POST /v1/schedules/{namespace}/{name}/run", svc.handleRunSchedule)
	}
	api.Handle("/mcp", MCPHandler(svc))
	mux.Handle("/", auth.Wrap(api))
	return instrument(mux, svc.Log)
}

// route is one entry of the gateway's HTTP route table. The table is the
// single source for Handler and for the OpenAPI document (/openapi.json), so
// a route cannot be served without being described, or the reverse.
type route struct {
	// pattern is the http.ServeMux pattern, "METHOD /path".
	pattern string
	handler http.HandlerFunc
	// public routes skip the bearer-token auth (they do their own, or none).
	public bool
}

// routes lists the routes served for the Service's enabled features. /mcp is
// not part of it: MCP tools describe themselves.
func (s *Service) routes() []route {
	rs := []route{
		{pattern: "GET /healthz", public: true, handler: func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("ok"))
		}},
		{pattern: "GET /openapi.json", handler: s.handleOpenAPI},
		{pattern: "GET /v1/knights", handler: s.handleListKnights},
		{pattern: "POST /v1/knights/{name}/invoke", handler: s.handleInvoke},
		{pattern: "GET /v1/tasks", handler: s.handleListTasks},
		{pattern: "GET /v1/tasks/{taskId}", handler: s.handleGetTask},
		{pattern: "GET /v1/tasks/{taskId}/events", handler: s.handleTaskEvents},
		{pattern: "GET /v1/dead-letters", handler: s.handleListDeadLetters},
		{pattern: "GET /v1/dead-letters/{taskId}", handler: s.handleGetDeadLetter},
		{pattern: "POST /v1/dead-letters/{taskId}/retry", handler: s.handleRetryDeadLetter},
	}
	if s.MissionsEnabled() {
		rs = append(rs,
			route{pattern: "POST /v1/missions", handler: s.handleStartMission},
			route{pattern: "GET /v1/missions", handler: s.handleListMissions},
			route{pattern: "GET /v1/missions/{namespace}/{name}", handler: s.handleGetMission},
			route{pattern: "DELETE /v1/missions/{namespace}/{name}", handler: s.handleDeleteMission},
		)
	}
	if s.RunsEnabled() {
		rs = append(rs,
			route{pattern: "GET /v1/chains", handler: s.handleListChains},
			route{pattern: "POST /v1/chains/{name}/runs", handler: s.handleRunChain},
			route{pattern: "GET /v1/runs", handler: s.handleListRuns},
			route{pattern: "GET /v1/runs/{namespace}/{run}", handler: s.handleGetRun},
			route{pattern: "GET /v1/runs/{namespace}/{run}/events", handler: s.handleRunEvents},
			route{pattern: "POST /v1/runs/{namespace}/{run}/cancel", handler: s.handleCancelRun},
		)
	}
	if s.ScheduleRunsEnabled() {
		rs = append(rs,
			route{pattern: "GET /v1/schedules/{namespace}/{name}/runs", handler: s.handleScheduleRuns},
			route{pattern: "GET /v1/schedules/{namespace}/{name}/runs/{run}", handler: s.handleScheduleRunResult},
		)
	}
	if s.HooksEnabled() {
		rs = append(rs,
			route{pattern: "POST /v1/hooks/{namespace}/{trigger}", public: true, handler: s.handleHook},
			route{pattern: "POST /v1/triggers/{namespace}/{name}/fire", handler: s.handleFireTrigger},
		)
	}
	return rs
}

func (s *Service) handleListKnights(w http.ResponseWriter, r *http.Request) {
	knights, err := s.ListKnights(r.Context(), r.URL.Query().Get("namespace"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"knights": knights})
}

const (
	idempotencyHeader      = "Idempotency-Key"
	idempotentReplayHeader = "Idempotent-Replay"
)

func (s *Service) handleInvoke(w http.ResponseWriter, r *http.Request) {
	var req InvokeRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, fmt.Errorf("%w: body: %v", ErrInvalid, err))
		return
	}
	req.IdempotencyKey = r.Header.Get(idempotencyHeader)
	rec, err := s.Invoke(r.Context(), r.PathValue("name"), req)
	if err != nil {
		writeError(w, err)
		return
	}
	if rec.Replayed {
		w.Header().Set(idempotentReplayHeader, "true")
	}
	code := http.StatusOK
	if !rec.Done() {
		code = http.StatusAccepted
		w.Header().Set("Location", "/v1/tasks/"+rec.TaskID)
	}
	writeJSON(w, code, rec)
}

func (s *Service) handleGetTask(w http.ResponseWriter, r *http.Request) {
	rec, err := s.GetTask(r.Context(), r.PathValue("taskId"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

func (s *Service) handleListTasks(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := TaskFilter{Knight: q.Get("knight"), RunID: q.Get("runId"), Status: TaskStatus(q.Get("status"))}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeError(w, fmt.Errorf("%w: limit: %v", ErrInvalid, err))
			return
		}
		f.Limit = n
	}
	tasks, err := s.ListTasks(r.Context(), f)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tasks": tasks})
}

func (s *Service) handleStartMission(w http.ResponseWriter, r *http.Request) {
	var req StartMissionRequest
	// Unknown fields are rejected: that is what keeps ephemeralSpec, secrets,
	// roundTableTemplate, images and the like out of gateway missions.
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, fmt.Errorf("%w: body: %v", ErrInvalid, err))
		return
	}
	if dec.More() {
		writeError(w, fmt.Errorf("%w: body: trailing data after the JSON object", ErrInvalid))
		return
	}
	v, err := s.StartMission(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Location", "/v1/missions/"+v.Namespace+"/"+v.Name)
	code := http.StatusCreated
	if !v.Done && req.WaitSeconds != nil && *req.WaitSeconds > 0 {
		code = http.StatusAccepted
	}
	writeJSON(w, code, v)
}

func (s *Service) handleGetMission(w http.ResponseWriter, r *http.Request) {
	wait, err := queryInt(r, "waitSeconds")
	if err != nil {
		writeError(w, err)
		return
	}
	v, err := s.GetMission(r.Context(), r.PathValue("namespace"), r.PathValue("name"), wait)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (s *Service) handleListMissions(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := MissionFilter{Namespace: q.Get("namespace"), Phase: q.Get("phase")}
	limit, err := queryInt(r, "limit")
	if err != nil {
		writeError(w, err)
		return
	}
	if limit != nil {
		f.Limit = *limit
	}
	missions, err := s.ListMissions(r.Context(), f)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"missions": missions})
}

func (s *Service) handleDeleteMission(w http.ResponseWriter, r *http.Request) {
	ns, name := r.PathValue("namespace"), r.PathValue("name")
	if err := s.DeleteMission(r.Context(), ns, name); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"deleted": ns + "/" + name})
}

func (s *Service) handleListChains(w http.ResponseWriter, r *http.Request) {
	chains, err := s.ListChains(r.Context(), r.URL.Query().Get("namespace"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"chains": chains})
}

func (s *Service) handleRunChain(w http.ResponseWriter, r *http.Request) {
	var req RunChainRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, fmt.Errorf("%w: body: %v", ErrInvalid, err))
		return
	}
	if dec.More() {
		writeError(w, fmt.Errorf("%w: body: trailing data after the JSON object", ErrInvalid))
		return
	}
	req.IdempotencyKey = r.Header.Get(idempotencyHeader)
	rec, err := s.RunChain(r.Context(), r.PathValue("name"), req)
	if err != nil {
		writeError(w, err)
		return
	}
	if rec.Replayed {
		w.Header().Set(idempotentReplayHeader, "true")
	}
	writeRun(w, rec)
}

func (s *Service) handleGetRun(w http.ResponseWriter, r *http.Request) {
	wait, err := queryInt(r, "waitSeconds")
	if err != nil {
		writeError(w, err)
		return
	}
	rec, err := s.GetRun(r.Context(), r.PathValue("namespace"), r.PathValue("run"), wait)
	if err != nil {
		writeError(w, err)
		return
	}
	writeRun(w, rec)
}

func (s *Service) handleCancelRun(w http.ResponseWriter, r *http.Request) {
	var req CancelRunRequest
	if r.Body != nil {
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			writeError(w, fmt.Errorf("%w: body: %v", ErrInvalid, err))
			return
		}
	}
	rec, err := s.CancelRun(r.Context(), r.PathValue("namespace"), r.PathValue("run"), req.Reason)
	if err != nil {
		writeError(w, err)
		return
	}
	writeRun(w, rec)
}

// writeRun answers 200 with a finished run and 202 with a Location to poll
// otherwise.
func writeRun(w http.ResponseWriter, rec *RunRecord) {
	code := http.StatusOK
	if !rec.Done {
		code = http.StatusAccepted
		w.Header().Set("Location", "/v1/runs/"+rec.Namespace+"/"+rec.Run)
	}
	writeJSON(w, code, rec)
}

func (s *Service) handleListRuns(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := RunFilter{Namespace: q.Get("namespace"), Chain: q.Get("chain"), Phase: q.Get("phase")}
	limit, err := queryInt(r, "limit")
	if err != nil {
		writeError(w, err)
		return
	}
	if limit != nil {
		f.Limit = *limit
	}
	runs, err := s.ListRuns(r.Context(), f)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": runs})
}

func (s *Service) handleListSchedules(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	schedules, err := s.ListSchedules(r.Context(), ScheduleFilter{Namespace: q.Get("namespace"), Table: q.Get("table")})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"schedules": schedules})
}

func (s *Service) handleGetSchedule(w http.ResponseWriter, r *http.Request) {
	v, err := s.GetSchedule(r.Context(), r.PathValue("namespace"), r.PathValue("name"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (s *Service) handleScheduleManifest(w http.ResponseWriter, r *http.Request) {
	b, err := s.ScheduleManifest(r.Context(), r.PathValue("namespace"), r.PathValue("name"))
	if err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/yaml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b)
}

// queryInt parses an optional integer query parameter.
func queryInt(r *http.Request, key string) (*int, error) {
	v := r.URL.Query().Get(key)
	if v == "" {
		return nil, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrInvalid, key, err)
	}
	return &n, nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	var conflict *ConflictError
	switch {
	case errors.As(err, &conflict):
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error(), "state": conflict.State})
		return
	case errors.Is(err, ErrUnprocessable):
		code = http.StatusUnprocessableEntity
	case errors.Is(err, ErrInvalid):
		code = http.StatusBadRequest
	case errors.Is(err, ErrForbidden):
		code = http.StatusForbidden
	case errors.Is(err, ErrNotFound):
		code = http.StatusNotFound
	case errors.Is(err, ErrBusy):
		code = http.StatusTooManyRequests
	case errors.Is(err, ErrUnavailable):
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

// TokenAuth checks "Authorization: Bearer <token>" against the tokens in a
// file (one per line, so a Secret can hold the old and new token during a
// rotation). A line is "<token>" or "<name> <token>"; the name (default
// "token") is the caller recorded for requests with that token. The file is
// re-read at most every refresh interval, so a rotated Secret takes effect
// without a restart.
//
// With ServiceAccounts set, a bearer token that is not a static token is
// checked as a Kubernetes ServiceAccount token instead.
type TokenAuth struct {
	path    string
	refresh time.Duration

	// ServiceAccounts authenticates in-cluster callers by their
	// ServiceAccount token. nil accepts static tokens only.
	ServiceAccounts *ServiceAccountAuth
	// Log records refused ServiceAccount tokens.
	Log logr.Logger

	mu       sync.Mutex
	tokens   []staticToken
	loadedAt time.Time
}

// staticToken is one line of the token file.
type staticToken struct {
	hash [32]byte
	name string
}

// defaultTokenName is the caller of a static token whose line has no name.
const defaultTokenName = "token"

// tokenNameRE is what a static token's name may look like: no "/", so it
// cannot be confused with a ServiceAccount caller.
var tokenNameRE = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._-]{0,61}[A-Za-z0-9])?$`)

// NewTokenAuth loads the token file and fails if it holds no usable token.
func NewTokenAuth(path string, refresh time.Duration) (*TokenAuth, error) {
	a := &TokenAuth{path: path, refresh: refresh, Log: logr.Discard()}
	if err := a.load(); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *TokenAuth) load() error {
	data, err := os.ReadFile(a.path)
	if err != nil {
		return fmt.Errorf("read token file: %w", err)
	}
	var tokens []staticToken
	for line := range strings.SplitSeq(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		name, tok := defaultTokenName, line
		if fields := strings.Fields(line); len(fields) == 2 {
			name, tok = fields[0], fields[1]
			if !tokenNameRE.MatchString(name) {
				return fmt.Errorf("token file %s: token names are letters, digits, '.', '_' and '-' (at most 63)", a.path)
			}
		} else if len(fields) > 2 {
			return fmt.Errorf("token file %s: a line is \"<token>\" or \"<name> <token>\"", a.path)
		}
		if len(tok) < minTokenLength {
			return fmt.Errorf("token file %s: tokens must be at least %d characters", a.path, minTokenLength)
		}
		tokens = append(tokens, staticToken{hash: sha256.Sum256([]byte(tok)), name: name})
	}
	if len(tokens) == 0 {
		return fmt.Errorf("token file %s holds no token", a.path)
	}
	a.tokens = tokens
	a.loadedAt = time.Now()
	return nil
}

// match returns the name of the static token tok, if it is one.
func (a *TokenAuth) match(tok string) (string, bool) {
	a.mu.Lock()
	if time.Since(a.loadedAt) > a.refresh {
		// Keep the last good tokens if the file is briefly unreadable or
		// empty mid-rotation, and don't retry before the next interval.
		if err := a.load(); err != nil {
			a.loadedAt = time.Now()
		}
	}
	tokens := a.tokens
	a.mu.Unlock()

	// Compare fixed-size hashes in constant time, always against every token.
	h := sha256.Sum256([]byte(tok))
	name, found := "", false
	for i := range tokens {
		if subtle.ConstantTimeCompare(h[:], tokens[i].hash[:]) == 1 && !found {
			name, found = tokens[i].name, true
		}
	}
	return name, found
}

func (a *TokenAuth) valid(tok string) bool {
	_, ok := a.match(tok)
	return ok
}

// authenticate returns the caller presenting tok.
func (a *TokenAuth) authenticate(r *http.Request, tok string) (Caller, error) {
	if name, ok := a.match(tok); ok {
		return Caller{Kind: CallerToken, Name: name}, nil
	}
	if a.ServiceAccounts == nil || tok == "" {
		return Caller{}, unauthenticated("unknown token")
	}
	return a.ServiceAccounts.Authenticate(r.Context(), tok)
}

// Wrap rejects requests without a valid bearer token (401), with a
// ServiceAccount token that is not allowed (403), or when the TokenReview
// fails (503), and records the caller on the request.
func (a *TokenAuth) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scheme, tok, found := strings.Cut(r.Header.Get("Authorization"), " ")
		var caller Caller
		err := unauthenticated("no bearer token")
		if found && strings.EqualFold(scheme, "Bearer") {
			caller, err = a.authenticate(r, strings.TrimSpace(tok))
		}
		if err != nil {
			code := authStatus(err)
			switch code {
			case http.StatusUnauthorized:
				a.Log.V(1).Info("Refused request: not authenticated", "reason", err.Error(), "path", r.URL.Path)
				w.Header().Set("WWW-Authenticate", `Bearer realm="roundtable-gateway"`)
				writeJSON(w, code, map[string]string{"error": "unauthorized"})
			case http.StatusForbidden:
				a.Log.Info("Refused request: caller not allowed", "reason", err.Error(), "path", r.URL.Path)
				writeJSON(w, code, map[string]string{"error": "forbidden: " + err.Error()})
			default:
				a.Log.Error(err, "Cannot authenticate request", "path", r.URL.Path)
				writeJSON(w, code, map[string]string{"error": "authentication unavailable"})
			}
			return
		}
		setRequestCaller(r.Context(), caller)
		inner := r.WithContext(WithCaller(r.Context(), caller))
		next.ServeHTTP(w, inner)
		// The API mux records the matched route on the copy; hand it back to
		// the metrics middleware, which reads it from r.
		r.Pattern = inner.Pattern
	})
}
