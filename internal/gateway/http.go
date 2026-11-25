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
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
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
	api.HandleFunc("GET /v1/knights", svc.handleListKnights)
	api.HandleFunc("POST /v1/knights/{name}/invoke", svc.handleInvoke)
	api.HandleFunc("GET /v1/tasks", svc.handleListTasks)
	api.HandleFunc("GET /v1/tasks/{taskId}", svc.handleGetTask)
	api.HandleFunc("GET /v1/tasks/{taskId}/events", svc.handleTaskEvents)
	api.HandleFunc("GET /v1/dead-letters", svc.handleListDeadLetters)
	api.HandleFunc("GET /v1/dead-letters/{taskId}", svc.handleGetDeadLetter)
	api.HandleFunc("POST /v1/dead-letters/{taskId}/retry", svc.handleRetryDeadLetter)
	if svc.MissionsEnabled() {
		api.HandleFunc("POST /v1/missions", svc.handleStartMission)
		api.HandleFunc("GET /v1/missions", svc.handleListMissions)
		api.HandleFunc("GET /v1/missions/{namespace}/{name}", svc.handleGetMission)
		api.HandleFunc("DELETE /v1/missions/{namespace}/{name}", svc.handleDeleteMission)
	}
	if svc.RunsEnabled() {
		api.HandleFunc("GET /v1/chains", svc.handleListChains)
		api.HandleFunc("POST /v1/chains/{name}/runs", svc.handleRunChain)
		api.HandleFunc("GET /v1/runs", svc.handleListRuns)
		api.HandleFunc("GET /v1/runs/{namespace}/{run}", svc.handleGetRun)
		api.HandleFunc("GET /v1/runs/{namespace}/{run}/events", svc.handleRunEvents)
	}
	api.Handle("/mcp", MCPHandler(svc))

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	if svc.HooksEnabled() {
		mux.HandleFunc("POST /v1/hooks/{namespace}/{trigger}", svc.handleHook)
	}
	mux.Handle("/", auth.Wrap(api))
	return instrument(mux)
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
// rotation). The file is re-read at most every refresh interval, so a
// rotated Secret takes effect without a restart.
type TokenAuth struct {
	path    string
	refresh time.Duration

	mu       sync.Mutex
	hashes   [][32]byte
	loadedAt time.Time
}

// NewTokenAuth loads the token file and fails if it holds no usable token.
func NewTokenAuth(path string, refresh time.Duration) (*TokenAuth, error) {
	a := &TokenAuth{path: path, refresh: refresh}
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
	var hashes [][32]byte
	for line := range strings.SplitSeq(string(data), "\n") {
		tok := strings.TrimSpace(line)
		if tok == "" {
			continue
		}
		if len(tok) < minTokenLength {
			return fmt.Errorf("token file %s: tokens must be at least %d characters", a.path, minTokenLength)
		}
		hashes = append(hashes, sha256.Sum256([]byte(tok)))
	}
	if len(hashes) == 0 {
		return fmt.Errorf("token file %s holds no token", a.path)
	}
	a.hashes = hashes
	a.loadedAt = time.Now()
	return nil
}

func (a *TokenAuth) valid(tok string) bool {
	a.mu.Lock()
	if time.Since(a.loadedAt) > a.refresh {
		// Keep the last good tokens if the file is briefly unreadable or
		// empty mid-rotation, and don't retry before the next interval.
		if err := a.load(); err != nil {
			a.loadedAt = time.Now()
		}
	}
	hashes := a.hashes
	a.mu.Unlock()

	// Compare fixed-size hashes in constant time.
	h := sha256.Sum256([]byte(tok))
	ok := 0
	for i := range hashes {
		ok |= subtle.ConstantTimeCompare(h[:], hashes[i][:])
	}
	return ok == 1
}

// Wrap rejects requests without a valid bearer token.
func (a *TokenAuth) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scheme, tok, found := strings.Cut(r.Header.Get("Authorization"), " ")
		if !found || !strings.EqualFold(scheme, "Bearer") || !a.valid(strings.TrimSpace(tok)) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="roundtable-gateway"`)
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}
