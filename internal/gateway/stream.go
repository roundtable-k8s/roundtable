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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

const (
	// streamHeartbeat is how often an idle SSE stream gets a comment line, so
	// proxies don't cut it.
	streamHeartbeat = 15 * time.Second
	// maxStepEventOutput bounds a step's output in a step event; the run
	// record in the result event has the (larger) full per-step output.
	maxStepEventOutput = 4 << 10
	// resultEventID is the SSE id of the result event; a client that already
	// saw it reconnects with Last-Event-ID set to it.
	resultEventID = "result"
)

// ErrTooManyStreams is returned when maxStreams streams are already open.
var ErrTooManyStreams = fmt.Errorf("%w: too many open event streams", ErrBusy)

// StreamEvent is one event of a task or run stream. An empty Name is a
// comment (heartbeat).
type StreamEvent struct {
	Name string
	ID   string
	Data any
}

// progressFunc reports progress of a blocking call (MCP progress
// notifications). force bypasses the caller's rate limit.
type progressFunc func(message string, force bool)

type progressKey struct{}

func withProgress(ctx context.Context, f progressFunc) context.Context {
	return context.WithValue(ctx, progressKey{}, f)
}

func progressFrom(ctx context.Context) progressFunc {
	f, _ := ctx.Value(progressKey{}).(progressFunc)
	return f
}

// acquireStream takes a slot for an open stream.
func (s *Service) acquireStream() (release func(), err error) {
	select {
	case <-s.closing:
		return nil, fmt.Errorf("%w: gateway is shutting down", ErrUnavailable)
	default:
	}
	if s.streams == nil {
		return func() {}, nil
	}
	select {
	case s.streams <- struct{}{}:
		return func() { <-s.streams }, nil
	default:
		return nil, ErrTooManyStreams
	}
}

// CloseStreams ends every open event stream and refuses new ones. Call it
// when the server starts shutting down (http.Server.RegisterOnShutdown), or
// Shutdown would wait on the streams until its deadline.
func (s *Service) CloseStreams() {
	s.closeOnce.Do(func() { close(s.closing) })
}

// streamContext is ctx, cancelled too when the gateway shuts down.
func (s *Service) streamContext(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-s.closing:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

// activityPump forwards the activity events of tasks into one channel.
type activityPump struct {
	svc  *Service
	ctx  context.Context
	out  chan []byte
	mu   sync.Mutex
	subs map[string]func() // taskID -> stop
	wg   sync.WaitGroup
}

func (s *Service) newActivityPump(ctx context.Context) *activityPump {
	return &activityPump{svc: s, ctx: ctx, out: make(chan []byte, 256), subs: map[string]func(){}}
}

// add subscribes to the activity of one task; adding twice is a no-op.
func (p *activityPump) add(key, subject string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.subs[key]; ok {
		return nil
	}
	w, err := p.svc.Bus.Watch(subject)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(p.ctx)
	p.subs[key] = cancel
	p.wg.Go(func() {
		defer w.Close()
		for {
			data, err := w.Next(ctx)
			if err != nil {
				if ctx.Err() == nil {
					p.svc.Log.Error(err, "Activity subscription failed", "subject", subject)
				}
				return
			}
			var buf bytes.Buffer
			if err := json.Compact(&buf, data); err != nil {
				continue // not JSON: nothing a client could use
			}
			select {
			case p.out <- buf.Bytes():
			case <-ctx.Done():
				return
			}
		}
	})
	return nil
}

// remove stops following one task.
func (p *activityPump) remove(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if stop, ok := p.subs[key]; ok {
		stop()
		delete(p.subs, key)
	}
}

// close stops every subscription and waits for the forwarders.
func (p *activityPump) close() {
	p.mu.Lock()
	for _, stop := range p.subs {
		stop()
	}
	p.mu.Unlock()
	p.wg.Wait()
}

// drain emits the activity already queued, without waiting for more.
func (p *activityPump) drain(emit func(StreamEvent) error) error {
	for {
		select {
		case data := <-p.out:
			if err := emit(StreamEvent{Name: "activity", Data: json.RawMessage(data)}); err != nil {
				return err
			}
		default:
			return nil
		}
	}
}

// FollowTask streams a task's live activity and then its result through emit,
// returning once the result was emitted, ctx ends or emit fails. A finished
// task yields just the result. Activity from before the call is not replayed:
// knights publish it on core NATS and it isn't stored.
func (s *Service) FollowTask(ctx context.Context, taskID string, emit func(StreamEvent) error) error {
	rec, err := s.GetTask(ctx, taskID)
	if err != nil {
		return err
	}
	if rec.Done() {
		return emit(StreamEvent{Name: "result", ID: resultEventID, Data: rec})
	}
	return s.followTask(ctx, rec, emit)
}

func (s *Service) followTask(ctx context.Context, rec *TaskRecord, emit func(StreamEvent) error) error {
	t, err := s.Resolver.resolve(ctx, rec.Namespace, rec.Knight)
	if err != nil {
		return err
	}
	pump := s.newActivityPump(ctx)
	defer pump.close()
	// Subscribe before re-reading the task so a result landing in between
	// is seen by one of the two.
	if err := pump.add(rec.TaskID, natspkg.ActivitySubject(t.SubjectPrefix, rec.Knight, rec.TaskID)); err != nil {
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	result, err := s.Bus.Watch(rec.ResultSubject)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	defer result.Close()
	resCh := make(chan []byte, 1)
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		if data, err := result.Next(rctx); err == nil {
			resCh <- data
		}
	}()

	// The result may already be there (or arrive between the first read and
	// the subscription).
	cur, err := s.GetTask(ctx, rec.TaskID)
	if err != nil {
		return err
	}
	if cur.Done() {
		return emit(StreamEvent{Name: "result", ID: resultEventID, Data: cur})
	}

	tick := time.NewTicker(s.heartbeat())
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case data := <-pump.out:
			if err := emit(StreamEvent{Name: "activity", Data: json.RawMessage(data)}); err != nil {
				return err
			}
		case data := <-resCh:
			if err := pump.drain(emit); err != nil {
				return err
			}
			s.complete(ctx, cur, data)
			return emit(StreamEvent{Name: "result", ID: resultEventID, Data: cur})
		case <-tick.C:
			if err := emit(StreamEvent{}); err != nil {
				return err
			}
		}
	}
}

func (s *Service) heartbeat() time.Duration {
	if s.Opts.StreamHeartbeat > 0 {
		return s.Opts.StreamHeartbeat
	}
	return streamHeartbeat
}

// stepEvent is the data of a "step" event.
type stepEvent struct {
	Name            string `json:"name"`
	Phase           string `json:"phase"`
	Error           string `json:"error,omitempty"`
	Output          string `json:"output,omitempty"`
	OutputTruncated bool   `json:"outputTruncated,omitempty"`
}

// FollowRun streams a gateway run: a "step" event whenever a step's phase
// changes (the current phases first), the activity of the steps that are
// running, and finally the run record as "result". A finished run yields its
// steps and the result at once.
func (s *Service) FollowRun(ctx context.Context, namespace, name string, emit func(StreamEvent) error) error {
	if !s.RunsEnabled() {
		return errRunsDisabled
	}
	ns, err := s.namespace(namespace)
	if err != nil {
		return err
	}
	if _, err := s.fetchRun(ctx, ns, name); err != nil {
		return err
	}
	return s.followRun(ctx, ns, name, emit)
}

func (s *Service) followRun(ctx context.Context, ns, name string, emit func(StreamEvent) error) error {
	pump := s.newActivityPump(ctx)
	defer pump.close()
	seen := map[string]aiv1alpha1.ChainStepPhase{}
	poll := time.NewTicker(s.Opts.Runs.PollInterval)
	defer poll.Stop()
	beat := time.NewTicker(s.heartbeat())
	defer beat.Stop()

	for {
		c, err := s.fetchRun(ctx, ns, name)
		if err != nil {
			return err
		}
		done := runDone(c)
		rec := runRecord(c, true)
		if rec.Phase != runPhasePending {
			if err := s.emitStepChanges(ctx, c, seen, pump, emit); err != nil {
				return err
			}
		}
		if done {
			if err := pump.drain(emit); err != nil {
				return err
			}
			return emit(StreamEvent{Name: "result", ID: resultEventID, Data: rec})
		}
	wait:
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case data := <-pump.out:
				if err := emit(StreamEvent{Name: "activity", Data: json.RawMessage(data)}); err != nil {
					return err
				}
			case <-beat.C:
				if err := emit(StreamEvent{}); err != nil {
					return err
				}
			case <-poll.C:
				break wait
			}
		}
	}
}

// emitStepChanges emits a step event for every step whose phase differs from
// seen, and starts or stops following the activity of running steps.
func (s *Service) emitStepChanges(ctx context.Context, c *aiv1alpha1.Chain,
	seen map[string]aiv1alpha1.ChainStepPhase, pump *activityPump, emit func(StreamEvent) error) error {
	knights := make(map[string]string, len(c.Spec.Steps))
	for _, st := range c.Spec.Steps {
		knights[st.Name] = st.KnightRef.Name
	}
	for i := range c.Status.StepStatuses {
		ss := &c.Status.StepStatuses[i]
		if ss.Phase == "" {
			continue
		}
		running := ss.Phase == aiv1alpha1.ChainStepPhaseRunning && ss.TaskID != ""
		if running {
			s.followStepActivity(ctx, c.Namespace, knights[ss.Name], ss.TaskID, pump)
		}
		if seen[ss.Name] == ss.Phase {
			continue
		}
		seen[ss.Name] = ss.Phase
		ev := stepEvent{Name: ss.Name, Phase: string(ss.Phase), Error: ss.Error}
		ev.Output, ev.OutputTruncated = truncate(ss.Output, maxStepEventOutput)
		if err := emit(StreamEvent{Name: "step", Data: ev}); err != nil {
			return err
		}
		if !running && ss.TaskID != "" {
			pump.remove(ss.TaskID)
		}
	}
	return nil
}

// followStepActivity subscribes to a running step's task; a step whose
// knight can't be resolved just has no activity.
func (s *Service) followStepActivity(ctx context.Context, ns, knight, taskID string, pump *activityPump) {
	if knight == "" {
		return
	}
	t, err := s.Resolver.resolve(ctx, ns, knight)
	if err != nil {
		s.Log.V(1).Info("Not following step activity", "knight", knight, "taskId", taskID, "error", err.Error())
		return
	}
	if err := pump.add(taskID, natspkg.ActivitySubject(t.SubjectPrefix, knight, taskID)); err != nil {
		s.Log.Error(err, "Failed to follow step activity", "knight", knight, "taskId", taskID)
	}
}

// progressMessage renders an activity event or step event for an MCP
// progress notification; ok is false for events not worth one.
func progressMessage(ev StreamEvent) (msg string, force, ok bool) {
	switch d := ev.Data.(type) {
	case stepEvent:
		msg = fmt.Sprintf("step %s: %s", d.Name, d.Phase)
		if d.Error != "" {
			msg += " (" + d.Error + ")"
		}
		return msg, true, true
	case json.RawMessage:
		var a natspkg.ActivityEvent
		if json.Unmarshal(d, &a) != nil {
			return "", false, false
		}
		prefix := ""
		if a.Step != "" {
			prefix = a.Step + ": "
		}
		switch a.Type {
		case natspkg.ActivityToolStarted:
			msg = prefix + "tool " + a.Tool
			if a.ArgsSummary != "" {
				msg += " " + a.ArgsSummary
			}
			return msg, false, true
		case natspkg.ActivityTaskStarted:
			return prefix + "task started", true, true
		}
	}
	return "", false, false
}

// runProgress follows the task or run in the background and reports it
// through f until stop is called (stop waits for the follower).
func (s *Service) startProgress(ctx context.Context, f progressFunc, follow func(context.Context, func(StreamEvent) error) error) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = follow(ctx, func(ev StreamEvent) error {
			if msg, force, ok := progressMessage(ev); ok {
				f(msg, force)
			}
			return nil
		})
	}()
	return func() {
		cancel()
		<-done
	}
}

// --- HTTP ---

// sseWriter writes Server-Sent Events.
type sseWriter struct {
	w  http.ResponseWriter
	rc *http.ResponseController
}

func newSSEWriter(w http.ResponseWriter) *sseWriter {
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no") // nginx: don't buffer the stream
	w.WriteHeader(http.StatusOK)
	s := &sseWriter{w: w, rc: http.NewResponseController(w)}
	_ = s.rc.Flush()
	return s
}

func (s *sseWriter) send(ev StreamEvent) error {
	var err error
	if ev.Name == "" {
		_, err = fmt.Fprint(s.w, ": heartbeat\n\n")
	} else {
		var data []byte
		if data, err = json.Marshal(ev.Data); err != nil {
			return err
		}
		if ev.ID != "" {
			_, err = fmt.Fprintf(s.w, "id: %s\n", ev.ID)
		}
		if err == nil {
			_, err = fmt.Fprintf(s.w, "event: %s\ndata: %s\n\n", ev.Name, data)
		}
	}
	if err != nil {
		return err
	}
	return s.rc.Flush()
}

// serveStream runs a follower as an SSE response. Errors before the first
// event are ordinary JSON errors; later ones end the stream with an "error"
// event.
func (s *Service) serveStream(w http.ResponseWriter, r *http.Request,
	follow func(context.Context, func(StreamEvent) error) error) {
	release, err := s.acquireStream()
	if err != nil {
		writeError(w, err)
		return
	}
	defer release()
	ctx, cancel := s.streamContext(r.Context())
	defer cancel()

	var sse *sseWriter
	emit := func(ev StreamEvent) error {
		if sse == nil {
			sse = newSSEWriter(w)
		}
		return sse.send(ev)
	}
	err = follow(ctx, emit)
	switch {
	case err == nil || ctx.Err() != nil:
		return
	case sse == nil:
		writeError(w, err)
	default:
		_ = sse.send(StreamEvent{Name: "error", Data: map[string]string{"error": err.Error()}})
	}
}

func (s *Service) handleTaskEvents(w http.ResponseWriter, r *http.Request) {
	taskID := r.PathValue("taskId")
	if r.Header.Get("Last-Event-ID") == resultEventID {
		// The client already has the result: tell EventSource not to reconnect.
		if rec, err := s.GetTask(r.Context(), taskID); err == nil && rec.Done() {
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}
	s.serveStream(w, r, func(ctx context.Context, emit func(StreamEvent) error) error {
		return s.FollowTask(ctx, taskID, emit)
	})
}

func (s *Service) handleRunEvents(w http.ResponseWriter, r *http.Request) {
	ns, run := r.PathValue("namespace"), r.PathValue("run")
	if r.Header.Get("Last-Event-ID") == resultEventID {
		if rec, err := s.GetRun(r.Context(), ns, run, nil); err == nil && rec.Done {
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}
	s.serveStream(w, r, func(ctx context.Context, emit func(StreamEvent) error) error {
		return s.FollowRun(ctx, ns, run, emit)
	})
}
