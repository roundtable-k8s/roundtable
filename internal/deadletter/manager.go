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

package deadletter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"

	rtmetrics "github.com/roundtable-k8s/roundtable/pkg/metrics"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

const (
	// fetchBatch and fetchWait bound one pull from the advisories stream.
	fetchBatch = 16
	fetchWait  = 2 * time.Second
	// consumerAckWait is the advisory consumer's ack wait; a failed
	// advisory is nak'ed with retryDelay and processed again.
	consumerAckWait = 30 * time.Second
	retryDelay      = 5 * time.Second
	// successMemory is how long a success:true result is remembered, so an
	// entry captured just after it (advisory processed late) starts resolved.
	successMemory = 15 * time.Minute
	// DefaultLostGrace is how long after a max-deliveries advisory the
	// original task is kept in the tasks stream, so the knights' own
	// watchLostTasks can still load it and publish their lost result (which
	// the chain controller, planner and gateway rely on).
	DefaultLostGrace = 45 * time.Second
)

// errDeletePending means the entry was captured but the original task is
// still within its grace period; the advisory is retried later.
var errDeletePending = errors.New("original task kept until the knight's lost report or the grace period")

// TableConfig identifies one RoundTable's NATS layout.
type TableConfig struct {
	// Table is the RoundTable name (KV key prefix and metric label).
	Table string
	// Prefix is spec.nats.subjectPrefix.
	Prefix string
	// TasksStream is spec.nats.tasksStream.
	TasksStream string
	// MaxAge is the dead-letter stream retention; 0 means DefaultMaxAge.
	MaxAge time.Duration
	// LostGrace is how long after the advisory the original task is kept
	// unless the knight's lost result arrives earlier; 0 means
	// DefaultLostGrace.
	LostGrace time.Duration
}

func (c TableConfig) lostGrace() time.Duration {
	if c.LostGrace > 0 {
		return c.LostGrace
	}
	return DefaultLostGrace
}

func (c TableConfig) maxAge() time.Duration {
	if c.MaxAge > 0 {
		return c.MaxAge
	}
	return DefaultMaxAge
}

// Manager runs one dead-letter watcher per RoundTable. It is safe for
// concurrent use; the RoundTable controller calls Ensure on every reconcile.
type Manager struct {
	log      logr.Logger
	now      func() time.Time
	mu       sync.Mutex
	watchers map[string]*watcher
}

// NewManager returns a Manager with no watchers.
func NewManager(log logr.Logger) *Manager {
	return &Manager{log: log, now: time.Now, watchers: map[string]*watcher{}}
}

// Ensure creates or updates the table's advisories and dead-letter streams,
// the KV bucket and the advisory consumer, and starts the table's watcher if
// it is not running on nc with the same configuration. key identifies the
// RoundTable (namespace/name).
func (m *Manager) Ensure(ctx context.Context, nc *nats.Conn, key string, cfg TableConfig) error {
	if nc == nil {
		return errors.New("dead-letter: no NATS connection")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("dead-letter: JetStream context: %w", err)
	}
	if err := ensureStreams(ctx, js, cfg); err != nil {
		return err
	}
	kv, err := ensureBucket(ctx, js)
	if err != nil {
		return err
	}
	if err := ensureConsumer(ctx, js, cfg); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if w, ok := m.watchers[key]; ok {
		if w.nc == nc && !nc.IsClosed() && w.cfg == cfg {
			return nil
		}
		w.stop()
		delete(m.watchers, key)
	}
	w := &watcher{
		cfg:     cfg,
		nc:      nc,
		js:      js,
		kv:      kv,
		log:     m.log.WithValues("roundTable", key),
		now:     m.now,
		success: map[string]time.Time{},
		open:    map[string]string{},
		lost:    map[string]time.Time{},
	}
	if err := w.start(); err != nil {
		w.stop()
		return err
	}
	m.watchers[key] = w
	return nil
}

// Stop stops the watcher of the RoundTable identified by key, if any. The
// streams, consumer and KV state are kept.
func (m *Manager) Stop(key string) {
	m.mu.Lock()
	w, ok := m.watchers[key]
	delete(m.watchers, key)
	m.mu.Unlock()
	if ok {
		w.stop()
	}
}

// Resolve marks the entry of taskID on the RoundTable identified by key
// resolved, as a later success result would: the task's work is done (#591,
// a merge step that timed out with its PRs queued). It also remembers the
// task as succeeded, so a failure result that arrives afterwards is captured
// resolved, not open. A table without a running watcher is a no-op.
func (m *Manager) Resolve(ctx context.Context, key, taskID string) error {
	if m == nil || taskID == "" {
		return nil
	}
	m.mu.Lock()
	w, ok := m.watchers[key]
	m.mu.Unlock()
	if !ok {
		return nil
	}
	return w.resolve(ctx, taskID)
}

// StopAll stops every watcher.
func (m *Manager) StopAll() {
	m.mu.Lock()
	ws := m.watchers
	m.watchers = map[string]*watcher{}
	m.mu.Unlock()
	for _, w := range ws {
		w.stop()
	}
}

// StopAllContext is StopAll bounded by ctx: it returns ctx.Err() if the
// watchers have not stopped by then (they keep stopping in the background).
func (m *Manager) StopAllContext(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.StopAll()
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ensureStream creates cfg's stream or updates it when subjects, max age or
// duplicate window differ.
func ensureStream(ctx context.Context, js jetstream.JetStream, cfg jetstream.StreamConfig) error {
	stream, err := js.Stream(ctx, cfg.Name)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		if _, err := js.CreateStream(ctx, cfg); err != nil {
			return fmt.Errorf("create stream %s: %w", cfg.Name, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("stream info %s: %w", cfg.Name, err)
	}
	cur := stream.CachedInfo().Config
	if slices.Equal(cur.Subjects, cfg.Subjects) && cur.MaxAge == cfg.MaxAge &&
		(cfg.Duplicates == 0 || cur.Duplicates == cfg.Duplicates) {
		return nil
	}
	cur.Subjects = cfg.Subjects
	cur.MaxAge = cfg.MaxAge
	if cfg.Duplicates > 0 {
		cur.Duplicates = cfg.Duplicates
	}
	if _, err := js.UpdateStream(ctx, cur); err != nil {
		return fmt.Errorf("update stream %s: %w", cfg.Name, err)
	}
	return nil
}

func ensureStreams(ctx context.Context, js jetstream.JetStream, cfg TableConfig) error {
	if err := ensureStream(ctx, js, jetstream.StreamConfig{
		Name:        AdvisoriesStreamName(cfg.TasksStream),
		Description: "Max-deliveries advisories of " + cfg.TasksStream + " (dead-letter input)",
		Subjects:    []string{AdvisorySubject(cfg.TasksStream)},
		Retention:   jetstream.LimitsPolicy,
		Storage:     jetstream.FileStorage,
		MaxAge:      AdvisoryMaxAge,
	}); err != nil {
		return fmt.Errorf("dead-letter advisories stream: %w", err)
	}
	maxAge := cfg.maxAge()
	if err := ensureStream(ctx, js, jetstream.StreamConfig{
		Name:        StreamName(cfg.TasksStream),
		Description: "Dead-lettered (lost and failed) tasks of " + cfg.TasksStream,
		Subjects:    []string{StreamSubject(cfg.Prefix)},
		Retention:   jetstream.LimitsPolicy,
		Storage:     jetstream.FileStorage,
		MaxAge:      maxAge,
		Duplicates:  min(DuplicateWindow, maxAge),
	}); err != nil {
		return fmt.Errorf("dead-letter stream: %w", err)
	}
	return nil
}

func ensureBucket(ctx context.Context, js jetstream.JetStream) (jetstream.KeyValue, error) {
	kv, err := js.KeyValue(ctx, KVBucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		kv, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
			Bucket:      KVBucket,
			Description: "Round Table dead-letter entry state (open, resolved, retried)",
			History:     5,
			TTL:         DefaultMaxAge,
			Storage:     jetstream.FileStorage,
		})
	}
	if err != nil {
		return nil, fmt.Errorf("dead-letter KV bucket: %w", err)
	}
	return kv, nil
}

func ensureConsumer(ctx context.Context, js jetstream.JetStream, cfg TableConfig) error {
	stream := AdvisoriesStreamName(cfg.TasksStream)
	_, err := js.Consumer(ctx, stream, ConsumerName)
	if err == nil {
		return nil
	}
	if !errors.Is(err, jetstream.ErrConsumerNotFound) {
		return fmt.Errorf("dead-letter consumer info: %w", err)
	}
	if _, err := js.CreateConsumer(ctx, stream, jetstream.ConsumerConfig{
		Durable:       ConsumerName,
		Description:   "Round Table operator: turns max-deliveries advisories into dead-letter entries",
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       consumerAckWait,
		MaxDeliver:    -1,
	}); err != nil {
		return fmt.Errorf("dead-letter consumer: %w", err)
	}
	return nil
}

// watcher captures one table's lost and failed tasks.
type watcher struct {
	cfg TableConfig
	nc  *nats.Conn
	js  jetstream.JetStream
	kv  jetstream.KeyValue
	log logr.Logger
	now func() time.Time

	ctx       context.Context // cancelled by stop
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	resultSub *nats.Subscription
	consumer  jetstream.Consumer
	kvWatch   jetstream.KeyWatcher

	mu      sync.Mutex
	success map[string]time.Time // task ID -> when a success result was seen
	open    map[string]string    // KV key -> knight, for open entries
	lost    map[string]time.Time // task ID -> when a knight's lost result was seen
}

func (w *watcher) start() error {
	ctx, cancel := context.WithCancel(context.Background())
	w.ctx, w.cancel = ctx, cancel

	cons, err := w.js.Consumer(ctx, AdvisoriesStreamName(w.cfg.TasksStream), ConsumerName)
	if err != nil {
		return fmt.Errorf("dead-letter advisory consumer: %w", err)
	}
	w.consumer = cons

	// A core subscription: it sees every result without creating a
	// JetStream consumer, so it can't clash with the WorkQueue results
	// stream's one-consumer-per-subject rule (the chain controller, planner
	// and gateway read the same subjects).
	w.resultSub, err = w.nc.Subscribe(w.cfg.Prefix+".results.>", w.onResult)
	if err != nil {
		return fmt.Errorf("dead-letter results subscription: %w", err)
	}

	w.kvWatch, err = w.kv.Watch(ctx, token(w.cfg.Table)+".>")
	if err != nil {
		return fmt.Errorf("dead-letter KV watch: %w", err)
	}
	if err := w.nc.Flush(); err != nil {
		return fmt.Errorf("dead-letter flush: %w", err)
	}

	w.wg.Add(2)
	go w.pullLoop(ctx)
	go w.watchState(ctx)
	w.log.Info("Dead-letter watcher started", "tasksStream", w.cfg.TasksStream,
		"deadLetterStream", StreamName(w.cfg.TasksStream))
	return nil
}

func (w *watcher) stop() {
	if w.cancel != nil {
		w.cancel()
	}
	if w.resultSub != nil {
		_ = w.resultSub.Unsubscribe()
	}
	if w.kvWatch != nil {
		_ = w.kvWatch.Stop()
	}
	// The durable advisory consumer is kept: only its fetch loop stops.
	w.wg.Wait()
	rtmetrics.DeadLettersOpen.DeletePartialMatch(prometheus.Labels{"table": w.cfg.Table})
}

// pullLoop processes stored advisories until ctx is cancelled.
func (w *watcher) pullLoop(ctx context.Context) {
	defer w.wg.Done()
	for ctx.Err() == nil {
		batch, err := w.consumer.Fetch(fetchBatch, jetstream.FetchMaxWait(fetchWait))
		if err == nil {
			for msg := range batch.Messages() {
				w.processAdvisory(ctx, msg)
			}
			err = batch.Error()
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) {
				continue
			}
			if errors.Is(err, nats.ErrConnectionClosed) {
				w.log.Info("Dead-letter advisory pull stopped", "error", err.Error())
				return
			}
			w.log.Error(err, "Dead-letter advisory fetch failed")
			select {
			case <-ctx.Done():
				return
			case <-time.After(retryDelay):
			}
		}
	}
}

// processAdvisory handles one advisory and acks it, or naks it for a retry.
func (w *watcher) processAdvisory(ctx context.Context, msg jetstream.Msg) {
	if err := w.handleAdvisory(ctx, msg.Data()); err != nil {
		if !errors.Is(err, errDeletePending) {
			w.log.Error(err, "Dead-letter advisory not processed, will retry")
		}
		_ = msg.NakWithDelay(retryDelay)
		return
	}
	_ = msg.Ack()
}

// handleAdvisory dead-letters the task behind one max-deliveries advisory and
// removes the never-acked original from the tasks stream once the knight has
// reported it lost or the grace period since the advisory has passed. It
// returns errDeletePending while the original is still being kept.
func (w *watcher) handleAdvisory(ctx context.Context, data []byte) error {
	var adv advisory
	if err := json.Unmarshal(data, &adv); err != nil || adv.StreamSeq == 0 {
		w.log.Info("Ignoring unreadable max-deliveries advisory", "data", string(data))
		return nil
	}
	e := &Entry{
		Knight:    knightFromConsumer(adv.Consumer),
		Reason:    ReasonLost,
		StreamSeq: adv.StreamSeq,
		Error: fmt.Sprintf("task delivered %d time(s) to consumer %s and never acknowledged: "+
			"the knight stopped before finishing it (not re-run)", adv.Deliveries, adv.Consumer),
	}
	stream := adv.Stream
	if stream == "" {
		stream = w.cfg.TasksStream
	}
	st, err := w.js.Stream(ctx, stream)
	if err != nil {
		return fmt.Errorf("load task %s/%d: %w", stream, adv.StreamSeq, err)
	}
	orig, err := st.GetMsg(ctx, adv.StreamSeq)
	switch {
	case err == nil:
		e.Knight = knightFromSubject(orig.Subject)
		var p natspkg.TaskPayload
		if json.Unmarshal(orig.Data, &p) == nil && (p.TaskID != "" || p.Task != "") {
			e.applyTask(&p)
		} else {
			e.applyTask(&natspkg.TaskPayload{Task: string(orig.Data)})
		}
	case errors.Is(err, jetstream.ErrMsgNotFound):
		// Already gone (deleted, aged out): record what the advisory has.
	default:
		return fmt.Errorf("load task %s/%d: %w", stream, adv.StreamSeq, err)
	}
	if e.TaskID == "" {
		e.TaskID = fmt.Sprintf("seq-%d", adv.StreamSeq)
		if e.Task != nil {
			e.Task.TaskID = e.TaskID
		}
	}
	if err := w.capture(ctx, e); err != nil {
		return err
	}
	if orig != nil {
		// The knights load the original on the same advisory to learn the
		// task ID for their lost result; deleting it first would leave the
		// chain controller, planner and gateway waiting for their timeout.
		if !w.lostReported(e.TaskID) && !adv.Timestamp.IsZero() &&
			w.now().Sub(adv.Timestamp) < w.cfg.lostGrace() {
			return errDeletePending
		}
		if err := st.DeleteMsg(ctx, adv.StreamSeq); err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
			return fmt.Errorf("delete lost task %s/%d: %w", stream, adv.StreamSeq, err)
		}
	}
	return nil
}

// onResult dead-letters a failed result or resolves an earlier entry.
func (w *watcher) onResult(msg *nats.Msg) {
	var r result
	if err := json.Unmarshal(msg.Data, &r); err != nil {
		return
	}
	taskID := r.GetTaskID()
	if taskID == "" {
		taskID = strings.TrimPrefix(msg.Subject, w.cfg.Prefix+".results.")
	}
	if r.Lost {
		w.mu.Lock()
		w.lost[taskID] = w.now()
		for id, t := range w.lost {
			if w.now().Sub(t) > successMemory {
				delete(w.lost, id)
			}
		}
		w.mu.Unlock()
	}
	if r.succeeded() {
		if err := w.resolve(w.ctx, taskID); err != nil {
			w.log.Error(err, "Dead-letter resolve failed", "taskId", taskID)
		}
		return
	}
	e := &Entry{
		TaskID: taskID,
		Knight: strings.ToLower(r.Knight),
		Reason: r.reason(),
		Error:  r.GetError(),
	}
	e.applyTask(r.task(taskID))
	if e.Knight == "" {
		e.Knight = "unknown"
	}
	if err := w.capture(w.ctx, e); err != nil {
		// Core NATS: there is no redelivery. The knight's own metric and the
		// dispatcher still saw the failure.
		w.log.Error(err, "Dead-letter capture of failed result failed", "taskId", taskID)
	}
}

// capture publishes e to the dead-letter stream once per task attempt and
// records its state.
func (w *watcher) capture(ctx context.Context, e *Entry) error {
	e.Table = w.cfg.Table
	e.Attempt = normAttempt(e.Attempt)
	key := Key(w.cfg.Table, e.TaskID)

	if ent, err := w.kv.Get(ctx, key); err == nil {
		var rec Record
		if json.Unmarshal(ent.Value(), &rec) == nil && rec.Attempt == e.Attempt {
			return nil // already captured (the other path got there first)
		}
	} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return fmt.Errorf("dead-letter state %s: %w", key, err)
	}

	e.DeadLetteredAt = w.now().UTC()
	body, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal dead-letter entry: %w", err)
	}
	subject := Subject(w.cfg.Prefix, e.Knight, e.TaskID)
	msg := nats.NewMsg(subject)
	msg.Data = body
	msg.Header.Set(jetstream.MsgIDHeader, MsgID(e.TaskID, e.Attempt))
	ack, err := w.js.PublishMsg(ctx, msg)
	if err != nil {
		return fmt.Errorf("publish dead-letter entry %s: %w", subject, err)
	}
	if ack.Duplicate {
		return nil
	}

	state := StateOpen
	if w.succeededRecently(e.TaskID) {
		state = StateResolved
	}
	rec, _ := json.Marshal(Record{
		State: state, Reason: e.Reason, Knight: e.Knight, Attempt: e.Attempt,
		Subject: subject, UpdatedAt: e.DeadLetteredAt,
	})
	if _, err := w.kv.Put(ctx, key, rec); err != nil {
		return fmt.Errorf("dead-letter state %s: %w", key, err)
	}
	rtmetrics.TasksDeadLetteredTotal.WithLabelValues(w.cfg.Table, e.Knight, string(e.Reason)).Inc()
	w.log.Info("Task dead-lettered", "taskId", e.TaskID, "knight", e.Knight,
		"reason", e.Reason, "attempt", e.Attempt, "state", state)
	return nil
}

// resolve marks an open entry for taskID resolved: the task did finish
// (e.g. a spurious lost report after an event-loop stall, pi-knight#84).
func (w *watcher) resolve(ctx context.Context, taskID string) error {
	now := w.now()
	w.mu.Lock()
	w.success[taskID] = now
	for id, t := range w.success {
		if now.Sub(t) > successMemory {
			delete(w.success, id)
		}
	}
	w.mu.Unlock()

	key := Key(w.cfg.Table, taskID)
	ent, err := w.kv.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var rec Record
	if err := json.Unmarshal(ent.Value(), &rec); err != nil || rec.State != StateOpen {
		return nil
	}
	rec.State = StateResolved
	rec.UpdatedAt = now.UTC()
	val, _ := json.Marshal(rec)
	if _, err := w.kv.Update(ctx, key, val, ent.Revision()); err != nil {
		return fmt.Errorf("resolve %s: %w", key, err)
	}
	w.log.Info("Dead-letter entry resolved by a later success", "taskId", taskID)
	return nil
}

func (w *watcher) lostReported(taskID string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	t, ok := w.lost[taskID]
	return ok && w.now().Sub(t) <= successMemory
}

func (w *watcher) succeededRecently(taskID string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	t, ok := w.success[taskID]
	return ok && w.now().Sub(t) <= successMemory
}

// watchState keeps roundtable_dead_letters_open in step with the KV bucket,
// including changes made elsewhere (the retry API sets retried).
func (w *watcher) watchState(ctx context.Context) {
	defer w.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case ent, ok := <-w.kvWatch.Updates():
			if !ok {
				return
			}
			if ent == nil {
				continue // end of the initial values
			}
			w.applyState(ent)
		}
	}
}

func (w *watcher) applyState(ent jetstream.KeyValueEntry) {
	w.mu.Lock()
	defer w.mu.Unlock()
	key := ent.Key()
	prevKnight, wasOpen := w.open[key]
	var rec Record
	isOpen := ent.Operation() == jetstream.KeyValuePut &&
		json.Unmarshal(ent.Value(), &rec) == nil && rec.State == StateOpen
	if isOpen {
		w.open[key] = rec.Knight
	} else {
		delete(w.open, key)
	}
	if wasOpen {
		w.setGaugeLocked(prevKnight)
	}
	if isOpen && (!wasOpen || rec.Knight != prevKnight) {
		w.setGaugeLocked(rec.Knight)
	}
}

func (w *watcher) setGaugeLocked(knight string) {
	n := 0
	for _, k := range w.open {
		if k == knight {
			n++
		}
	}
	rtmetrics.DeadLettersOpen.WithLabelValues(w.cfg.Table, knight).Set(float64(n))
}

// Connection returns the NATS connection behind c when it exposes one.
func Connection(c natspkg.Client) *nats.Conn {
	if p, ok := c.(natspkg.ConnProvider); ok {
		return p.Conn()
	}
	return nil
}
