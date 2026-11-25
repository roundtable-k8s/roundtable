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
)

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
func (m *Manager) Ensure(nc *nats.Conn, key string, cfg TableConfig) error {
	if nc == nil {
		return errors.New("dead-letter: no NATS connection")
	}
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("dead-letter: JetStream context: %w", err)
	}
	if err := ensureStreams(js, cfg); err != nil {
		return err
	}
	kv, err := ensureBucket(js)
	if err != nil {
		return err
	}
	if err := ensureConsumer(js, cfg); err != nil {
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

// ensureStream creates cfg's stream or updates it when subjects, max age or
// duplicate window differ.
func ensureStream(js nats.JetStreamContext, cfg *nats.StreamConfig) error {
	info, err := js.StreamInfo(cfg.Name)
	if errors.Is(err, nats.ErrStreamNotFound) {
		if _, err := js.AddStream(cfg); err != nil {
			return fmt.Errorf("create stream %s: %w", cfg.Name, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("stream info %s: %w", cfg.Name, err)
	}
	cur := info.Config
	if slices.Equal(cur.Subjects, cfg.Subjects) && cur.MaxAge == cfg.MaxAge &&
		(cfg.Duplicates == 0 || cur.Duplicates == cfg.Duplicates) {
		return nil
	}
	cur.Subjects = cfg.Subjects
	cur.MaxAge = cfg.MaxAge
	if cfg.Duplicates > 0 {
		cur.Duplicates = cfg.Duplicates
	}
	if _, err := js.UpdateStream(&cur); err != nil {
		return fmt.Errorf("update stream %s: %w", cfg.Name, err)
	}
	return nil
}

func ensureStreams(js nats.JetStreamContext, cfg TableConfig) error {
	if err := ensureStream(js, &nats.StreamConfig{
		Name:        AdvisoriesStreamName(cfg.TasksStream),
		Description: "Max-deliveries advisories of " + cfg.TasksStream + " (dead-letter input)",
		Subjects:    []string{AdvisorySubject(cfg.TasksStream)},
		Retention:   nats.LimitsPolicy,
		Storage:     nats.FileStorage,
		MaxAge:      AdvisoryMaxAge,
	}); err != nil {
		return fmt.Errorf("dead-letter advisories stream: %w", err)
	}
	maxAge := cfg.maxAge()
	if err := ensureStream(js, &nats.StreamConfig{
		Name:        StreamName(cfg.TasksStream),
		Description: "Dead-lettered (lost and failed) tasks of " + cfg.TasksStream,
		Subjects:    []string{StreamSubject(cfg.Prefix)},
		Retention:   nats.LimitsPolicy,
		Storage:     nats.FileStorage,
		MaxAge:      maxAge,
		Duplicates:  min(DuplicateWindow, maxAge),
	}); err != nil {
		return fmt.Errorf("dead-letter stream: %w", err)
	}
	return nil
}

func ensureBucket(js nats.JetStreamContext) (nats.KeyValue, error) {
	kv, err := js.KeyValue(KVBucket)
	if errors.Is(err, nats.ErrBucketNotFound) {
		kv, err = js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket:      KVBucket,
			Description: "Round Table dead-letter entry state (open, resolved, retried)",
			History:     5,
			TTL:         DefaultMaxAge,
			Storage:     nats.FileStorage,
		})
	}
	if err != nil {
		return nil, fmt.Errorf("dead-letter KV bucket: %w", err)
	}
	return kv, nil
}

func ensureConsumer(js nats.JetStreamContext, cfg TableConfig) error {
	stream := AdvisoriesStreamName(cfg.TasksStream)
	_, err := js.ConsumerInfo(stream, ConsumerName)
	if err == nil {
		return nil
	}
	if !errors.Is(err, nats.ErrConsumerNotFound) {
		return fmt.Errorf("dead-letter consumer info: %w", err)
	}
	if _, err := js.AddConsumer(stream, &nats.ConsumerConfig{
		Durable:       ConsumerName,
		Description:   "Round Table operator: turns max-deliveries advisories into dead-letter entries",
		DeliverPolicy: nats.DeliverAllPolicy,
		AckPolicy:     nats.AckExplicitPolicy,
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
	js  nats.JetStreamContext
	kv  nats.KeyValue
	log logr.Logger
	now func() time.Time

	cancel    context.CancelFunc
	wg        sync.WaitGroup
	resultSub *nats.Subscription
	pullSub   *nats.Subscription
	kvWatch   nats.KeyWatcher

	mu      sync.Mutex
	success map[string]time.Time // task ID -> when a success result was seen
	open    map[string]string    // KV key -> knight, for open entries
}

func (w *watcher) start() error {
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel

	sub, err := w.js.PullSubscribe(AdvisorySubject(w.cfg.TasksStream), ConsumerName,
		nats.Bind(AdvisoriesStreamName(w.cfg.TasksStream), ConsumerName))
	if err != nil {
		return fmt.Errorf("dead-letter advisory subscription: %w", err)
	}
	w.pullSub = sub

	// A core subscription: it sees every result without creating a
	// JetStream consumer, so it can't clash with the WorkQueue results
	// stream's one-consumer-per-subject rule (the chain controller, planner
	// and gateway read the same subjects).
	w.resultSub, err = w.nc.Subscribe(w.cfg.Prefix+".results.>", w.onResult)
	if err != nil {
		return fmt.Errorf("dead-letter results subscription: %w", err)
	}

	w.kvWatch, err = w.kv.Watch(token(w.cfg.Table) + ".>")
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
	w.wg.Wait()
	if w.pullSub != nil {
		// Unsubscribe, not Drain/Delete: the durable consumer must survive.
		_ = w.pullSub.Unsubscribe()
	}
	rtmetrics.DeadLettersOpen.DeletePartialMatch(prometheus.Labels{"table": w.cfg.Table})
}

// pullLoop processes stored advisories until ctx is cancelled.
func (w *watcher) pullLoop(ctx context.Context) {
	defer w.wg.Done()
	for ctx.Err() == nil {
		fctx, cancel := context.WithTimeout(ctx, fetchWait)
		msgs, err := w.pullSub.Fetch(fetchBatch, nats.Context(fctx))
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) {
				continue
			}
			if errors.Is(err, nats.ErrConnectionClosed) || errors.Is(err, nats.ErrBadSubscription) {
				w.log.Info("Dead-letter advisory pull stopped", "error", err.Error())
				return
			}
			w.log.Error(err, "Dead-letter advisory fetch failed")
			select {
			case <-ctx.Done():
				return
			case <-time.After(retryDelay):
			}
			continue
		}
		for _, msg := range msgs {
			if err := w.handleAdvisory(msg.Data); err != nil {
				w.log.Error(err, "Dead-letter advisory not processed, will retry")
				_ = msg.NakWithDelay(retryDelay)
				continue
			}
			_ = msg.Ack()
		}
	}
}

// handleAdvisory dead-letters the task behind one max-deliveries advisory and
// removes the never-acked original from the tasks stream.
func (w *watcher) handleAdvisory(data []byte) error {
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
	orig, err := w.js.GetMsg(stream, adv.StreamSeq)
	switch {
	case err == nil:
		e.Knight = knightFromSubject(orig.Subject)
		var p natspkg.TaskPayload
		if json.Unmarshal(orig.Data, &p) == nil && (p.TaskID != "" || p.Task != "") {
			e.applyTask(&p)
		} else {
			e.applyTask(&natspkg.TaskPayload{Task: string(orig.Data)})
		}
	case errors.Is(err, nats.ErrMsgNotFound):
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
	if err := w.capture(e); err != nil {
		return err
	}
	if orig != nil {
		if err := w.js.DeleteMsg(stream, adv.StreamSeq); err != nil && !errors.Is(err, nats.ErrMsgNotFound) {
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
	if r.succeeded() {
		if err := w.resolve(taskID); err != nil {
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
	if err := w.capture(e); err != nil {
		// Core NATS: there is no redelivery. The knight's own metric and the
		// dispatcher still saw the failure.
		w.log.Error(err, "Dead-letter capture of failed result failed", "taskId", taskID)
	}
}

// capture publishes e to the dead-letter stream once per task attempt and
// records its state.
func (w *watcher) capture(e *Entry) error {
	e.Table = w.cfg.Table
	e.Attempt = normAttempt(e.Attempt)
	key := Key(w.cfg.Table, e.TaskID)

	if ent, err := w.kv.Get(key); err == nil {
		var rec Record
		if json.Unmarshal(ent.Value(), &rec) == nil && rec.Attempt == e.Attempt {
			return nil // already captured (the other path got there first)
		}
	} else if !errors.Is(err, nats.ErrKeyNotFound) {
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
	msg.Header.Set(nats.MsgIdHdr, MsgID(e.TaskID, e.Attempt))
	ack, err := w.js.PublishMsg(msg)
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
	if _, err := w.kv.Put(key, rec); err != nil {
		return fmt.Errorf("dead-letter state %s: %w", key, err)
	}
	rtmetrics.TasksDeadLetteredTotal.WithLabelValues(w.cfg.Table, e.Knight, string(e.Reason)).Inc()
	w.log.Info("Task dead-lettered", "taskId", e.TaskID, "knight", e.Knight,
		"reason", e.Reason, "attempt", e.Attempt, "state", state)
	return nil
}

// resolve marks an open entry for taskID resolved: the task did finish
// (e.g. a spurious lost report after an event-loop stall, pi-knight#84).
func (w *watcher) resolve(taskID string) error {
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
	ent, err := w.kv.Get(key)
	if errors.Is(err, nats.ErrKeyNotFound) {
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
	if _, err := w.kv.Update(key, val, ent.Revision()); err != nil {
		return fmt.Errorf("resolve %s: %w", key, err)
	}
	w.log.Info("Dead-letter entry resolved by a later success", "taskId", taskID)
	return nil
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

func (w *watcher) applyState(ent nats.KeyValueEntry) {
	w.mu.Lock()
	defer w.mu.Unlock()
	key := ent.Key()
	prevKnight, wasOpen := w.open[key]
	var rec Record
	isOpen := ent.Operation() == nats.KeyValuePut &&
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
