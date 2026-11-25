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
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/nats-io/nats.go"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

// maxFireBody caps the body of POST /v1/triggers/{namespace}/{name}/fire.
const maxFireBody = 4 << 10

// TriggerServiceAccountPrefix starts the name of the ServiceAccount a
// schedule Trigger's CronJob runs as: trigger-<name>, in the Trigger's
// namespace.
const TriggerServiceAccountPrefix = "trigger-"

// FireRequest is the optional body of POST /v1/triggers/{namespace}/{name}/fire.
type FireRequest struct {
	// ScheduledTime is the schedule time this firing is for (the CronJob's
	// scheduled time). Omitted: now. Truncated to the minute either way.
	ScheduledTime *time.Time `json:"scheduledTime,omitempty"`
}

// FireResult is the body of a 202 from POST /v1/triggers/{namespace}/{name}/fire.
type FireResult struct {
	Subject string `json:"subject"`
	Stream  string `json:"stream"`
	Seq     uint64 `json:"seq"`
	// Duplicate is true when JetStream dropped the firing as a repeat of
	// one for the same scheduled time.
	Duplicate bool `json:"duplicate,omitempty"`
	// Key is the firing key: the scheduled time, RFC 3339 in UTC.
	Key           string    `json:"key"`
	ScheduledTime time.Time `json:"scheduledTime"`
}

// ScheduleEvent is the body published for a schedule firing.
type ScheduleEvent struct {
	// Trigger is "<namespace>/<name>".
	Trigger       string    `json:"trigger"`
	ScheduledTime time.Time `json:"scheduledTime"`
	Key           string    `json:"key"`
	// Caller is who fired it (the Trigger's ServiceAccount or a token name).
	Caller string `json:"caller,omitempty"`
}

// ScheduleFiringKey is the firing key of a schedule time: the time truncated
// to the minute, RFC 3339 in UTC, the same key the in-process scheduler
// records. Two firings for the same slot share it.
func ScheduleFiringKey(t time.Time) string {
	return t.UTC().Truncate(time.Minute).Format(time.RFC3339)
}

// authorizeFire allows the Trigger's own ServiceAccount
// (<namespace>/trigger-<name>) and static tokens; any other caller gets
// ErrForbidden.
func authorizeFire(ctx context.Context, namespace, name string) error {
	c, ok := CallerFrom(ctx)
	if !ok {
		return fmt.Errorf("%w: no authenticated caller", ErrForbidden)
	}
	switch c.Kind {
	case CallerToken:
		return nil
	case CallerServiceAccount:
		if c.Name == namespace+"/"+TriggerServiceAccountPrefix+name {
			return nil
		}
	}
	return fmt.Errorf("%w: %s may not fire Trigger %s/%s", ErrForbidden, c, namespace, name)
}

// lookupSchedule returns the schedule Trigger at namespace/name: ErrNotFound
// for an unknown or deleting Trigger or a namespace callers may not target,
// ErrUnprocessable for a Trigger without source.schedule.
func (s *Service) lookupSchedule(ctx context.Context, namespace, name string) (*aiv1alpha1.Trigger, error) {
	notFound := fmt.Errorf("%w: trigger %s/%s", ErrNotFound, namespace, name)
	if namespace == "" || len(validation.IsDNS1123Label(namespace)) > 0 || len(validation.IsDNS1123Subdomain(name)) > 0 {
		return nil, notFound
	}
	if _, err := s.namespace(namespace); err != nil {
		return nil, notFound
	}
	trg := &aiv1alpha1.Trigger{}
	if err := s.Hooks.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, trg); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, notFound
		}
		return nil, fmt.Errorf("%w: get trigger: %v", ErrUnavailable, err)
	}
	if !trg.DeletionTimestamp.IsZero() {
		return nil, notFound
	}
	if trg.Spec.Source.Schedule == nil {
		return nil, fmt.Errorf("%w: trigger %s/%s has no source.schedule", ErrUnprocessable, namespace, name)
	}
	return trg, nil
}

// readFireRequest decodes the optional body.
func readFireRequest(w http.ResponseWriter, r *http.Request) (FireRequest, error) {
	var req FireRequest
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxFireBody))
	if err != nil {
		return req, fmt.Errorf("%w: body: %v", ErrInvalid, err)
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return req, nil
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return req, fmt.Errorf("%w: body: %v", ErrInvalid, err)
	}
	if req.ScheduledTime != nil && req.ScheduledTime.IsZero() {
		return req, fmt.Errorf("%w: scheduledTime is zero", ErrInvalid)
	}
	return req, nil
}

// handleFireTrigger serves POST /v1/triggers/{namespace}/{name}/fire: a
// schedule Trigger's CronJob fires it. The gateway publishes a schedule event
// keyed by the scheduled time to the Trigger's subject on the table's events
// stream; the trigger controller dedupes, applies concurrency and records the
// firing. 202 with the stream sequence and firing key; 403 for a caller other
// than the Trigger's own ServiceAccount or a static token; 404 for an unknown
// Trigger; 422 for a Trigger without source.schedule.
func (s *Service) handleFireTrigger(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ns, name := r.PathValue("namespace"), r.PathValue("name")
	log := s.Log.WithValues("namespace", ns, "trigger", name, "caller", callerName(ctx))
	if err := authorizeFire(ctx, ns, name); err != nil {
		log.Info("Refused Trigger fire", "reason", err.Error())
		writeError(w, err)
		return
	}
	trg, err := s.lookupSchedule(ctx, ns, name)
	if err != nil {
		if errors.Is(err, ErrUnavailable) {
			log.Error(err, "Schedule Trigger lookup failed")
		}
		writeError(w, err)
		return
	}
	rtName := aiv1alpha1.RefName(trg.Spec.RoundTableRef)
	setRequestTable(ctx, rtName)
	req, err := readFireRequest(w, r)
	if err != nil {
		writeError(w, err)
		return
	}
	at := s.now()
	if req.ScheduledTime != nil {
		at = *req.ScheduledTime
	}
	at = at.UTC().Truncate(time.Minute)
	key := ScheduleFiringKey(at)

	rt := &aiv1alpha1.RoundTable{}
	if err := s.Hooks.Get(ctx, types.NamespacedName{Namespace: ns, Name: rtName}, rt); err != nil {
		log.Error(err, "Schedule Trigger's RoundTable not readable", "roundTable", rtName)
		writeError(w, fmt.Errorf("%w: the Trigger's RoundTable is not available", ErrUnavailable))
		return
	}
	if rt.Spec.NATS.SubjectPrefix == "" {
		writeError(w, fmt.Errorf("%w: the Trigger's RoundTable has no subject prefix", ErrUnavailable))
		return
	}
	data, err := json.Marshal(ScheduleEvent{Trigger: ns + "/" + name, ScheduledTime: at, Key: key, Caller: callerName(ctx)})
	if err != nil {
		writeError(w, err)
		return
	}
	header := nats.Header{}
	header.Set("Content-Type", "application/json")
	header.Set(natspkg.EventKeyHeader, key)
	header.Set(natspkg.ScheduledTimeHeader, key)
	res := &FireResult{
		Subject:       natspkg.ScheduleEventSubject(rt.Spec.NATS.SubjectPrefix, ns, name),
		Stream:        rt.Spec.NATS.EventsStreamName(),
		Key:           key,
		ScheduledTime: at,
	}
	// The message id makes a retried call for the same slot one stream
	// message within the stream's duplicate window; past it, the controller
	// dedupes by the firing key.
	msgID := ns + "/" + name + "/schedule/" + key
	res.Seq, res.Duplicate, err = s.Bus.PublishEvent(ctx, res.Stream, res.Subject, data, header, msgID)
	if err != nil {
		log.Error(err, "Failed to publish schedule firing", "subject", res.Subject)
		writeError(w, fmt.Errorf("%w: publish failed", ErrUnavailable))
		return
	}
	log.Info("Fired schedule Trigger", "subject", res.Subject, "seq", res.Seq, "key", key, "duplicate", res.Duplicate)
	writeJSON(w, http.StatusAccepted, res)
}
