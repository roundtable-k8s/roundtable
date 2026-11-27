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
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"

	aiv1alpha1 "github.com/roundtable-k8s/roundtable/api/v1alpha1"
	natspkg "github.com/roundtable-k8s/roundtable/pkg/nats"
)

const (
	// defaultHookMaxBody caps a webhook delivery's body.
	defaultHookMaxBody = 1 << 20
	// defaultHookRate and defaultHookBurst are the per-Trigger rate limit.
	defaultHookRate  = 2
	defaultHookBurst = 20
	// maxHookMsgID bounds a sender's delivery id.
	maxHookMsgID = 200
	// hookLimiterIdle is how long an unused per-Trigger limiter is kept.
	hookLimiterIdle = 10 * time.Minute
	// hookLimiterSweep is the limiter count above which idle ones are
	// dropped.
	hookLimiterSweep = 1024
)

// hookIDHeaders are the sender headers whose value becomes Nats-Msg-Id, in
// order of preference.
var hookIDHeaders = []string{"X-Github-Delivery", "Ce-Id", "Idempotency-Key"}

// hookForwardHeaders are the request headers kept as NATS headers (besides
// every Ce-* header). Credentials and signatures are never forwarded.
var hookForwardHeaders = []string{"Content-Type", "X-Github-Event", "X-Github-Delivery", "Idempotency-Key"}

// errHookUnauthorized is a missing or wrong webhook credential.
var errHookUnauthorized = errors.New("unauthorized")

// errHookCredentialUnusable marks a Trigger credential that cannot be met: no
// reference, a Secret that is missing, forbidden, unlabelled or too short. It
// is a misconfiguration, refused like a bad credential (401). Any other
// failure to read the credential wraps ErrUnavailable (503, retryable).
var errHookCredentialUnusable = errors.New("webhook credential unusable")

// HookOptions configures the webhook endpoint.
type HookOptions struct {
	// MaxBodyBytes caps a delivery's body (default 1 MiB).
	MaxBodyBytes int64
	// Rate is the deliveries per second each Trigger accepts on average
	// (default 2); Burst how many it accepts at once (default 20).
	Rate  float64
	Burst int
}

func (o *HookOptions) defaults() {
	if o.MaxBodyBytes <= 0 {
		o.MaxBodyBytes = defaultHookMaxBody
	}
	if o.Rate <= 0 {
		o.Rate = defaultHookRate
	}
	if o.Burst <= 0 {
		o.Burst = defaultHookBurst
	}
}

// HookResult is the body of a 202 from POST /v1/hooks/{namespace}/{trigger}.
type HookResult struct {
	Subject string `json:"subject"`
	Stream  string `json:"stream"`
	// Seq is the delivery's stream sequence (the original one for a
	// duplicate).
	Seq uint64 `json:"seq"`
	// Duplicate is true when JetStream dropped the delivery as a repeat of
	// an earlier one with the same id.
	Duplicate bool `json:"duplicate,omitempty"`
}

// HooksEnabled reports whether the webhook endpoint is served.
func (s *Service) HooksEnabled() bool { return s.Hooks != nil }

// hookLimiter is a token bucket per Trigger.
type hookLimiter struct {
	rate  rate.Limit
	burst int

	mu      sync.Mutex
	entries map[string]*hookLimiterEntry
}

type hookLimiterEntry struct {
	lim  *rate.Limiter
	used time.Time
}

func newHookLimiter(perSecond float64, burst int) *hookLimiter {
	return &hookLimiter{rate: rate.Limit(perSecond), burst: burst, entries: map[string]*hookLimiterEntry{}}
}

// allow takes a token from key's bucket; when it can't, it returns how long
// until one is available.
func (l *hookLimiter) allow(key string, now time.Time) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.entries) > hookLimiterSweep {
		for k, e := range l.entries {
			if now.Sub(e.used) > hookLimiterIdle {
				delete(l.entries, k)
			}
		}
	}
	e := l.entries[key]
	if e == nil {
		e = &hookLimiterEntry{lim: rate.NewLimiter(l.rate, l.burst)}
		l.entries[key] = e
	}
	e.used = now
	if e.lim.AllowN(now, 1) {
		return true, 0
	}
	r := e.lim.ReserveN(now, 1)
	wait := r.DelayFrom(now)
	r.CancelAt(now)
	return false, wait
}

// lookupHook returns the webhook Trigger at namespace/name. An unknown
// Trigger, one without source.webhook and one in a namespace callers may not
// target all give the same ErrNotFound, so a sender learns nothing about
// which Triggers exist.
func (s *Service) lookupHook(ctx context.Context, namespace, name string) (*aiv1alpha1.Trigger, error) {
	notFound := fmt.Errorf("%w: hook", ErrNotFound)
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
	if trg.Spec.Source.Webhook == nil || !trg.DeletionTimestamp.IsZero() {
		return nil, notFound
	}
	return trg, nil
}

// hookCredential reads the Trigger's credential: the referenced key of a
// Secret in the Trigger's namespace labelled ai.roundtable.io/webhook-secret.
func (s *Service) hookCredential(ctx context.Context, trg *aiv1alpha1.Trigger) ([]byte, error) {
	auth := trg.Spec.Source.Webhook.Auth
	ref := auth.BearerTokenSecretRef
	if hookUsesHMAC(trg) {
		ref = auth.HMACSecretRef
	}
	if ref == nil || ref.Name == "" || ref.Key == "" {
		return nil, fmt.Errorf("%w: no auth secret reference", errHookCredentialUnusable)
	}
	sec := &corev1.Secret{}
	if err := s.Hooks.Get(ctx, types.NamespacedName{Namespace: trg.Namespace, Name: ref.Name}, sec); err != nil {
		if apierrors.IsNotFound(err) || apierrors.IsForbidden(err) {
			return nil, fmt.Errorf("%w: get secret %s: %v", errHookCredentialUnusable, ref.Name, err)
		}
		return nil, fmt.Errorf("%w: get secret %s: %v", ErrUnavailable, ref.Name, err)
	}
	if sec.Labels[aiv1alpha1.LabelWebhookSecret] != labelTrue {
		return nil, fmt.Errorf("%w: secret %s is not labelled %s=true", errHookCredentialUnusable, ref.Name, aiv1alpha1.LabelWebhookSecret)
	}
	val := []byte(strings.TrimRight(string(sec.Data[ref.Key]), "\r\n"))
	if len(val) < minTokenLength {
		return nil, fmt.Errorf("%w: secret %s key %s must hold at least %d characters", errHookCredentialUnusable, ref.Name, ref.Key, minTokenLength)
	}
	return val, nil
}

// hookUsesHMAC reports whether the Trigger authenticates senders by HMAC
// signature (else by bearer token).
func hookUsesHMAC(trg *aiv1alpha1.Trigger) bool {
	return trg.Spec.Source.Webhook.Auth.HMACSecretRef != nil
}

// verifyHook checks the request's credential against the Trigger's. A bearer
// token is compared through fixed-size hashes in constant time; an HMAC
// signature is computed over the raw body and compared with hmac.Equal.
func verifyHook(r *http.Request, body, secret []byte, isHMAC bool) bool {
	if !isHMAC {
		scheme, tok, found := strings.Cut(r.Header.Get("Authorization"), " ")
		if !found || !strings.EqualFold(scheme, "Bearer") {
			return false
		}
		got := sha256.Sum256([]byte(strings.TrimSpace(tok)))
		want := sha256.Sum256(secret)
		return subtle.ConstantTimeCompare(got[:], want[:]) == 1
	}
	sig := r.Header.Get("X-Hub-Signature-256")
	if sig == "" {
		sig = r.Header.Get("X-Signature")
	}
	algo, hexSig, found := strings.Cut(strings.TrimSpace(sig), "=")
	if !found || !strings.EqualFold(algo, "sha256") {
		return false
	}
	got, err := hex.DecodeString(hexSig)
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	return hmac.Equal(got, mac.Sum(nil))
}

// hookMsgID is the delivery's Nats-Msg-Id: the sender's delivery id scoped
// to the Trigger (JetStream dedupes per stream, and the stream is shared by
// every Trigger of the table), or "" when the sender sent none.
func hookMsgID(r *http.Request, trg *aiv1alpha1.Trigger) (string, error) {
	for _, h := range hookIDHeaders {
		id := strings.TrimSpace(r.Header.Get(h))
		if id == "" {
			continue
		}
		if len(id) > maxHookMsgID {
			return "", fmt.Errorf("%w: %s exceeds %d characters", ErrInvalid, h, maxHookMsgID)
		}
		for _, c := range id {
			if c <= ' ' || c > '~' {
				return "", fmt.Errorf("%w: %s must be printable ASCII without spaces", ErrInvalid, h)
			}
		}
		return trg.Namespace + "/" + trg.Name + "/" + id, nil
	}
	return "", nil
}

// hookHeaders are the NATS headers published with a delivery.
func hookHeaders(r *http.Request) nats.Header {
	h := nats.Header{}
	for k, v := range r.Header {
		if strings.HasPrefix(k, "Ce-") {
			h[k] = v
		}
	}
	for _, k := range hookForwardHeaders {
		if v := r.Header.Values(k); len(v) > 0 {
			h[k] = v
		}
	}
	return h
}

// handleHook serves POST /v1/hooks/{namespace}/{trigger}: it authenticates a
// webhook sender with the Trigger's own credential (never the gateway's API
// tokens) and publishes the body to the Trigger's subject on the table's
// events stream. 202 with the stream sequence; 401 on a bad credential; 404
// for an unknown or non-webhook Trigger; 413 for an oversize body; 429 when
// the Trigger's rate limit is exhausted.
func (s *Service) handleHook(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ns, name := r.PathValue("namespace"), r.PathValue("trigger")
	log := s.Log.WithValues("namespace", ns, "trigger", name)
	trg, err := s.lookupHook(ctx, ns, name)
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			log.Error(err, "Webhook Trigger lookup failed")
		}
		writeError(w, err)
		return
	}
	setRequestTable(ctx, aiv1alpha1.RefName(trg.Spec.RoundTableRef))
	if ok, wait := s.hookLimits.allow(ns+"/"+name, s.now()); !ok {
		w.Header().Set("Retry-After", retryAfterSeconds(wait))
		writeError(w, fmt.Errorf("%w: rate limit for this hook exceeded", ErrBusy))
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.Opts.Hooks.MaxBodyBytes))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeJSON(w, http.StatusRequestEntityTooLarge,
				map[string]string{"error": fmt.Sprintf("body exceeds %d bytes", s.Opts.Hooks.MaxBodyBytes)})
			return
		}
		writeError(w, fmt.Errorf("%w: body: %v", ErrInvalid, err))
		return
	}

	isHMAC := hookUsesHMAC(trg)
	secret, err := s.hookCredential(ctx, trg)
	if err != nil {
		if !errors.Is(err, errHookCredentialUnusable) {
			// The credential could not be read (API timeout, 5xx, cancelled
			// context): the sender should retry. The body says nothing about
			// whether the credential exists.
			log.Error(err, "Webhook Trigger credential not readable")
			w.Header().Set("Retry-After", "5")
			writeError(w, fmt.Errorf("%w: the webhook could not be checked, retry later", ErrUnavailable))
			return
		}
		// A misconfigured credential can't be met: refuse like a bad one,
		// and tell the operator, not the sender.
		log.Info("Refused webhook: Trigger credential unusable", "reason", err.Error())
		writeHookUnauthorized(w, isHMAC)
		return
	}
	if !verifyHook(r, body, secret, isHMAC) {
		log.Info("Refused webhook: bad credential", "hmac", isHMAC)
		writeHookUnauthorized(w, isHMAC)
		return
	}

	msgID, err := hookMsgID(r, trg)
	if err != nil {
		writeError(w, err)
		return
	}
	rt := &aiv1alpha1.RoundTable{}
	rtName := aiv1alpha1.RefName(trg.Spec.RoundTableRef)
	if err := s.Hooks.Get(ctx, types.NamespacedName{Namespace: ns, Name: rtName}, rt); err != nil {
		log.Error(err, "Webhook Trigger's RoundTable not readable", "roundTable", rtName)
		writeError(w, fmt.Errorf("%w: the Trigger's RoundTable is not available", ErrUnavailable))
		return
	}
	if rt.Spec.NATS.SubjectPrefix == "" {
		writeError(w, fmt.Errorf("%w: the Trigger's RoundTable has no subject prefix", ErrUnavailable))
		return
	}
	res := &HookResult{
		Subject: natspkg.WebhookEventSubject(rt.Spec.NATS.SubjectPrefix, ns, name),
		Stream:  rt.Spec.NATS.EventsStreamName(),
	}
	res.Seq, res.Duplicate, err = s.Bus.PublishEvent(ctx, res.Stream, res.Subject, body, hookHeaders(r), msgID)
	if err != nil {
		log.Error(err, "Failed to publish webhook delivery", "subject", res.Subject)
		writeError(w, fmt.Errorf("%w: publish failed", ErrUnavailable))
		return
	}
	log.Info("Accepted webhook delivery", "subject", res.Subject, "seq", res.Seq, "duplicate", res.Duplicate)
	writeJSON(w, http.StatusAccepted, res)
}

func writeHookUnauthorized(w http.ResponseWriter, isHMAC bool) {
	if !isHMAC {
		w.Header().Set("WWW-Authenticate", `Bearer realm="roundtable-hook"`)
	}
	writeJSON(w, http.StatusUnauthorized, map[string]string{"error": errHookUnauthorized.Error()})
}
