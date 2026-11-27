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
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	authnv1 "k8s.io/api/authentication/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Caller kinds: the caller_kind metric label and the callerKind log field.
const (
	// CallerServiceAccount is a Kubernetes ServiceAccount, authenticated by
	// a TokenReview of its token.
	CallerServiceAccount = "serviceaccount"
	// CallerToken is a static token from the gateway's token file.
	CallerToken = "token"
	// callerNone is an unauthenticated request (refused, /healthz, webhooks).
	callerNone = "none"
)

// DefaultServiceAccountAudience is the audience a ServiceAccount token must
// be issued for (the projected token's audience).
const DefaultServiceAccountAudience = "roundtable-gateway"

// serviceAccountPrefix starts the user name of a ServiceAccount token:
// system:serviceaccount:<namespace>:<name>.
const serviceAccountPrefix = "system:serviceaccount:"

// maxBearerBytes bounds a bearer token sent on to a TokenReview.
const maxBearerBytes = 16 << 10

// Caller is who made a request.
type Caller struct {
	// Kind is CallerServiceAccount or CallerToken.
	Kind string
	// Name is "<namespace>/<name>" for a ServiceAccount, the token's name for
	// a static token.
	Name string
}

// String is the caller as recorded in logs, task records and the
// ai.roundtable.io/caller annotation. Token names hold no "/", so a
// ServiceAccount and a token cannot look alike.
func (c Caller) String() string { return c.Name }

type callerKey struct{}

// WithCaller returns ctx carrying the request's caller.
func WithCaller(ctx context.Context, c Caller) context.Context {
	return context.WithValue(ctx, callerKey{}, c)
}

// CallerFrom returns the caller of the request in ctx, if authenticated.
func CallerFrom(ctx context.Context) (Caller, bool) {
	c, ok := ctx.Value(callerKey{}).(Caller)
	return c, ok
}

// callerName is the recorded caller of ctx ("" when none).
func callerName(ctx context.Context) string {
	c, _ := CallerFrom(ctx)
	return c.String()
}

// authError is a refused credential: 401 (not authenticated) or 403
// (authenticated but not allowed).
type authError struct {
	forbidden bool
	reason    string
}

func (e *authError) Error() string { return e.reason }

func unauthenticated(format string, args ...any) error {
	return &authError{reason: fmt.Sprintf(format, args...)}
}

func forbidden(format string, args ...any) error {
	return &authError{forbidden: true, reason: fmt.Sprintf(format, args...)}
}

// reviewLimitedError is a TokenReview refused by the review rate limit: 429,
// retry after wait.
type reviewLimitedError struct{ wait time.Duration }

func (e *reviewLimitedError) Error() string {
	return fmt.Sprintf("%v: too many token reviews, retry later", ErrBusy)
}

func (e *reviewLimitedError) Unwrap() error { return ErrBusy }

// retryAfterSeconds is the Retry-After value for a wait (at least 1).
func retryAfterSeconds(wait time.Duration) string {
	return strconv.Itoa(int(math.Ceil(max(wait.Seconds(), 1))))
}

// Defaults of the TokenReview rate limit: generous, so normal callers never
// reach it.
const (
	DefaultReviewRate  = 50.0
	DefaultReviewBurst = 100
)

// reviewLimitKey is the single bucket every uncached TokenReview draws from.
const reviewLimitKey = "tokenreview"

// TokenReviewer validates a bearer token for the given audiences.
type TokenReviewer interface {
	Review(ctx context.Context, token string, audiences []string) (authnv1.TokenReviewStatus, error)
}

// KubeTokenReviewer creates TokenReviews through the API server (needs create
// on tokenreviews.authentication.k8s.io).
type KubeTokenReviewer struct{ Client client.Client }

// Review implements TokenReviewer.
func (k KubeTokenReviewer) Review(ctx context.Context, token string, audiences []string) (authnv1.TokenReviewStatus, error) {
	tr := &authnv1.TokenReview{Spec: authnv1.TokenReviewSpec{Token: token, Audiences: audiences}}
	if err := k.Client.Create(ctx, tr); err != nil {
		return authnv1.TokenReviewStatus{}, err
	}
	return tr.Status, nil
}

// ServiceAccountAuth authenticates in-cluster callers by their Kubernetes
// ServiceAccount token: a TokenReview with the gateway's audience must
// authenticate it as system:serviceaccount:<namespace>:<name>, in an allowed
// namespace. Every such ServiceAccount may use the whole API.
//
// Results are cached by token digest: a success until shortly before the
// token expires (at most MaxCacheTTL), a refusal for FailureTTL.
type ServiceAccountAuth struct {
	Reviewer TokenReviewer
	// Audience the token must be issued for (default
	// DefaultServiceAccountAudience).
	Audience string
	// AllowedNamespaces restricts the ServiceAccounts' namespaces. Empty or
	// holding "*" allows any namespace.
	AllowedNamespaces []string
	// MaxCacheTTL caps how long a success is cached (default 5m), so a
	// revoked token (its pod or ServiceAccount deleted) stops working soon.
	MaxCacheTTL time.Duration
	// ExpirySkew is how long before a token's expiry its cached success is
	// dropped (default 30s).
	ExpirySkew time.Duration
	// FailureTTL is how long a refusal is cached (default 5s).
	FailureTTL time.Duration
	// MaxEntries bounds the cache (default 4096).
	MaxEntries int
	// ReviewRate is the TokenReviews per second the gateway sends to the API
	// server on average, cache hits excluded (default DefaultReviewRate).
	ReviewRate float64
	// ReviewBurst is how many TokenReviews may go out at once (default
	// DefaultReviewBurst). Beyond them a caller gets 429 with Retry-After.
	ReviewBurst int

	now     func() time.Time
	mu      sync.Mutex
	cache   map[[32]byte]saEntry
	limiter *hookLimiter
}

type saEntry struct {
	caller  Caller
	err     error
	expires time.Time
}

func (a *ServiceAccountAuth) defaults() {
	if a.Audience == "" {
		a.Audience = DefaultServiceAccountAudience
	}
	if a.MaxCacheTTL <= 0 {
		a.MaxCacheTTL = 5 * time.Minute
	}
	if a.ExpirySkew <= 0 {
		a.ExpirySkew = 30 * time.Second
	}
	if a.FailureTTL <= 0 {
		a.FailureTTL = 5 * time.Second
	}
	if a.MaxEntries <= 0 {
		a.MaxEntries = 4096
	}
	if a.ReviewRate <= 0 {
		a.ReviewRate = DefaultReviewRate
	}
	if a.ReviewBurst <= 0 {
		a.ReviewBurst = DefaultReviewBurst
	}
	if a.limiter == nil {
		a.limiter = newHookLimiter(a.ReviewRate, a.ReviewBurst)
	}
	if a.now == nil {
		a.now = time.Now
	}
	if a.cache == nil {
		a.cache = map[[32]byte]saEntry{}
	}
}

// Authenticate returns the ServiceAccount the token belongs to. A refused
// token is an *authError; a failed TokenReview wraps ErrUnavailable.
func (a *ServiceAccountAuth) Authenticate(ctx context.Context, token string) (Caller, error) {
	if token == "" || len(token) > maxBearerBytes {
		return Caller{}, unauthenticated("no usable bearer token")
	}
	key := sha256.Sum256([]byte(token))
	a.mu.Lock()
	a.defaults()
	now := a.now()
	if e, ok := a.cache[key]; ok {
		if now.Before(e.expires) {
			a.mu.Unlock()
			return e.caller, e.err
		}
		delete(a.cache, key)
	}
	a.mu.Unlock()

	// The expiry claim is read unverified: it only shortens the cache and
	// skips a TokenReview for a token that is already expired.
	exp, hasExp := jwtExpiry(token)
	if hasExp && !now.Before(exp) {
		a.store(key, saEntry{err: unauthenticated("token expired"), expires: now.Add(a.FailureTTL)})
		return Caller{}, unauthenticated("token expired")
	}

	// Only a cache miss reaches here: bound how many reviews go to the API
	// server, whatever the tokens.
	if ok, wait := a.limiter.allow(reviewLimitKey, now); !ok {
		return Caller{}, &reviewLimitedError{wait: wait}
	}
	status, err := a.Reviewer.Review(ctx, token, []string{a.Audience})
	if err != nil {
		// Not cached: the API server may be back on the next request.
		return Caller{}, fmt.Errorf("%w: token review: %v", ErrUnavailable, err)
	}
	caller, err := a.check(status)
	if err != nil {
		a.store(key, saEntry{err: err, expires: now.Add(a.FailureTTL)})
		return Caller{}, err
	}
	until := now.Add(a.MaxCacheTTL)
	if hasExp {
		until = minTime(until, exp.Add(-a.ExpirySkew))
	}
	if now.Before(until) {
		a.store(key, saEntry{caller: caller, expires: until})
	}
	return caller, nil
}

// check turns a TokenReview status into the caller, or a refusal.
func (a *ServiceAccountAuth) check(st authnv1.TokenReviewStatus) (Caller, error) {
	if !st.Authenticated {
		reason := st.Error
		if reason == "" {
			reason = "token not authenticated"
		}
		return Caller{}, unauthenticated("%s", reason)
	}
	if !slices.Contains(st.Audiences, a.Audience) {
		return Caller{}, unauthenticated("token not issued for audience %q", a.Audience)
	}
	rest, ok := strings.CutPrefix(st.User.Username, serviceAccountPrefix)
	ns, name, ok2 := strings.Cut(rest, ":")
	if !ok || !ok2 || ns == "" || name == "" || strings.Contains(name, ":") {
		return Caller{}, forbidden("user %q is not a ServiceAccount", st.User.Username)
	}
	if len(a.AllowedNamespaces) > 0 && !slices.Contains(a.AllowedNamespaces, "*") &&
		!slices.Contains(a.AllowedNamespaces, ns) {
		return Caller{}, forbidden("ServiceAccount %s/%s: namespace %q is not allowed", ns, name, ns)
	}
	return Caller{Kind: CallerServiceAccount, Name: ns + "/" + name}, nil
}

// store caches e, keeping the cache within MaxEntries.
func (a *ServiceAccountAuth) store(key [32]byte, e saEntry) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.defaults()
	if _, ok := a.cache[key]; !ok && len(a.cache) >= a.MaxEntries {
		now := a.now()
		for k, v := range a.cache {
			if !now.Before(v.expires) {
				delete(a.cache, k)
			}
		}
		// Still full: drop arbitrary entries (map order) to make room.
		for k := range a.cache {
			if len(a.cache) < a.MaxEntries {
				break
			}
			delete(a.cache, k)
		}
	}
	a.cache[key] = e
}

// cached reports the number of cache entries (tests).
func (a *ServiceAccountAuth) cached() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.cache)
}

// jwtExpiry reads the exp claim of a JWT without verifying it.
func jwtExpiry(token string) (time.Time, bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, false
	}
	var claims struct {
		Exp *json.Number `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Exp == nil {
		return time.Time{}, false
	}
	sec, err := claims.Exp.Int64()
	if err != nil {
		f, ferr := claims.Exp.Float64()
		if ferr != nil {
			return time.Time{}, false
		}
		sec = int64(f)
	}
	return time.Unix(sec, 0), true
}

func minTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

// authStatus maps an authentication error to its HTTP status.
func authStatus(err error) int {
	var ae *authError
	switch {
	case errors.As(err, &ae) && ae.forbidden:
		return 403
	case errors.Is(err, ErrBusy):
		return 429
	case errors.Is(err, ErrUnavailable):
		return 503
	default:
		return 401
	}
}
