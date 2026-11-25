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

// Package firetrigger is the manager's fire-trigger subcommand: the one call
// a schedule Trigger's CronJob pod makes to the gateway,
// POST /v1/triggers/{namespace}/{name}/fire, authenticated with the pod's
// projected ServiceAccount token. It needs no Kubernetes client.
package firetrigger

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// Command is the subcommand's name: manager fire-trigger ...
const Command = "fire-trigger"

// DefaultTokenFile is where the CronJob pod mounts its projected token.
const DefaultTokenFile = "/var/run/secrets/roundtable/trigger/token"

// DefaultTimeout bounds the whole call.
const DefaultTimeout = 30 * time.Second

// maxErrorBody bounds how much of an error response is reported.
const maxErrorBody = 512

// Options is one fire-trigger call.
type Options struct {
	// Gateway is the gateway's base URL (http://host:port).
	Gateway string
	// Namespace and Name identify the Trigger.
	Namespace, Name string
	// TokenFile holds the bearer token (the projected ServiceAccount token).
	TokenFile string
	// ScheduledTime is sent as the firing's scheduled time; zero lets the
	// gateway use now.
	ScheduledTime time.Time
	// Timeout bounds the call (default DefaultTimeout).
	Timeout time.Duration
	// Client makes the request (default http.DefaultClient's transport).
	Client *http.Client
}

// Result is the gateway's 202 body.
type Result struct {
	Subject   string `json:"subject"`
	Seq       uint64 `json:"seq"`
	Duplicate bool   `json:"duplicate"`
	Key       string `json:"key"`
}

// Fire makes the call and returns the gateway's answer, or an error saying
// why the Trigger was not fired.
func Fire(ctx context.Context, o Options) (*Result, error) {
	base, err := url.Parse(strings.TrimSpace(o.Gateway))
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" {
		return nil, fmt.Errorf("--gateway %q must be an http(s) URL", o.Gateway)
	}
	if o.Namespace == "" || o.Name == "" {
		return nil, errors.New("--trigger must be <namespace>/<name>")
	}
	raw, err := os.ReadFile(o.TokenFile)
	if err != nil {
		return nil, fmt.Errorf("read token: %w", err)
	}
	token := strings.TrimSpace(string(raw))
	if token == "" {
		return nil, fmt.Errorf("token file %s is empty", o.TokenFile)
	}
	body := []byte("{}")
	if !o.ScheduledTime.IsZero() {
		body, _ = json.Marshal(map[string]time.Time{"scheduledTime": o.ScheduledTime.UTC()})
	}
	timeout := o.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	endpoint := base.JoinPath("v1", "triggers", o.Namespace, o.Name, "fire")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	hc := o.Client
	if hc == nil {
		hc = &http.Client{}
	}
	resp, err := hc.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("gateway did not answer within %s: %w", timeout, err)
		}
		return nil, fmt.Errorf("call gateway: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode != http.StatusAccepted {
		msg := strings.TrimSpace(string(respBody))
		var e struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(respBody, &e) == nil && e.Error != "" {
			msg = e.Error
		}
		if len(msg) > maxErrorBody {
			msg = msg[:maxErrorBody] + "..."
		}
		return nil, fmt.Errorf("gateway refused to fire Trigger %s/%s: %s: %s", o.Namespace, o.Name, resp.Status, msg)
	}
	res := &Result{}
	if err := json.Unmarshal(respBody, res); err != nil {
		return nil, fmt.Errorf("decode gateway answer: %w", err)
	}
	return res, nil
}

// Run parses the subcommand's arguments and fires the Trigger, writing one
// line to stdout on success and the reason to stderr on failure. It returns
// the process exit code.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet(Command, flag.ContinueOnError)
	fs.SetOutput(stderr)
	var o Options
	var trigger, scheduled string
	fs.StringVar(&o.Gateway, "gateway", os.Getenv("ROUNDTABLE_GATEWAY_URL"), "Base URL of the Round Table gateway.")
	fs.StringVar(&trigger, "trigger", "", "The Trigger to fire, <namespace>/<name>.")
	fs.StringVar(&o.TokenFile, "token-file", DefaultTokenFile, "File holding the bearer token (the projected ServiceAccount token).")
	fs.StringVar(&scheduled, "scheduled-time", "",
		"The schedule time this firing is for (RFC 3339). Empty: the gateway uses now, truncated to the minute.")
	fs.DurationVar(&o.Timeout, "timeout", DefaultTimeout, "Upper bound on the call.")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintf(stderr, "%s: unexpected arguments %q\n", Command, fs.Args())
		return 2
	}
	ns, name, ok := strings.Cut(trigger, "/")
	if !ok || ns == "" || name == "" || strings.Contains(name, "/") {
		_, _ = fmt.Fprintf(stderr, "%s: --trigger must be <namespace>/<name>, got %q\n", Command, trigger)
		return 2
	}
	o.Namespace, o.Name = ns, name
	if scheduled != "" {
		t, err := time.Parse(time.RFC3339, scheduled)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "%s: --scheduled-time: %v\n", Command, err)
			return 2
		}
		o.ScheduledTime = t
	}
	res, err := Fire(ctx, o)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "%s: %v\n", Command, err)
		return 1
	}
	dup := ""
	if res.Duplicate {
		dup = " (duplicate)"
	}
	_, _ = fmt.Fprintf(stdout, "fired Trigger %s/%s: key %s, %s seq %d%s\n", ns, name, res.Key, res.Subject, res.Seq, dup)
	return 0
}
