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

// Package github is a minimal GitHub REST client for the issue pickup
// controller: list labelled issues, edit labels and assignees, comment.
// It needs only the issues permission (read/write) on the target repos.
package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DefaultAPIURL is the public GitHub REST endpoint.
const DefaultAPIURL = "https://api.github.com"

// Issue is the subset of a GitHub issue the pickup controller reads.
type Issue struct {
	Number int    `json:"number"`
	Title  string `json:"title"`
	Body   string `json:"body"`
	// HTMLURL is the issue's web URL.
	HTMLURL string  `json:"html_url"`
	Labels  []Label `json:"labels"`
	// PullRequest is set when the item is a pull request (the issues API
	// lists both).
	PullRequest *struct{} `json:"pull_request,omitempty"`
}

// Label is a GitHub label.
type Label struct {
	Name string `json:"name"`
}

// HasLabel reports whether the issue carries the named label
// (case-insensitive, like GitHub).
func (i *Issue) HasLabel(name string) bool {
	for _, l := range i.Labels {
		if strings.EqualFold(l.Name, name) {
			return true
		}
	}
	return false
}

// Client calls the GitHub REST API with a bearer token.
type Client struct {
	// BaseURL defaults to DefaultAPIURL.
	BaseURL string
	Token   string
	HTTP    *http.Client
}

// NewClient returns a client for baseURL ("" means DefaultAPIURL).
func NewClient(baseURL, token string) *Client {
	if baseURL == "" {
		baseURL = DefaultAPIURL
	}
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), Token: token, HTTP: &http.Client{Timeout: 30 * time.Second}}
}

// APIError is a non-2xx GitHub response.
type APIError struct {
	Method, Path string
	Status       int
	Message      string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("github %s %s: %d %s", e.Method, e.Path, e.Status, e.Message)
}

func (c *Client) do(ctx context.Context, method, path string, query url.Values, body, out any) error {
	u := c.BaseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("User-Agent", "roundtable-operator")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("github %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		var msg struct {
			Message string `json:"message"`
		}
		_ = json.Unmarshal(data, &msg)
		return &APIError{Method: method, Path: path, Status: resp.StatusCode, Message: msg.Message}
	}
	if out != nil && len(data) > 0 {
		return json.Unmarshal(data, out)
	}
	return nil
}

func splitRepo(repo string) (string, string, error) {
	owner, name, ok := strings.Cut(repo, "/")
	if !ok || owner == "" || name == "" || strings.Contains(name, "/") {
		return "", "", fmt.Errorf("repo %q is not owner/name", repo)
	}
	return url.PathEscape(owner), url.PathEscape(name), nil
}

func issuePath(repo string, number int, suffix string) (string, error) {
	owner, name, err := splitRepo(repo)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("/repos/%s/%s/issues/%d%s", owner, name, number, suffix), nil
}

// ListOpenIssues returns up to 100 open issues (not pull requests) carrying
// label, oldest first.
func (c *Client) ListOpenIssues(ctx context.Context, repo, label string) ([]Issue, error) {
	owner, name, err := splitRepo(repo)
	if err != nil {
		return nil, err
	}
	q := url.Values{
		"state": {"open"}, "labels": {label}, "sort": {"created"},
		"direction": {"asc"}, "per_page": {"100"},
	}
	var all []Issue
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/%s/issues", owner, name), q, nil, &all); err != nil {
		return nil, err
	}
	issues := all[:0]
	for _, i := range all {
		if i.PullRequest == nil {
			issues = append(issues, i)
		}
	}
	return issues, nil
}

// PullRequest is the subset of a GitHub pull request the pickup controller
// reads.
type PullRequest struct {
	Number  int     `json:"number"`
	Title   string  `json:"title"`
	HTMLURL string  `json:"html_url"`
	Labels  []Label `json:"labels"`
	Head    struct {
		Ref string `json:"ref"`
	} `json:"head"`
}

// HasLabel reports whether the pull request carries the named label
// (case-insensitive, like GitHub).
func (p *PullRequest) HasLabel(name string) bool {
	for _, l := range p.Labels {
		if strings.EqualFold(l.Name, name) {
			return true
		}
	}
	return false
}

// ListOpenPullRequests returns the open pull requests carrying label (the
// first 100 open PRs, newest first; needs the pull requests read permission).
func (c *Client) ListOpenPullRequests(ctx context.Context, repo, label string) ([]PullRequest, error) {
	owner, name, err := splitRepo(repo)
	if err != nil {
		return nil, err
	}
	q := url.Values{"state": {"open"}, "per_page": {"100"}}
	var all []PullRequest
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/%s/pulls", owner, name), q, nil, &all); err != nil {
		return nil, err
	}
	prs := all[:0]
	for _, p := range all {
		if p.HasLabel(label) {
			prs = append(prs, p)
		}
	}
	return prs, nil
}

// AddLabels adds labels to an issue (labels that do not exist are created).
func (c *Client) AddLabels(ctx context.Context, repo string, number int, labels ...string) error {
	p, err := issuePath(repo, number, "/labels")
	if err != nil {
		return err
	}
	return c.do(ctx, http.MethodPost, p, nil, map[string][]string{"labels": labels}, nil)
}

// RemoveLabel removes a label from an issue. A label the issue does not have
// is not an error.
func (c *Client) RemoveLabel(ctx context.Context, repo string, number int, label string) error {
	p, err := issuePath(repo, number, "/labels/"+url.PathEscape(label))
	if err != nil {
		return err
	}
	err = c.do(ctx, http.MethodDelete, p, nil, nil, nil)
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound {
		return nil
	}
	return err
}

// AddAssignees assigns users to an issue. GitHub silently ignores logins
// that cannot be assigned.
func (c *Client) AddAssignees(ctx context.Context, repo string, number int, logins ...string) error {
	p, err := issuePath(repo, number, "/assignees")
	if err != nil {
		return err
	}
	return c.do(ctx, http.MethodPost, p, nil, map[string][]string{"assignees": logins}, nil)
}

// Comment posts a comment on an issue.
func (c *Client) Comment(ctx context.Context, repo string, number int, body string) error {
	p, err := issuePath(repo, number, "/comments")
	if err != nil {
		return err
	}
	return c.do(ctx, http.MethodPost, p, nil, map[string]string{"body": body}, nil)
}
