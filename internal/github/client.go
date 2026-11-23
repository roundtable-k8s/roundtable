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

// Package github is the small GitHub REST client the PR feedback watcher
// uses: pull requests, check runs, merge-queue (merge_group) workflow runs,
// job logs, labels, comments and issues. It only needs read access to pull
// requests, checks and Actions runs (public repositories need none) and
// write access to issues and pull requests.
package github

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DefaultBaseURL is the public GitHub REST API.
const DefaultBaseURL = "https://api.github.com"

// maxLogBytes caps how much of a job log is read.
const maxLogBytes = 8 << 20

// Client calls the GitHub REST API with a token.
type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

// New returns a client for baseURL (DefaultBaseURL when empty).
func New(baseURL, token string) *Client {
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), Token: token, HTTP: &http.Client{Timeout: 30 * time.Second}}
}

// Error is a non-2xx response.
type Error struct {
	Method, Path string
	Status       int
	Body         string
}

func (e *Error) Error() string {
	return fmt.Sprintf("github %s %s: %d %s", e.Method, e.Path, e.Status, e.Body)
}

// IsNotFound reports whether err is a 404 (or a 410) from GitHub.
func IsNotFound(err error) bool {
	if e, ok := err.(*Error); ok {
		return e.Status == http.StatusNotFound || e.Status == http.StatusGone
	}
	return false
}

func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, rd)
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
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return &Error{Method: method, Path: path, Status: resp.StatusCode, Body: strings.TrimSpace(string(msg))}
	}
	if out == nil {
		return nil
	}
	if s, ok := out.(*string); ok {
		b, err := io.ReadAll(io.LimitReader(resp.Body, maxLogBytes))
		if err != nil {
			return err
		}
		*s = string(b)
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// User is a GitHub account.
type User struct {
	Login string `json:"login"`
}

// Label is an issue or pull request label.
type Label struct {
	Name string `json:"name"`
}

// PullRequest is the subset of a pull request the watcher reads.
type PullRequest struct {
	Number  int64   `json:"number"`
	HTMLURL string  `json:"html_url"`
	State   string  `json:"state"`
	Title   string  `json:"title"`
	Draft   bool    `json:"draft"`
	Merged  bool    `json:"merged"`
	User    User    `json:"user"`
	Labels  []Label `json:"labels"`
	Head    struct {
		SHA string `json:"sha"`
		Ref string `json:"ref"`
	} `json:"head"`
	Base struct {
		Ref string `json:"ref"`
	} `json:"base"`
}

// HasLabel reports whether the pull request carries label name.
func (p *PullRequest) HasLabel(name string) bool {
	for _, l := range p.Labels {
		if strings.EqualFold(l.Name, name) {
			return true
		}
	}
	return false
}

// ListOpenPulls returns the open pull requests of repo (owner/name), newest
// first, at most 100.
func (c *Client) ListOpenPulls(ctx context.Context, repo string) ([]PullRequest, error) {
	var out []PullRequest
	err := c.do(ctx, http.MethodGet, "/repos/"+repo+"/pulls?state=open&per_page=100", nil, &out)
	return out, err
}

// GetPull returns one pull request.
func (c *Client) GetPull(ctx context.Context, repo string, number int64) (*PullRequest, error) {
	out := &PullRequest{}
	err := c.do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/pulls/%d", repo, number), nil, out)
	return out, err
}

// CheckRun is one check run on a commit. For GitHub Actions the check run ID
// is also the job ID.
type CheckRun struct {
	ID          int64      `json:"id"`
	Name        string     `json:"name"`
	Status      string     `json:"status"`
	Conclusion  string     `json:"conclusion"`
	HTMLURL     string     `json:"html_url"`
	DetailsURL  string     `json:"details_url"`
	StartedAt   *time.Time `json:"started_at"`
	CompletedAt *time.Time `json:"completed_at"`
	App         *struct {
		Slug string `json:"slug"`
	} `json:"app"`
	Output struct {
		Title   string `json:"title"`
		Summary string `json:"summary"`
	} `json:"output"`
}

// IsActions reports whether the check run is a GitHub Actions job.
func (r *CheckRun) IsActions() bool {
	return r.App != nil && r.App.Slug == "github-actions"
}

// ListCheckRuns returns every check run (all attempts, not only the latest)
// on commit sha.
func (c *Client) ListCheckRuns(ctx context.Context, repo, sha string) ([]CheckRun, error) {
	var out struct {
		CheckRuns []CheckRun `json:"check_runs"`
	}
	err := c.do(ctx, http.MethodGet, "/repos/"+repo+"/commits/"+url.PathEscape(sha)+"/check-runs?filter=all&per_page=100", nil, &out)
	return out.CheckRuns, err
}

// WorkflowRun is one GitHub Actions workflow run.
type WorkflowRun struct {
	ID         int64     `json:"id"`
	Name       string    `json:"name"`
	HeadBranch string    `json:"head_branch"`
	HeadSHA    string    `json:"head_sha"`
	Event      string    `json:"event"`
	Status     string    `json:"status"`
	Conclusion string    `json:"conclusion"`
	HTMLURL    string    `json:"html_url"`
	CreatedAt  time.Time `json:"created_at"`
}

// ListMergeGroupRuns returns the newest merge_group workflow runs of repo
// (at most 100), newest first.
func (c *Client) ListMergeGroupRuns(ctx context.Context, repo string) ([]WorkflowRun, error) {
	var out struct {
		WorkflowRuns []WorkflowRun `json:"workflow_runs"`
	}
	err := c.do(ctx, http.MethodGet, "/repos/"+repo+"/actions/runs?event=merge_group&per_page=100", nil, &out)
	return out.WorkflowRuns, err
}

// Job is one job of a workflow run.
type Job struct {
	ID         int64  `json:"id"`
	Name       string `json:"name"`
	Conclusion string `json:"conclusion"`
	HTMLURL    string `json:"html_url"`
}

// ListRunJobs returns the jobs of the latest attempt of a workflow run.
func (c *Client) ListRunJobs(ctx context.Context, repo string, runID int64) ([]Job, error) {
	var out struct {
		Jobs []Job `json:"jobs"`
	}
	err := c.do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/actions/runs/%d/jobs?per_page=100", repo, runID), nil, &out)
	return out.Jobs, err
}

// JobLog returns the plain-text log of an Actions job (at most 8 MiB).
func (c *Client) JobLog(ctx context.Context, repo string, jobID int64) (string, error) {
	var out string
	err := c.do(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/actions/jobs/%d/logs", repo, jobID), nil, &out)
	return out, err
}

// AddLabels adds labels to an issue or pull request (GitHub creates labels
// that do not exist yet).
func (c *Client) AddLabels(ctx context.Context, repo string, number int64, labels ...string) error {
	return c.do(ctx, http.MethodPost, fmt.Sprintf("/repos/%s/issues/%d/labels", repo, number), map[string]any{"labels": labels}, nil)
}

// Comment posts a comment on an issue or pull request.
func (c *Client) Comment(ctx context.Context, repo string, number int64, body string) error {
	return c.do(ctx, http.MethodPost, fmt.Sprintf("/repos/%s/issues/%d/comments", repo, number), map[string]any{"body": body}, nil)
}

// Issue is the subset of an issue the watcher reads.
type Issue struct {
	Number  int64  `json:"number"`
	Title   string `json:"title"`
	HTMLURL string `json:"html_url"`
}

// ListOpenIssuesWithLabel returns open issues carrying label (at most 100).
func (c *Client) ListOpenIssuesWithLabel(ctx context.Context, repo, label string) ([]Issue, error) {
	var out []Issue
	err := c.do(ctx, http.MethodGet, "/repos/"+repo+"/issues?state=open&per_page=100&labels="+url.QueryEscape(label), nil, &out)
	return out, err
}

// CreateIssue opens an issue.
func (c *Client) CreateIssue(ctx context.Context, repo, title, body string, labels ...string) (*Issue, error) {
	out := &Issue{}
	err := c.do(ctx, http.MethodPost, "/repos/"+repo+"/issues", map[string]any{"title": title, "body": body, "labels": labels}, out)
	return out, err
}
