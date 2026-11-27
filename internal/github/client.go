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

// Package github is a thin wrapper over google/go-github for the issue pickup
// and PR feedback controllers: list labelled issues and pull requests, edit
// labels and assignees, comment, read checks and job logs. It maps the
// library's pointer-heavy types into the small structs below.
package github

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	gh "github.com/google/go-github/v93/github"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/roundtable-k8s/roundtable/pkg/metrics"
)

// DefaultAPIURL is the public GitHub REST endpoint.
const DefaultAPIURL = "https://api.github.com"

// Issue is the subset of a GitHub issue the pickup controller reads.
type Issue struct {
	Number int    `json:"number"`
	Title  string `json:"title"`
	Body   string `json:"body"`
	// HTMLURL is the issue's web URL.
	HTMLURL string `json:"html_url"`
	// State is "open" or "closed".
	State string `json:"state,omitempty"`
	// StateReason is why a closed issue was closed: "completed",
	// "not_planned" or "reopened" (empty for old or open issues).
	StateReason string  `json:"state_reason,omitempty"`
	Labels      []Label `json:"labels"`
	// User is the issue's author.
	User User `json:"user"`
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

// Client calls the GitHub API through go-github.
type Client struct {
	gh *gh.Client
}

// NewClient returns a client for baseURL ("" means DefaultAPIURL). An empty
// token makes unauthenticated requests.
func NewClient(baseURL, token string) *Client {
	opts := []gh.ClientOptionsFunc{
		gh.WithTimeout(30 * time.Second),
		gh.WithUserAgent("roundtable-operator"),
	}
	if token != "" {
		opts = append(opts, gh.WithAuthToken(token))
	}
	if baseURL != "" && strings.TrimRight(baseURL, "/") != DefaultAPIURL {
		opts = append(opts, gh.WithURLs(&baseURL, nil))
	}
	c, err := gh.NewClient(opts...)
	if err != nil {
		// Only a malformed baseURL fails; surface it on first use.
		return &Client{gh: nil}
	}
	return &Client{gh: c}
}

func (c *Client) api() (*gh.Client, error) {
	if c.gh == nil {
		return nil, errors.New("github: invalid API base URL")
	}
	return c.gh, nil
}

// graphQLURL returns the GraphQL endpoint for a REST API base URL. On
// api.github.com that is <base>/graphql. On GitHub Enterprise Server the REST
// base ends in /api/v3 and GraphQL lives at the sibling /api/graphql.
func graphQLURL(base string) string {
	b := strings.TrimRight(base, "/")
	if root, ok := strings.CutSuffix(b, "/api/v3"); ok {
		return root + "/api/graphql"
	}
	return b + "/graphql"
}

func splitRepo(repo string) (string, string, error) {
	owner, name, ok := strings.Cut(repo, "/")
	if !ok || owner == "" || name == "" || strings.Contains(name, "/") {
		return "", "", fmt.Errorf("repo %q is not owner/name", repo)
	}
	return owner, name, nil
}

// maxLogBytes caps how much of a job log is read.
const maxLogBytes = 8 << 20

// maxPages bounds how many pages a paginated list call follows (of 100).
const maxPages = 5

// paginate follows resp.NextPage for up to maxPages pages of 100, passing
// each page's ListOptions to fetch and collecting what it returns.
func paginate[T any](call string, fetch func(gh.ListOptions) ([]T, *gh.Response, error)) ([]T, error) {
	var all []T
	opt := gh.ListOptions{PerPage: 100}
	for pages := 1; pages <= maxPages; pages++ {
		items, resp, err := fetch(opt)
		if err != nil {
			return all, err
		}
		all = append(all, items...)
		if resp == nil || resp.NextPage == 0 {
			break
		}
		if pages == maxPages {
			// The cap is reached with more pages left: say so, the rest of
			// the result set is not read.
			metrics.GithubPaginationTruncatedTotal.WithLabelValues(call).Inc()
			logf.Log.WithName("github").V(1).Info("pagination truncated at page cap; later results are not read", "call", call, "maxPages", maxPages)
			break
		}
		opt.Page = resp.NextPage
	}
	return all, nil
}

// IsNotFound reports whether err is a 404 or 410 from GitHub.
func IsNotFound(err error) bool {
	var er *gh.ErrorResponse
	if errors.As(err, &er) && er.Response != nil {
		return er.Response.StatusCode == http.StatusNotFound || er.Response.StatusCode == http.StatusGone
	}
	return false
}

func statusIs(err error, code int) bool {
	var er *gh.ErrorResponse
	return errors.As(err, &er) && er.Response != nil && er.Response.StatusCode == code
}

func toIssue(i *gh.Issue) Issue {
	out := Issue{
		Number: i.GetNumber(), Title: i.GetTitle(), Body: i.GetBody(), HTMLURL: i.GetHTMLURL(),
		State: i.GetState(), StateReason: i.GetStateReason(), Labels: toLabels(i.Labels),
		User: User{Login: i.GetUser().GetLogin()},
	}
	if i.PullRequestLinks != nil {
		out.PullRequest = &struct{}{}
	}
	return out
}

func toLabels(in []*gh.Label) []Label {
	out := make([]Label, 0, len(in))
	for _, l := range in {
		out = append(out, Label{Name: l.GetName()})
	}
	return out
}

func toPull(p *gh.PullRequest) PullRequest {
	out := PullRequest{
		Number: p.GetNumber(), HTMLURL: p.GetHTMLURL(), State: p.GetState(), Title: p.GetTitle(), Body: p.GetBody(),
		Draft: p.GetDraft(), Merged: p.GetMerged(), MergeableState: p.GetMergeableState(),
		NodeID: p.GetNodeID(), User: User{Login: p.GetUser().GetLogin()}, Labels: toLabels(p.Labels),
	}
	if p.AutoMerge != nil {
		out.AutoMerge = &struct{}{}
	}
	out.Head.SHA, out.Head.Ref = p.GetHead().GetSHA(), p.GetHead().GetRef()
	out.Base.Ref = p.GetBase().GetRef()
	return out
}

// ListOpenIssues returns the open issues (not pull requests) carrying label,
// oldest first.
func (c *Client) ListOpenIssues(ctx context.Context, repo, label string) ([]Issue, error) {
	all, err := c.listIssues(ctx, repo, &gh.IssueListByRepoOptions{
		State: "open", Labels: []string{label}, Sort: "created", Direction: "asc",
	})
	if err != nil {
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

func (c *Client) listIssues(ctx context.Context, repo string, opts *gh.IssueListByRepoOptions) ([]Issue, error) {
	g, err := c.api()
	if err != nil {
		return nil, err
	}
	owner, name, err := splitRepo(repo)
	if err != nil {
		return nil, err
	}
	raw, err := paginate("list_issues", func(lo gh.ListOptions) ([]*gh.Issue, *gh.Response, error) {
		o := *opts
		o.ListOptions = lo
		return g.Issues.ListByRepo(ctx, owner, name, &o)
	})
	out := make([]Issue, 0, len(raw))
	for _, i := range raw {
		out = append(out, toIssue(i))
	}
	return out, err
}

// IssueQuery filters ListIssuesByQuery.
type IssueQuery struct {
	// Labels an item must all carry.
	Labels []string
	// State is "open", "closed" or "all" ("" means open).
	State string
	// PullRequests selects pull requests instead of issues.
	PullRequests bool
}

// Rate is GitHub's rate limit as last reported.
type Rate struct {
	Limit     int
	Remaining int
	Reset     time.Time
}

// ListIssuesByQuery lists a repository's issues or pull requests matching q
// through the issues endpoint (which filters pull requests by label too),
// oldest first, and returns the rate limit reported by the last response
// (zero when none was received).
func (c *Client) ListIssuesByQuery(ctx context.Context, repo string, q IssueQuery) ([]Issue, Rate, error) {
	var rate Rate
	g, err := c.api()
	if err != nil {
		return nil, rate, err
	}
	owner, name, err := splitRepo(repo)
	if err != nil {
		return nil, rate, err
	}
	state := q.State
	if state == "" {
		state = "open"
	}
	opts := gh.IssueListByRepoOptions{State: state, Labels: q.Labels, Sort: "created", Direction: "asc"}
	raw, err := paginate("list_issues_query", func(lo gh.ListOptions) ([]*gh.Issue, *gh.Response, error) {
		o := opts
		o.ListOptions = lo
		items, resp, err := g.Issues.ListByRepo(ctx, owner, name, &o)
		if resp != nil && resp.Rate.Limit > 0 {
			rate = Rate{Limit: resp.Rate.Limit, Remaining: resp.Rate.Remaining, Reset: resp.Rate.Reset.Time}
		}
		return items, resp, err
	})
	if err != nil {
		return nil, rate, err
	}
	out := make([]Issue, 0, len(raw))
	for _, i := range raw {
		item := toIssue(i)
		if (item.PullRequest != nil) == q.PullRequests {
			out = append(out, item)
		}
	}
	return out, rate, nil
}

// RateLimited reports whether err is GitHub's primary or secondary rate
// limit, and when to try again (zero when GitHub did not say).
func RateLimited(err error) (time.Time, bool) {
	var rl *gh.RateLimitError
	if errors.As(err, &rl) {
		return rl.Rate.Reset.Time, true
	}
	var abuse *gh.AbuseRateLimitError
	if errors.As(err, &abuse) {
		if abuse.RetryAfter != nil && *abuse.RetryAfter > 0 {
			return time.Now().Add(*abuse.RetryAfter), true
		}
		return time.Time{}, true
	}
	return time.Time{}, false
}

// GetIssue returns one issue.
func (c *Client) GetIssue(ctx context.Context, repo string, number int) (*Issue, error) {
	g, err := c.api()
	if err != nil {
		return nil, err
	}
	owner, name, err := splitRepo(repo)
	if err != nil {
		return nil, err
	}
	i, _, err := g.Issues.Get(ctx, owner, name, number)
	if err != nil {
		return &Issue{}, err
	}
	out := toIssue(i)
	return &out, nil
}

// ListOpenPullRequests returns the open pull requests carrying label, newest
// first (needs the pull requests read permission).
func (c *Client) ListOpenPullRequests(ctx context.Context, repo, label string) ([]PullRequest, error) {
	all, err := c.ListOpenPulls(ctx, repo)
	if err != nil {
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
	g, err := c.api()
	if err != nil {
		return err
	}
	owner, name, err := splitRepo(repo)
	if err != nil {
		return err
	}
	_, _, err = g.Issues.AddLabelsToIssue(ctx, owner, name, number, labels)
	return err
}

// RemoveLabel removes a label from an issue. A label the issue does not have
// is not an error.
func (c *Client) RemoveLabel(ctx context.Context, repo string, number int, label string) error {
	g, err := c.api()
	if err != nil {
		return err
	}
	owner, name, err := splitRepo(repo)
	if err != nil {
		return err
	}
	_, err = g.Issues.RemoveLabelForIssue(ctx, owner, name, number, label)
	if statusIs(err, http.StatusNotFound) {
		return nil
	}
	return err
}

// AddAssignees assigns users to an issue. GitHub silently ignores logins
// that cannot be assigned.
func (c *Client) AddAssignees(ctx context.Context, repo string, number int, logins ...string) error {
	g, err := c.api()
	if err != nil {
		return err
	}
	owner, name, err := splitRepo(repo)
	if err != nil {
		return err
	}
	_, _, err = g.Issues.AddAssignees(ctx, owner, name, number, logins)
	return err
}

// Comment posts a comment on an issue.
func (c *Client) Comment(ctx context.Context, repo string, number int, body string) error {
	g, err := c.api()
	if err != nil {
		return err
	}
	owner, name, err := splitRepo(repo)
	if err != nil {
		return err
	}
	_, _, err = g.Issues.CreateComment(ctx, owner, name, number, gh.IssueCommentRequest{Body: body})
	return err
}

// Comment is an issue comment.
type Comment struct {
	Body string `json:"body"`
}

// ListComments returns the comments of an issue, oldest first.
func (c *Client) ListComments(ctx context.Context, repo string, number int) ([]Comment, error) {
	g, err := c.api()
	if err != nil {
		return nil, err
	}
	owner, name, err := splitRepo(repo)
	if err != nil {
		return nil, err
	}
	raw, err := paginate("list_comments", func(lo gh.ListOptions) ([]*gh.IssueComment, *gh.Response, error) {
		return g.Issues.ListComments(ctx, owner, name, number, &gh.IssueListCommentsOptions{ListOptions: lo})
	})
	out := make([]Comment, 0, len(raw))
	for _, cm := range raw {
		out = append(out, Comment{Body: cm.GetBody()})
	}
	return out, err
}

// CloseIssue closes an issue as completed.
func (c *Client) CloseIssue(ctx context.Context, repo string, number int) error {
	g, err := c.api()
	if err != nil {
		return err
	}
	owner, name, err := splitRepo(repo)
	if err != nil {
		return err
	}
	closed, completed := "closed", "completed"
	_, _, err = g.Issues.Update(ctx, owner, name, number, gh.UpdateIssueRequest{State: &closed, StateReason: &completed})
	return err
}

// User is a GitHub account.
type User struct {
	Login string `json:"login"`
}

// PullRequest is the subset of a pull request the PR feedback watcher reads.
type PullRequest struct {
	Number  int    `json:"number"`
	HTMLURL string `json:"html_url"`
	State   string `json:"state"`
	Title   string `json:"title"`
	Body    string `json:"body"`
	Draft   bool   `json:"draft"`
	Merged  bool   `json:"merged"`
	// MergeableState is GitHub's merge state (the REST form of GraphQL's
	// mergeStateStatus): "dirty" means merge conflicts with the base branch.
	// Only a single-PR GET returns it, and "unknown" until GitHub computed it.
	MergeableState string `json:"mergeable_state"`
	// NodeID is the GraphQL ID (needed to enable auto-merge).
	NodeID string `json:"node_id"`
	// AutoMerge is set while auto-merge is enabled (queued).
	AutoMerge *struct{} `json:"auto_merge"`
	// InMergeQueue is set by LoadMergeQueueState when GraphQL shows the pull
	// request in a merge queue. REST auto_merge can be null for a PR placed
	// straight into a queue, so it alone does not tell queued from open.
	InMergeQueue bool    `json:"-"`
	User         User    `json:"user"`
	Labels       []Label `json:"labels"`
	Head         struct {
		SHA string `json:"sha"`
		Ref string `json:"ref"`
	} `json:"head"`
	Base struct {
		Ref string `json:"ref"`
	} `json:"base"`
}

// Queued reports whether the pull request waits to merge: auto-merge is on
// (REST) or it is in a merge queue (GraphQL, see LoadMergeQueueState).
func (p *PullRequest) Queued() bool { return p.AutoMerge != nil || p.InMergeQueue }

// HasLabel reports whether the pull request carries label name
// (case-insensitive).
func (p *PullRequest) HasLabel(name string) bool {
	for _, l := range p.Labels {
		if strings.EqualFold(l.Name, name) {
			return true
		}
	}
	return false
}

// ListOpenPulls returns the open pull requests of repo (owner/name), newest
// first, following up to maxPages pages.
func (c *Client) ListOpenPulls(ctx context.Context, repo string) ([]PullRequest, error) {
	g, err := c.api()
	if err != nil {
		return nil, err
	}
	owner, name, err := splitRepo(repo)
	if err != nil {
		return nil, err
	}
	raw, err := paginate("list_pull_requests", func(lo gh.ListOptions) ([]*gh.PullRequest, *gh.Response, error) {
		return g.PullRequests.List(ctx, owner, name, &gh.PullRequestListOptions{State: "open", ListOptions: lo})
	})
	out := make([]PullRequest, 0, len(raw))
	for _, p := range raw {
		out = append(out, toPull(p))
	}
	return out, err
}

// GetPull returns one pull request.
func (c *Client) GetPull(ctx context.Context, repo string, number int) (*PullRequest, error) {
	g, err := c.api()
	if err != nil {
		return nil, err
	}
	owner, name, err := splitRepo(repo)
	if err != nil {
		return nil, err
	}
	p, _, err := g.PullRequests.Get(ctx, owner, name, number)
	if err != nil {
		return &PullRequest{}, err
	}
	out := toPull(p)
	return &out, nil
}

// Review is one review on a pull request.
type Review struct {
	ID    int64  `json:"id"`
	State string `json:"state"`
	Body  string `json:"body"`
	User  User   `json:"user"`
}

// ListReviews returns the reviews of a pull request, oldest first.
func (c *Client) ListReviews(ctx context.Context, repo string, number int) ([]Review, error) {
	g, err := c.api()
	if err != nil {
		return nil, err
	}
	owner, name, err := splitRepo(repo)
	if err != nil {
		return nil, err
	}
	raw, err := paginate("list_reviews", func(lo gh.ListOptions) ([]*gh.PullRequestReview, *gh.Response, error) {
		return g.PullRequests.ListReviews(ctx, owner, name, number, &lo)
	})
	out := make([]Review, 0, len(raw))
	for _, r := range raw {
		out = append(out, Review{ID: r.GetID(), State: r.GetState(), Body: r.GetBody(), User: User{Login: r.GetUser().GetLogin()}})
	}
	return out, err
}

// CreateReview posts a review with the COMMENT event (never approves or
// requests changes: the verdict lives in the body).
func (c *Client) CreateReview(ctx context.Context, repo string, number int, body string) error {
	g, err := c.api()
	if err != nil {
		return err
	}
	owner, name, err := splitRepo(repo)
	if err != nil {
		return err
	}
	_, _, err = g.PullRequests.CreateReview(ctx, owner, name, number, &gh.PullRequestReviewRequest{Body: &body, Event: new("COMMENT")})
	return err
}

// EnableAutoMerge turns on squash auto-merge for a pull request (GraphQL: the
// REST API has no endpoint for it). The pull request's merge-queue and
// required-check rules still gate the merge.
func (c *Client) EnableAutoMerge(ctx context.Context, nodeID string) error {
	g, err := c.api()
	if err != nil {
		return err
	}
	body := map[string]any{
		"query":     "mutation($id: ID!) { enablePullRequestAutoMerge(input: {pullRequestId: $id, mergeMethod: SQUASH}) { clientMutationId } }",
		"variables": map[string]string{"id": nodeID},
	}
	req, err := g.NewRequest(ctx, http.MethodPost, graphQLURL(g.BaseURL()), body)
	if err != nil {
		return err
	}
	var out struct {
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if _, err := g.Do(req, &out); err != nil {
		return err
	}
	if len(out.Errors) > 0 {
		return fmt.Errorf("enable auto-merge: %s", out.Errors[0].Message)
	}
	return nil
}

// LoadMergeQueueState asks GraphQL (isInMergeQueue / mergeQueueEntry) whether
// an open pull request is in a merge queue and records it in pr.InMergeQueue.
// It is a no-op (no call) for a merged, closed or draft PR, one already
// showing auto_merge, or one without a node ID. A GraphQL error is returned
// to the caller: in the verify pass it fails the pass, which is retried on the
// next poll.
func (c *Client) LoadMergeQueueState(ctx context.Context, pr *PullRequest) error {
	if pr.AutoMerge != nil || pr.Merged || pr.State == "closed" || pr.Draft || pr.NodeID == "" {
		return nil
	}
	g, err := c.api()
	if err != nil {
		return err
	}
	body := map[string]any{
		"query":     "query($id: ID!) { node(id: $id) { ... on PullRequest { isInMergeQueue mergeQueueEntry { id } } } }",
		"variables": map[string]string{"id": pr.NodeID},
	}
	req, err := g.NewRequest(ctx, http.MethodPost, graphQLURL(g.BaseURL()), body)
	if err != nil {
		return err
	}
	var out struct {
		Data struct {
			Node *struct {
				IsInMergeQueue  bool `json:"isInMergeQueue"`
				MergeQueueEntry *struct {
					ID string `json:"id"`
				} `json:"mergeQueueEntry"`
			} `json:"node"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if _, err := g.Do(req, &out); err != nil {
		return err
	}
	if len(out.Errors) > 0 {
		return fmt.Errorf("merge queue state: %s", out.Errors[0].Message)
	}
	if n := out.Data.Node; n != nil {
		pr.InMergeQueue = n.IsInMergeQueue || n.MergeQueueEntry != nil
	}
	return nil
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

func ts(t *gh.Timestamp) *time.Time {
	if t == nil {
		return nil
	}
	v := t.Time
	return &v
}

// ListCheckRuns returns every check run (all attempts, not only the latest)
// on commit sha.
func (c *Client) ListCheckRuns(ctx context.Context, repo, sha string) ([]CheckRun, error) {
	g, err := c.api()
	if err != nil {
		return nil, err
	}
	owner, name, err := splitRepo(repo)
	if err != nil {
		return nil, err
	}
	raw, err := paginate("list_check_runs", func(lo gh.ListOptions) ([]*gh.CheckRun, *gh.Response, error) {
		res, resp, err := g.Checks.ListCheckRunsForRef(ctx, owner, name, sha, &gh.ListCheckRunsOptions{Filter: new("all"), ListOptions: lo})
		if res == nil {
			return nil, resp, err
		}
		return res.CheckRuns, resp, err
	})
	out := make([]CheckRun, 0, len(raw))
	for _, r := range raw {
		cr := CheckRun{
			ID: r.GetID(), Name: r.GetName(), Status: r.GetStatus(), Conclusion: r.GetConclusion(),
			HTMLURL: r.GetHTMLURL(), DetailsURL: r.GetDetailsURL(),
			StartedAt: ts(r.StartedAt), CompletedAt: ts(r.CompletedAt),
		}
		if r.App != nil {
			cr.App = &struct {
				Slug string `json:"slug"`
			}{Slug: r.App.GetSlug()}
		}
		cr.Output.Title, cr.Output.Summary = r.GetOutput().GetTitle(), r.GetOutput().GetSummary()
		out = append(out, cr)
	}
	return out, err
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

// mergeGroupWindow is how far back merge_group runs are read. A merge-queue
// attempt is only relevant to PR feedback while its pull request is queued,
// so one day of runs is enough and normally fits on one page.
const mergeGroupWindow = 24 * time.Hour

// MergeGroupFailureWindow is how far back the failed-job read looks for the
// merge_group run of a queue failure that is being fixed. A fix can land well
// after the attempt that failed, so this is wider than mergeGroupWindow.
const MergeGroupFailureWindow = 72 * time.Hour

// mergeGroupCreatedFilter builds the created qualifier for runs created at or
// after now minus window. The timestamp is second-granular, so the read covers
// exactly the requested window.
func mergeGroupCreatedFilter(now time.Time, window time.Duration) string {
	return ">=" + now.Add(-window).UTC().Format(time.RFC3339)
}

// ListMergeGroupRuns returns the merge_group workflow runs of repo, newest
// first, created within mergeGroupWindow. See ListMergeGroupRunsWithin.
func (c *Client) ListMergeGroupRuns(ctx context.Context, repo string) ([]WorkflowRun, error) {
	return c.ListMergeGroupRunsWithin(ctx, repo, mergeGroupWindow)
}

// ListMergeGroupRunsWithin returns the merge_group workflow runs of repo, newest
// first, created within window, following up to maxPages pages (see maxPages) so a
// busy merge queue cannot hide an older failed run on page 2+. The cap also
// bounds how long a huge history can stall a reconcile; runs beyond it are not
// seen (counted by roundtable_github_pagination_truncated_total).
func (c *Client) ListMergeGroupRunsWithin(ctx context.Context, repo string, window time.Duration) ([]WorkflowRun, error) {
	g, err := c.api()
	if err != nil {
		return nil, err
	}
	owner, name, err := splitRepo(repo)
	if err != nil {
		return nil, err
	}
	raw, err := paginate("list_merge_group_runs", func(lo gh.ListOptions) ([]*gh.WorkflowRun, *gh.Response, error) {
		res, resp, err := g.Actions.ListRepositoryWorkflowRuns(ctx, owner, name, &gh.ListWorkflowRunsOptions{Event: "merge_group", Created: mergeGroupCreatedFilter(time.Now(), window), ListOptions: lo})
		if res == nil {
			return nil, resp, err
		}
		return res.WorkflowRuns, resp, err
	})
	out := make([]WorkflowRun, 0, len(raw))
	for _, r := range raw {
		out = append(out, WorkflowRun{
			ID: r.GetID(), Name: r.GetName(), HeadBranch: r.GetHeadBranch(), HeadSHA: r.GetHeadSHA(),
			Event: r.GetEvent(), Status: r.GetStatus(), Conclusion: r.GetConclusion(),
			HTMLURL: r.GetHTMLURL(), CreatedAt: r.GetCreatedAt().Time,
		})
	}
	return out, err
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
	g, err := c.api()
	if err != nil {
		return nil, err
	}
	owner, name, err := splitRepo(repo)
	if err != nil {
		return nil, err
	}
	raw, err := paginate("list_run_jobs", func(lo gh.ListOptions) ([]*gh.WorkflowJob, *gh.Response, error) {
		res, resp, err := g.Actions.ListWorkflowJobs(ctx, owner, name, runID, &gh.ListWorkflowJobsOptions{ListOptions: lo})
		if res == nil {
			return nil, resp, err
		}
		return res.Jobs, resp, err
	})
	out := make([]Job, 0, len(raw))
	for _, j := range raw {
		out = append(out, Job{ID: j.GetID(), Name: j.GetName(), Conclusion: j.GetConclusion(), HTMLURL: j.GetHTMLURL()})
	}
	return out, err
}

// JobLog returns the plain-text log of an Actions job (at most 8 MiB).
func (c *Client) JobLog(ctx context.Context, repo string, jobID int64) (string, error) {
	g, err := c.api()
	if err != nil {
		return "", err
	}
	owner, name, err := splitRepo(repo)
	if err != nil {
		return "", err
	}
	u, _, err := g.Actions.GetWorkflowJobLogs(ctx, owner, name, jobID, 3)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", err
	}
	resp, err := g.Client().Do(req)
	if err != nil {
		// The transport error carries the pre-signed log URL; drop it.
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return "", fmt.Errorf("github job log %d: download from %s: %w", jobID, u.Host, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", fmt.Errorf("github job log %d: %s", jobID, resp.Status)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxLogBytes))
	return string(data), err
}

// ListOpenIssuesWithLabel returns open issues (and pull requests) carrying
// label.
func (c *Client) ListOpenIssuesWithLabel(ctx context.Context, repo, label string) ([]Issue, error) {
	return c.listIssues(ctx, repo, &gh.IssueListByRepoOptions{State: "open", Labels: []string{label}})
}

// CreateIssue opens an issue.
func (c *Client) CreateIssue(ctx context.Context, repo, title, body string, labels ...string) (*Issue, error) {
	g, err := c.api()
	if err != nil {
		return nil, err
	}
	owner, name, err := splitRepo(repo)
	if err != nil {
		return nil, err
	}
	i, _, err := g.Issues.Create(ctx, owner, name, gh.CreateIssueRequest{Title: title, Body: &body, Labels: labels})
	if err != nil {
		return &Issue{}, err
	}
	out := toIssue(i)
	return &out, nil
}

// EnsureLabel creates the repo label name with the given color (hex, no #)
// and description if the repo does not have it, and reports whether it did.
// An existing label is never modified.
func (c *Client) EnsureLabel(ctx context.Context, repo, name, color, description string) (bool, error) {
	g, err := c.api()
	if err != nil {
		return false, err
	}
	owner, repoName, err := splitRepo(repo)
	if err != nil {
		return false, err
	}
	_, _, err = g.Issues.GetLabel(ctx, owner, repoName, name)
	if err == nil {
		return false, nil
	}
	if !statusIs(err, http.StatusNotFound) {
		return false, err
	}
	_, _, err = g.Issues.CreateLabel(ctx, owner, repoName, gh.CreateIssueLabelRequest{Name: name, Color: &color, Description: &description})
	switch {
	case err == nil:
		return true, nil
	case statusIs(err, http.StatusUnprocessableEntity):
		return false, nil // created concurrently (already_exists)
	}
	return false, err
}
