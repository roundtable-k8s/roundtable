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

// Command stubgithub is a minimal GitHub REST server for the e2e tests. It
// holds repositories, issues, labels and comments in memory and answers only
// the routes the issue pickup controller calls (see README.md), so a scenario
// can run on kind without a token or network access. It is stdlib only.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// labelDef is a repository label.
type labelDef struct {
	Name        string `json:"name"`
	Color       string `json:"color"`
	Description string `json:"description"`
}

// labelRef is the {"name": ...} object GitHub returns for an issue's labels.
type labelRef struct {
	Name string `json:"name"`
}

// user is the {"login": ...} object GitHub returns for authors.
type user struct {
	Login string `json:"login"`
}

// comment is an issue comment.
type comment struct {
	ID   int    `json:"id"`
	Body string `json:"body"`
	User user   `json:"user"`
}

// issue is the issue shape the operator reads. Comments are kept out of the
// JSON: GitHub's "comments" field is a count, and the stub state endpoint
// reports them separately.
type issue struct {
	Number    int        `json:"number"`
	Title     string     `json:"title"`
	Body      string     `json:"body"`
	State     string     `json:"state"`
	HTMLURL   string     `json:"html_url"`
	User      user       `json:"user"`
	Labels    []labelRef `json:"labels"`
	Assignees []user     `json:"assignees"`
	comments  []comment
}

func (i *issue) hasLabel(name string) bool {
	for _, l := range i.Labels {
		if strings.EqualFold(l.Name, name) {
			return true
		}
	}
	return false
}

// repoState is one repository's labels and issues.
type repoState struct {
	labels  map[string]labelDef // keyed by lower-cased name
	issues  map[int]*issue
	nextNum int
}

// stub is the in-memory state behind every route.
type stub struct {
	mu        sync.Mutex
	repos     map[string]*repoState // keyed by "owner/name"
	commentID int
}

func newStub() *stub {
	return &stub{repos: map[string]*repoState{}}
}

// repo returns the named repository, creating it when create is set. The caller
// must hold s.mu.
func (s *stub) repo(name string, create bool) *repoState {
	r, ok := s.repos[name]
	if !ok && create {
		r = &repoState{labels: map[string]labelDef{}, issues: map[int]*issue{}, nextNum: 1}
		s.repos[name] = r
	}
	return r
}

// ensureLabel adds a label to the repository if it is missing. The caller must
// hold s.mu.
func (r *repoState) ensureLabel(name string) {
	key := strings.ToLower(name)
	if _, ok := r.labels[key]; !ok {
		r.labels[key] = labelDef{Name: name}
	}
}

// newIssue appends an issue to the repository, creating the repository and any
// missing labels. The caller must hold s.mu.
func (s *stub) newIssue(repoName, title, body string, labels []string) *issue {
	r := s.repo(repoName, true)
	iss := &issue{
		Number:  r.nextNum,
		Title:   title,
		Body:    body,
		State:   "open",
		HTMLURL: fmt.Sprintf("https://github.com/%s/issues/%d", repoName, r.nextNum),
		User:    user{Login: "stub-user"},
		Labels:  []labelRef{},
	}
	r.nextNum++
	r.issues[iss.Number] = iss
	for _, l := range labels {
		if !iss.hasLabel(l) {
			iss.Labels = append(iss.Labels, labelRef{Name: l})
		}
		r.ensureLabel(l)
	}
	return iss
}

// handler returns the stub GitHub API. Requests under /repos/ need a non-empty
// Authorization header, so a missing token Secret fails the test instead of
// passing silently. The /_stub/ control routes need none.
func handler() http.Handler {
	s := newStub()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /repos/{owner}/{repo}/issues", s.listIssues)
	mux.HandleFunc("GET /repos/{owner}/{repo}/issues/{number}", s.getIssue)
	mux.HandleFunc("GET /repos/{owner}/{repo}/pulls", s.listPulls)
	mux.HandleFunc("POST /repos/{owner}/{repo}/issues/{number}/assignees", s.addAssignees)
	mux.HandleFunc("POST /repos/{owner}/{repo}/issues/{number}/labels", s.addLabels)
	mux.HandleFunc("DELETE /repos/{owner}/{repo}/issues/{number}/labels/{name}", s.removeLabel)
	mux.HandleFunc("POST /repos/{owner}/{repo}/issues/{number}/comments", s.createComment)
	mux.HandleFunc("GET /repos/{owner}/{repo}/labels/{name}", s.getLabel)
	mux.HandleFunc("POST /repos/{owner}/{repo}/labels", s.createLabel)
	mux.HandleFunc("POST /_stub/issues", s.seedIssue)
	mux.HandleFunc("GET /_stub/state", s.state)
	mux.HandleFunc("/", notFound)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/repos/") && strings.TrimSpace(r.Header.Get("Authorization")) == "" {
			writeMessage(w, http.StatusUnauthorized, "Requires authentication")
			return
		}
		mux.ServeHTTP(w, r)
	})
}

// notFound answers an unknown route with a JSON 404 and logs it, so a route the
// operator calls but the stub lacks is obvious in the stub's logs.
func notFound(w http.ResponseWriter, r *http.Request) {
	log.Printf("stubgithub: unhandled route %s %s", r.Method, r.URL.Path)
	writeMessage(w, http.StatusNotFound, "Not Found")
}

func (s *stub) listIssues(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	repo := s.repo(r.PathValue("owner")+"/"+r.PathValue("repo"), false)
	if repo == nil {
		writeMessage(w, http.StatusNotFound, "Not Found")
		return
	}
	state := r.URL.Query().Get("state")
	if state == "" {
		state = "open"
	}
	var want []string
	if raw := r.URL.Query().Get("labels"); raw != "" {
		want = strings.Split(raw, ",")
	}
	out := []*issue{}
	for _, iss := range sortedIssues(repo) {
		if state != "all" && iss.State != state {
			continue
		}
		if !hasAll(iss, want) {
			continue
		}
		out = append(out, iss)
	}
	writeJSON(w, http.StatusOK, out)
}

// listPulls answers the open-pull-request poll that issue pickup makes before
// claiming anything (the breaking-PR hold). The stub has no pull requests.
func (s *stub) listPulls(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, []any{})
}

// addAssignees takes {"assignees": [...]} and adds each login not yet assigned.
func (s *stub) addAssignees(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Assignees []string `json:"assignees"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeMessage(w, http.StatusUnprocessableEntity, "body must be {\"assignees\": [...]}")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	iss, ok := s.lookupIssue(r)
	if !ok {
		writeMessage(w, http.StatusNotFound, "Not Found")
		return
	}
	for _, login := range req.Assignees {
		found := false
		for _, a := range iss.Assignees {
			found = found || strings.EqualFold(a.Login, login)
		}
		if !found {
			iss.Assignees = append(iss.Assignees, user{Login: login})
		}
	}
	writeJSON(w, http.StatusCreated, iss)
}

func (s *stub) getIssue(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	iss, ok := s.lookupIssue(r)
	if !ok {
		writeMessage(w, http.StatusNotFound, "Not Found")
		return
	}
	writeJSON(w, http.StatusOK, iss)
}

// addLabels takes the JSON array GitHub expects and adds each label that the
// issue does not already carry.
func (s *stub) addLabels(w http.ResponseWriter, r *http.Request) {
	var names []string
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&names); err != nil {
		writeMessage(w, http.StatusUnprocessableEntity, "body must be a JSON array of label names")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	repo := s.repo(r.PathValue("owner")+"/"+r.PathValue("repo"), false)
	iss, ok := s.lookupIssue(r)
	if repo == nil || !ok {
		writeMessage(w, http.StatusNotFound, "Not Found")
		return
	}
	for _, n := range names {
		repo.ensureLabel(n)
		if !iss.hasLabel(n) {
			iss.Labels = append(iss.Labels, labelRef{Name: n})
		}
	}
	writeJSON(w, http.StatusOK, iss.Labels)
}

// removeLabel answers 404 for a label the issue does not carry, as GitHub does.
func (s *stub) removeLabel(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	iss, ok := s.lookupIssue(r)
	if !ok {
		writeMessage(w, http.StatusNotFound, "Not Found")
		return
	}
	name := r.PathValue("name")
	kept := iss.Labels[:0]
	found := false
	for _, l := range iss.Labels {
		if strings.EqualFold(l.Name, name) {
			found = true
			continue
		}
		kept = append(kept, l)
	}
	if !found {
		writeMessage(w, http.StatusNotFound, "Label does not exist")
		return
	}
	iss.Labels = kept
	writeJSON(w, http.StatusOK, iss.Labels)
}

func (s *stub) createComment(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Body string `json:"body"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil || req.Body == "" {
		writeMessage(w, http.StatusUnprocessableEntity, "body is required")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	iss, ok := s.lookupIssue(r)
	if !ok {
		writeMessage(w, http.StatusNotFound, "Not Found")
		return
	}
	s.commentID++
	c := comment{ID: s.commentID, Body: req.Body, User: user{Login: "stub-user"}}
	iss.comments = append(iss.comments, c)
	writeJSON(w, http.StatusCreated, c)
}

func (s *stub) getLabel(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	repo := s.repo(r.PathValue("owner")+"/"+r.PathValue("repo"), false)
	if repo == nil {
		writeMessage(w, http.StatusNotFound, "Not Found")
		return
	}
	l, ok := repo.labels[strings.ToLower(r.PathValue("name"))]
	if !ok {
		writeMessage(w, http.StatusNotFound, "Not Found")
		return
	}
	writeJSON(w, http.StatusOK, l)
}

func (s *stub) createLabel(w http.ResponseWriter, r *http.Request) {
	var l labelDef
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&l); err != nil || l.Name == "" {
		writeMessage(w, http.StatusUnprocessableEntity, "name is required")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	repo := s.repo(r.PathValue("owner")+"/"+r.PathValue("repo"), false)
	if repo == nil {
		writeMessage(w, http.StatusNotFound, "Not Found")
		return
	}
	if _, exists := repo.labels[strings.ToLower(l.Name)]; exists {
		writeMessage(w, http.StatusUnprocessableEntity, "Validation Failed: already_exists")
		return
	}
	repo.labels[strings.ToLower(l.Name)] = l
	writeJSON(w, http.StatusCreated, l)
}

// seedIssue is the control route that creates a repository on first use and
// opens an issue in it. Body: {"repo": "owner/name", "title", "body", "labels"}.
func (s *stub) seedIssue(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Repo   string   `json:"repo"`
		Title  string   `json:"title"`
		Body   string   `json:"body"`
		Labels []string `json:"labels"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeMessage(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if strings.Count(req.Repo, "/") != 1 || strings.HasPrefix(req.Repo, "/") || strings.HasSuffix(req.Repo, "/") {
		writeMessage(w, http.StatusBadRequest, "repo must be owner/name")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	iss := s.newIssue(req.Repo, req.Title, req.Body, req.Labels)
	writeJSON(w, http.StatusCreated, iss)
}

// state is the control route that reads a repository back: its labels, and
// every issue with its label names and comment bodies. Query: repo=owner/name.
func (s *stub) state(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("repo")
	s.mu.Lock()
	defer s.mu.Unlock()
	repo := s.repo(name, false)
	if repo == nil {
		writeMessage(w, http.StatusNotFound, "repo not seeded: "+name)
		return
	}
	type issueState struct {
		Number   int      `json:"number"`
		Title    string   `json:"title"`
		State    string   `json:"state"`
		Labels   []string `json:"labels"`
		Comments []string `json:"comments"`
	}
	type repoStateView struct {
		Repo   string       `json:"repo"`
		Labels []string     `json:"labels"`
		Issues []issueState `json:"issues"`
	}
	view := repoStateView{Repo: name, Labels: []string{}, Issues: []issueState{}}
	for _, l := range repo.labels {
		view.Labels = append(view.Labels, l.Name)
	}
	sort.Strings(view.Labels)
	for _, iss := range sortedIssues(repo) {
		is := issueState{Number: iss.Number, Title: iss.Title, State: iss.State, Labels: []string{}, Comments: []string{}}
		for _, l := range iss.Labels {
			is.Labels = append(is.Labels, l.Name)
		}
		for _, c := range iss.comments {
			is.Comments = append(is.Comments, c.Body)
		}
		view.Issues = append(view.Issues, is)
	}
	writeJSON(w, http.StatusOK, view)
}

// lookupIssue resolves {number} in the path to an issue of {owner}/{repo}. The
// caller must hold s.mu.
func (s *stub) lookupIssue(r *http.Request) (*issue, bool) {
	repo := s.repo(r.PathValue("owner")+"/"+r.PathValue("repo"), false)
	if repo == nil {
		return nil, false
	}
	n, err := strconv.Atoi(r.PathValue("number"))
	if err != nil {
		return nil, false
	}
	iss, ok := repo.issues[n]
	return iss, ok
}

func sortedIssues(repo *repoState) []*issue {
	out := make([]*issue, 0, len(repo.issues))
	for _, iss := range repo.issues {
		out = append(out, iss)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Number < out[j].Number })
	return out
}

func hasAll(iss *issue, labels []string) bool {
	for _, l := range labels {
		if !iss.hasLabel(l) {
			return false
		}
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeMessage(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"message": msg})
}

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()
	log.Printf("stubgithub listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, handler()))
}
