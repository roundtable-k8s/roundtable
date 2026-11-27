# stubgithub: in-memory GitHub REST API for e2e

A stdlib-only HTTP server that answers the GitHub REST routes the issue pickup
controller calls, so an e2e scenario can run on kind without a token or network.
Point the operator at it with `--github-api-url=http://stubgithub:8080`.

## Routes

| Method | Path | Behaviour |
|--------|------|-----------|
| GET | `/repos/{owner}/{repo}/issues?state=&labels=a,b` | Issues in number order; `state` defaults to `open`; `labels` must all match |
| GET | `/repos/{owner}/{repo}/issues/{n}` | One issue |
| POST | `/repos/{owner}/{repo}/issues/{n}/labels` | Body is a JSON array of names; missing labels are created |
| DELETE | `/repos/{owner}/{repo}/issues/{n}/labels/{name}` | 404 if the issue does not carry the label |
| POST | `/repos/{owner}/{repo}/issues/{n}/comments` | Body `{"body": "..."}`; 201 |
| GET | `/repos/{owner}/{repo}/labels/{name}` | 404 if the repository lacks the label |
| POST | `/repos/{owner}/{repo}/labels` | Body `{"name", "color", "description"}`; 422 if it exists |

Every `/repos/` route needs a non-empty `Authorization` header; without one the
stub answers 401, so a missing token Secret fails the test. The first matched
`/repos/` route that names a repository creates it (empty), so `EnsureLabel`
works before anything is seeded; a path that matches no route (an unknown
sub-route) creates nothing and answers 404; unlike GitHub, an unknown repository is not a 404. Any
unknown route answers 404 with a JSON body and is logged as `unhandled route`.
Issues carry `assignees` as `[]` when none are assigned, never `null`.

## Control routes (no auth)

- `POST /_stub/issues` with `{"repo": "owner/name", "title", "body", "labels"}`
  opens an issue (creating the repository on first use) and returns it with 201.
  A repo that is not `owner/name` gets 400.
- `GET /_stub/state?repo=owner/name` returns the repository's labels and every
  issue with its assignee logins, label names and comment bodies. A repo the
  stub has never seen (no seed and no `/repos/` call) gets 404.

## Build and run locally

```bash
make stubgithub-image                  # builds stubgithub:e2e
docker run --rm -p 8080:8080 stubgithub:e2e
curl -s -X POST localhost:8080/_stub/issues -d '{"repo":"acme/widgets","title":"t","labels":["ready"]}'
curl -s 'localhost:8080/repos/acme/widgets/issues?labels=ready' -H 'Authorization: token x'
```

## Tests

```bash
CGO_ENABLED=0 go test ./test/e2e/stubgithub/...
```

The tests call the stub through the operator's own `internal/github` client, so
a route the client uses and the stub lacks fails the unit test. The package sits
under `test/e2e`, so `make test` does not run it.
