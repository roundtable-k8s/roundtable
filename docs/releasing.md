# Cutting a release

1. **Check main is green.** Tag only a commit whose CI run (build, lint, tests, helm-check) passed.
2. **Confirm `Chart.yaml` `version`** only needs to have no `v` tag yet: the git tag you push in step 3 sets the released version. The `helm` job on main fails when `v<version>` already exists, so a released chart version is never overwritten.
3. **Tag and push:** `git tag vX.Y.Z <sha> && git push origin vX.Y.Z`.

## What the Release workflow publishes

- Operator image `ghcr.io/roundtable-k8s/roundtable` tagged `vX.Y.Z`, `latest` and the commit SHA.
- Helm chart `oci://ghcr.io/roundtable-k8s/charts/roundtable-operator` at version `X.Y.Z` with `appVersion: vX.Y.Z`. With `image.tag` empty, the operator (and gateway and trigger) image defaults to `appVersion`, so the chart installs the image built from the same tag.
- `images.piKnight.tag` and `images.dashboard.tag` in the packaged chart are set to the current `main` SHAs of `knights` and `roundtable-ui`, so the pair is fixed at release time.
- A GitHub Release with the operator binaries.

## Main-branch builds

Every push to main publishes the chart as a pre-release, `X.Y.Z-dev.<short sha>`, with `appVersion` set to the commit SHA. It never touches a released version.

## After the release

Bump `version` in `charts/roundtable-operator/Chart.yaml` to the next version (e.g. `0.25.0` to `0.26.0`) in a PR, so main builds publish as `0.26.0-dev.*`. Installs that pin image tags and the chart digest themselves (such as dapper-cluster) keep working unchanged.
