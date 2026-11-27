# -*- mode: Python -*-
# Inner loop for operator development: kind + Tilt. `make tilt-up` creates the
# kind cluster and runs `tilt up`; see docs/local-development.md.
#
# Edit a Go file under cmd/, api/, internal/ or pkg/ and Tilt recompiles on the
# host and live-updates the binary into the operator pod, which restarts the
# process in place (no image rebuild).

# Pin the extensions (restart_process, helm_resource) to a tilt-extensions
# commit instead of the moving default branch: redefining the 'default'
# extension repo makes every ext:// load use this ref.
v1alpha1.extension_repo(
    name='default',
    url='https://github.com/tilt-dev/tilt-extensions',
    ref='e72f135bc8aed2bb3d63f227f5508e6deb4560a9',
)

load('ext://restart_process', 'docker_build_with_restart')

NAMESPACE = 'roundtable'
RELEASE = 'roundtable-operator'
CHART = 'charts/roundtable-operator'
# One pin for both paths: read the default from hack/quickstart-kind.sh.
def _nats_pin():
    for line in str(read_file('hack/quickstart-kind.sh')).split('\n'):
        if line.startswith('NATS_CHART_VERSION='):
            return line.split(':-')[1].split('}')[0]
    fail('NATS_CHART_VERSION not found in hack/quickstart-kind.sh')
NATS_CHART_VERSION = os.getenv('NATS_CHART_VERSION', _nats_pin())
ARCH = str(local('go env GOARCH', quiet=True)).strip()

# Only ever act on a kind cluster, never on a "real" one.
if not k8s_context().startswith('kind-'):
    fail('refusing to run against context %r: expected a kind cluster (make tilt-up)' % k8s_context())

# --- Namespace and CRDs. The CRDs are far over the 256KiB annotation limit of
# client-side apply, so they are applied server-side outside Tilt's own apply.
local_resource(
    'namespace',
    'kubectl create namespace %s --dry-run=client -o yaml | kubectl apply -f -' % NAMESPACE,
    labels=['infra'],
)
local_resource(
    'crds',
    'kubectl apply --server-side --force-conflicts -f %s/crds' % CHART,
    deps=[CHART + '/crds'],
    resource_deps=['namespace'],
    labels=['infra'],
)

# --- NATS with JetStream, from the pinned nats chart (same settings as
# hack/quickstart-kind.sh).
load('ext://helm_resource', 'helm_resource', 'helm_repo')
helm_repo('nats', 'https://nats-io.github.io/k8s/helm/charts/', resource_name='nats-repo', labels=['infra'])
helm_resource(
    'nats',
    'nats/nats',
    namespace=NAMESPACE,
    release_name='nats',
    resource_deps=['nats-repo', 'namespace'],
    flags=[
        '--version', NATS_CHART_VERSION,
        '--set', 'config.jetstream.enabled=true',
        '--set', 'config.jetstream.fileStore.pvc.enabled=false',
        '--set', 'natsBox.enabled=false',
    ],
    labels=['infra'],
)

# --- Operator: compile on the host, live-update the binary into the pod.
local_resource(
    'compile',
    ('mkdir -p .tilt && CGO_ENABLED=0 GOOS=linux GOARCH=%s go build -o .tilt/manager ./cmd/main.go '
     + '&& CGO_ENABLED=0 GOOS=linux GOARCH=%s go build -o .tilt/gateway ./cmd/gateway') % (ARCH, ARCH),
    deps=['cmd', 'api', 'internal', 'pkg', 'go.mod', 'go.sum'],
    ignore=['**/*_test.go'],
    labels=['operator'],
)

docker_build_with_restart(
    'roundtable-operator-dev',
    '.',
    dockerfile='Dockerfile.dev',
    entrypoint=['/app/manager'],
    only=['.tilt'],
    live_update=[sync('.tilt/manager', '/app/manager'), sync('.tilt/gateway', '/app/gateway')],
)

watch_file(CHART)
watch_file('hack/tilt-values.yaml')
watch_file('hack/quickstart-kind.sh')
objs = decode_yaml_stream(local(
    'helm template %s %s --namespace %s -f hack/tilt-values.yaml' % (RELEASE, CHART, NAMESPACE),
    quiet=True,
))
# Live update and the restart wrapper write the binary and /tmp inside the
# container, so the operator container's root filesystem must be writable in dev.
for o in objs:
    if o['kind'] == 'Deployment' and o['metadata']['name'] == RELEASE:
        for c in o['spec']['template']['spec']['containers']:
            c.setdefault('securityContext', {})['readOnlyRootFilesystem'] = False
k8s_yaml(encode_yaml_stream(objs))
k8s_resource(
    RELEASE,
    resource_deps=['crds', 'nats', 'compile'],
    labels=['operator'],
)

# --- Example fleet: one RoundTable and one Knight.
local_resource(
    'dev-fleet',
    'kubectl apply -f examples/dev/ && for i in $(seq 60); do kubectl -n %s get deployment dev-knight && exit 0; sleep 2; done; exit 1' % NAMESPACE,
    deps=['examples/dev'],
    resource_deps=[RELEASE],
    labels=['fleet'],
)
