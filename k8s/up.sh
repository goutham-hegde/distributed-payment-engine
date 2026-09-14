#!/usr/bin/env bash
#
# M10. The whole stack on a local kind cluster, from nothing, idempotently.
#
#   ./k8s/up.sh                          create the cluster if needed, build + load images, install
#   IMAGE_TAG=<tag> ./k8s/up.sh          skip the build and install images already loaded
#   ./k8s/up.sh --set rollout.preStopSleepSeconds=0      extra arguments go to `helm upgrade`
#
# Every kubectl and helm call names the context. On this machine the current context is usually
# another project's cluster, and an unpinned `apply` would install a payment engine into it without
# a word. `kind create cluster` also SWITCHES the current context to the new cluster; this script
# puts back whatever it was.
#
# Cannot run beside the Compose stack: both publish localhost:8084 and :9090. Stop that first
# (`docker compose -f infra/docker-compose.yml stop`), and give Docker the memory - see k8s/README.md.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

CLUSTER=dpe
CTX="kind-$CLUSTER"
K=(kubectl --context "$CTX")
SERVICES=(payment-orchestrator account-service payment-gateway ui)
# Third-party images, loaded from the local Docker cache so the node does not pull them - and so a
# cluster can be rebuilt offline. Versions match infra/docker-compose.yml.
INFRA_IMAGES=(postgres:16-alpine redpandadata/redpanda:v25.3.17 redis:7-alpine prom/prometheus:v3.7.3
              busybox:1.37 grafana/k6:2.2.0)   # the last two: k8s/verify-netpol.sh, k8s/rollout-under-load.sh

step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
die()  { echo "error: $*" >&2; exit 2; }

for tool in docker kind kubectl helm openssl; do
    command -v "$tool" >/dev/null || die "$tool is not on PATH"
done
docker info >/dev/null 2>&1 || die "Docker is not running"

if docker ps --format '{{.Names}}' | grep -qx dpe-ui; then
    die "the Compose stack is running (dpe-ui holds localhost:8084) - docker compose -f infra/docker-compose.yml stop"
fi

# ---------------------------------------------------------------------------- cluster

step "cluster '$CLUSTER'"
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
    echo "exists"
    # A stopped node container (docker stop, or Docker Desktop restarted) is a cluster kind still
    # lists and kubectl cannot reach.
    docker start "$CLUSTER-control-plane" >/dev/null
else
    previous="$(kubectl config current-context 2>/dev/null || true)"
    kind create cluster --config k8s/kind/cluster.yaml
    if [ -n "$previous" ] && [ "$previous" != "$CTX" ]; then
        kubectl config use-context "$previous" >/dev/null
        echo "current kubectl context restored to '$previous' - this cluster is '$CTX'"
    fi
fi
"${K[@]}" wait --for=condition=Ready node --all --timeout=120s >/dev/null

# ---------------------------------------------------------------------------- images

if [ -n "${IMAGE_TAG:-}" ]; then
    TAG="$IMAGE_TAG"
    step "images: using tag $TAG (not building)"
else
    # The commit, so a pod names the code it runs. A dirty tree gets a unique suffix instead: an
    # image built from uncommitted changes must not carry a commit's name, and reusing one tag for
    # two builds would leave running pods on the old image (IfNotPresent sees the tag and stops).
    TAG="$(git rev-parse --short HEAD)"
    if [ -n "$(git status --porcelain)" ]; then TAG="$TAG-dirty-$(date +%s)"; fi
    step "images: building $TAG"
    # Compose's build definitions ARE the image definitions - one Dockerfile per image, one place.
    docker compose -f infra/docker-compose.yml build "${SERVICES[@]}"
    for svc in "${SERVICES[@]}"; do
        docker tag "dpe-$svc:latest" "dpe/$svc:$TAG"
    done
fi

# NOT `kind load docker-image`. It imports with `ctr images import --all-platforms`, and an image
# pulled from Docker Hub is a multi-platform INDEX of which Docker Desktop's containerd image store
# holds one platform - so the import fails on the first missing one:
#   ctr: content digest sha256:b882...: not found
# Our own images are built single-platform and loaded fine, which made it look like a problem with
# postgres. Exporting just this platform and importing that works for both kinds.
NODE="$CLUSTER-control-plane"
PLATFORM="$(docker version --format '{{.Server.Os}}/{{.Server.Arch}}')"
load_image() {
    local img="$1"
    if docker exec "$NODE" crictl inspecti "$img" >/dev/null 2>&1; then
        echo "  $img - already on the node"
        return
    fi
    docker image inspect "$img" >/dev/null 2>&1 || docker pull -q "$img"
    echo "  $img - importing ($PLATFORM)"
    docker save --platform "$PLATFORM" "$img" \
        | docker exec -i "$NODE" ctr --namespace=k8s.io images import --digests --snapshotter=overlayfs - >/dev/null
}

step "images: loading into the node"
for svc in "${SERVICES[@]}"; do
    load_image "dpe/$svc:$TAG"
done
for img in "${INFRA_IMAGES[@]}"; do
    load_image "$img"
done

# ---------------------------------------------------------------------------- infrastructure

step "infrastructure (namespace dpe-infra)"
"${K[@]}" apply -f k8s/infra/00-namespace.yaml
# ConfigMaps FROM the Compose files, never copies of them: one init script, one bootstrap file.
"${K[@]}" -n dpe-infra create configmap postgres-init \
    --from-file=10-init-databases.sh=infra/postgres/init-databases.sh --dry-run=client -o yaml \
    | "${K[@]}" apply -f -
"${K[@]}" -n dpe-infra create configmap redpanda-bootstrap \
    --from-file=bootstrap.yaml=infra/redpanda/bootstrap.yaml --dry-run=client -o yaml \
    | "${K[@]}" apply -f -
"${K[@]}" apply -f k8s/infra/
"${K[@]}" -n dpe-infra rollout status statefulset/postgres --timeout=180s
"${K[@]}" -n dpe-infra rollout status statefulset/redpanda --timeout=180s
"${K[@]}" -n dpe-infra rollout status deployment/redis --timeout=120s

# ---------------------------------------------------------------------------- signing key

step "signing key"
"${K[@]}" create namespace dpe --dry-run=client -o yaml | "${K[@]}" apply -f -
if "${K[@]}" -n dpe get secret dpe-signing-key >/dev/null 2>&1; then
    echo "dpe-signing-key exists - kept (a new key would invalidate every token in flight)"
else
    # The shapes SigningKeys reads: base64 PKCS#8 DER private, base64 X.509 DER public. Written to
    # files and loaded --from-file, so the key never appears on a command line (ps would show it).
    #
    # `pkcs8 -topk8` is not optional. `genpkey -outform DER` in OpenSSL 3.2 writes an RSA key as
    # PKCS#1 - SEQUENCE { 0, modulus, ... } with no algorithm identifier - although its PEM output is
    # PKCS#8. Java then fails "algid parse error, not a sequence", and the orchestrator (correctly)
    # refuses to start rather than sign with anything else. The first cluster crash-looped on it.
    keydir="$(mktemp -d)"
    trap 'rm -rf "$keydir"' EXIT
    openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$keydir/key.pem" 2>/dev/null
    openssl pkcs8 -topk8 -nocrypt -in "$keydir/key.pem" -outform DER -out "$keydir/key.der"
    openssl pkey -in "$keydir/key.pem" -pubout -outform DER -out "$keydir/pub.der"
    base64 -w0 "$keydir/key.der" > "$keydir/private-key"
    base64 -w0 "$keydir/pub.der" > "$keydir/public-key"
    "${K[@]}" -n dpe create secret generic dpe-signing-key \
        --from-file=private-key="$keydir/private-key" --from-file=public-key="$keydir/public-key"
    rm -rf "$keydir"
    echo "dpe-signing-key created (RSA 2048). It exists only in this cluster."
fi

# ---------------------------------------------------------------------------- the chart

step "helm upgrade --install dpe ($TAG)"
helm upgrade --install dpe k8s/helm/dpe \
    --kube-context "$CTX" --namespace dpe \
    --set image.tag="$TAG" \
    --set-file prometheus.alertRules=infra/prometheus/alerts.yml \
    --wait --timeout 10m \
    "$@"

"${K[@]}" -n dpe get pods -o wide
echo
echo "Console     http://localhost:8084"
echo "Prometheus  http://localhost:9090/targets"
