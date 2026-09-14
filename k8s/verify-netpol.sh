#!/usr/bin/env bash
#
# M10. Proves the NetworkPolicies are ENFORCED, not merely stored.
#
#   ./k8s/verify-netpol.sh          exit 0 every expectation held, 1 one did not, 2 could not run
#
# The API server accepts a NetworkPolicy on any cluster; only the network plugin decides whether it
# means anything. So a policy counts as working only after a connection it forbids has FAILED - and
# a failure alone proves nothing either, because "nothing is listening" fails the same way. Every
# BLOCKED expectation below is therefore paired with an ALLOWED one against the same port: the
# port is demonstrably open to the right client and closed to the wrong one.
#
# Probes are busybox `nc -w 3`: exit 0 = TCP connected; non-zero = timed out or refused.

set -uo pipefail

CTX="${KUBE_CONTEXT:-kind-dpe}"
NS=dpe
K=(kubectl --context "$CTX")
IMAGE=busybox:1.37
failures=0

pass() { printf '  \033[32mPASS\033[0m  %s\n' "$1"; }
fail() { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; failures=$((failures + 1)); }
die()  { echo "error: $*" >&2; exit 2; }

"${K[@]}" -n "$NS" get deploy payment-orchestrator >/dev/null 2>&1 \
    || die "no payment-orchestrator in namespace $NS on context $CTX - run k8s/up.sh first"

# ------------------------------------------------------------------ probe pods

PODS=()
cleanup() {
    for p in "${PODS[@]}"; do "${K[@]}" delete pod --wait=false --ignore-not-found $p >/dev/null 2>&1; done
}
trap cleanup EXIT

# probe_pod <namespace> <name> [labels]
#
# PID 1 gets no default signal handlers, so a bare `sleep 600` ignores SIGTERM and every probe pod
# sat out the full 30 s grace period - long enough for the next run to collide with its name
# ("object is being deleted: pods np-stranger already exists"). Hence the trap, and a per-run suffix.
probe_pod() {
    local ns="$1" name="$2" labels="${3:-}"
    "${K[@]}" -n "$ns" run "$name" --image="$IMAGE" --restart=Never \
        ${labels:+--labels="$labels"} --command -- sh -c 'trap "exit 0" TERM; sleep 600 & wait' \
        >/dev/null || die "could not start $ns/$name"
    PODS+=("-n $ns $name")
}

RUN="$(date +%s)"
STRANGER="np-stranger-$RUN" CLIENT="np-client-$RUN" OUTSIDER="np-outsider-$RUN"
echo "Starting probe pods ($IMAGE)"
probe_pod "$NS" "$STRANGER"                         # a pod in the app namespace, no special label
probe_pod "$NS" "$CLIENT" "dpe/api-client=true"      # what k6 runs as
probe_pod default "$OUTSIDER"                       # a pod in some other namespace
for p in "$NS $STRANGER" "$NS $CLIENT" "default $OUTSIDER"; do
    set -- $p
    "${K[@]}" -n "$1" wait --for=condition=Ready "pod/$2" --timeout=90s >/dev/null \
        || die "probe pod $1/$2 did not become ready"
done

# connects <ns> <pod> <host> <port>  -> 0 if TCP connected
connects() {
    "${K[@]}" -n "$1" exec "$2" -- nc -w 3 "$3" "$4" </dev/null >/dev/null 2>&1
}

expect() {
    local want="$1" label="$2"; shift 2
    if connects "$@"; then got=ALLOWED; else got=BLOCKED; fi
    if [ "$got" = "$want" ]; then pass "$label ($got)"; else fail "$label - expected $want, got $got"; fi
}

ORCH_IP="$("${K[@]}" -n "$NS" get pod -l app.kubernetes.io/name=payment-orchestrator \
    -o jsonpath='{.items[0].status.podIP}')"
ACCT_IP="$("${K[@]}" -n "$NS" get pod -l app.kubernetes.io/name=account-service \
    -o jsonpath='{.items[0].status.podIP}')"
[ -n "$ORCH_IP" ] && [ -n "$ACCT_IP" ] || die "could not read pod IPs"

echo
echo "The management port (permitAll since M6 - the reason these policies exist):"
expect BLOCKED "pod in $NS -> orchestrator :9091"          "$NS" "$STRANGER" "$ORCH_IP" 9091
expect BLOCKED "api client -> orchestrator :9091"          "$NS" "$CLIENT"   "$ORCH_IP" 9091
expect BLOCKED "pod in default -> account-service :9092"   default "$OUTSIDER" "$ACCT_IP" 9092
# The positive control for this port is Prometheus itself: every target it scrapes is a TCP
# connection to a management port that the probes above could not open.
#
# By NAME, not by count: the first version compared count(up == 1) with a count of pods, and a pod
# still terminating after a rollout sat in one list and not the other ("5 of 4 targets up").
scraped="$("${K[@]}" -n "$NS" exec deploy/prometheus -- \
    wget -qO- 'http://localhost:9090/api/v1/query?query=up{job="dpe"}==1' 2>/dev/null \
    | grep -o '"instance":"[^"]*"' | cut -d'"' -f4)"
# Running pods with a management port and no deletionTimestamp - the ones that must be scraped.
pods="$("${K[@]}" -n "$NS" get pods -l app.kubernetes.io/part-of=dpe --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.metadata.name}{"|"}{.metadata.deletionTimestamp}{"|"}{.spec.containers[0].ports[?(@.name=="management")].containerPort}{"\n"}{end}' \
    | awk -F'|' '$2 == "" && $3 != "" { print $1 }')"
missing=""
for p in $pods; do printf '%s\n' "$scraped" | grep -qx "$p" || missing="$missing $p"; done
if [ -n "$pods" ] && [ -z "$missing" ]; then
    pass "prometheus -> every management port ($(printf '%s\n' $pods | grep -c .) pods, all up) (ALLOWED)"
else
    fail "prometheus -> management ports: not up for:${missing:- (no pods found)}"
fi

echo
echo "The API port:"
expect ALLOWED "api client -> orchestrator :8081"          "$NS" "$CLIENT"   payment-orchestrator 8081
expect BLOCKED "pod in $NS -> orchestrator :8081"          "$NS" "$STRANGER" payment-orchestrator 8081

echo
echo "The stateful tier (dpe-infra):"
expect ALLOWED "pod in $NS -> postgres :5432"              "$NS" "$STRANGER" postgres.dpe-infra 5432
expect BLOCKED "pod in default -> postgres :5432"          default "$OUTSIDER" postgres.dpe-infra 5432
expect ALLOWED "pod in $NS -> redis :6379"                 "$NS" "$STRANGER" redis.dpe-infra 6379
expect BLOCKED "pod in default -> redis :6379"             default "$OUTSIDER" redis.dpe-infra 6379

echo
if [ "$failures" -eq 0 ]; then
    echo "NetworkPolicies are enforced."
    exit 0
fi
echo "$failures expectation(s) failed."
exit 1
