#!/usr/bin/env bash
#
# M10. The milestone's claim, as a test: a rolling deploy of every service, under load, loses
# nothing.
#
#   ./k8s/rollout-under-load.sh
#   RESTART="payment-orchestrator" ./k8s/rollout-under-load.sh     restart only these, in order
#   VUS=40 HOLD=240s ./k8s/rollout-under-load.sh                   busier, longer
#   DISRUPT=kill ./k8s/rollout-under-load.sh                       SIGKILL instead of a rollout
#
#   DISRUPT=freeze ./k8s/rollout-under-load.sh                     SIGSTOP instead: a hung process
#
# DISRUPT=kill SIGKILLs the JVM of the orchestrator pod that holds reply partitions, from the node
# (no preStop, no SIGTERM, no LeaveGroup). DISRUPT=freeze SIGSTOPs it instead: alive to the kernel,
# silent to everyone else - a stalled node, a pathological pause - until the kubelet's liveness
# probe restarts it. Unlike a quick crash-restart, nothing rejoins the group early, so the survivor
# keeps its partitions and its sweeper keeps running while the frozen member's partitions are
# orphaned. Either is a crash, not a deploy: money must still be right (I1-I5, S1-S4), but
# customers may see failures, so completions and timeouts are REPORTED, not asserted.
#
# Exit: 0 held and every k6 threshold met, 1 correctness REFUTED, 2 refused to start,
#       3 correct but a k6 threshold missed (e.g. a request that failed during a restart).
#
# What it does, in the order that makes the verdict mean something:
#   1. refuses to start unless the system is at rest (the chaos suite's definition: no saga in
#      flight, no unpublished outbox row, zero consumer lag) - otherwise it asserts about two runs;
#   2. opens accounts, THEN records the I3 baseline (funding is money entering the ledger);
#   3. runs loadtest/transfers.js as an in-cluster Job, `users` profile. IN the cluster, against the
#      Service: `kubectl port-forward` pins ONE pod and dies with it, so it cannot see a rollout at
#      all - it would measure the one thing a rolling deploy is guaranteed to break;
#   4. `kubectl rollout restart` each deployment in turn, waiting for each to finish;
#   5. waits for quiescence, then judges from the DATABASES - every saga the run started, not the
#      ones a VU happened to be polling - plus I1-I5 and S1-S4, plus k6's thresholds.

set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

CTX="${KUBE_CONTEXT:-kind-dpe}"
K=(kubectl --context "$CTX")
NS=dpe
CONSOLE="${CONSOLE_URL:-http://localhost:8084}"
ORCH="$CONSOLE/api/orchestrator"
ACCT="$CONSOLE/api/accounts"
export ORCHESTRATOR_URL="$ORCH"                  # for scripts/token.sh
export PG_EXEC="kubectl --context $CTX -n dpe-infra exec -i postgres-0 --"
K6_IMAGE="${K6_IMAGE:-grafana/k6:2.2.0}"
ACCOUNTS_PER_OWNER="${ACCOUNTS_PER_OWNER:-20}"
OPENING_BALANCE="${OPENING_BALANCE:-10000000}"
RESTART="${RESTART:-payment-orchestrator account-service payment-gateway}"
DISRUPT="${DISRUPT:-rollout}"
case "$DISRUPT" in rollout|kill|freeze) ;; *) echo "DISRUPT must be rollout, kill or freeze" >&2; exit 2 ;; esac
VUS="${VUS:-20}"
RAMP="${RAMP:-20s}"
HOLD="${HOLD:-200s}"
THINK_MIN="${THINK_MIN:-1}"
THINK_MAX="${THINK_MAX:-2}"
# How long after the ramp the first restart starts - long enough for a steady state to exist.
SETTLE_BEFORE_RESTART="${SETTLE_BEFORE_RESTART:-30}"

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-rollout"
RUN_DIR="$ROOT/k8s/results/$RUN_ID"
mkdir -p "$RUN_DIR"
exec > >(tee -a "$RUN_DIR/run.log") 2>&1

log()  { printf '\033[90m%s\033[0m  %s\n' "$(date +%H:%M:%S)" "$*"; }
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
die()  { printf '\033[31merror:\033[0m %s\n' "$*" >&2; exit 2; }

sql() {
    $PG_EXEC psql -U postgres -d "$1" -tAq -v ON_ERROR_STOP=1 -c "$2" | tr -d '\r' | sed '/^$/d'
}

group_lag() {
    "${K[@]}" -n dpe-infra exec redpanda-0 -- rpk group describe "$1" 2>/dev/null \
        | awk '$1 == "TOTAL-LAG" { print $2 }'
}

# The chaos suite's quiescence (chaos/lib.sh wait_quiescent), against the cluster.
wait_quiescent() {
    local timeout="$1" deadline=$((SECONDS + $1)) sagas pending lag g l
    while :; do
        sagas="$(sql payments_db "SELECT COUNT(*) FROM saga_instances WHERE status NOT IN ('COMPLETED','COMPENSATED','FAILED')")"
        pending=0
        for db in payments_db accounts_db gateway_db; do
            pending=$((pending + $(sql "$db" "SELECT COUNT(*) FROM outbox WHERE published_at IS NULL")))
        done
        lag=0
        for g in payment-orchestrator account-service payment-gateway; do
            l="$(group_lag "$g")"; case "$l" in ''|*[!0-9]*) l=0 ;; esac
            lag=$((lag + l))
        done
        if [ "$sagas" = 0 ] && [ "$pending" = 0 ] && [ "$lag" = 0 ]; then log "quiescent"; return 0; fi
        if [ $SECONDS -ge $deadline ]; then
            log "NOT quiescent after ${timeout}s: $sagas saga(s) in flight, $pending outbox row(s), $lag lag"
            return 1
        fi
        sleep 3
    done
}

json_field() { sed -n "s/.*\"$1\":\"\([^\"]*\)\".*/\1/p"; }

# ----------------------------------------------------------------------------------- preflight

step "preflight ($CTX)"
for d in payment-orchestrator account-service payment-gateway ui prometheus; do
    "${K[@]}" -n "$NS" rollout status "deploy/$d" --timeout=10s >/dev/null 2>&1 \
        || die "deployment $d is not fully rolled out - run k8s/up.sh"
done
"${K[@]}" -n "$NS" get job k6 >/dev/null 2>&1 && die "a k6 Job already exists - another run in progress? (kubectl -n $NS delete job k6)"
wait_quiescent 60 || die "not quiescent before the run - this run would be judged on someone else's work"
"${K[@]}" -n "$NS" get pods -o wide

# ----------------------------------------------------------------------------------- seeding

step "seeding $((ACCOUNTS_PER_OWNER * 2)) accounts"
OP_TOKEN="$(./scripts/token.sh operator)" || die "could not get an operator token through $ORCH"
: > "$RUN_DIR/accounts.txt"
for owner in alice bob; do
    for _ in $(seq "$ACCOUNTS_PER_OWNER"); do
        id="$(curl -sS -m 30 -X POST "$ACCT/accounts" \
            -H "Authorization: Bearer $OP_TOKEN" -H 'Content-Type: application/json' \
            -d "{\"ownerId\":\"$owner\",\"currency\":\"INR\",\"openingBalanceMinor\":$OPENING_BALANCE}" \
            | json_field id)"
        [ -n "$id" ] || die "could not open an account for $owner"
        printf '%s %s\n' "$owner" "$id" >> "$RUN_DIR/accounts.txt"
    done
done
want=$((ACCOUNTS_PER_OWNER * 2))
ids="$(awk '{ printf "%s\x27%s\x27", (NR > 1 ? "," : ""), $2 }' "$RUN_DIR/accounts.txt")"
deadline=$((SECONDS + 60))
until [ "$(sql payments_db "SELECT COUNT(*) FROM account_owners WHERE account_id IN ($ids)")" = "$want" ]; do
    [ $SECONDS -lt $deadline ] || die "accounts never reached the orchestrator's projection"
    sleep 1
done
awk 'BEGIN { printf "[" } NR > 1 { printf "," } { printf "{\"owner\":\"%s\",\"id\":\"%s\"}", $1, $2 }
     END { print "]" }' "$RUN_DIR/accounts.txt" > "$RUN_DIR/accounts.json"
log "$want accounts open and visible to the orchestrator"

BASELINE_FILE="$RUN_DIR/i3-baseline" ./scripts/verify-invariants.sh baseline

# ----------------------------------------------------------------------------------- load

step "k6 ($K6_IMAGE, users: $VUS VUs, ramp $RAMP, hold $HOLD, think ${THINK_MIN}-${THINK_MAX}s)"
since="$(sql payments_db "SELECT now()")"
"${K[@]}" -n "$NS" create configmap k6-run --from-file=transfers.js=loadtest/transfers.js \
    --from-file=accounts.json="$RUN_DIR/accounts.json" --dry-run=client -o yaml | "${K[@]}" apply -f - >/dev/null

"${K[@]}" -n "$NS" apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: k6
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        # The NetworkPolicy allowance for reaching the API ports (see the chart's network-policies.yaml).
        dpe/api-client: "true"
    spec:
      restartPolicy: Never
      automountServiceAccountToken: false
      containers:
        - name: k6
          image: $K6_IMAGE
          command: ["sh", "-c"]
          # The JSON summary is printed after a marker so it survives the pod: the logs are the only
          # thing left of a finished Job.
          args:
            - k6 run --no-usage-report --summary-export /work/summary.json /scripts/transfers.js;
              code=\$?; echo "=== SUMMARY_JSON"; cat /work/summary.json; echo; exit \$code
          env:
            - { name: PROFILE, value: users }
            - { name: VUS, value: "$VUS" }
            - { name: RAMP, value: "$RAMP" }
            - { name: HOLD, value: "$HOLD" }
            - { name: THINK_MIN, value: "$THINK_MIN" }
            - { name: THINK_MAX, value: "$THINK_MAX" }
          volumeMounts:
            - { name: run, mountPath: /scripts/transfers.js, subPath: transfers.js }
            - { name: run, mountPath: /work/accounts.json, subPath: accounts.json }
            - { name: work, mountPath: /work }
          resources:
            requests: { cpu: 250m, memory: 256Mi }
            limits: { memory: 1Gi }
      volumes:
        - { name: run, configMap: { name: k6-run } }
        - { name: work, emptyDir: {} }
EOF
"${K[@]}" -n "$NS" wait --for=condition=Ready pod -l job-name=k6 --timeout=60s >/dev/null || die "k6 pod never started"
started=$SECONDS
ramp_s="${RAMP%s}"
log "k6 running; first restart in $((ramp_s + SETTLE_BEFORE_RESTART))s"
sleep $((ramp_s + SETTLE_BEFORE_RESTART))

# ----------------------------------------------------------------------------------- the deploy

: > "$RUN_DIR/restarts.txt"
if [ "$DISRUPT" != rollout ]; then
    case "$DISRUPT" in kill) SIG=KILL ;; freeze) SIG=STOP ;; esac
    step "SIG$SIG: the orchestrator pod holding reply partitions"
    # The pod whose consumers own dpe.account.events.v1 partitions - killing an idle one proves
    # nothing about the consumer group.
    victim_ip="$("${K[@]}" -n dpe-infra exec redpanda-0 -- rpk group describe payment-orchestrator 2>/dev/null \
        | awk '$1 == "dpe.account.events.v1" { print $NF; exit }')"
    victim="$("${K[@]}" -n "$NS" get pods -l app.kubernetes.io/name=payment-orchestrator \
        -o jsonpath="{range .items[?(@.status.podIP==\"$victim_ip\")]}{.metadata.name}{end}")"
    [ -n "$victim" ] || die "could not find the pod at $victim_ip holding reply partitions"
    desc="$("${K[@]}" -n dpe-infra exec redpanda-0 -- rpk group describe payment-orchestrator 2>/dev/null)"
    held="$(printf '%s\n' "$desc" | awk -v ip="$victim_ip" '$NF == ip && $1 !~ /dlt$/ { n++ } END { print n + 0 }')"
    # By MEMBER id, not by IP: the container restarts inside the same pod, with the same IP, and
    # rejoins as a new member - an IP-based wait would see the new member and call it "still dead".
    members="$(printf '%s\n' "$desc" | awk -v ip="$victim_ip" '$NF == ip { print $(NF-2) }' | sort -u)"

    # A CRASH, which is not what `kubectl delete pod --grace-period=0 --force` is. That deletes the
    # API object at once, but the container still receives a shutdown signal: the first run of this
    # mode saw its partitions handed over in 4 s - possible only if the consumers sent LeaveGroup, so
    # the JVM had run its shutdown - and zero failed requests. A fault that is quietly a graceful
    # stop makes the scenario pass. SIGKILL from the NODE (the container's own PID namespace ignores
    # a SIGKILL to its PID 1) is what an OOMKill is: no preStop, no LeaveGroup, no close.
    cid="$("${K[@]}" -n "$NS" get pod "$victim" -o jsonpath='{.status.containerStatuses[0].containerID}')"
    cid="${cid#containerd://}"
    node="$("${K[@]}" -n "$NS" get pod "$victim" -o jsonpath='{.spec.nodeName}')"
    pid="$(docker exec "$node" crictl inspect -o go-template --template '{{.info.pid}}' "$cid" 2>/dev/null)"
    [ -n "$pid" ] && [ "$pid" -gt 1 ] 2>/dev/null || die "could not find the host pid of $victim's container"
    log "SIG$SIG $victim ($victim_ip, pid $pid on $node, holds $held reply partitions; k6 at +$((SECONDS - started))s)"
    t0=$SECONDS
    docker exec "$node" kill -"$SIG" "$pid"
    # How long the dead members keep their partitions: a member that sent no LeaveGroup is noticed
    # only when session.timeout.ms (45 s here) passes without a heartbeat.
    while :; do
        desc="$("${K[@]}" -n dpe-infra exec redpanda-0 -- rpk group describe payment-orchestrator 2>/dev/null)"
        # An empty answer (mid-rebalance, or rpk failing) is not "reassigned".
        if printf '%s\n' "$desc" | grep -q '^STATE *Stable'; then
            still=0
            for m in $members; do printf '%s\n' "$desc" | grep -qF "$m" && still=1; done
            [ "$still" = 0 ] && break
        fi
        sleep 1
    done
    orphaned=$((SECONDS - t0))
    log "the dead members' partitions were reassigned after ${orphaned}s"
    printf 'SIG%s %s orphaned %ss\n' "$SIG" "$victim" "$orphaned" >> "$RUN_DIR/restarts.txt"
    "${K[@]}" -n "$NS" wait --for=condition=Ready "pod/$victim" --timeout=5m >/dev/null
    log "$victim ready again $((SECONDS - t0))s after the SIG$SIG (restarts: $("${K[@]}" -n "$NS" get pod "$victim" -o jsonpath='{.status.containerStatuses[0].restartCount}'))"
    RESTART=""
else
    step "rolling restart: $RESTART"
fi
for d in $RESTART; do
    t0=$SECONDS
    log "restart $d (k6 at +$((SECONDS - started))s)"
    "${K[@]}" -n "$NS" rollout restart "deploy/$d" >/dev/null
    if "${K[@]}" -n "$NS" rollout status "deploy/$d" --timeout=5m >/dev/null; then
        log "$d rolled out in $((SECONDS - t0))s"
        printf '%s %s\n' "$d" "$((SECONDS - t0))" >> "$RUN_DIR/restarts.txt"
    else
        log "$d did NOT finish rolling out in 5m"
        printf '%s FAILED\n' "$d" >> "$RUN_DIR/restarts.txt"
    fi
done
restarts_done=$((SECONDS - started))
"${K[@]}" -n "$NS" get pods -o wide

step "waiting for k6 to finish"
until "${K[@]}" -n "$NS" get job k6 -o jsonpath='{.status.conditions[*].type}' 2>/dev/null | grep -qE 'Complete|Failed'; do
    sleep 5
done
"${K[@]}" -n "$NS" logs job/k6 > "$RUN_DIR/k6.log" 2>&1
K6_EXIT="$("${K[@]}" -n "$NS" get pod -l job-name=k6 -o jsonpath='{.items[0].status.containerStatuses[0].state.terminated.exitCode}')"
sed -n '/=== SUMMARY_JSON/,$p' "$RUN_DIR/k6.log" | sed '1d' > "$RUN_DIR/summary.json"
sed '/=== SUMMARY_JSON/,$d' "$RUN_DIR/k6.log" | grep -vE '^\s*(running|default|users) ' | tail -60
log "k6 exited $K6_EXIT after $((SECONDS - started))s (restarts finished at +${restarts_done}s)"
"${K[@]}" -n "$NS" delete job k6 --wait=false >/dev/null
"${K[@]}" -n "$NS" delete configmap k6-run >/dev/null

# ----------------------------------------------------------------------------------- verdict

step "draining"
wait_quiescent 300 || true

step "what the system did (from saga_instances, not from k6)"
sql payments_db "
    SELECT '    ' || rpad(s.status, 12) || lpad(COUNT(*)::text, 7)
           || COALESCE('   ' || string_agg(DISTINCT t.failure_reason, ', '), '')
    FROM saga_instances s JOIN transfers t ON t.id = s.transfer_id
    WHERE s.created_at >= '$since' GROUP BY s.status ORDER BY COUNT(*) DESC"
total="$(sql payments_db "SELECT COUNT(*) FROM saga_instances WHERE created_at >= '$since'")"
completed="$(sql payments_db "SELECT COUNT(*) FROM saga_instances WHERE created_at >= '$since' AND status = 'COMPLETED'")"
timedout="$(sql payments_db "SELECT COUNT(*) FROM saga_instances WHERE created_at >= '$since' AND timed_out_at IS NOT NULL")"
sql payments_db "
    SELECT format('    settle (accept -> terminal): p50 %s s, p99 %s s, max %s s',
           round(percentile_cont(0.50) WITHIN GROUP (ORDER BY secs)::numeric, 2),
           round(percentile_cont(0.99) WITHIN GROUP (ORDER BY secs)::numeric, 2),
           round(MAX(secs)::numeric, 2))
    FROM (SELECT EXTRACT(EPOCH FROM completed_at - created_at) AS secs
          FROM saga_instances WHERE created_at >= '$since') x"

echo
echo "k6, the customer's view (summary.json):"
python3 -c '
import json, sys
m = json.load(sys.stdin).get("metrics", {})
def g(name, key):
    v = m.get(name, {})
    return v.get(key, v.get("values", {}).get(key, "-"))
print("    requests                      %s" % g("http_reqs", "count"))
print("    create failed (non-202/503)   rate %s" % g("http_req_failed{op:create}", "value"))
print("    all requests failed           rate %s" % g("http_req_failed", "value"))
print("    transfer_completed            rate %s" % g("transfer_completed", "value"))
print("    replay_consistent             rate %s" % g("replay_consistent", "value"))
print("    admission refusals (503)      %s" % g("admission_refused", "count"))
print("    create p99 (ms)               %s" % g("http_req_duration{op:create}", "p(99)"))
print("    settle p99 (ms)               %s" % g("transfer_settle_ms", "p(99)"))
' < "$RUN_DIR/summary.json" 2>/dev/null || echo "    (no summary.json - see $RUN_DIR/k6.log)"

step "checks"
failures=0
if [ "$DISRUPT" != rollout ]; then
    # A crash may fail payments; it may not lose money. Reported, and the invariants decide.
    printf '  NOTE  %s of %s sagas COMPLETED, %s timed out (a crash is allowed to cost customers)\n' \
        "$completed" "$total" "$timedout"
elif [ "$total" -gt 0 ] && [ "$completed" = "$total" ]; then
    printf '  \033[32mPASS\033[0m  every saga the run started COMPLETED (%s of %s)\n' "$completed" "$total"
else
    printf '  \033[31mFAIL\033[0m  %s of %s sagas COMPLETED\n' "$completed" "$total"; failures=$((failures + 1))
fi
if [ "$DISRUPT" != rollout ]; then
    :
elif [ "$timedout" = "0" ]; then
    printf '  \033[32mPASS\033[0m  no saga reached its deadline during the rollout\n'
else
    printf '  \033[31mFAIL\033[0m  %s saga(s) timed out - a restart stalled the pipeline past 30 s\n' "$timedout"; failures=$((failures + 1))
fi
if grep -q FAILED "$RUN_DIR/restarts.txt"; then
    printf '  \033[31mFAIL\033[0m  a rollout did not finish\n'; failures=$((failures + 1))
fi
BASELINE_FILE="$RUN_DIR/i3-baseline" ./scripts/verify-invariants.sh || failures=$((failures + 1))

echo
printf 'Results:      %s\n' "k8s/results/$RUN_ID"
if [ "$failures" -ne 0 ]; then
    printf '\033[31mCORRECTNESS:  REFUTED (%s check(s) failed)\033[0m\n' "$failures"
    exit 1
fi
printf '\033[32mCORRECTNESS:  HELD\033[0m\n'
if [ "$K6_EXIT" = "0" ]; then echo "k6:           every threshold met"; exit 0; fi
echo "k6:           exited $K6_EXIT - a threshold was missed (see k6.log)"
exit 3
