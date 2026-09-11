#!/usr/bin/env bash
#
# The M8 load run.
#
#   ./loadtest/run.sh                          PROFILE=smoke: 5 customers, 30 s - does the harness work
#   PROFILE=knee  ./loadtest/run.sh            stepped arrival rate - where does it stop keeping up
#   PROFILE=users ./loadtest/run.sh            1,000 concurrent customers, 30-90 s think time
#   PROFILE=users THINK_MIN=5 THINK_MAX=15 ./loadtest/run.sh     the same thousand, six times busier
#
# Every knob transfers.js reads can be set the same way (VUS, RAMP, HOLD, STAGES, RETRY_RATE, ...).
# ACCOUNTS_PER_OWNER (default 100) and OPENING_BALANCE (default 1000000) shape the seed.
#
# Exit: 0 correct and within SLO, 1 correctness REFUTED, 2 refused to start, 3 correct but SLO missed.
#
# A load run is structurally a chaos scenario with no fault, so it borrows the chaos harness whole:
#
#   * the same LOCK - a load run and a chaos scenario on one stack would each read the other's
#     traffic as its own, and report plausible wrong answers rather than errors (Session 18);
#   * the same refusal to start on a system that is not at rest;
#   * the same order: open the accounts, THEN record the I3 baseline - funding an account is money
#     entering the ledger, and after the baseline I3 would correctly call it a violation;
#   * the same verdict: I1-I5, plus S1-S4 judged on violations NEW to this run.
#
# Correctness and latency are reported separately and never averaged into one grade. A run that
# held every invariant and missed its latency SLO is a capacity finding; a run that met the SLO and
# broke an invariant is a bug, and no latency figure makes up for it.

source "$(dirname "$0")/../chaos/lib.sh"

LOAD_DIR="$ROOT_DIR/loadtest"
PROFILE="${PROFILE:-smoke}"
K6_IMAGE="${K6_IMAGE:-grafana/k6:2.2.0}"
ACCOUNTS_PER_OWNER="${ACCOUNTS_PER_OWNER:-100}"
OPENING_BALANCE="${OPENING_BALANCE:-1000000}"
# After the last iteration the saga pipeline may still be draining what the edge accepted. Long,
# because under overload that backlog is the result, and cutting the wait short would report it as
# stuck sagas instead.
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-900}"

export PROFILE

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$PROFILE"
RUN_DIR="$LOAD_DIR/results/$RUN_ID"
mkdir -p "$RUN_DIR"

# Everything this run prints is also its record. ONE redirect for the whole script rather than
# `run_checks | tee`, which would run the checks in a subshell: CHECK_FAILURES would be incremented
# there and read as 0 here, and a refuted run would report HELD.
exec > >(tee -a "$RUN_DIR/run.log") 2>&1

# ----------------------------------------------------------------------------------- seeding

# One POST /accounts. Prints "<owner> <id>", or "<owner> " when refused - the caller counts.
#
# The line is built first and written with ONE printf. Eight of these share a pipe, and a write
# under PIPE_BUF is atomic while two writes are not: the first version printed the owner and then
# let python print the id, the processes interleaved, and 91 of 200 lines came out garbled - every
# account opened, a hundred of them silently dropped from the list.
open_one() {
    local owner="$1" id
    id="$(curl -sS -m 30 -X POST "$ACCT/accounts" \
        -H "Authorization: Bearer $OP_TOKEN" -H 'Content-Type: application/json' \
        -d "{\"ownerId\":\"$owner\",\"currency\":\"INR\",\"openingBalanceMinor\":$OPENING_BALANCE}" \
        | json_field id 2>/dev/null)"
    printf '%s %s\n' "$owner" "$id"
}
export -f open_one
export ACCT OPENING_BALANCE

# chaos/lib.sh's open_account fetches a token and waits for the projection per account, which is
# right for three accounts and two minutes for two hundred. Same two guarantees, paid once each.
seed_accounts() {
    local want=$((ACCOUNTS_PER_OWNER * 2)) got ids deadline seen
    step "seeding $want accounts ($ACCOUNTS_PER_OWNER each for alice and bob, $OPENING_BALANCE minor units each)"
    OP_TOKEN="$(token operator)" || die "could not get an operator token"
    export OP_TOKEN
    for owner in alice bob; do
        seq "$ACCOUNTS_PER_OWNER" | xargs -P 8 -I{} bash -c 'open_one "$1"' _ "$owner"
    done | awk 'NF == 2' > "$RUN_DIR/accounts.txt"

    got="$(grep -c . "$RUN_DIR/accounts.txt")"
    [ "$got" = "$want" ] || die "opened $got of $want accounts"

    # The orchestrator learns of each account from an AccountOpened event. A transfer from an
    # account it has not heard of yet is a 403 - which would read as an authorization failure
    # under load, not the replication lag it is.
    awk '{ print $2 }' "$RUN_DIR/accounts.txt" > "$WORK/seed-ids"
    ids="$(in_list "$WORK/seed-ids")"
    deadline=$((SECONDS + 60))
    while :; do
        seen="$(sql payments_db "SELECT COUNT(*) FROM account_owners WHERE account_id IN ($ids)")"
        [ "$seen" = "$want" ] && break
        [ $SECONDS -lt $deadline ] || die "only $seen of $want accounts reached the orchestrator's projection"
        sleep 1
    done
    log "all $want accounts visible to the orchestrator"

    # awk, not python: python3 on this machine is the Windows build, which cannot open a POSIX path
    # like /g/project/... - hand it files on stdin or not at all.
    awk 'BEGIN { printf "[" } NR > 1 { printf "," } { printf "{\"owner\":\"%s\",\"id\":\"%s\"}", $1, $2 }
         END { print "]" }' "$RUN_DIR/accounts.txt" > "$RUN_DIR/accounts.json"
}

# ----------------------------------------------------------------------------------- sampling

# Container CPU and memory every 5 s. The point is the COMPARISON: a bottleneck that is a thread
# asleep holds a queue at low CPU, and one that is a saturated core does not - which one this is
# decides what fixing it looks like.
sample_containers() {
    while :; do
        docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' 2>/dev/null \
            | sed "s/^/$(date +%H:%M:%S),/" >> "$RUN_DIR/docker-stats.csv"
        sleep 5
    done
}

# One PromQL instant query, printed as "<label value> <number>" lines.
prom() {
    local label="$1" query="$2"
    curl -sS -m 10 -G 'http://localhost:9090/api/v1/query' --data-urlencode "query=$query" 2>/dev/null \
        | python3 -c 'import json,sys
label=sys.argv[1]
try:
    for r in json.load(sys.stdin)["data"]["result"]:
        print("    %-22s %s" % (r["metric"].get(label, "-"), round(float(r["value"][1]), 3)))
except Exception:
    print("    (prometheus unavailable)")' "$label"
}

# ----------------------------------------------------------------------------------- report

report() {
    local since="$1" secs="$2"
    step "$SCENARIO - what the system did (from saga_instances, not from k6)"

    # Precise where k6's settle time is not: this is commit-of-the-transfer to commit-of-the-last-
    # step, with no polling interval in it, for EVERY saga the run started rather than for the ones
    # a VU happened to still be watching.
    echo "Outcomes:"
    sql payments_db "
        SELECT '    ' || rpad(s.status, 12) || lpad(COUNT(*)::text, 7)
               || COALESCE('   ' || string_agg(DISTINCT t.failure_reason, ', '), '')
        FROM saga_instances s JOIN transfers t ON t.id = s.transfer_id
        WHERE s.created_at >= '$since'
        GROUP BY s.status ORDER BY COUNT(*) DESC"

    echo
    echo "Per 30 s window, by when the saga STARTED (seconds from accept to terminal):"
    printf '    %-8s %8s %8s %8s %8s   %7s %7s %7s %7s\n' window started "/s" done timedout p50 p95 p99 max
    sql payments_db "
        WITH w AS (
            SELECT date_bin('30 seconds', s.created_at, '$since') AS win, s.status, s.timed_out_at,
                   EXTRACT(EPOCH FROM s.completed_at - s.created_at) AS secs
            FROM saga_instances s WHERE s.created_at >= '$since')
        SELECT format('    %-8s %8s %8s %8s %8s   %7s %7s %7s %7s',
                   to_char(win, 'HH24:MI:SS'), COUNT(*), round(COUNT(*) / 30.0, 1),
                   COUNT(*) FILTER (WHERE status = 'COMPLETED'),
                   COUNT(*) FILTER (WHERE timed_out_at IS NOT NULL),
                   round(percentile_cont(0.50) WITHIN GROUP (ORDER BY secs)::numeric, 2),
                   round(percentile_cont(0.95) WITHIN GROUP (ORDER BY secs)::numeric, 2),
                   round(percentile_cont(0.99) WITHIN GROUP (ORDER BY secs)::numeric, 2),
                   round(MAX(secs)::numeric, 2))
        FROM w GROUP BY win ORDER BY win"

    echo
    echo "Throughput: accepted vs settled, over the whole run:"
    sql payments_db "
        SELECT format('    accepted %s/s, settled %s/s (first accept to last terminal: %s s)',
                   round(COUNT(*) / GREATEST(EXTRACT(EPOCH FROM MAX(created_at) - MIN(created_at)), 1)::numeric, 1),
                   round(COUNT(*) / GREATEST(EXTRACT(EPOCH FROM MAX(completed_at) - MIN(created_at)), 1)::numeric, 1),
                   round(EXTRACT(EPOCH FROM MAX(completed_at) - MIN(created_at))::numeric, 0))
        FROM saga_instances WHERE created_at >= '$since'"

    echo
    echo "Peaks during the run (Prometheus, ${secs}s window, 10 s scrape - a peak shorter than that is invisible):"
    echo "  outbox backlog (rows):";          prom application "max by (application) (max_over_time(dpe_outbox_backlog[${secs}s]))"
    echo "  oldest unpublished row (s):";     prom application "max by (application) (max_over_time(dpe_outbox_age_seconds[${secs}s]))"
    echo "  sagas in flight:";                prom application "max by (application) (max_over_time(sum by (application) (dpe_saga_inflight)[${secs}s:10s]))"
    echo "  Hikari threads waiting:";         prom application "max by (application) (max_over_time(hikaricp_connections_pending[${secs}s]))"
    echo "  consumer lag (records):";         prom application "max by (application) (max_over_time(sum by (application) (kafka_consumer_fetch_manager_records_lag)[${secs}s:10s]))"
    echo "  GC pause, worst (s):";            prom application "max by (application) (max_over_time(jvm_gc_pause_seconds_max[${secs}s]))"
    # M8's two load-shedding mechanisms. Zero is not automatically good news: at a rate the
    # pipeline cannot drain, zero refusals means the edge is still promising work it cannot do.
    echo "  admission refusals (503 AT_CAPACITY):"; prom application "sum by (application) (increase(dpe_admission_refused_total[${secs}s]))"
    echo "  bulkhead refusals (503 BUSY):";       prom application "sum by (application) (increase(dpe_bulkhead_rejected_total[${secs}s]))"
    echo "  bulkhead permits in use, peak:";      prom application "max by (application) (max_over_time(dpe_bulkhead_in_use[${secs}s]))"

    if [ -s "$RUN_DIR/docker-stats.csv" ]; then
        echo
        echo "Container CPU during the run (100% = one core; this machine has $(nproc)):"
        python3 -c 'import csv, sys, collections
cpu = collections.defaultdict(list)
for row in csv.reader(sys.stdin):
    if len(row) >= 3 and row[2].endswith("%"):
        cpu[row[1]].append(float(row[2][:-1]))
for name, xs in sorted(cpu.items(), key=lambda kv: -max(kv[1])):
    xs.sort()
    print("    %-18s mean %6.1f%%   p95 %6.1f%%   max %6.1f%%" % (
        name, sum(xs) / len(xs), xs[int(0.95 * (len(xs) - 1))], xs[-1]))' < "$RUN_DIR/docker-stats.csv"
    fi
}

# ----------------------------------------------------------------------------------- the run

begin_scenario "load: $PROFILE ($RUN_ID)"
seed_accounts
record_baseline

# The database's clock, not this shell's: every query below compares it against created_at.
since="$(sql payments_db "SELECT now()")"
started_at=$SECONDS

sample_containers &
SAMPLER=$!
trap 'kill $SAMPLER 2>/dev/null; cleanup' EXIT

step "k6 ($K6_IMAGE, PROFILE=$PROFILE) - results in loadtest/results/$RUN_ID"
# k6 runs INSIDE the Compose network and talks to payment-orchestrator:8081 directly. From the
# host it would go through Docker Desktop's port forwarder, which is a userspace proxy with its own
# ceiling - the run would measure that. Capped at 2 GB so it cannot take the RAM the stack needs.
docker run --rm --name dpe-k6 --network dpe_default --memory 2g \
    -v "$(cygpath -m "$LOAD_DIR"):/scripts:ro" \
    -v "$(cygpath -m "$RUN_DIR"):/work" \
    -e PROFILE -e VUS -e MAX_VUS -e RAMP -e HOLD -e THINK_MIN -e THINK_MAX -e STAGES \
    -e RETRY_RATE -e POLL_TIMEOUT -e POLL_INTERVAL \
    -e K6_WEB_DASHBOARD=true -e K6_WEB_DASHBOARD_EXPORT=/work/report.html \
    "$K6_IMAGE" run --no-usage-report --summary-export /work/summary.json /scripts/transfers.js
K6_EXIT=$?
k6_secs=$((SECONDS - started_at))
log "k6 exited $K6_EXIT after ${k6_secs}s"

step "draining"
# Not the harness's 10 s: under overload the edge has accepted work the pipeline has not done, and
# how long it takes to catch up is part of the result.
drain_from=$SECONDS
wait_quiescent "$DRAIN_TIMEOUT" || true
log "drained ${SECONDS}s after the start, $((SECONDS - drain_from))s after k6 finished"
kill $SAMPLER 2>/dev/null

report "$since" "$((SECONDS - started_at + 15))"
run_checks

echo
case "$K6_EXIT" in
    # knee sets no latency thresholds - it exists to exceed them - so k6 passing there means only
    # that the correctness thresholds held. The first knee run printed "MET" over a collapse.
    0)  if [ "$PROFILE" = "knee" ]; then slo="not judged (knee exists to exceed it - read the per-window table)"
        else slo="MET"; fi ;;
    99) slo="MISSED (k6 thresholds - see the summary above)" ;;
    *)  slo="UNKNOWN (k6 exited $K6_EXIT)" ;;
esac
printf 'SLO:          %s\n' "$slo"
if [ "$CHECK_FAILURES" -eq 0 ]; then
    printf '\033[32mCORRECTNESS:  HELD\033[0m\n'
    [ "$K6_EXIT" = "0" ] && exit 0
    exit 3
fi
printf '\033[31mCORRECTNESS:  REFUTED (%s check(s) failed)\033[0m\n' "$CHECK_FAILURES"
exit 1
