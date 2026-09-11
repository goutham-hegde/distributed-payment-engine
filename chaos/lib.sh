#!/usr/bin/env bash
#
# Shared machinery for the chaos scenarios. Sourced, never run.
#
# Every scenario has the same skeleton, and this file exists so that the skeleton cannot be got
# wrong in one of eight places:
#
#   begin_scenario     refuse to start on a dirty system - a scenario that inherits in-flight
#                      sagas from the last one is asserting about both, and proves neither
#   open_account       fund fresh accounts, and wait until the orchestrator can SEE them
#   record_baseline    only now - I3 compares two instants, and opening a funded account after
#                      the baseline issues new money that I3 would correctly call a violation
#   ... inject the fault, drive traffic, WAIT FOR TERMINAL SAGAS, restore the fault ...
#   finish_scenario    wait for quiescence, then every invariant plus the stranded-money checks
#
# The order "wait for terminal, THEN restore" is the one rule M6 learned the hard way: the API
# answers 202 before the gateway is ever reached, so restoring a fault straight after the last
# POST asserts against a system that had already recovered.

set -uo pipefail

# Git Bash rewrites any argument that looks like a POSIX path into a Windows one before docker sees
# it. Harmless for everything here today, and a baffling "no such file" the day it is not.
export MSYS_NO_PATHCONV=1

if ! command -v docker >/dev/null 2>&1; then
    export PATH="$PATH:/c/Program Files/Docker/Docker/resources/bin"
fi

CHAOS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$CHAOS_DIR/.." && pwd)"
COMPOSE=(docker compose -f "$ROOT_DIR/infra/docker-compose.yml")

ORCH="${ORCHESTRATOR_URL:-http://localhost:8081}"
ACCT="${ACCOUNT_URL:-http://localhost:8082}"
GW="${GATEWAY_URL:-http://localhost:8083}"
PG="${PG_CONTAINER:-dpe-postgres}"

# Which broker the stack is running on: redpanda (the default Compose file) or kafka (with
# infra/docker-compose.kafka.yml on top). Detected from what is running rather than configured, so
# a scenario cannot be pointed at one broker while the services talk to the other - that run would
# kill a broker nobody uses and report that the system shrugged it off.
if [ -z "${BROKER:-}" ]; then
    if [ "$(docker inspect -f '{{.State.Running}}' dpe-kafka 2>/dev/null)" = "true" ]; then
        BROKER=kafka
    else
        BROKER=redpanda
    fi
fi
BROKER_CONTAINER="dpe-$BROKER"

# The saga's whole-saga deadline is 30s and the sweeper runs every 5s, so the slowest honest path
# to a terminal state is ~35s after the last fault clears. Quiescence waits comfortably past that.
QUIESCE_TIMEOUT="${QUIESCE_TIMEOUT:-150}"

WORK="$(mktemp -d "${TMPDIR:-/tmp}/dpe-chaos.XXXXXX")"
SCENARIO=""
CHECK_FAILURES=0

# ----------------------------------------------------------------------------------- output

log()  { printf '\033[90m%s\033[0m  %s\n' "$(date +%H:%M:%S)" "$*"; }
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
pass() { printf '  \033[32mPASS\033[0m  %s\n' "$*"; }
fail() { printf '  \033[31mFAIL\033[0m  %s\n' "$*"; CHECK_FAILURES=$((CHECK_FAILURES + 1)); }
die()  { printf '\033[31merror:\033[0m %s\n' "$*" >&2; exit 2; }

# ----------------------------------------------------------------------------------- data

# One value, no headers, whitespace stripped. ON_ERROR_STOP so a typo in a query is an error and
# not an empty string that compares equal to something.
sql() {
    local db="$1" query="$2"
    docker exec -i "$PG" psql -U postgres -d "$db" -tAq -v ON_ERROR_STOP=1 -c "$query" \
        | tr -d '\r' | sed '/^$/d'
}

# Reads one field out of a JSON document on stdin. python rather than jq for the same reason as
# scripts/token.sh: it is already on every machine that can build this repo.
json_field() {
    python3 -c 'import json,sys
d=json.load(sys.stdin)
v=d.get(sys.argv[1])
print("" if v is None else v)' "$1"
}

# ----------------------------------------------------------------------------------- auth

token() {
    "$ROOT_DIR/scripts/token.sh" "$@"
}

# ----------------------------------------------------------------------------------- setup

# Opens a funded CUSTOMER account and prints its id.
#
# Does not return until the orchestrator's ownership projection has the row. Opening is a write
# to accounts_db, and the orchestrator learns of it from an AccountOpened event some hundreds of
# milliseconds later - a transfer fired in that window is refused 403 by the edge check, which
# would read as an authorization bug rather than the replication lag it is.
open_account() {
    local owner="$1" balance="$2" op body id
    op="$(token operator)" || die "could not get an operator token"
    body="$(curl -sS -X POST "$ACCT/accounts" \
        -H "Authorization: Bearer $op" -H 'Content-Type: application/json' \
        -d "{\"ownerId\":\"$owner\",\"currency\":\"INR\",\"openingBalanceMinor\":$balance}")"
    id="$(printf '%s' "$body" | json_field id)"
    [ -n "$id" ] || die "could not open an account for $owner: $body"

    local deadline=$((SECONDS + 30))
    until [ "$(sql payments_db "SELECT COUNT(*) FROM account_owners WHERE account_id = '$id'")" = "1" ]; do
        [ $SECONDS -lt $deadline ] || die "account $id never reached the orchestrator's projection"
        sleep 0.3
    done
    printf '%s' "$id"
}

balance_of() {
    sql accounts_db "SELECT balance_minor FROM accounts WHERE id = '$1'"
}

record_baseline() {
    "$ROOT_DIR/scripts/verify-invariants.sh" baseline
}

# ----------------------------------------------------------------------------------- traffic

# POSTs one transfer. Prints "<http status> <transfer id>" - the id is empty when the request was
# refused, and the caller decides whether a refusal is a failure (it is not, under a broker
# outage the API is SUPPOSED to keep accepting; under a partition from the database it is not).
post_transfer() {
    local tok="$1" from="$2" to="$3" amount="$4" key="${5:-$(python3 -c 'import uuid;print(uuid.uuid4())')}"
    local out code body
    out="$(curl -sS -m 20 -w '\n%{http_code}' -X POST "$ORCH/api/v1/transfers" \
        -H "Authorization: Bearer $tok" -H 'Content-Type: application/json' \
        -H "Idempotency-Key: $key" \
        -d "{\"fromAccountId\":\"$from\",\"toAccountId\":\"$to\",\"amountMinor\":$amount,\"currency\":\"INR\"}" \
        2>/dev/null)"
    code="$(printf '%s' "$out" | tail -n1)"
    body="$(printf '%s' "$out" | sed '$d')"
    printf '%s %s\n' "${code:-000}" "$(printf '%s' "$body" | json_field transferId 2>/dev/null)"
}
export -f post_transfer json_field
export ORCH

# Fires N transfers with P in flight at once and appends "<status> <id>" lines to $out.
fire_transfers() {
    local n="$1" parallel="$2" tok="$3" from="$4" to="$5" amount="$6" out="$7"
    seq "$n" | xargs -P "$parallel" -I{} bash -c 'post_transfer "$@"' _ "$tok" "$from" "$to" "$amount" >> "$out"
}

accepted_ids() {
    awk '$1 == 202 && $2 != "" { print $2 }' "$1"
}

# Turns an id file into a SQL IN-list body: 'a','b','c'
in_list() {
    sed "s/.*/'&'/" "$1" | paste -sd, -
}

# ----------------------------------------------------------------------------------- the gateway

# Sets the gateway's fault knobs, and DIES if they did not take.
#
# The first version of this function logged a warning on failure, and the first run of scenario 4
# fired thirty transfers at a gateway that had answered 401 and was approving everything - M5 put
# /admin/** behind OPERATOR, and SimulationController's Javadoc still says it is unauthenticated.
# Every standard check passed. Only the scenario's own "were they compensated?" assertion noticed.
#
# That is the rule this function now enforces: A FAULT INJECTOR THAT CAN FAIL QUIETLY MAKES EVERY
# SCENARIO PASS. So it authenticates, requires a 200, and echoes the knobs the gateway reports
# holding AFTER the call - the scenario's log then records what was actually in force, not what
# was asked for.
gateway_set() {
    local out code body
    out="$(curl -sS -m 5 -w '\n%{http_code}' -X POST "$GW/admin/simulation" \
        -H "Authorization: Bearer $(token operator)" -H 'Content-Type: application/json' \
        -d "$1" 2>&1)"
    code="$(printf '%s' "$out" | tail -n1)"
    body="$(printf '%s' "$out" | sed '$d')"
    [ "$code" = "200" ] || die "gateway refused the fault injection (HTTP $code): $body"
    log "gateway now: $body"
}

gateway_reset() {
    gateway_set '{"failureRate":0.0,"latencyMs":50,"timeoutRate":0.0,"duplicateCallbackRate":0.0}'
}

# ----------------------------------------------------------------------------------- waiting

wait_healthy() {
    local container="$1" timeout="${2:-180}" deadline=$((SECONDS + ${2:-180}))
    log "waiting for $container to report healthy"
    until [ "$(docker inspect -f '{{.State.Health.Status}}' "$container" 2>/dev/null)" = "healthy" ]; do
        [ $SECONDS -lt $deadline ] || die "$container not healthy after ${timeout}s"
        sleep 2
    done
}

# Waits until every transfer in the id file has a TERMINAL saga. This is the wait that must come
# before restoring a fault.
wait_terminal() {
    local ids="$1" timeout="${2:-$QUIESCE_TIMEOUT}" deadline n left
    n="$(wc -l < "$ids" | tr -d ' ')"
    [ "$n" -gt 0 ] || return 0
    deadline=$((SECONDS + timeout))
    while :; do
        left="$(sql payments_db "SELECT COUNT(*) FROM saga_instances WHERE transfer_id IN ($(in_list "$ids"))
                 AND status NOT IN ('COMPLETED','COMPENSATED','FAILED')")"
        [ "$left" = "0" ] && { log "all $n sagas terminal"; return 0; }
        [ $SECONDS -lt $deadline ] || { log "$left of $n sagas still in flight after ${timeout}s"; return 1; }
        sleep 2
    done
}

# QUIESCENCE: no saga in flight AND nothing waiting in any outbox. Both halves matter - a saga can
# be terminal while its final reply's duplicate is still queued, and an outbox row is a message
# that has not happened yet.
#
# Returns 1 rather than exiting: a system that never quiesces is a RESULT (I4 is about to fail and
# say why), not a reason to skip the checks.
#
# AND no consumer lag. The first version had only the two database halves, and scenario 1's first
# run met that definition with twelve ChargeGateway commands sitting unread in the gateway's topic:
# the gateway's consumer had silently stopped fetching after the broker restart, the sweeper had
# compensated every saga, and every outbox was empty. A message that has been published and not
# consumed is exactly as much "work in flight" as an outbox row is; it is only in a different box.
#
# group_lag prints one group's total lag, or nothing if the broker could not be asked. The Kafka
# tool prints "-" as CURRENT-OFFSET for a partition the group has never committed on; its whole
# log is then unread, so the log-end offset IS the lag, not zero.
group_lag() {
    local g="$1"
    case "$BROKER" in
        redpanda)
            docker exec dpe-redpanda rpk group describe "$g" 2>/dev/null \
                | awk '$1 == "TOTAL-LAG" { print $2 }' ;;
        kafka)
            docker exec dpe-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
                --bootstrap-server localhost:9092 --describe --group "$g" 2>/dev/null \
                | awk '$1 == g && $3 ~ /^[0-9]+$/ {
                           if ($6 ~ /^[0-9]+$/) s += $6; else if ($5 ~ /^[0-9]+$/) s += $5; seen = 1
                       }
                       END { if (seen) print s + 0 }' g="$g" ;;
    esac
}

consumer_lag() {
    local total=0 g lag
    if [ "$BROKER" = "kafka" ]; then
        # One JVM start for all three groups instead of three: this runs every 3 s while waiting
        # for quiescence, and kafka-consumer-groups.sh takes seconds to boot inside a 1 GB box.
        docker exec dpe-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
            --bootstrap-server localhost:9092 --describe --all-groups 2>/dev/null \
            | awk '($1 == "payment-orchestrator" || $1 == "account-service" || $1 == "payment-gateway") && $3 ~ /^[0-9]+$/ {
                       if ($6 ~ /^[0-9]+$/) s += $6; else if ($5 ~ /^[0-9]+$/) s += $5
                   }
                   END { print s + 0 }'
        return
    fi
    for g in payment-orchestrator account-service payment-gateway; do
        lag="$(group_lag "$g")"
        case "$lag" in ''|*[!0-9]*) lag=0 ;; esac
        total=$((total + lag))
    done
    printf '%s' "$total"
}

wait_quiescent() {
    local timeout="${1:-$QUIESCE_TIMEOUT}" deadline sagas pending lag
    deadline=$((SECONDS + timeout))
    log "waiting for quiescence (up to ${timeout}s)"
    while :; do
        sagas="$(sql payments_db "SELECT COUNT(*) FROM saga_instances WHERE status NOT IN ('COMPLETED','COMPENSATED','FAILED')")"
        pending=0
        for db in payments_db accounts_db gateway_db; do
            pending=$((pending + $(sql "$db" "SELECT COUNT(*) FROM outbox WHERE published_at IS NULL")))
        done
        lag="$(consumer_lag)"
        if [ "$sagas" = "0" ] && [ "$pending" = "0" ] && [ "$lag" = "0" ]; then
            log "quiescent"
            return 0
        fi
        if [ $SECONDS -ge $deadline ]; then
            log "NOT quiescent after ${timeout}s: ${sagas} saga(s) in flight, ${pending} outbox row(s) unpublished, ${lag} message(s) of consumer lag"
            return 1
        fi
        sleep 3
    done
}

# ----------------------------------------------------------------------------------- lifecycle

cleanup() {
    # A scenario that dies half way must not leave the next one a declining gateway, a paused
    # broker, or a service off the network. Every restore here is idempotent. The gateway reset is
    # in a subshell because gateway_set dies on failure, and a dying cleanup must still unpause.
    ( gateway_reset ) >/dev/null 2>&1 || true
    for c in "$BROKER_CONTAINER" dpe-orchestrator dpe-account dpe-gateway; do
        [ "$(docker inspect -f '{{.State.Paused}}' "$c" 2>/dev/null)" = "true" ] && docker unpause "$c" >/dev/null
    done
    # Scenario 6 stops Redis and scenario 8 takes account-service off the network.
    [ "$(docker inspect -f '{{.State.Running}}' dpe-redis 2>/dev/null)" = "false" ] && docker start dpe-redis >/dev/null
    if ! docker inspect -f '{{json .NetworkSettings.Networks}}' dpe-account 2>/dev/null | grep -q dpe_default; then
        docker network connect --alias account-service --alias dpe-account dpe_default dpe-account 2>/dev/null
    fi
    # Scenario 8 MODE=crash kills it while it is cut off.
    [ "$(docker inspect -f '{{.State.Running}}' dpe-account 2>/dev/null)" = "false" ] && docker start dpe-account >/dev/null
    rm -rf "$WORK"
    [ "${LOCK_HELD:-0}" = "1" ] && rm -rf "$LOCK_DIR"
}

# ONE SCENARIO AT A TIME. Two harnesses against one stack each pause, cut, fund and baseline under
# the other, and what comes out is not an error but plausible, wrong verdicts: in Session 18 a
# stopped run-all's loop outlived its wrapper and ran 07 and 08 on top of a re-run of 05 and 06,
# which reported miscounted callbacks, an I3 "violation" of 6000 (the other harness funding accounts
# after this one's baseline) and an aim that never landed. mkdir is atomic, so it is the lock; the
# pid inside lets a lock left by a SIGKILLed run be recognised as stale instead of blocking forever.
LOCK_DIR="$CHAOS_DIR/.lock"
take_lock() {
    if ! mkdir "$LOCK_DIR" 2>/dev/null; then
        local holder
        holder="$(cat "$LOCK_DIR/pid" 2>/dev/null)"
        if [ -n "$holder" ] && kill -0 "$holder" 2>/dev/null; then
            die "another chaos scenario is running (pid $holder) - two at once give wrong answers, not errors"
        fi
        log "removing a stale lock left by pid ${holder:-unknown}"
        rm -rf "$LOCK_DIR"
        mkdir "$LOCK_DIR" 2>/dev/null || die "could not take the chaos lock at $LOCK_DIR"
    fi
    echo "$$" > "$LOCK_DIR/pid"
    LOCK_HELD=1
}

begin_scenario() {
    SCENARIO="$1"
    step "$SCENARIO"
    take_lock
    log "broker: $BROKER ($BROKER_CONTAINER)"
    trap cleanup EXIT
    gateway_reset
    for c in dpe-postgres "$BROKER_CONTAINER" dpe-orchestrator dpe-account dpe-gateway; do
        wait_healthy "$c" 60
    done
    if ! wait_quiescent 60; then
        die "system is not quiescent before the scenario starts - fix that first, or this run asserts about two things at once"
    fi
    DEAD_LETTERS_BEFORE="$(dead_letter_count)"
    snapshot_violations
}

dead_letter_count() {
    local total=0
    for db in payments_db accounts_db gateway_db; do
        total=$((total + $(sql "$db" "SELECT COUNT(*) FROM dead_letters WHERE replayed_at IS NULL")))
    done
    printf '%s' "$total"
}

# ----------------------------------------------------------------------------------- the checks

# Each of S1-S4 is written as a query that lists its VIOLATIONS, one id per line, rather than as a
# count - so that a scenario can be judged on the violations IT created.
#
# The five invariants are global and must be: money is either conserved or it is not. S1-S4 are
# global too, but a violation of them is a thing somebody has to reconcile, and until they do it
# persists - a hold stranded by scenario 2 is still stranded when scenario 7 finishes. Judged
# globally, one refuted scenario would refute every scenario after it, and the suite would report
# one bug eight times. So begin_scenario snapshots the violations already present, and a check fails
# only on NEW ones; inherited ones are printed as a note, never hidden.
#
# The harness is allowed to join two databases where a service is not: it is an operator's tool
# holding a superuser connection for the length of a test, not a component in the payment path.
# The rule about cross-database reads is about what a SERVICE may hold.

# The queries themselves live in scripts/lib/stranded.sh, shared with verify-invariants.sh and
# reconcile.sh - see there for what each one catches. What is particular to the harness is only the
# judgement: new violations fail a scenario, inherited ones are a note.
source "$ROOT_DIR/scripts/lib/stranded.sh"
violations_s1() { stranded_s1; }
violations_s2() { stranded_s2; }
violations_s3() { stranded_s3; }
violations_s4() { stranded_s4; }

snapshot_violations() {
    local c
    for c in s1 s2 s3 s4; do "violations_$c" > "$WORK/$c.before"; done
}

# check <id> <label when clean> <label for violations>
check_new() {
    local c="$1" ok_label="$2" bad_label="$3" new inherited
    "violations_$c" > "$WORK/$c.after"
    new="$(comm -13 "$WORK/$c.before" "$WORK/$c.after")"
    inherited="$(comm -12 "$WORK/$c.before" "$WORK/$c.after" | sed '/^$/d' | wc -l | tr -d ' ')"
    if [ -z "$new" ]; then
        pass "$ok_label"
    else
        fail "$bad_label: $(printf '%s
' "$new" | wc -l | tr -d ' ') new - $(printf '%s
' "$new" | head -3 | paste -sd' ' -)"
    fi
    [ "$inherited" != "0" ] && printf '  [33mNOTE[0m  %s: %s violation(s) inherited from before this scenario, not counted
' "${c^^}" "$inherited"
    return 0
}

run_checks() {
    step "$SCENARIO - verdict"

    # --no-stranded: S1-S4 are judged below on NEW violations only.
    if "$ROOT_DIR/scripts/verify-invariants.sh" --no-stranded; then
        pass "I1-I5 (scripts/verify-invariants.sh)"
    else
        fail "I1-I5 (scripts/verify-invariants.sh) - see the output above"
    fi

    echo
    echo "Beyond the five (new violations only):"
    check_new s1 "S1  no money stranded in an ACTIVE hold after quiescence"                  "S1  STRANDED - ACTIVE hold(s) with every saga terminal"
    check_new s2 "S2  every approved PSP charge belongs to a COMPLETED transfer"                  "S2  CHARGED at the PSP but not COMPLETED"
    check_new s3 "S3  every COMPLETED transfer has an approved PSP charge"                  "S3  COMPLETED with no approved PSP charge"
    check_new s4 "S4  no transfer PENDING under a terminal saga"                  "S4  PENDING under a terminal saga"

    # Informational, not a check: a dead letter is the system asking for a human, which some
    # scenarios produce on purpose.
    local dl_after
    dl_after="$(dead_letter_count)"
    if [ "$dl_after" != "$DEAD_LETTERS_BEFORE" ]; then
        printf '  [33mNOTE[0m  dead letters awaiting replay: %s -> %s
' "$DEAD_LETTERS_BEFORE" "$dl_after"
    fi
}

finish_scenario() {
    # Short: every scenario has already waited out its own recovery before its specific checks. A
    # second full-length wait here only doubles the runtime of the scenarios that FAILED to settle,
    # which are precisely the ones whose answer is already known.
    wait_quiescent 10 || true
    run_checks
    echo
    if [ "$CHECK_FAILURES" -eq 0 ]; then
        printf '\033[32m%s: HYPOTHESIS HELD\033[0m\n' "$SCENARIO"
        exit 0
    fi
    printf '\033[31m%s: HYPOTHESIS REFUTED (%s check(s) failed)\033[0m\n' "$SCENARIO" "$CHECK_FAILURES"
    exit 1
}

# A scenario-specific assertion, reported alongside the standard ones.
expect_eq() {
    local label="$1" actual="$2" expected="$3"
    if [ "$actual" = "$expected" ]; then
        pass "$label ($actual)"
    else
        fail "$label: got $actual, expected $expected"
    fi
}
