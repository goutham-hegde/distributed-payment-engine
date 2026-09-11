#!/usr/bin/env bash
#
# SCENARIO 8 - account-service is cut off from the network in the middle of a transaction.
#
# Different from scenario 2 in the way that matters: a killed process on a live network closes its
# sockets and Postgres rolls its transaction back at once. A partition closes nothing. Its Kafka
# consumer is still "in" the group until the session times out, and a transaction that was open at
# the cut still holds its row locks, its advisory lock and its uncommitted inbox row on the
# database side until Postgres notices the peer is gone - which, with default TCP keepalives, is two
# hours.
#
#   MODE=partition  (default) cut, wait OUTAGE seconds, reconnect. The process keeps running.
#   MODE=crash      cut, then SIGKILL the process while it is cut off, then start a fresh one.
#                   The machine-lost-power case: nothing ever tells Postgres, so without a server
#                   side timeout the orphan's locks outlive the process by hours.
#
# THE CUT IS AIMED. The first version cut at a random moment and, in two runs, landed on no open
# transaction - "the half-open lock case was not exercised" was its honest verdict twice. A
# transaction here lasts milliseconds. So the harness now freezes the container (docker pause), asks
# Postgres whether one of its backends holds a transaction id with the client frozen, and cuts only
# then; otherwise it thaws, waits 100 ms and tries again.
#
# HYPOTHESIS: the orphaned transaction is reaped by idle_in_transaction_session_timeout (30 s), the
# service recovers without anybody terminating a backend by hand, every saga reaches a terminal
# state, and every invariant holds. Measured before that setting existed (M7, MODE=crash): the
# orphan was still there 150 s later, the restarted service's only consumer thread was blocked on
# its uncommitted inbox row, and lag climbed without bound.
#
# The reconnect restores the Compose DNS alias explicitly. `docker network connect` without
# --alias gives the container back its network but not its service name, so everything that
# addresses it as `account-service` (Prometheus, the console's nginx) would still be partitioned
# after the "heal" - a heal that half-heals, which reads as a bug in the thing under test.

source "$(dirname "$0")/lib.sh"

# No N: traffic runs until the cut lands (see below).
AMOUNT=1500
OUTAGE="${OUTAGE:-20}"
MODE="${MODE:-partition}"
NET="${NET:-dpe_default}"
# idle_in_transaction_session_timeout, plus margin for the check interval.
REAP_WITHIN="${REAP_WITHIN:-45}"

case "$MODE" in partition|crash) ;; *) die "MODE must be partition or crash, not '$MODE'" ;; esac

begin_scenario "08 account-service cut off mid-transaction (MODE=$MODE)"

alice_acct="$(open_account alice 1000000)"
bob_acct="$(open_account bob 0)"
record_baseline
alice_tok="$(token alice)"

# A backend of accounts_db that holds a transaction id in an open transaction while its client is
# frozen: it has written, or row-locked (a relay claim's FOR UPDATE over real rows gets one too -
# and holds outbox row locks, so it is as real an orphan as a reserve). A plain SELECT has none.
open_writing_tx="SELECT COUNT(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
                  WHERE a.datname = 'accounts_db' AND a.state LIKE 'idle in transaction%'
                    AND l.locktype = 'transactionid' AND l.granted"
orphans="SELECT COUNT(*) FROM pg_stat_activity
          WHERE datname = 'accounts_db' AND state LIKE 'idle in transaction%'
            AND now() - state_change > interval '5 seconds'"

# Traffic runs until the cut has landed, not for a fixed N. A fixed batch drains in a few seconds,
# each aiming try costs most of a second (two docker CLI calls and a psql), and the chance that a
# pause lands inside a millisecond transaction is small per try - the first version caught one on
# try 1 three times running and then missed 300 times in a row once the batch had simply finished.
touch "$WORK/traffic-on"
( while [ -f "$WORK/traffic-on" ]; do
      fire_transfers 4 4 "$alice_tok" "$alice_acct" "$bob_acct" "$AMOUNT" "$WORK/sent"
  done ) &
traffic=$!
sleep 2

aimed=no
for try in $(seq 1 300); do
    docker pause dpe-account >/dev/null
    if [ "$(sql postgres "$open_writing_tx")" != "0" ]; then
        log "try $try: a writing transaction is open with its client frozen - CUT"
        docker network disconnect "$NET" dpe-account
        aimed=yes
        break
    fi
    docker unpause dpe-account >/dev/null
    sleep 0.1
done
[ "$aimed" = "yes" ] || { docker unpause dpe-account >/dev/null 2>&1; rm -f "$WORK/traffic-on"; wait "$traffic"; die "never caught an open writing transaction in 300 tries"; }

if [ "$MODE" = "crash" ]; then
    log "KILL account-service while it is cut off - nothing will ever tell Postgres"
    docker kill dpe-account >/dev/null
else
    docker unpause dpe-account >/dev/null
fi
t_cut=$SECONDS
# A few more seconds of traffic, so the cut also lands on sagas that start while it is in force.
sleep 3
rm -f "$WORK/traffic-on"
wait "$traffic"
accepted_ids "$WORK/sent" > "$WORK/ids"

log "open writing transactions with no live client, right after the cut: $(sql postgres "$open_writing_tx")"
sleep "$OUTAGE"

log "RECONNECT account-service (with its service alias)"
docker network connect --alias account-service --alias dpe-account "$NET" dpe-account
[ "$MODE" = "crash" ] && docker start dpe-account >/dev/null
wait_healthy dpe-account 180

# Reaped by the server, not by us. Measured from the cut: the orphan went idle then.
reaped=no
while [ $((SECONDS - t_cut)) -lt $((REAP_WITHIN + OUTAGE)) ]; do
    [ "$(sql postgres "$orphans")" = "0" ] && { reaped=yes; break; }
    sleep 2
done
if [ "$reaped" = "yes" ]; then
    log "orphaned transaction reaped $((SECONDS - t_cut))s after the cut"
else
    log "orphaned transaction STILL THERE $((SECONDS - t_cut))s after the cut"
fi

wait_terminal "$WORK/ids" || true
wait_quiescent || true

step "scenario-specific"
ids="$(in_list "$WORK/ids")"
log "outcomes: $(sql payments_db "SELECT string_agg(status || ' ' || n, ', ') FROM (SELECT status, COUNT(*) n FROM saga_instances WHERE transfer_id IN ($ids) GROUP BY status) s")"
expect_eq "the orphaned transaction was reaped by Postgres, not by hand" "$reaped" "yes"
expect_eq "no accounts_db backend left idle in a transaction" "$(sql postgres "$orphans")" "0"
expect_eq "no accounts_db backend left waiting on a lock" \
    "$(sql postgres "SELECT COUNT(*) FROM pg_stat_activity WHERE datname = 'accounts_db' AND wait_event_type = 'Lock'")" "0"
expect_eq "bob credited exactly for the COMPLETED transfers" "$(balance_of "$bob_acct")" \
    "$(( $(sql payments_db "SELECT COUNT(*) FROM transfers WHERE id IN ($ids) AND status = 'COMPLETED'") * AMOUNT ))"

finish_scenario
