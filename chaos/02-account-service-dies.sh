#!/usr/bin/env bash
#
# SCENARIO 2 - account-service is killed (SIGKILL, no graceful shutdown) mid-saga, and stays dead
# past the saga deadline.
#
# Two timings, because "between reserve and commit" is not one moment and the two halves of it
# break different things:
#
#   WHEN=after-charge    (default) killed once every saga is RESERVED, with the gateway slowed so
#                        the charge is still in progress. The PSP approves while the participant
#                        that would settle the hold is dead, so each saga sits in CHARGED with its
#                        CommitFunds waiting in the topic.
#
#   WHEN=before-reserve  killed BEFORE the transfers are fired. Each ReserveFunds waits in the
#                        topic and the saga sits in STARTED.
#
# HYPOTHESIS (as written in the plan): the saga timeout fires, compensation runs, and every
# invariant holds once account-service is back.
#
#   ./chaos/02-account-service-dies.sh
#   WHEN=before-reserve ./chaos/02-account-service-dies.sh

source "$(dirname "$0")/lib.sh"

WHEN="${WHEN:-after-charge}"
N="${N:-6}"
AMOUNT=7000
# The saga deadline is 30s from start() and the sweeper runs every 5s. Staying down 45s past the
# first POST guarantees every saga in the batch has been swept at least once before the
# participant comes back - which is the situation the scenario is about.
OUTAGE="${OUTAGE:-45}"

begin_scenario "02 account-service dies ($WHEN)"

alice_acct="$(open_account alice 1000000)"
bob_acct="$(open_account bob 0)"
record_baseline

states() {
    sql payments_db "SELECT COALESCE(string_agg(status || ' ' || n, ', ' ORDER BY status), 'none')
                       FROM (SELECT status, COUNT(*) n FROM saga_instances
                              WHERE transfer_id IN ($(in_list "$WORK/ids")) GROUP BY status) s"
}

alice_tok="$(token alice)"

if [ "$WHEN" = "before-reserve" ]; then
    log "KILL account-service"
    docker kill dpe-account >/dev/null
    t0=$SECONDS
    fire_transfers "$N" "$N" "$alice_tok" "$alice_acct" "$bob_acct" "$AMOUNT" "$WORK/sent"
    accepted_ids "$WORK/sent" > "$WORK/ids"
    log "fired $(wc -l < "$WORK/ids" | tr -d ' ') transfers into a dead participant: $(states)"
else
    # Slow enough that the kill lands inside the charge, fast enough that the whole batch is
    # charged well inside the 30s deadline even if every transfer hashes to one partition.
    gateway_set '{"latencyMs":3000}'
    t0=$SECONDS
    fire_transfers "$N" "$N" "$alice_tok" "$alice_acct" "$bob_acct" "$AMOUNT" "$WORK/sent"
    accepted_ids "$WORK/sent" > "$WORK/ids"

    deadline=$((SECONDS + 20))
    until [ "$(sql payments_db "SELECT COUNT(*) FROM saga_instances WHERE transfer_id IN ($(in_list "$WORK/ids")) AND status = 'STARTED'")" = "0" ]; do
        [ $SECONDS -lt $deadline ] || die "reserves did not complete: $(states)"
        sleep 0.5
    done
    log "every reserve is done: $(states)"
    log "KILL account-service"
    docker kill dpe-account >/dev/null

    deadline=$((SECONDS + 30))
    until [ "$(sql payments_db "SELECT COUNT(*) FROM saga_instances WHERE transfer_id IN ($(in_list "$WORK/ids")) AND status = 'RESERVED'")" = "0" ]; do
        [ $SECONDS -lt $deadline ] || break
        sleep 1
    done
    log "gateway has answered with the participant dead: $(states)"
fi

remaining=$((OUTAGE - (SECONDS - t0)))
[ "$remaining" -gt 0 ] && { log "holding the outage for ${remaining}s more (past the deadline)"; sleep "$remaining"; }
log "deadline passed with account-service still dead: $(states)"

log "START account-service"
docker start dpe-account >/dev/null
wait_healthy dpe-account 180
gateway_reset

wait_terminal "$WORK/ids" 90 || true
log "after recovery: $(states)"
wait_quiescent 60 || true

step "scenario-specific"
ids="$(in_list "$WORK/ids")"
expect_eq "every POST accepted while the participant was dead" "$(wc -l < "$WORK/ids" | tr -d ' ')" "$N"
log "holds for these transfers: $(sql accounts_db "SELECT COALESCE(string_agg(status || ' ' || n, ', '), 'none') FROM (SELECT status, COUNT(*) n FROM holds WHERE transfer_id IN ($ids) GROUP BY status) s")"
log "transfers: $(sql payments_db "SELECT COALESCE(string_agg(status || ' ' || n, ', '), 'none') FROM (SELECT status, COUNT(*) n FROM transfers WHERE id IN ($ids) GROUP BY status) s")"
log "bob's balance: $(balance_of "$bob_acct")   alice's: $(balance_of "$alice_acct")"

# The outcome each timing SHOULD reach, asserted directly - S1-S4 only say nothing contradicts,
# and a scenario must also say what it expected (M7). The two timings sit on opposite sides of the
# saga's pivot, so they must end in opposite places.
count_status() { sql payments_db "SELECT COUNT(*) FROM transfers WHERE id IN ($ids) AND status = '$1'"; }
if [ "$WHEN" = "before-reserve" ]; then
    # Before the pivot: recovery runs backward. The timeout's release is addressed by transfer id,
    # so whichever of it and the queued ReserveFunds lands first, no money stays moved.
    expect_eq "every transfer FAILED (the customer was told so)" "$(count_status FAILED)" "$N"
    expect_eq "alice is whole - no money left in CLEARING"        "$(balance_of "$alice_acct")" "1000000"
    expect_eq "bob received nothing"                              "$(balance_of "$bob_acct")" "0"
else
    # After the pivot: forward only. The PSP has the money, so the timeout re-sends CommitFunds
    # until the returning participant settles it. A release here would be the pre-M7 defect.
    expect_eq "every transfer COMPLETED (forward recovery)"       "$(count_status COMPLETED)" "$N"
    expect_eq "bob received every transfer"                       "$(balance_of "$bob_acct")" "$((N * AMOUNT))"
    expect_eq "no ReleaseFunds was ever sent past the pivot" \
        "$(sql payments_db "SELECT COUNT(*) FROM outbox WHERE event_type = 'ReleaseFunds' AND aggregate_id IN ($ids)")" "0"
fi

finish_scenario
