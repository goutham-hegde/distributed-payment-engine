#!/usr/bin/env bash
#
# SCENARIO 1 - the broker is killed while transfers are being accepted.
#
# HYPOTHESIS: the API keeps accepting (a POST writes Postgres, never Kafka), every command waits
# in an outbox, and the relays publish it all when the broker returns. Nothing is lost, nothing
# is sent twice to effect, and every invariant holds.
#
# The OUTAGE length is the variable that matters, and the scenario takes it as a parameter:
#
#   OUTAGE=15  (default) shorter than the 30s saga deadline. This is the hypothesis as written.
#   OUTAGE=45  longer than the deadline. The sweeper runs on the orchestrator's database, which is
#              still up, so it times the sagas out while their ReserveFunds is still sitting in
#              the outbox - and the outbox does not know the saga gave up. Run this one and look
#              at S1.
#
# `docker kill` + `docker start` restarts the SAME container, so the broker's log survives -
# Redpanda's on the image's anonymous volume, Kafka's on the container's own filesystem. Which
# broker is killed is whichever the stack is running on (BROKER in lib.sh); run it against both
# with infra/docker-compose.kafka.yml, because the consumer stall this scenario found has only ever
# been seen on one of them.

source "$(dirname "$0")/lib.sh"

N="${N:-12}"
AMOUNT=4000
OUTAGE="${OUTAGE:-15}"

begin_scenario "01 broker dies (${OUTAGE}s outage)"

alice_acct="$(open_account alice 1000000)"
bob_acct="$(open_account bob 0)"
record_baseline
alice_tok="$(token alice)"

# Some traffic before the kill, so the outage lands on sagas at every stage rather than only on
# ones that had not started.
fire_transfers 4 4 "$alice_tok" "$alice_acct" "$bob_acct" "$AMOUNT" "$WORK/sent"

log "KILL $BROKER"
killed_at="$(sql payments_db "SELECT now()")"
docker kill "$BROKER_CONTAINER" >/dev/null
t0=$SECONDS

fire_transfers "$((N - 4))" 4 "$alice_tok" "$alice_acct" "$bob_acct" "$AMOUNT" "$WORK/sent"
accepted_ids "$WORK/sent" > "$WORK/ids"
refused="$(awk '$1 != 202' "$WORK/sent" | wc -l | tr -d ' ')"
unpublished="$(sql payments_db "SELECT COUNT(*) FROM outbox WHERE published_at IS NULL")"
log "with the broker dead: $(wc -l < "$WORK/ids" | tr -d ' ') accepted, $refused refused, $unpublished command(s) waiting in the orchestrator's outbox"

remaining=$((OUTAGE - (SECONDS - t0)))
[ "$remaining" -gt 0 ] && sleep "$remaining"

log "START $BROKER"
docker start "$BROKER_CONTAINER" >/dev/null
wait_healthy "$BROKER_CONTAINER" 120

wait_terminal "$WORK/ids" || true
wait_quiescent || true

step "scenario-specific"
ids="$(in_list "$WORK/ids")"
expect_eq "the API accepted every POST during the outage" "$refused" "0"
expect_eq "commands were held in the outbox, not lost" "$([ "$unpublished" -gt 0 ] && echo yes || echo no)" "yes"
log "outcomes: $(sql payments_db "SELECT string_agg(status || ' ' || n, ', ') FROM (SELECT status, COUNT(*) n FROM saga_instances WHERE transfer_id IN ($ids) GROUP BY status) s")"
# The assertion the first version of this scenario did not make, and passed without. Its first run
# reported HYPOTHESIS HELD with every one of twelve transfers COMPENSATED - the invariants were all
# true, and "bob credited exactly for the COMPLETED transfers" was 0 = 0. An outage shorter than the
# saga deadline should DELAY payments, not fail them; a run where it fails all of them is a finding
# (in that run: the gateway's consumer never resumed after the broker restart), not a pass.
#
# "Sub-deadline" is judged on the outage the SYSTEM saw, not on OUTAGE. OUTAGE is kill-to-start;
# what the sagas wait out is kill-to-first-publish, which adds however long the broker takes to
# serve again. On Redpanda that is a couple of seconds. On Apache Kafka it measured ~14 s, so
# OUTAGE=15 was a 31 s outage against a 30 s deadline: the four pre-kill sagas timed out 0.85 s
# before the first publish and were (correctly) compensated, and the old `OUTAGE -lt 30` check
# called that a refutation. The margin is one sweep interval plus the pre-kill traffic.
# Only rows WRITTEN after the kill: they cannot have been published until the broker was back. The
# first version took the first publish after the kill instant and read 0 s on four runs of six - the
# relay's in-flight sends from before the kill were acknowledged in the same instant it landed.
effective="$(sql payments_db "SELECT COALESCE(ROUND(EXTRACT(EPOCH FROM MIN(published_at) - '$killed_at'::timestamptz)), -1)
                               FROM outbox WHERE created_at > '$killed_at'::timestamptz + interval '1 second'
                                 AND published_at IS NOT NULL")"
log "effective outage (kill -> first publish of a command written after it): ${effective}s, of which ${OUTAGE}s was the broker being down"
if [ "$effective" -ge 0 ] && [ "$effective" -lt 25 ]; then
    expect_eq "a sub-deadline outage delays payments, it does not fail them: all COMPLETED" \
        "$(sql payments_db "SELECT COUNT(*) FROM saga_instances WHERE transfer_id IN ($ids) AND status = 'COMPLETED'")" "$N"
else
    log "not a sub-deadline outage on this broker - asserting correctness only, not that every payment completed"
fi
lags="$(group_lag payment-gateway)"
expect_eq "the gateway's consumer group has caught up (lag)" "${lags:-unknown}" "0"
# Every ledger movement is either a completed transfer or a round trip - never a one-way debit.
expect_eq "bob credited exactly for the COMPLETED transfers" "$(balance_of "$bob_acct")" \
    "$(( $(sql payments_db "SELECT COUNT(*) FROM transfers WHERE id IN ($ids) AND status = 'COMPLETED'") * AMOUNT ))"

finish_scenario
