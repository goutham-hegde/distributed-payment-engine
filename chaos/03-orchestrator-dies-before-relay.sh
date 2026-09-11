#!/usr/bin/env bash
#
# SCENARIO 3 - the orchestrator is killed after committing transfers and BEFORE its relay has
# published their commands.
#
# That window is 500ms wide in normal operation (the relay's poll interval), which is far too
# narrow to hit by timing a kill. So the scenario makes it wide on purpose: the broker is PAUSED
# first, the transfers are accepted and committed, the scenario confirms their ReserveFunds rows
# are sitting unpublished, and only then is the orchestrator killed. The claim under test is
# exactly the outbox's claim - a committed row is a message that WILL be sent, by whichever
# process next runs the relay, with no memory of the process that wrote it.
#
# HYPOTHESIS: the restarted orchestrator publishes every stranded command; no saga is lost, none
# is started twice, every invariant holds.
#
# Watch the timing. The saga deadline is 30s of WALL CLOCK from start(), and it keeps running
# while the orchestrator is dead. If the restart takes as long as the deadline, the sweeper and
# the relay race on first boot for the same sagas. That race is logged, not hidden.

source "$(dirname "$0")/lib.sh"

N="${N:-8}"
AMOUNT=2500

begin_scenario "03 orchestrator dies before its relay publishes"

alice_acct="$(open_account alice 1000000)"
bob_acct="$(open_account bob 0)"
record_baseline
alice_tok="$(token alice)"

log "PAUSE $BROKER (the relay can no longer publish)"
docker pause "$BROKER_CONTAINER" >/dev/null

t0=$SECONDS
fire_transfers "$N" 4 "$alice_tok" "$alice_acct" "$bob_acct" "$AMOUNT" "$WORK/sent"
accepted_ids "$WORK/sent" > "$WORK/ids"
ids="$(in_list "$WORK/ids")"

stranded="$(sql payments_db "SELECT COUNT(*) FROM outbox WHERE published_at IS NULL AND event_type = 'ReserveFunds' AND aggregate_id IN ($ids)")"
log "committed and unpublished: $stranded ReserveFunds row(s)"

log "KILL payment-orchestrator"
docker kill dpe-orchestrator >/dev/null
log "UNPAUSE $BROKER"
docker unpause "$BROKER_CONTAINER" >/dev/null
log "START payment-orchestrator"
docker start dpe-orchestrator >/dev/null
wait_healthy dpe-orchestrator 180
log "orchestrator back $((SECONDS - t0))s after the first saga started (deadline is 30s)"

wait_terminal "$WORK/ids" || true
wait_quiescent || true

step "scenario-specific"
expect_eq "every ReserveFunds was stranded at the kill" "$stranded" "$N"
expect_eq "each saga's ReserveFunds published exactly once (one outbox row each)" \
    "$(sql payments_db "SELECT COUNT(*) FROM outbox WHERE event_type = 'ReserveFunds' AND aggregate_id IN ($ids) AND published_at IS NOT NULL")" "$N"
expect_eq "one hold per transfer, never two" \
    "$(sql accounts_db "SELECT COUNT(*) FROM holds WHERE transfer_id IN ($ids)")" "$N"
log "outcomes: $(sql payments_db "SELECT string_agg(status || ' ' || n, ', ') FROM (SELECT status, COUNT(*) n FROM saga_instances WHERE transfer_id IN ($ids) GROUP BY status) s")"
# M7, Fix E. Before it, the restarted sweeper ran for ~23 s before the consumer rejoined its group
# and FAILED sagas whose replies were already waiting unread. Gated on the listener holding its
# partitions (plus a grace to drain them), nothing here gives the sweeper a reason to act.
expect_eq "every saga COMPLETED - none timed out while the orchestrator was deaf" \
    "$(sql payments_db "SELECT COUNT(*) FROM saga_instances WHERE transfer_id IN ($ids) AND status = 'COMPLETED'")" "$N"
log "sweeper gate, from the orchestrator's log: $(docker logs --since 5m dpe-orchestrator 2>&1 | grep -oE 'saga timeouts paused|sweeping resumes' | sort | uniq -c | tr '\n' ' ')"

finish_scenario
