#!/usr/bin/env bash
#
# SCENARIO 5 - the PSP misbehaves the two ways real PSPs do.
#
# PART A - every approval is delivered TWICE (duplicateCallbackRate 1.0).
#
#   The plan's hypothesis was "the inbox dedupes; no double credit". Half of that is wrong, and
#   the scenario is written to show which half. The gateway publishes the duplicate as a SEPARATE
#   outbox row with a NEW message id - it is a genuinely separate delivery attempt, as a retried
#   webhook is - so the orchestrator's inbox, keyed on message id, ACCEPTS it. What absorbs the
#   duplicate is the saga's state guard: the second GatewayApproved finds the saga already past
#   RESERVED and is recorded as a SKIPPED step. So the assertions are:
#     - the inbox holds TWO GatewayApproved rows per transfer   (the inbox did not dedupe)
#     - one SKIPPED ChargeGateway step per transfer              (the state machine did)
#     - bob credited exactly N x amount                          (no double credit)
#   The inbox dedupes REDELIVERIES of one message. It cannot dedupe two messages that mean the
#   same thing; only something that knows what they mean can.
#
# PART B - the PSP stops answering (timeoutRate 1.0), then a human replays the dead letters.
#
#   Each charge throws, is retried with backoff, and dead-letters. The saga never hears back, sits
#   in RESERVED, and the sweeper compensates it: sender refunded, transfer FAILED. Then the
#   operator does the obvious thing with a dead letter queue - replays it - and the PSP, now
#   healthy, CHARGES a transfer the system has already reported as failed and refunded.
#
#   HYPOTHESIS (as the plan would have it): invariants hold. PREDICTION: I1-I5 all pass - they
#   are statements about accounts_db and payments_db, and the damage is in gateway_db. S2 fails.

source "$(dirname "$0")/lib.sh"

N="${N:-10}"
AMOUNT=3000
PART="${PART:-both}"

begin_scenario "05 gateway timeouts and duplicate callbacks ($PART)"

alice_acct="$(open_account alice 1000000)"
bob_acct="$(open_account bob 0)"
record_baseline
alice_tok="$(token alice)"

if [ "$PART" = "both" ] || [ "$PART" = "a" ]; then
    step "part A - every PSP callback delivered twice"
    gateway_set '{"duplicateCallbackRate":1.0}'
    fire_transfers "$N" 5 "$alice_tok" "$alice_acct" "$bob_acct" "$AMOUNT" "$WORK/sent_a"
    accepted_ids "$WORK/sent_a" > "$WORK/ids_a"
    wait_terminal "$WORK/ids_a"
    gateway_reset
    wait_quiescent || true

    ids="$(in_list "$WORK/ids_a")"
    expect_eq "A: sagas COMPLETED" \
        "$(sql payments_db "SELECT COUNT(*) FROM saga_instances WHERE transfer_id IN ($ids) AND status = 'COMPLETED'")" "$N"
    # payload->'payload'->>'transferId': the outbox column holds the ENVELOPE. The inbox holds no
    # payload at all, so the gateway's own outbox is where the per-transfer duplicate is counted.
    expect_eq "A: gateway published 2 approvals per transfer" \
        "$(sql gateway_db "SELECT COUNT(*) FROM outbox WHERE event_type = 'GatewayApproved' AND aggregate_id IN ($ids)")" "$((2 * N))"
    expect_eq "A: the INBOX accepted both (it keys on message id, and these are two messages)" \
        "$(sql payments_db "SELECT COUNT(*) FROM inbox WHERE message_id IN (
             SELECT message_id FROM saga_steps st JOIN saga_instances s ON s.id = st.saga_id
              WHERE s.transfer_id IN ($ids) AND st.step_name = 'ChargeGateway' AND st.outcome IN ('SUCCEEDED','SKIPPED'))")" "$((2 * N))"
    expect_eq "A: the STATE GUARD skipped one per transfer" \
        "$(sql payments_db "SELECT COUNT(*) FROM saga_steps st JOIN saga_instances s ON s.id = st.saga_id
             WHERE s.transfer_id IN ($ids) AND st.step_name = 'ChargeGateway' AND st.outcome = 'SKIPPED'")" "$N"
    expect_eq "A: bob credited exactly once per transfer" "$(balance_of "$bob_acct")" "$((N * AMOUNT))"
fi

if [ "$PART" = "both" ] || [ "$PART" = "b" ]; then
    step "part B - the PSP goes silent, then the dead letters are replayed"
    dl_gateway_before="$(sql gateway_db "SELECT COUNT(*) FROM dead_letters WHERE replayed_at IS NULL")"
    gateway_set '{"timeoutRate":1.0}'
    fire_transfers "$N" 5 "$alice_tok" "$alice_acct" "$bob_acct" "$AMOUNT" "$WORK/sent_b"
    accepted_ids "$WORK/sent_b" > "$WORK/ids_b"
    ids="$(in_list "$WORK/ids_b")"

    # Terminal here means the sweeper compensated them - the PSP never answered.
    wait_terminal "$WORK/ids_b"
    gateway_reset
    wait_quiescent || true
    expect_eq "B: sagas COMPENSATED by the sweeper" \
        "$(sql payments_db "SELECT COUNT(*) FROM saga_instances WHERE transfer_id IN ($ids) AND status = 'COMPENSATED'")" "$N"
    new_dl="$(( $(sql gateway_db "SELECT COUNT(*) FROM dead_letters WHERE replayed_at IS NULL") - dl_gateway_before ))"
    expect_eq "B: each silent charge dead-lettered at the gateway" "$new_dl" "$N"

    log "operator replays the gateway's dead letters (the PSP is healthy again)"
    curl -sS -m 10 -X POST "$GW/admin/dead-letters/replay" -H "Authorization: Bearer $(token operator)"; echo
    sleep 5
    wait_quiescent || true
    log "gateway charges for the compensated transfers: $(sql gateway_db "SELECT COALESCE(string_agg(status || ' ' || n, ', '), 'none') FROM (SELECT status, COUNT(*) n FROM gateway_charges WHERE transfer_id IN ($ids) GROUP BY status) s")"
    log "transfers: $(sql payments_db "SELECT COALESCE(string_agg(status || ' ' || n, ', '), 'none') FROM (SELECT status, COUNT(*) n FROM transfers WHERE id IN ($ids) GROUP BY status) s")"
    # M7: the timeout's VoidCharge reached the gateway BEFORE the replay and left a tombstone, so
    # the replayed charges find it and are declined. Before the fix this was ten APPROVED charges
    # on ten refunded transfers.
    expect_eq "B: the replay charged nobody" \
        "$(sql gateway_db "SELECT COUNT(*) FROM gateway_charges WHERE transfer_id IN ($ids) AND status = 'APPROVED'")" "0"
    expect_eq "B: every transfer carries a void tombstone at the gateway" \
        "$(sql gateway_db "SELECT COUNT(*) FROM gateway_charges WHERE transfer_id IN ($ids) AND status = 'VOIDED'")" "$N"
fi

finish_scenario
