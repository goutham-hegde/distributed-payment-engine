#!/usr/bin/env bash
#
# SCENARIO 4 - the gateway declines every charge.
#
# HYPOTHESIS: every transfer is compensated, and every sender's balance is restored EXACTLY -
# per account, not merely in total. A total can balance while one customer is short and another
# is over; conservation is necessary and not sufficient.
#
# Also asserted: no money reached a recipient, and nothing reached a dead letter. A decline is a
# business outcome, not a failure; it must travel the saga's reply path and never the error path.

source "$(dirname "$0")/lib.sh"

N="${N:-30}"
AMOUNT=5000

begin_scenario "04 gateway declines everything"

alice_acct="$(open_account alice 1000000)"
bob_acct="$(open_account bob 0)"
record_baseline
alice_before="$(balance_of "$alice_acct")"

log "gateway: failureRate 1.0"
gateway_set '{"failureRate":1.0}'

log "firing $N transfers"
fire_transfers "$N" 10 "$(token alice)" "$alice_acct" "$bob_acct" "$AMOUNT" "$WORK/sent"
accepted_ids "$WORK/sent" > "$WORK/ids"

# Wait for terminal sagas BEFORE restoring - see lib.sh.
wait_terminal "$WORK/ids"
gateway_reset
wait_quiescent || true

step "scenario-specific"
ids="$(in_list "$WORK/ids")"
expect_eq "every POST accepted"                "$(wc -l < "$WORK/ids" | tr -d ' ')" "$N"
expect_eq "sagas COMPENSATED"                  "$(sql payments_db "SELECT COUNT(*) FROM saga_instances WHERE transfer_id IN ($ids) AND status = 'COMPENSATED'")" "$N"
expect_eq "transfers FAILED with GATEWAY_DECLINED" "$(sql payments_db "SELECT COUNT(*) FROM transfers WHERE id IN ($ids) AND status = 'FAILED' AND failure_reason = 'GATEWAY_DECLINED'")" "$N"
expect_eq "alice's balance restored exactly"   "$(balance_of "$alice_acct")" "$alice_before"
expect_eq "bob received nothing"               "$(balance_of "$bob_acct")" "0"
# Every compensation is visible as a round trip in alice's ledger: one debit and one credit per
# transfer, nothing rolled back. Two entries per transfer on her side, not zero.
expect_eq "alice's ledger shows 2N entries (debit + credit per transfer)" \
    "$(sql accounts_db "SELECT COUNT(*) FROM ledger_entries WHERE account_id = '$alice_acct' AND transfer_id IN ($ids)")" "$((2 * N))"
expect_eq "no new dead letters (a decline is not an error)" "$(dead_letter_count)" "$DEAD_LETTERS_BEFORE"

finish_scenario
