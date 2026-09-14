#!/usr/bin/env bash
#
# SCENARIO 7 - many concurrent transfers out of one account that cannot afford them all.
#
# HYPOTHESIS: no lost update and no overdraft - which here can be stated as EXACT arithmetic
# rather than as "looks fine". The account holds CAPACITY x AMOUNT and receives CAPACITY + EXTRA
# requests at once, so:
#
#   exactly CAPACITY complete           (a lost update would let more through, or fewer)
#   exactly EXTRA fail INSUFFICIENT_FUNDS
#   the sender ends at exactly 0        (never negative - and I5's CHECK constraint would refuse
#                                        the write if the application ever tried)
#
# Every reserve locks the sender AND a CLEARING shard in sorted-id order; that ordering is what lets
# this run without a single deadlock rather than with deadlocks detected and retried. The scenario
# counts deadlocks in the Postgres log window to prove the "without".
#
# Since M8 this is genuinely concurrent at the database. Until then account-service had ONE consumer
# thread, so the 90 reserves arrived in parallel over HTTP and were then applied one at a time -
# the lock order was only ever exercised by the Java concurrency tests. With three consumer threads
# (one per partition) and CLEARING sharded, three reserves out of this one account really do
# contend for its row, which is what the scenario always claimed to test.

source "$(dirname "$0")/lib.sh"

CAPACITY="${CAPACITY:-60}"
EXTRA="${EXTRA:-30}"
PARALLEL="${PARALLEL:-30}"
AMOUNT=1000

begin_scenario "07 hot account ($((CAPACITY + EXTRA)) transfers, room for $CAPACITY)"

alice_acct="$(open_account alice $((CAPACITY * AMOUNT)))"
bob_acct="$(open_account bob 0)"
carol_acct="$(open_account bob 0)"
record_baseline
alice_tok="$(token alice)"
since="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

# Two recipients, so the recipients' rows are contended too, not only the sender's.
half=$(( (CAPACITY + EXTRA) / 2 ))
fire_transfers "$half" "$((PARALLEL / 2))" "$alice_tok" "$alice_acct" "$bob_acct" "$AMOUNT" "$WORK/sent" &
fire_transfers "$((CAPACITY + EXTRA - half))" "$((PARALLEL / 2))" "$alice_tok" "$alice_acct" "$carol_acct" "$AMOUNT" "$WORK/sent2"
wait
cat "$WORK/sent2" >> "$WORK/sent"
accepted_ids "$WORK/sent" > "$WORK/ids"

wait_terminal "$WORK/ids" || true
wait_quiescent || true

step "scenario-specific"
ids="$(in_list "$WORK/ids")"
expect_eq "every POST accepted" "$(wc -l < "$WORK/ids" | tr -d ' ')" "$((CAPACITY + EXTRA))"
expect_eq "exactly CAPACITY completed" \
    "$(sql payments_db "SELECT COUNT(*) FROM transfers WHERE id IN ($ids) AND status = 'COMPLETED'")" "$CAPACITY"
expect_eq "exactly EXTRA refused for insufficient funds" \
    "$(sql payments_db "SELECT COUNT(*) FROM transfers WHERE id IN ($ids) AND status = 'FAILED' AND failure_reason = 'INSUFFICIENT_FUNDS'")" "$EXTRA"
expect_eq "sender drained to exactly zero" "$(balance_of "$alice_acct")" "0"
expect_eq "recipients received exactly the sender's balance" \
    "$(( $(balance_of "$bob_acct") + $(balance_of "$carol_acct") ))" "$((CAPACITY * AMOUNT))"
expect_eq "deadlocks in the postgres log during the run" \
    "$(docker logs --since "$since" "$PG" 2>&1 | grep -c 'deadlock detected')" "0"

finish_scenario
