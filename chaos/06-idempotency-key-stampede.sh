#!/usr/bin/env bash
#
# SCENARIO 6 - one Idempotency-Key, sent 100 times at once.
#
# HYPOTHESIS: exactly one transfer is created, and every caller is told about THAT transfer.
#
# What actually decides it is the unique index on idempotency_records, not the Redis lock: the
# lock only thins the herd. A duplicate that reaches Postgres while the first claim is uncommitted
# BLOCKS on the index tuple, then replays the committed answer. So the scenario also runs with
# Redis stopped (REDIS=off), where there is no lock at all and the database is alone - which is
# the configuration that proves the claim rather than illustrates it.
#
#   ./chaos/06-idempotency-key-stampede.sh
#   REDIS=off ./chaos/06-idempotency-key-stampede.sh

source "$(dirname "$0")/lib.sh"

COPIES="${COPIES:-100}"
PARALLEL="${PARALLEL:-50}"
AMOUNT=1234
REDIS="${REDIS:-on}"

begin_scenario "06 idempotency-key stampede (x$COPIES, redis $REDIS)"

alice_acct="$(open_account alice 1000000)"
bob_acct="$(open_account bob 0)"
record_baseline
alice_tok="$(token alice)"
key="chaos-06-$(python3 -c 'import uuid;print(uuid.uuid4())')"

if [ "$REDIS" = "off" ]; then
    log "STOP redis - the unique index is on its own"
    docker stop dpe-redis >/dev/null
fi

log "sending $COPIES copies of one request, $PARALLEL at a time, key $key"
seq "$COPIES" | xargs -P "$PARALLEL" -I{} bash -c 'post_transfer "$@"' _ \
    "$alice_tok" "$alice_acct" "$bob_acct" "$AMOUNT" "$key" > "$WORK/sent"

[ "$REDIS" = "off" ] && { log "START redis"; docker start dpe-redis >/dev/null; }

awk '$2 != "" { print $2 }' "$WORK/sent" | sort -u > "$WORK/ids"
wait_terminal "$WORK/ids" || true
wait_quiescent || true

step "scenario-specific"
log "responses: $(awk '{ print $1 }' "$WORK/sent" | sort | uniq -c | awk '{ printf "%s x HTTP %s, ", $1, $2 }')"
expect_eq "every caller answered 202" "$(awk '$1 == 202' "$WORK/sent" | wc -l | tr -d ' ')" "$COPIES"
expect_eq "every answer names the SAME transfer" "$(wc -l < "$WORK/ids" | tr -d ' ')" "1"
expect_eq "one transfer row from this account" \
    "$(sql payments_db "SELECT COUNT(*) FROM transfers WHERE from_account_id = '$alice_acct'")" "1"
expect_eq "one idempotency record for the key" \
    "$(sql payments_db "SELECT COUNT(*) FROM idempotency_records WHERE idempotency_key = '$key'")" "1"
expect_eq "bob paid once" "$(balance_of "$bob_acct")" "$AMOUNT"

finish_scenario
