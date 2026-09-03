#!/usr/bin/env bash
#
# Asserts the five non-negotiable invariants against the running stack.
#
# This script is the project's definition of "correct". Every chaos scenario (M7) and the k6
# load run (M8) ends by calling it, and a non-zero exit means money was created, destroyed, or
# stranded - regardless of what the HTTP responses said.
#
#   ./scripts/verify-invariants.sh            check I1, I2, I4, I5 (and I3 if a baseline exists)
#   ./scripts/verify-invariants.sh baseline   record the current total as the I3 baseline
#
# The queries here are deliberately the same statements as
# account-service/src/test/java/com/dpe/account/support/LedgerInvariants.java, so what the test
# suite proves and what the chaos suite proves cannot drift apart.

set -uo pipefail

PG_CONTAINER="${PG_CONTAINER:-dpe-postgres}"
BASELINE_FILE="${BASELINE_FILE:-$(dirname "$0")/.i3-baseline}"

failures=0

# psql flags: -t strips headers, -A unaligned, -v ON_ERROR_STOP=1 makes SQL errors exit non-zero
# rather than printing a notice and returning success.
q() {
    local db="$1" sql="$2"
    docker exec -i "$PG_CONTAINER" \
        psql -U postgres -d "$db" -tAq -v ON_ERROR_STOP=1 -c "$sql" 2>/dev/null | tr -d '[:space:]'
}

table_exists() {
    local db="$1" table="$2"
    [ "$(q "$db" "SELECT to_regclass('public.$table') IS NOT NULL")" = "t" ]
}

pass() { printf '  \033[32mPASS\033[0m  %s\n' "$1"; }
fail() { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; failures=$((failures + 1)); }
skip() { printf '  \033[90mSKIP\033[0m  %s\n' "$1"; }

if ! docker exec "$PG_CONTAINER" pg_isready -U postgres >/dev/null 2>&1; then
    echo "error: postgres container '$PG_CONTAINER' is not accepting connections" >&2
    echo "       start the stack with: docker compose -f infra/docker-compose.yml up -d" >&2
    exit 2
fi

# --------------------------------------------------------------------- I3 baseline capture

total_customer_money() {
    q accounts_db "SELECT COALESCE(SUM(balance_minor), 0) FROM accounts WHERE account_type = 'CUSTOMER'"
}

if [ "${1:-}" = "baseline" ]; then
    total="$(total_customer_money)"
    echo "$total" > "$BASELINE_FILE"
    echo "I3 baseline recorded: ${total} minor units of customer money"
    exit 0
fi

echo "Verifying ledger invariants against '${PG_CONTAINER}'"

# --------------------------------------------------------------------- I1

# Every posting writes rows summing to zero, so the sum over the whole table is zero forever.
# This one query is the difference between "we tested it" and "creation is structurally
# impossible".
i1="$(q accounts_db "SELECT COALESCE(SUM(amount_minor), 0) FROM ledger_entries")"
if [ "$i1" = "0" ]; then
    pass "I1  global ledger sum is zero"
else
    fail "I1  global ledger sum is ${i1}, expected 0 - money was created or destroyed"
fi

# --------------------------------------------------------------------- I2

# The denormalized balance column must agree with the append-only entries it caches. A code path
# that writes one without the other shows up here rather than drifting silently.
i2="$(q accounts_db "
    SELECT COUNT(*) FROM (
        SELECT a.id
          FROM accounts a
          LEFT JOIN ledger_entries e ON e.account_id = a.id
         GROUP BY a.id, a.balance_minor
        HAVING a.balance_minor <> COALESCE(SUM(e.amount_minor), 0)
    ) drifted")"
if [ "$i2" = "0" ]; then
    pass "I2  every account balance equals the sum of its ledger entries"
else
    fail "I2  ${i2} account(s) whose balance disagrees with their ledger entries"
fi

# --------------------------------------------------------------------- I3

if [ -f "$BASELINE_FILE" ]; then
    baseline="$(tr -d '[:space:]' < "$BASELINE_FILE")"
    current="$(total_customer_money)"
    # Once holds exist (M3), money sitting in a hold is still money and is added back in here.
    if table_exists accounts_db holds; then
        held="$(q accounts_db "SELECT COALESCE(SUM(amount_minor), 0) FROM holds WHERE status = 'ACTIVE'")"
        current=$((current + held))
    fi
    if [ "$current" = "$baseline" ]; then
        pass "I3  total money conserved (${current})"
    else
        fail "I3  total money is ${current}, baseline was ${baseline}"
    fi
else
    skip "I3  no baseline recorded - run: $0 baseline"
fi

# --------------------------------------------------------------------- I4

# Owned by payment-orchestrator and only meaningful once the saga exists (M3). Skipping loudly
# is better than silently passing a check that never ran.
if table_exists payments_db saga_instances; then
    i4="$(q payments_db "
        SELECT COUNT(*) FROM saga_instances
         WHERE status NOT IN ('COMPLETED', 'COMPENSATED', 'FAILED')")"
    if [ "$i4" = "0" ]; then
        pass "I4  no saga left in a non-terminal state"
    else
        fail "I4  ${i4} saga(s) stuck in a non-terminal state - money may be stranded"
    fi
else
    skip "I4  saga_instances does not exist yet (arrives with M3)"
fi

# --------------------------------------------------------------------- I5

# Scoped to CUSTOMER accounts. The SYSTEM account is negative by design - its balance is the
# amount of money issued into the ledger, which is what it is there to record.
i5="$(q accounts_db "SELECT COUNT(*) FROM accounts WHERE account_type = 'CUSTOMER' AND balance_minor < 0")"
if [ "$i5" = "0" ]; then
    pass "I5  no customer account holds a negative balance"
else
    fail "I5  ${i5} customer account(s) with a negative balance - an overdraft got through"
fi

# ---------------------------------------------------------------------

echo
if [ "$failures" -eq 0 ]; then
    echo "All invariants hold."
    exit 0
fi
echo "${failures} invariant(s) VIOLATED."
exit 1
