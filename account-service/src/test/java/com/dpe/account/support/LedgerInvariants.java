package com.dpe.account.support;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * The five invariants, asserted in SQL.
 *
 * <p>These are the project's definition of correct. They are written as queries rather than as
 * assertions over objects on purpose: the same statements run in
 * {@code scripts/verify-invariants.sh} against a live stack after every chaos scenario and the
 * k6 load run, so what the tests check and what the chaos suite checks cannot drift.
 */
public final class LedgerInvariants {

    private LedgerInvariants() {
    }

    /** I1, I2 and I5 - the three that hold at every instant, with no baseline required. */
    public static void assertAll(JdbcTemplate jdbc) {
        assertI1GlobalSumIsZero(jdbc);
        assertI2BalancesMatchEntries(jdbc);
        assertI5NoNegativeCustomerBalance(jdbc);
    }

    /**
     * I1 - money is neither created nor destroyed. Every posting writes rows summing to zero, so
     * the global sum over the whole table is zero forever. This single query is what makes
     * "no money was lost" a provable claim rather than a hope.
     */
    public static void assertI1GlobalSumIsZero(JdbcTemplate jdbc) {
        Long sum = jdbc.queryForObject(
                "SELECT COALESCE(SUM(amount_minor), 0) FROM ledger_entries", Long.class);
        assertThat(sum)
                .as("I1: global ledger sum must be exactly zero (money created or destroyed)")
                .isZero();
    }

    /**
     * I2 - the denormalized {@code accounts.balance_minor} agrees with the append-only entries
     * it caches. This is the check that justifies keeping the fast column at all: if any code
     * path ever writes one without the other, this fails loudly instead of drifting silently.
     */
    public static void assertI2BalancesMatchEntries(JdbcTemplate jdbc) {
        List<Map<String, Object>> drifted = jdbc.queryForList("""
                SELECT a.id,
                       a.balance_minor                      AS cached,
                       COALESCE(SUM(e.amount_minor), 0)     AS derived
                  FROM accounts a
                  LEFT JOIN ledger_entries e ON e.account_id = a.id
                 GROUP BY a.id, a.balance_minor
                HAVING a.balance_minor <> COALESCE(SUM(e.amount_minor), 0)
                """);
        assertThat(drifted)
                .as("I2: accounts.balance_minor must equal SUM(its ledger entries)")
                .isEmpty();
    }

    /**
     * I3 - conservation. Money in flight still counts as money, so once holds exist (M3) the
     * held amounts are added back in. Requires a baseline captured before the run, which is why
     * it is not part of {@link #assertAll}.
     */
    public static void assertI3TotalIsConserved(JdbcTemplate jdbc, long expectedTotalMinor) {
        Long total = jdbc.queryForObject("""
                SELECT COALESCE(SUM(balance_minor), 0)
                  FROM accounts
                 WHERE account_type = 'CUSTOMER'
                """, Long.class);
        assertThat(total)
                .as("I3: total customer money must be conserved across the run")
                .isEqualTo(expectedTotalMinor);
    }

    /**
     * I5 - no overdraft slipped through under concurrency.
     *
     * <p>Scoped to CUSTOMER accounts. The SYSTEM account's balance is negative by design: it is
     * the amount of money issued into the ledger, and excluding it here is the difference
     * between an invariant that is precise and one that is merely approximately true.
     */
    public static void assertI5NoNegativeCustomerBalance(JdbcTemplate jdbc) {
        List<Map<String, Object>> negative = jdbc.queryForList("""
                SELECT id, balance_minor
                  FROM accounts
                 WHERE account_type = 'CUSTOMER'
                   AND balance_minor < 0
                """);
        assertThat(negative)
                .as("I5: no customer account may hold a negative balance")
                .isEmpty();
    }

    /** Total customer money, for capturing an I3 baseline before a run. */
    public static long totalCustomerMoney(JdbcTemplate jdbc) {
        Long total = jdbc.queryForObject(
                "SELECT COALESCE(SUM(balance_minor), 0) FROM accounts WHERE account_type = 'CUSTOMER'",
                Long.class);
        return total == null ? 0L : total;
    }
}
