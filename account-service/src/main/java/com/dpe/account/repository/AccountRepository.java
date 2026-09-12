package com.dpe.account.repository;

import com.dpe.account.domain.Account;
import com.dpe.account.domain.AccountType;
import jakarta.persistence.LockModeType;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface AccountRepository extends JpaRepository<Account, UUID> {

    /**
     * Reads an account and holds a row-level write lock on it until the surrounding transaction
     * ends. Emits {@code SELECT ... FOR UPDATE}.
     *
     * <p>This is what makes the lost update impossible. Postgres defaults to READ COMMITTED,
     * which prevents dirty reads but happily lets two transactions both read a balance of 1000,
     * both approve a debit of 600, and both write 400. A second caller of this method blocks
     * until the first commits, and then reads the committed value.
     *
     * <p><strong>Call this once per account, in a deterministic order.</strong> There is
     * deliberately no batch {@code WHERE id IN (...) FOR UPDATE} variant here: the order in
     * which Postgres locks the rows matched by an {@code IN} list is not part of the contract,
     * even with an {@code ORDER BY}, so a batch fetch silently reintroduces the deadlock this
     * milestone is about. Two sorted single-row calls are explicit and provably ordered.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select a from Account a where a.id = :id")
    Optional<Account> findByIdForUpdate(@Param("id") UUID id);

    /**
     * M8: the CLEARING shards, in a stable order. Read by {@code ClearingAccounts}, which picks one
     * per transfer. The order only has to agree between instances, and it does - they all read it
     * from the same table.
     */
    @Query("select a.id from Account a where a.accountType = :type and a.currency = :currency "
            + "order by a.id")
    List<UUID> findIdsByTypeAndCurrency(@Param("type") AccountType type,
                                        @Param("currency") String currency);

    /**
     * <b>M6 part 3, invariant I5.</b> Customer accounts holding a negative balance - an overdraft
     * that got through.
     *
     * <p>Scoped to CUSTOMER on purpose and not as a convenience. The SYSTEM issuance account is
     * negative <i>by design</i>: money enters the ledger by being debited from it, so its balance
     * is minus the total ever issued. A check written without the type predicate fails on a
     * healthy system from the very first funded account, and a check that is always red is a check
     * that gets switched off.
     */
    @Query(value = """
            SELECT COUNT(*) FROM accounts
            WHERE account_type = 'CUSTOMER' AND balance_minor < 0
            """, nativeQuery = true)
    long countCustomerAccountsWithNegativeBalance();

    /**
     * <b>M6 part 3, invariant I2.</b> How many accounts disagree with the sum of their own ledger
     * entries.
     *
     * <p>{@code balance_minor} is a cache of an append-only truth, and this is the query that says
     * whether the cache is still honest. Any code path that writes one without the other shows up
     * here rather than drifting silently until someone reconciles by hand.
     *
     * <p><b>This is a full comparison of both tables and it must never become a gauge.</b> A LEFT
     * JOIN so an account with no entries yet is compared against zero rather than dropped - an
     * account whose balance is non-zero with no entries behind it is the single most interesting
     * row this query can find, and an INNER JOIN would hide exactly that one.
     *
     * <p>Deliberately the same statement as {@code scripts/verify-invariants.sh} and as
     * {@code LedgerInvariants} in the test sources. Three copies, and they are kept identical
     * word for word so that what the tests prove, what the chaos suite proves and what this
     * endpoint reports cannot drift apart.
     */
    @Query(value = """
            SELECT COUNT(*) FROM (
                SELECT a.id
                  FROM accounts a
                  LEFT JOIN ledger_entries e ON e.account_id = a.id
                 GROUP BY a.id, a.balance_minor
                HAVING a.balance_minor <> COALESCE(SUM(e.amount_minor), 0)
            ) drifted
            """, nativeQuery = true)
    long countAccountsDriftedFromLedger();

    /** Total money held by customer accounts. Half of the I3 conservation total; holds are the other. */
    @Query(value = """
            SELECT COALESCE(SUM(balance_minor), 0) FROM accounts WHERE account_type = 'CUSTOMER'
            """, nativeQuery = true)
    long totalCustomerBalance();

    /** How many accounts I2 compared, so a passing check carries evidence that it ran. */
    @Query(value = "SELECT COUNT(*) FROM accounts", nativeQuery = true)
    long countAllAccounts();
}
