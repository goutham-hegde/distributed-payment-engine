package com.dpe.account.repository;

import com.dpe.account.domain.LedgerEntry;
import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface LedgerEntryRepository extends JpaRepository<LedgerEntry, Long> {

    List<LedgerEntry> findByTransferIdOrderByIdAsc(UUID transferId);

    List<LedgerEntry> findByAccountIdOrderByIdAsc(UUID accountId);

    /**
     * Invariant I1: the global sum of every ledger entry ever written must be exactly zero.
     * {@code coalesce} so an empty ledger reports 0 rather than null.
     */
    @Query("select coalesce(sum(e.amountMinor), 0) from LedgerEntry e")
    long sumAllAmounts();

    /**
     * Invariant I2, one account at a time: the denormalized {@code accounts.balance_minor} must
     * equal the sum of that account's entries.
     */
    @Query("select coalesce(sum(e.amountMinor), 0) from LedgerEntry e where e.accountId = :accountId")
    long sumAmountsForAccount(@Param("accountId") UUID accountId);

    /**
     * <b>M6 part 3.</b> The newest page of one account's history.
     *
     * <p>Keyset-paged on the primary key alone, which is simpler than the transfer list's
     * {@code (created_at, id)} for a reason worth noticing: {@code ledger_entries.id} is a
     * BIGSERIAL, so it is already a total order and needs no tiebreak column. Ordering by
     * {@code created_at} instead would have needed one, because two legs of the same posting are
     * written in the same transaction and share a timestamp to the microsecond.
     *
     * <p>Newest first. It rides {@code idx_ledger_entries_account} (account_id, id DESC), so the
     * common page - the leading edge - is the cheapest one to serve.
     *
     * @param limit ask for one more than the page size; the extra row is how the caller learns
     *              whether a next page exists without a second COUNT query
     */
    @Query(value = """
            SELECT * FROM ledger_entries
            WHERE account_id = :accountId
            ORDER BY id DESC
            LIMIT :limit
            """, nativeQuery = true)
    List<LedgerEntry> findFirstPageFor(@Param("accountId") UUID accountId,
                                       @Param("limit") int limit);

    /**
     * The page after entry {@code cursorId}.
     *
     * <p>Stable in a way keyset pagination usually only approximates: {@code ledger_entries} is
     * append-only, so a row can never be updated out from under a cursor and never deleted behind
     * one. New rows only ever appear at the newest end, which is the end the client has already
     * passed. A client paging backwards through history sees exactly the rows that existed when
     * it started, plus nothing.
     */
    @Query(value = """
            SELECT * FROM ledger_entries
            WHERE account_id = :accountId AND id < :cursorId
            ORDER BY id DESC
            LIMIT :limit
            """, nativeQuery = true)
    List<LedgerEntry> findPageAfter(@Param("accountId") UUID accountId,
                                    @Param("cursorId") long cursorId,
                                    @Param("limit") int limit);

    /** How many entries I1 summed, so a passing check carries evidence that it ran. */
    @Query("select count(e) from LedgerEntry e")
    long countEntries();
}
