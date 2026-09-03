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
}
