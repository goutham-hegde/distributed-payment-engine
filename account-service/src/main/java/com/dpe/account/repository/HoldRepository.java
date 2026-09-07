package com.dpe.account.repository;

import com.dpe.account.domain.Hold;
import jakarta.persistence.LockModeType;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface HoldRepository extends JpaRepository<Hold, UUID> {

    /**
     * Reads a hold and locks it until the surrounding transaction ends.
     *
     * <p>Needed because commit and release both read the status, decide, and then write - a
     * read-modify-write, which is the shape that loses updates. Two concurrent settlements of
     * the same hold reading ACTIVE at the same time would both proceed.
     *
     * <p>The UNIQUE constraint on {@code (transfer_id, account_id, entry_type)} would still stop
     * the second one from double-spending, so this lock is not what makes the system correct. It
     * is what makes the second one fail as a clean blocked-then-no-op rather than as a constraint
     * violation that has to be untangled after the fact.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select h from Hold h where h.id = :id")
    Optional<Hold> findByIdForUpdate(@Param("id") UUID id);

    /** One hold per transfer per account, enforced by {@code holds_one_per_account_per_transfer}. */
    Optional<Hold> findByTransferId(UUID transferId);

    /** Invariant I3's second term, and the gauge M6 graphs as "money currently in flight". */
    @Query("select coalesce(sum(h.amountMinor), 0) from Hold h where h.status = com.dpe.account.domain.HoldStatus.ACTIVE")
    long sumActiveHolds();
}
