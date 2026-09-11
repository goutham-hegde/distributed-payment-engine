package com.dpe.account.repository;

import com.dpe.account.domain.TransferVoid;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface TransferVoidRepository extends JpaRepository<TransferVoid, UUID> {

    /**
     * Serializes every saga command about one transfer, until the surrounding transaction ends.
     *
     * <p>Why it is needed even though Kafka already orders a transfer's commands on one partition:
     * the two decisions this protects read DIFFERENT tables. A reserve checks for a tombstone and
     * then writes a hold; a void checks for a hold and then writes a tombstone. Under READ
     * COMMITTED those two can interleave - each sees nothing, each writes - and the result is a hold
     * nobody will ever release, which is the exact bug the tombstone exists to close. No single
     * UNIQUE constraint spans both tables, so the mutual exclusion has to be a lock.
     *
     * <p>Partition order makes the interleaving rare, not impossible: during a rebalance the old
     * owner of a partition can still be finishing a record while the new owner starts reading from
     * the last committed offset, and a replayed dead letter is a second copy of a command the
     * partition already delivered.
     *
     * <p>A transaction-scoped advisory lock rather than a row lock, because in the case that matters
     * there is no row yet to lock. {@code hashtextextended} maps the id onto the lock's 64-bit key
     * space; two transfers colliding on a hash only ever wait for each other, never corrupt each
     * other. It is always taken FIRST in {@code ReservationService}, before any account or hold
     * lock, so it cannot join a lock cycle.
     */
    @Query(value = "SELECT CAST(pg_advisory_xact_lock(hashtextextended(CAST(:transferId AS text), 0)) AS text)",
            nativeQuery = true)
    String lockTransfer(@Param("transferId") UUID transferId);
}
