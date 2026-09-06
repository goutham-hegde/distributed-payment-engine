package com.dpe.orchestrator.readmodel;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface TransferProjectionRepository extends JpaRepository<TransferProjection, UUID> {

    /**
     * Records that a transfer completed.
     *
     * <p><b>Deliberately not idempotent.</b> A second call for the same transfer increments
     * {@code apply_count} instead of being a no-op, so that a duplicate delivery which gets past
     * the inbox leaves evidence. Had this been a plain insert, the primary key would have
     * rejected the duplicate and a completely broken dedup gate would still have produced a
     * correct-looking table - the test would be asserting the primary key's behaviour, not the
     * inbox's.
     *
     * <p>Production read models are usually the opposite: idempotent by construction, so a
     * duplicate is harmless even if the inbox is bypassed. Defence in depth is the better default.
     * This one gives that up in exchange for being able to prove the gate works.
     */
    @Modifying
    @Query(value = """
            INSERT INTO transfer_projection (transfer_id, from_account_id, to_account_id,
                                             amount_minor, currency, status)
            VALUES (:transferId, :fromAccountId, :toAccountId, :amountMinor, :currency, 'COMPLETED')
            ON CONFLICT (transfer_id) DO UPDATE
                SET apply_count  = transfer_projection.apply_count + 1,
                    last_seen_at = now()
            """, nativeQuery = true)
    void recordCompleted(@Param("transferId") UUID transferId,
                         @Param("fromAccountId") UUID fromAccountId,
                         @Param("toAccountId") UUID toAccountId,
                         @Param("amountMinor") long amountMinor,
                         @Param("currency") String currency);
}
