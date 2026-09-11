package com.dpe.gateway.repository;

import com.dpe.gateway.domain.GatewayCharge;
import jakarta.persistence.LockModeType;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface GatewayChargeRepository extends JpaRepository<GatewayCharge, UUID> {

    /**
     * The idempotency lookup: has this transfer already been charged?
     *
     * <p>Note that this is a fast path, not the guarantee. Two concurrent deliveries can both
     * find nothing here and both proceed; what stops the second from creating a duplicate charge
     * is the UNIQUE constraint on {@code transfer_id}, which is evaluated by Postgres under the
     * index lock at INSERT time. Reading it first only avoids the exception in the common case.
     *
     * <p>Getting that ordering backwards - trusting the SELECT and treating the constraint as
     * belt-and-braces - is the mistake. The constraint is the braces AND the belt; this query is
     * an optimisation.
     */
    Optional<GatewayCharge> findByTransferId(UUID transferId);

    /** The charge for a transfer, locked - the void reads its status and then changes it. */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select c from GatewayCharge c where c.transferId = :transferId")
    Optional<GatewayCharge> findByTransferIdForUpdate(@Param("transferId") UUID transferId);

    /**
     * M7: the tombstone - a VOIDED row for a transfer that has not been charged, so that it never
     * can be.
     *
     * <p>{@code ON CONFLICT DO NOTHING} rather than a plain save, and the difference is the whole
     * race. If a charge for this transfer is being inserted concurrently and has not committed, this
     * statement BLOCKS on the uncommitted index entry, then either inserts (the charge rolled back)
     * or does nothing (it committed) - and the caller's next statement, under READ COMMITTED, sees
     * the committed charge and reverses it. So the void cannot lose a race to the charge. The charge
     * CAN lose one to the void: its plain INSERT fails on the constraint, which is the right outcome
     * - no charge - arrived at through a retry that finds the tombstone. Same "the insert that
     * blocks" behaviour M4 relies on in {@code idempotency_records}.
     *
     * @return 1 if the tombstone was written, 0 if the transfer already had a row
     */
    @Modifying
    @Query(value = """
            INSERT INTO gateway_charges
                (id, transfer_id, amount_minor, currency, status, voided_at, void_reason)
            VALUES (:id, :transferId, :amountMinor, :currency, 'VOIDED', now(), :reason)
            ON CONFLICT (transfer_id) DO NOTHING
            """, nativeQuery = true)
    int insertTombstone(@Param("id") UUID id, @Param("transferId") UUID transferId,
                        @Param("amountMinor") long amountMinor, @Param("currency") String currency,
                        @Param("reason") String reason);
}
