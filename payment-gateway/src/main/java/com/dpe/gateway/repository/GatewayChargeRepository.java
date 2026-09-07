package com.dpe.gateway.repository;

import com.dpe.gateway.domain.GatewayCharge;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

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
}
