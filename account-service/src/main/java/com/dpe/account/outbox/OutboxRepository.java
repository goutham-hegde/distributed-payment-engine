package com.dpe.account.outbox;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OutboxRepository extends JpaRepository<OutboxMessage, UUID> {

    /**
     * Claims a batch of unpublished messages for this relay instance, oldest first.
     *
     * <p>Must be called inside a transaction. The rows stay locked until that transaction ends,
     * and that lock is the only thing stopping a second relay instance from publishing the same
     * message.
     *
     * <h2>{@code FOR UPDATE SKIP LOCKED}, clause by clause</h2>
     *
     * <p>{@code FOR UPDATE} takes a row-level write lock on everything the query returns.
     * {@code SKIP LOCKED} changes what happens when a row is already locked by someone else:
     * instead of <i>waiting</i> for it, the query pretends it does not exist and moves on. That
     * one word is what turns this table into a work queue. Ten relay instances polling
     * simultaneously each get a disjoint batch, immediately, with no coordination and no
     * contention - and no row is ever handed to two of them.
     *
     * <p>Without {@code SKIP LOCKED} the second relay would block until the first committed,
     * serialising the whole system behind one worker. With plain {@code SKIP LOCKED} but no
     * {@code FOR UPDATE} there would be no lock to skip, and every relay would publish every row.
     *
     * <h2>What this costs you</h2>
     *
     * <p><b>Global ordering.</b> Relay A claims row 1, relay B skips it and claims row 2, and B
     * is faster - so message 2 reaches Kafka first. This is not a bug to be fixed, it is the
     * price of parallel draining, and it is why the message key is the aggregate id: order is
     * guaranteed <i>per aggregate</i>, never globally. Say it that way in an interview.
     *
     * <h2>Why this is a native query</h2>
     *
     * <p>JPQL has no vocabulary for {@code SKIP LOCKED}. {@code @Lock(PESSIMISTIC_WRITE)} gives
     * you {@code FOR UPDATE} and then blocks, which is exactly the behaviour to avoid here.
     * Hibernate's {@code Timeouts.SKIP_LOCKED} hint exists but is dialect-dependent and silently
     * degrades to plain {@code FOR UPDATE} where unsupported - a silent downgrade in the
     * component whose whole job is not to double-publish is not a trade worth making.
     */
    @Query(value = """
            SELECT * FROM outbox
            WHERE published_at IS NULL
            ORDER BY created_at, id
            LIMIT :batchSize
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxMessage> claimUnpublished(@Param("batchSize") int batchSize);

    /** Backlog depth. M6 turns this into the gauge that says whether the relay is keeping up. */
    long countByPublishedAtIsNull();
}
