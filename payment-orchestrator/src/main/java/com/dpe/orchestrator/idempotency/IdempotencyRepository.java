package com.dpe.orchestrator.idempotency;

import java.time.OffsetDateTime;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/**
 * The two statements the gate is built from, plus the read-back.
 *
 * <p>Both writes are native SQL for the same reason {@code InboxRepository.insertIfAbsent} is:
 * {@code ON CONFLICT DO NOTHING} has no JPA equivalent, and the JPA alternative -
 * {@code save()} inside a {@code catch (DataIntegrityViolationException)} - does not work here at
 * all. A constraint violation marks the Hibernate session rollback-only, so every subsequent
 * statement in the transaction fails, <b>including the read of the row you were about to
 * replay</b>. The exception cannot be caught and recovered from inside the transaction that has
 * to keep going.
 *
 * <p>Moving the decision into the statement keeps the transaction usable and turns "is this a
 * duplicate" into a returned row count. It is still the database enforcing uniqueness against the
 * primary key index, atomically with the insert - not a {@code SELECT} followed by an
 * {@code INSERT}, which has a window in between that a retry storm will find.
 */
public interface IdempotencyRepository
        extends JpaRepository<IdempotencyRecord, IdempotencyRecord.Key> {

    /**
     * Claims {@code (clientId, key)} for this request.
     *
     * <p>Returns {@code 1} if the claim was ours - do the work - and {@code 0} if the key already
     * exists, in which case {@code findById} returns what the first request answered.
     *
     * <h2>The blocking behaviour, which is the point</h2>
     *
     * <p>When a duplicate arrives while the first request transaction is still open, this
     * statement does <b>not</b> return 0 immediately. Postgres finds an uncommitted index tuple
     * for the same key and blocks the second inserter until the first transaction ends:
     *
     * <ul>
     *   <li>the first COMMITs - this returns 0, and the stored response is there to be read;</li>
     *   <li>the first ROLLS BACK - the tuple never existed, this returns 1, and the second
     *       request does the work itself.</li>
     * </ul>
     *
     * <p>That is mutual exclusion with correct handoff on failure, out of an index. It is what a
     * distributed lock is usually reached for, without the lease, the TTL or the clock
     * assumption - and it is the reason Redis is an optimization here rather than the guarantee.
     * See {@code docs/adr/0002-idempotency.md}.
     *
     * <p>The row is deliberately inserted with a NULL response, because the response is not known
     * yet. {@link #complete} fills it in <b>inside the same transaction</b>, so no other
     * transaction ever observes the incomplete row.
     *
     * @return 1 if this request owns the key, 0 if it is a duplicate
     */
    @Modifying
    @Query(value = """
            INSERT INTO idempotency_records
                (client_id, idempotency_key, request_fingerprint, expires_at)
            VALUES (:clientId, :key, :fingerprint, :expiresAt)
            ON CONFLICT (client_id, idempotency_key) DO NOTHING
            """, nativeQuery = true)
    int claim(@Param("clientId") String clientId,
              @Param("key") String key,
              @Param("fingerprint") String fingerprint,
              @Param("expiresAt") OffsetDateTime expiresAt);

    /**
     * Records what this request answered, so every retry can be answered the same way.
     *
     * <p>MUST run in the transaction that {@link #claim}ed the key and that wrote the transfer.
     * Split across two transactions, a crash in between leaves a claimed key with no response,
     * and every retry of that intent is then answered by a row that says "someone did this" and
     * cannot say what happened - the transfer exists and the client can never learn its id.
     *
     * <p>No cast: {@code response_body} is TEXT, so what is stored is byte-identical to what was
     * serialized. It was jsonb at first, which silently broke the verbatim-replay guarantee -
     * Postgres re-renders jsonb with its own key order and spacing on the way out.
     */
    @Modifying
    @Query(value = """
            UPDATE idempotency_records
               SET response_status = :status,
                   response_body   = :body,
                   transfer_id     = :transferId
             WHERE client_id = :clientId
               AND idempotency_key = :key
            """, nativeQuery = true)
    int complete(@Param("clientId") String clientId,
                 @Param("key") String key,
                 @Param("status") int status,
                 @Param("body") String body,
                 @Param("transferId") UUID transferId);

    /**
     * Deletes keys past their retention window.
     *
     * <p>Bounded per call rather than one unbounded DELETE: an unbounded delete of a busy day of
     * keys holds a lock on every row it touches for as long as it runs, and this table is on the
     * path of every write request. The caller loops instead.
     */
    @Modifying
    @Query(value = """
            DELETE FROM idempotency_records
             WHERE (client_id, idempotency_key) IN (
                   SELECT client_id, idempotency_key
                     FROM idempotency_records
                    WHERE expires_at < now()
                    ORDER BY expires_at
                    LIMIT :limit)
            """, nativeQuery = true)
    int deleteExpired(@Param("limit") int limit);
}
