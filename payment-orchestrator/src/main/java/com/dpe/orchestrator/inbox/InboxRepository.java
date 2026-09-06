package com.dpe.orchestrator.inbox;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface InboxRepository extends JpaRepository<InboxMessage, UUID> {

    /**
     * Claims {@code messageId} for processing. Returns {@code 1} if this is the first time the
     * message has been seen and {@code 0} if it has already been consumed.
     *
     * <h2>Why this is an upsert and not {@code save()} plus a caught exception</h2>
     *
     * <p>The obvious implementation is {@code inbox.save(...)} wrapped in a
     * {@code catch (DataIntegrityViolationException)}. It does not work, for a reason that has
     * nothing to do with the outbox pattern and everything to do with JPA: <b>a constraint
     * violation poisons the persistence context.</b> Hibernate marks the transaction
     * rollback-only, and every subsequent operation in it - including the business write you were
     * about to skip to - fails. You cannot catch that exception and carry on inside the same
     * transaction, which is the only place the business write is allowed to happen.
     *
     * <p>{@code ON CONFLICT DO NOTHING} moves the same decision into the statement. No exception
     * is raised, the transaction stays usable, and the affected-row count tells you which case
     * you are in.
     *
     * <p>This is still the database enforcing uniqueness, not application logic: Postgres
     * evaluates the conflict against the primary key index while holding the row lock, atomically
     * with the insert. It is not a {@code SELECT}-then-{@code INSERT}, which would have a race
     * window between the two statements. The check being a returned count rather than a thrown
     * exception is an ergonomics choice; the guarantee is identical.
     *
     * @return 1 if inserted (process the message), 0 if it was already there (skip it)
     */
    @Modifying
    @Query(value = """
            INSERT INTO inbox (message_id, topic, event_type)
            VALUES (:messageId, :topic, :eventType)
            ON CONFLICT (message_id) DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(@Param("messageId") UUID messageId,
                       @Param("topic") String topic,
                       @Param("eventType") String eventType);
}
