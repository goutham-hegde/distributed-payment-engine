package com.dpe.messaging.deadletter;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface DeadLetterRepository extends JpaRepository<DeadLetter, UUID> {

    /**
     * Records a dead letter, or does nothing if this exact Kafka record has already been
     * recorded.
     *
     * <p>Same statement shape as {@link com.dpe.messaging.inbox.InboxRepository#insertIfAbsent},
     * and for the same reason: the dead letter listener is at-least-once like every other
     * consumer here, so it will occasionally see a record twice, and a {@code SELECT}-then-
     * {@code INSERT} has a window between the two statements. {@code ON CONFLICT DO NOTHING}
     * moves the decision into the statement, where Postgres evaluates it against the unique index
     * while holding the row lock - and leaves the transaction usable, which a caught constraint
     * violation would not.
     *
     * <p>The conflict target is {@code (original_topic, original_partition, original_offset)} and
     * not the message id. See {@code V4__dead_letters.sql} for why that choice decides whether a
     * replay that fails again is visible or silently swallowed.
     *
     * @return 1 if this failure is new, 0 if it was already recorded
     */
    @Modifying
    @Query(value = """
            INSERT INTO dead_letters (
                id, message_id, original_topic, original_partition, original_offset,
                message_key, event_type, payload, exception_type, exception_message, attempts)
            VALUES (
                :id, :messageId, :topic, :partitionNo, :offsetNo,
                :messageKey, :eventType, :payload, :exceptionType, :exceptionMessage, :attempts)
            ON CONFLICT (original_topic, original_partition, original_offset) DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(@Param("id") UUID id,
                       @Param("messageId") UUID messageId,
                       @Param("topic") String topic,
                       @Param("partitionNo") int partitionNo,
                       @Param("offsetNo") long offsetNo,
                       @Param("messageKey") String messageKey,
                       @Param("eventType") String eventType,
                       @Param("payload") String payload,
                       @Param("exceptionType") String exceptionType,
                       @Param("exceptionMessage") String exceptionMessage,
                       @Param("attempts") int attempts);

    /** The queue depth. M6 turns this into a gauge and M6.5 into one of the console tiles. */
    long countByReplayedAtIsNull();

    /**
     * Claims pending letters for replay, locking them so that two operators pressing "replay
     * everything" at the same moment cannot each publish the same message.
     *
     * <p>{@code FOR UPDATE SKIP LOCKED} rather than a plain {@code FOR UPDATE}, for the same
     * reason the outbox uses it: the second caller should be handed the letters the first did not
     * take rather than block behind it. Must be called inside a transaction, or the lock is
     * released the instant the statement returns and guards nothing.
     */
    @Query(value = """
            SELECT * FROM dead_letters
            WHERE replayed_at IS NULL
            ORDER BY created_at, id
            LIMIT :batchSize
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<DeadLetter> claimPending(@Param("batchSize") int batchSize);

    List<DeadLetter> findTop100ByReplayedAtIsNullOrderByCreatedAtAsc();

    List<DeadLetter> findByMessageKeyOrderByCreatedAtAsc(String messageKey);
}
