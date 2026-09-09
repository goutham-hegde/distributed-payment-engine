package com.dpe.messaging.deadletter;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.hibernate.annotations.CreationTimestamp;

/**
 * One message the consumer could not process, parked for a human.
 *
 * <p>Shaped almost exactly like {@link com.dpe.messaging.outbox.OutboxMessage}, and the symmetry
 * is not a coincidence: a dead letter is an outbox row that has to travel backwards. It carries
 * the payload verbatim, the key, and the headers a replay must reproduce, and its
 * {@code replayed_at} plays the role {@code published_at} plays for the outbox.
 *
 * <p>The payload is a String and is never deserialized here. It cannot be: a message can be in
 * this table precisely because it does not parse. Anything that tried to interpret it would fail
 * on the rows that matter most.
 */
@Entity
@Table(name = "dead_letters")
public class DeadLetter {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    /** The business message id, or {@code null} when the failed record carried no header. */
    @Column(name = "message_id", updatable = false)
    private UUID messageId;

    @Column(name = "original_topic", nullable = false, updatable = false, length = 128)
    private String originalTopic;

    @Column(name = "original_partition", nullable = false, updatable = false)
    private int originalPartition;

    @Column(name = "original_offset", nullable = false, updatable = false)
    private long originalOffset;

    /** The aggregate id. A replay must publish with this key or it changes partition. */
    @Column(name = "message_key", updatable = false, length = 255)
    private String messageKey;

    @Column(name = "event_type", updatable = false, length = 64)
    private String eventType;

    @Column(name = "payload", updatable = false)
    private String payload;

    @Column(name = "exception_type", updatable = false, length = 255)
    private String exceptionType;

    @Column(name = "exception_message", updatable = false)
    private String exceptionMessage;

    @Column(name = "attempts", nullable = false, updatable = false)
    private int attempts;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    @Column(name = "replayed_at")
    private OffsetDateTime replayedAt;

    @Column(name = "replay_count", nullable = false)
    private int replayCount;

    protected DeadLetter() {
        // for JPA
    }

    /**
     * Marks this letter as sent back to its original topic.
     *
     * <p>Call only after the broker has acknowledged the replay, for the same reason
     * {@link com.dpe.messaging.outbox.OutboxMessage#markPublished} says so: marking first and
     * sending second loses the message if the send then fails, and this row is the copy of last
     * resort.
     *
     * <p>{@code replayCount} increments rather than being a boolean, because a letter can be
     * replayed more than once - the fix did not work and the operator tries again - and "how many
     * times has someone tried to push this through" is the first question asked about a row that
     * keeps coming back.
     */
    public void markReplayed(OffsetDateTime at) {
        this.replayedAt = at;
        this.replayCount++;
    }

    public UUID getId() {
        return id;
    }

    public UUID getMessageId() {
        return messageId;
    }

    public String getOriginalTopic() {
        return originalTopic;
    }

    public int getOriginalPartition() {
        return originalPartition;
    }

    public long getOriginalOffset() {
        return originalOffset;
    }

    public String getMessageKey() {
        return messageKey;
    }

    public String getEventType() {
        return eventType;
    }

    public String getPayload() {
        return payload;
    }

    public String getExceptionType() {
        return exceptionType;
    }

    public String getExceptionMessage() {
        return exceptionMessage;
    }

    public int getAttempts() {
        return attempts;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    public OffsetDateTime getReplayedAt() {
        return replayedAt;
    }

    public int getReplayCount() {
        return replayCount;
    }
}
