package com.dpe.messaging.inbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.hibernate.annotations.CreationTimestamp;

/**
 * A record that this service has already processed a given message.
 *
 * <p>The id is the producer's outbox row id and is assigned, never generated - see
 * {@link com.dpe.events.EventEnvelope#messageId()} for why every other candidate key is wrong.
 */
@Entity
@Table(name = "inbox")
public class InboxMessage {

    @Id
    @Column(name = "message_id", nullable = false, updatable = false)
    private UUID messageId;

    @Column(name = "topic", nullable = false, updatable = false, length = 128)
    private String topic;

    @Column(name = "event_type", nullable = false, updatable = false, length = 64)
    private String eventType;

    @CreationTimestamp
    @Column(name = "received_at", nullable = false, updatable = false)
    private OffsetDateTime receivedAt;

    protected InboxMessage() {
        // for JPA
    }

    public InboxMessage(UUID messageId, String topic, String eventType) {
        this.messageId = messageId;
        this.topic = topic;
        this.eventType = eventType;
    }

    public UUID getMessageId() {
        return messageId;
    }

    public String getTopic() {
        return topic;
    }

    public String getEventType() {
        return eventType;
    }

    public OffsetDateTime getReceivedAt() {
        return receivedAt;
    }
}
