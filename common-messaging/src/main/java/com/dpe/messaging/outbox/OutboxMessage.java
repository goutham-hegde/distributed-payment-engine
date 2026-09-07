package com.dpe.messaging.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * One message waiting to be published, or already published and kept as an audit record.
 *
 * <p>An unusual entity in two ways, both deliberate.
 *
 * <p><b>The id is assigned in Java, not by the database.</b> There is no {@code @GeneratedValue}.
 * The producing code needs the message id in hand before the row is inserted, because that id is
 * what travels in the Kafka message and what the consumer dedupes on. A database-generated id
 * would only be knowable after the INSERT, which is too late to be useful and would tempt someone
 * into generating a second, different id for the message - at which point redelivery stops being
 * detectable.
 *
 * <p><b>{@code payload} is a String, not a typed object.</b> It is serialized once, by
 * {@link OutboxWriter}, inside the business transaction. The relay never deserializes it and so
 * never needs to know the event classes; adding a new message type does not touch the relay, and
 * a payload that cannot be serialized fails while the transaction can still roll back.
 */
@Entity
@Table(name = "outbox")
public class OutboxMessage {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "aggregate_type", nullable = false, updatable = false, length = 64)
    private String aggregateType;

    /** Becomes the Kafka message key, which is what scopes the ordering guarantee. */
    @Column(name = "aggregate_id", nullable = false, updatable = false)
    private UUID aggregateId;

    @Column(name = "topic", nullable = false, updatable = false, length = 128)
    private String topic;

    @Column(name = "event_type", nullable = false, updatable = false, length = 64)
    private String eventType;

    // Maps to Postgres jsonb. Without the explicit type code Hibernate would bind a String as
    // varchar and Postgres would refuse the implicit cast to jsonb at INSERT time.
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", nullable = false, updatable = false)
    private String payload;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    /** {@code null} until the broker has acknowledged the send. The relay's whole working set. */
    @Column(name = "published_at")
    private OffsetDateTime publishedAt;

    @Column(name = "attempts", nullable = false)
    private int attempts;

    @Column(name = "last_error")
    private String lastError;

    protected OutboxMessage() {
        // for JPA
    }

    OutboxMessage(UUID id, String aggregateType, UUID aggregateId, String topic, String eventType,
                  String payload) {
        this.id = id;
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.topic = topic;
        this.eventType = eventType;
        this.payload = payload;
        this.attempts = 0;
    }

    /**
     * Marks this row as sent. Call only after the broker has ACKED the send - a row marked
     * published before the ack can be lost outright if the send then fails, which converts the
     * design from at-least-once to at-most-once and loses money.
     */
    public void markPublished(OffsetDateTime at) {
        this.publishedAt = at;
    }

    /**
     * Records a failed publish attempt. The row stays unpublished and will be retried by a later
     * poll; {@code attempts} is what lets M4 decide a message is poison rather than unlucky.
     */
    public void recordFailure(String error) {
        this.attempts++;
        // Truncated: a driver-level exception chain can run to kilobytes, and this column is
        // for triage, not for forensics. The full stack trace belongs in the logs.
        this.lastError = error == null ? null : error.substring(0, Math.min(error.length(), 1000));
    }

    public UUID getId() {
        return id;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public UUID getAggregateId() {
        return aggregateId;
    }

    public String getTopic() {
        return topic;
    }

    public String getEventType() {
        return eventType;
    }

    public String getPayload() {
        return payload;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    public OffsetDateTime getPublishedAt() {
        return publishedAt;
    }

    public int getAttempts() {
        return attempts;
    }

    public String getLastError() {
        return lastError;
    }
}
