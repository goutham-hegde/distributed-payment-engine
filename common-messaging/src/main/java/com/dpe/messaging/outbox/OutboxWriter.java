package com.dpe.messaging.outbox;

import com.dpe.events.EventEnvelope;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * Appends a message to the outbox as part of the caller's transaction.
 *
 * <p><b>There is no {@code @Transactional} on this class, and that is the point.</b> It must join
 * the transaction that is already open - the one posting the ledger entries - so that the
 * business rows and the message row commit or roll back together. Annotating it
 * {@code REQUIRES_NEW} would give the message its own transaction and reintroduce the dual-write
 * bug in a subtler form: the message could commit while the transfer rolled back, telling the
 * rest of the system about money that never moved.
 *
 * <p>Consequently this class is only ever safe to call from inside a transactional method. There
 * is no runtime check for that; the discipline is structural.
 */
@Component
public class OutboxWriter {

    private final OutboxRepository outbox;
    private final ObjectMapper objectMapper;

    public OutboxWriter(OutboxRepository outbox, ObjectMapper objectMapper) {
        this.outbox = outbox;
        this.objectMapper = objectMapper;
    }

    /**
     * Wraps {@code payload} in an {@link EventEnvelope} and stores it for the relay to publish.
     *
     * @param aggregateType what kind of thing this message is about, e.g. {@code "Transfer"}
     * @param aggregateId   the entity id. Becomes the Kafka partition key, so this choice is what
     *                      determines which messages are ordered with respect to each other.
     * @param topic         destination, from {@link com.dpe.events.Topics}
     * @param eventType     payload discriminator, e.g. {@link com.dpe.events.FundsTransferred#TYPE}
     * @return the message id, which is also the outbox row's primary key and the value the
     *         consumer will dedupe on
     */
    public UUID append(String aggregateType, UUID aggregateId, String topic, String eventType,
                       Object payload) {
        UUID messageId = UUID.randomUUID();
        EventEnvelope<Object> envelope =
                new EventEnvelope<>(messageId, eventType, aggregateId, Instant.now(), payload);

        String json;
        try {
            json = objectMapper.writeValueAsString(envelope);
        } catch (JacksonException e) {
            // Jackson 3 made every exception unchecked, so this catch is not forced on us - it
            // exists only to name the failing event type. It rethrows rather than recovering:
            // serialization failing means we cannot describe what we just did, so the safe
            // outcome is for the business transaction to roll back with it rather than commit a
            // change nobody can be told about.
            throw new IllegalStateException(
                    "Could not serialize outbox payload of type " + eventType, e);
        }

        outbox.save(new OutboxMessage(messageId, aggregateType, aggregateId, topic, eventType, json));
        return messageId;
    }
}
