package com.dpe.events;

import java.time.Instant;
import java.util.UUID;

/**
 * The wire format for every message this system publishes.
 *
 * <p>The envelope exists so that the transport-level concerns - identity, type, routing,
 * causality - live outside the business payload and never change when the payload does. A
 * consumer can dedupe and route a message it cannot yet deserialize.
 *
 * @param messageId   <b>the outbox row's primary key</b>, and the only correct dedup key in this
 *                    system. It is generated once, inside the business transaction that produced
 *                    the message, and is therefore <i>identical on every redelivery</i>.
 *                    <p>The tempting alternatives are all wrong. A Kafka partition/offset pair
 *                    changes when the relay republishes after crashing between the send and the
 *                    {@code published_at} update - which is precisely the case the inbox exists
 *                    to absorb, so dedup would never fire. A UUID minted at send time has the
 *                    same defect. The {@link #aggregateId} alone is too coarse: one transfer
 *                    legitimately emits several messages.
 * @param eventType   discriminator for the payload shape, so a consumer can dispatch without
 *                    relying on the topic having exactly one type on it.
 * @param aggregateId the entity this message is about - the transfer id here. <b>This is also
 *                    the Kafka partition key.</b> Kafka guarantees order within a partition
 *                    only, and the relay's {@code FOR UPDATE SKIP LOCKED} claim gives no global
 *                    order either, so the guarantee this system actually offers is: <i>ordered
 *                    per aggregate, unordered across aggregates.</i> Keying by aggregate id is
 *                    what makes that true rather than aspirational.
 * @param occurredAt  when the producing transaction committed, not when the relay published.
 *                    The gap between the two is relay lag, and keeping both timestamps is how
 *                    M6 measures it.
 * @param payload     the business event.
 */
public record EventEnvelope<T>(
        UUID messageId,
        String eventType,
        UUID aggregateId,
        Instant occurredAt,
        T payload) {

    /** Kafka header carrying {@link #messageId}, so a consumer can dedupe before deserializing. */
    public static final String MESSAGE_ID_HEADER = "dpe-message-id";

    /** Kafka header carrying {@link #eventType}, for routing without deserializing. */
    public static final String EVENT_TYPE_HEADER = "dpe-event-type";
}
