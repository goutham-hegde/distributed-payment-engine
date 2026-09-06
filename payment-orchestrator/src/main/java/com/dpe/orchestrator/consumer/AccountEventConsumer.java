package com.dpe.orchestrator.consumer;

import com.dpe.events.EventEnvelope;
import com.dpe.events.FundsTransferred;
import com.dpe.events.Topics;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Subscribes to the events account-service publishes, and hands each one to
 * {@link AccountEventHandler}.
 *
 * <p>This class does transport work only - decode, route, acknowledge. All of the correctness
 * lives one class along, in the handler's transaction.
 *
 * <h2>The three things this class gets right, in order of how easy they are to get wrong</h2>
 *
 * <p><b>1. It acknowledges after the handler returns, never before.</b> The offset commit is what
 * tells Kafka "you may stop sending me this". Commit it before the work is durable - which is
 * what {@code enable.auto.commit=true} does, on a timer, with no idea what your code is doing -
 * and a crash in between loses the message permanently. That is at-most-once, and for a payment
 * event it means the orchestrator never learns that money moved. Manual acknowledgement after a
 * committed transaction is the only ordering that gives at-least-once.
 *
 * <p><b>2. It does not acknowledge on failure.</b> An exception propagates out of this method,
 * the offset is not committed, and the container's error handler will redeliver. The message is
 * retried rather than dropped. The cost of that choice is that a message which can never succeed
 * blocks its partition forever - the poison-message problem, which is what the dead letter topic
 * in M4 exists to solve. Until then, blocking is the correct behaviour: stalling loudly beats
 * discarding a payment event quietly.
 *
 * <p><b>3. The transaction lives in the handler, not here.</b> If this method were
 * {@code @Transactional}, the {@code ack.acknowledge()} below would run <i>inside</i> the
 * transaction, before the commit - reintroducing exactly the problem in (1). Keeping the
 * transactional boundary in a separate bean means it has genuinely committed by the time control
 * returns here.
 */
@Component
public class AccountEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(AccountEventConsumer.class);

    private static final TypeReference<EventEnvelope<FundsTransferred>> FUNDS_TRANSFERRED =
            new TypeReference<>() {
            };

    private final AccountEventHandler handler;
    private final ObjectMapper objectMapper;

    public AccountEventConsumer(AccountEventHandler handler, ObjectMapper objectMapper) {
        this.handler = handler;
        this.objectMapper = objectMapper;
    }

    // groupId comes from spring.kafka.consumer.group-id. One group per service: every instance
    // of payment-orchestrator shares it, so a message is delivered to exactly one instance and
    // adding instances divides the partitions rather than duplicating the work.
    @KafkaListener(topics = Topics.ACCOUNT_EVENTS)
    public void onAccountEvent(ConsumerRecord<String, String> record, Acknowledgment ack)
            throws Exception {

        String eventType = header(record, EventEnvelope.EVENT_TYPE_HEADER);
        String rawMessageId = header(record, EventEnvelope.MESSAGE_ID_HEADER);

        if (rawMessageId == null) {
            // Nothing can be deduped without an id, and applying it blind risks double-processing
            // money. Drop it and say so loudly - this can only be a producer bug.
            log.error("message on {} partition {} offset {} has no {} header; skipping",
                    record.topic(), record.partition(), record.offset(),
                    EventEnvelope.MESSAGE_ID_HEADER);
            ack.acknowledge();
            return;
        }
        UUID messageId = UUID.fromString(rawMessageId);

        if (!FundsTransferred.TYPE.equals(eventType)) {
            // A type this version does not know about. Acknowledged rather than retried: it is
            // not going to become understandable on the next attempt, and blocking the partition
            // over a message meant for a newer consumer would take the service down on deploy.
            log.warn("ignoring unknown event type '{}' (message {})", eventType, messageId);
            ack.acknowledge();
            return;
        }

        EventEnvelope<FundsTransferred> envelope =
                objectMapper.readValue(record.value(), FUNDS_TRANSFERRED);

        boolean applied = handler.handle(messageId, record.topic(), envelope);

        // Reached only if the handler committed. A duplicate returns false and is still
        // acknowledged - it has been dealt with, which is the whole point.
        ack.acknowledge();

        log.debug("message {} ({}) {}", messageId, eventType,
                applied ? "applied" : "skipped as duplicate");
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
