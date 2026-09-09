package com.dpe.orchestrator.saga;

import com.dpe.events.AccountOpened;
import com.dpe.events.EventEnvelope;
import com.dpe.events.FundsCommitted;
import com.dpe.events.FundsReleased;
import com.dpe.events.FundsReserved;
import com.dpe.events.FundsTransferred;
import com.dpe.events.GatewayApproved;
import com.dpe.events.GatewayDeclined;
import com.dpe.events.ReserveRejected;
import com.dpe.events.Topics;
import com.dpe.orchestrator.consumer.AccountEventHandler;
import com.dpe.orchestrator.readmodel.AccountOwnerHandler;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

/**
 * The orchestrator's single inbound listener. Subscribes to both reply topics - account-service's
 * events and the gateway's - and routes each message to the one handler that owns it.
 *
 * <p>Transport only. Acknowledge after the handler commits, never before; do not acknowledge on
 * failure; keep the transaction one class along so {@code ack.acknowledge()} cannot run inside it.
 *
 * <h2>Why there is exactly ONE listener in this service, and one consumer group</h2>
 *
 * <p>This started M3 as a second listener alongside M2's {@code AccountEventConsumer}, with its
 * own group id, and that would have been a silent, expensive bug. Both would have subscribed to
 * {@link Topics#ACCOUNT_EVENTS}; two consumer groups over one topic means <b>both</b> receive
 * <b>every</b> message. And the inbox's primary key is the message id <i>alone</i>.
 *
 * <p>So whichever group got there first would insert the inbox row, and the second would find the
 * message already recorded and skip it as a duplicate - <b>correctly, by its own rules, and
 * catastrophically</b>. Saga replies would be silently swallowed by the projection consumer's
 * dedup, at random, depending on which thread won. Every saga would stall in STARTED and the
 * sweeper would compensate perfectly healthy transfers. Nothing would log an error, because
 * nothing went wrong from any single component's point of view.
 *
 * <p>{@code V1__inbox_and_read_model.sql} warns about exactly this in a column comment: a service
 * running two consumer groups over one topic must widen the key to
 * {@code (message_id, consumer_group)}. Two ways out, then:
 *
 * <ol>
 *   <li>Widen the inbox key. More general, and it makes every dedup lookup carry a group id that
 *       exists only to serve a situation this service does not actually need.</li>
 *   <li><b>Have one group.</b> One listener, routing by event type to the handler that owns it,
 *       so every message passes through exactly one gate. Chosen.</li>
 * </ol>
 *
 * <p>The general lesson is worth more than the fix: <b>a dedup key must be unique across
 * everything that shares the table.</b> A key that is unique per message but not per consumer is
 * fine right up to the day someone adds a second consumer, and then it fails by being too
 * effective rather than by erroring.
 */
@Component
public class SagaReplyConsumer {

    private static final Logger log = LoggerFactory.getLogger(SagaReplyConsumer.class);

    private static final TypeReference<EventEnvelope<FundsReserved>> FUNDS_RESERVED =
            new TypeReference<>() {
            };
    private static final TypeReference<EventEnvelope<ReserveRejected>> RESERVE_REJECTED =
            new TypeReference<>() {
            };
    private static final TypeReference<EventEnvelope<GatewayApproved>> GATEWAY_APPROVED =
            new TypeReference<>() {
            };
    private static final TypeReference<EventEnvelope<GatewayDeclined>> GATEWAY_DECLINED =
            new TypeReference<>() {
            };
    private static final TypeReference<EventEnvelope<FundsCommitted>> FUNDS_COMMITTED =
            new TypeReference<>() {
            };
    private static final TypeReference<EventEnvelope<FundsReleased>> FUNDS_RELEASED =
            new TypeReference<>() {
            };
    private static final TypeReference<EventEnvelope<FundsTransferred>> FUNDS_TRANSFERRED =
            new TypeReference<>() {
            };
    private static final TypeReference<EventEnvelope<AccountOpened>> ACCOUNT_OPENED =
            new TypeReference<>() {
            };

    private final SagaReplyHandler sagaReplies;
    private final AccountEventHandler projections;
    private final AccountOwnerHandler owners;
    private final ObjectMapper objectMapper;

    public SagaReplyConsumer(SagaReplyHandler sagaReplies, AccountEventHandler projections,
                             AccountOwnerHandler owners, ObjectMapper objectMapper) {
        this.sagaReplies = sagaReplies;
        this.projections = projections;
        this.owners = owners;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = {Topics.ACCOUNT_EVENTS, Topics.GATEWAY_EVENTS})
    public void onReply(ConsumerRecord<String, String> record, Acknowledgment ack) {

        String eventType = header(record, EventEnvelope.EVENT_TYPE_HEADER);
        String rawMessageId = header(record, EventEnvelope.MESSAGE_ID_HEADER);

        if (rawMessageId == null) {
            // Undedupable, and acting on it risks moving money twice. Dropped loudly - this can
            // only be a producer bug.
            log.error("reply on {} partition {} offset {} has no {} header; skipping",
                    record.topic(), record.partition(), record.offset(),
                    EventEnvelope.MESSAGE_ID_HEADER);
            ack.acknowledge();
            return;
        }
        UUID messageId = UUID.fromString(rawMessageId);
        String topic = record.topic();

        // M2's read model. Routed here rather than to SagaReplyHandler because it has its own
        // gate; each message must pass through exactly one, or the second would see the first's
        // inbox row and skip the work.
        if (FundsTransferred.TYPE.equals(eventType)) {
            boolean applied = projections.handle(messageId, topic,
                    objectMapper.readValue(record.value(), FUNDS_TRANSFERRED));
            ack.acknowledge();
            log.debug("projection {} {}", messageId, applied ? "applied" : "skipped as duplicate");
            return;
        }

        // M5's ownership projection. A third kind of work on this topic and still the same
        // consumer group - see AccountOwnerHandler for why that is a rule rather than a habit.
        // Its own gate, for the same reason the projection above has one: one claim per piece of
        // work, or the second is skipped as a duplicate of the first.
        if (AccountOpened.TYPE.equals(eventType)) {
            boolean applied = owners.handle(messageId, topic,
                    objectMapper.readValue(record.value(), ACCOUNT_OPENED));
            ack.acknowledge();
            log.debug("ownership {} {}", messageId, applied ? "applied" : "skipped as duplicate");
            return;
        }

        Object reply = switch (eventType == null ? "" : eventType) {
            case FundsReserved.TYPE   -> objectMapper.readValue(record.value(), FUNDS_RESERVED).payload();
            case ReserveRejected.TYPE -> objectMapper.readValue(record.value(), RESERVE_REJECTED).payload();
            case GatewayApproved.TYPE -> objectMapper.readValue(record.value(), GATEWAY_APPROVED).payload();
            case GatewayDeclined.TYPE -> objectMapper.readValue(record.value(), GATEWAY_DECLINED).payload();
            case FundsCommitted.TYPE  -> objectMapper.readValue(record.value(), FUNDS_COMMITTED).payload();
            case FundsReleased.TYPE   -> objectMapper.readValue(record.value(), FUNDS_RELEASED).payload();
            default -> null;
        };

        if (reply == null) {
            // A type this version does not know. Acknowledged rather than retried: it will not
            // become understandable on the next attempt, and blocking the partition over a
            // message meant for a newer consumer would take the service down on deploy.
            //
            // Still written to the inbox, so "we saw this and chose not to act" is a fact in the
            // database rather than a log line that has since rotated away.
            log.warn("ignoring unknown reply type '{}' (message {})", eventType, messageId);
            sagaReplies.skip(messageId, topic, String.valueOf(eventType));
            ack.acknowledge();
            return;
        }

        boolean applied = sagaReplies.handle(messageId, topic, eventType, reply);

        // Reached only if the handler committed. A duplicate returns false and is still
        // acknowledged - it has been dealt with, which is the whole point.
        ack.acknowledge();

        log.debug("reply {} ({}) {}", messageId, eventType,
                applied ? "applied" : "skipped as duplicate");
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
