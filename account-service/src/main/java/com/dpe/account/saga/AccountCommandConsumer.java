package com.dpe.account.saga;

import com.dpe.events.CommitFunds;
import com.dpe.events.EventEnvelope;
import com.dpe.events.ReleaseFunds;
import com.dpe.events.ReserveFunds;
import com.dpe.events.Topics;
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
 * Subscribes to the commands the orchestrator issues to this service.
 *
 * <p>Transport only: decode, route, acknowledge. Identical discipline to the orchestrator's
 * {@code AccountEventConsumer} - acknowledge only after the handler's transaction has committed,
 * do not acknowledge on failure, and keep the transaction in the handler so that
 * {@code ack.acknowledge()} cannot run inside it.
 */
@Component
public class AccountCommandConsumer {

    private static final Logger log = LoggerFactory.getLogger(AccountCommandConsumer.class);

    private static final TypeReference<EventEnvelope<ReserveFunds>> RESERVE_FUNDS =
            new TypeReference<>() {
            };
    private static final TypeReference<EventEnvelope<CommitFunds>> COMMIT_FUNDS =
            new TypeReference<>() {
            };
    private static final TypeReference<EventEnvelope<ReleaseFunds>> RELEASE_FUNDS =
            new TypeReference<>() {
            };

    private final AccountCommandHandler handler;
    private final ObjectMapper objectMapper;

    public AccountCommandConsumer(AccountCommandHandler handler, ObjectMapper objectMapper) {
        this.handler = handler;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = Topics.ACCOUNT_COMMANDS)
    public void onCommand(ConsumerRecord<String, String> record, Acknowledgment ack) {

        String eventType = header(record, EventEnvelope.EVENT_TYPE_HEADER);
        String rawMessageId = header(record, EventEnvelope.MESSAGE_ID_HEADER);

        if (rawMessageId == null) {
            // Undedupable. Acting on it risks moving money twice, so it is dropped loudly -
            // this can only be a producer bug.
            log.error("command on {} partition {} offset {} has no {} header; skipping",
                    record.topic(), record.partition(), record.offset(),
                    EventEnvelope.MESSAGE_ID_HEADER);
            ack.acknowledge();
            return;
        }
        UUID messageId = UUID.fromString(rawMessageId);

        // The type header decides which record class to deserialize into. Reading it from a
        // header rather than sniffing the JSON means an unknown type is identified without
        // parsing a body this version may not understand.
        Object command = switch (eventType == null ? "" : eventType) {
            case ReserveFunds.TYPE -> objectMapper.readValue(record.value(), RESERVE_FUNDS).payload();
            case CommitFunds.TYPE  -> objectMapper.readValue(record.value(), COMMIT_FUNDS).payload();
            case ReleaseFunds.TYPE -> objectMapper.readValue(record.value(), RELEASE_FUNDS).payload();
            default -> null;
        };

        if (command == null) {
            // A command type this version does not know. Acknowledged rather than retried: it
            // will not become understandable on the next attempt, and blocking the partition
            // over a message meant for a newer consumer would take the service down on deploy.
            //
            // It is still recorded in the inbox, so "we saw this and chose not to act" is a fact
            // in the database rather than a line in a log that has since rotated away.
            log.warn("ignoring unknown command type '{}' (message {})", eventType, messageId);
            handler.skip(messageId, record.topic(), String.valueOf(eventType));
            ack.acknowledge();
            return;
        }

        boolean applied = handler.handle(messageId, record.topic(), eventType, command);

        // Reached only if the handler committed. A duplicate returns false and is still
        // acknowledged - it has been dealt with, which is the whole point.
        ack.acknowledge();

        log.debug("command {} ({}) {}", messageId, eventType,
                applied ? "applied" : "skipped as duplicate");
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
