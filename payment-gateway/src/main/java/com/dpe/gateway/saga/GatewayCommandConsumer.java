package com.dpe.gateway.saga;

import com.dpe.events.ChargeGateway;
import com.dpe.events.EventEnvelope;
import com.dpe.events.Topics;
import com.dpe.events.VoidCharge;
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
 * Subscribes to {@link Topics#GATEWAY_COMMANDS}. Transport only.
 *
 * <p>Acknowledges after the handler commits, never before, and does not acknowledge on failure -
 * so a simulated PSP timeout leaves the offset uncommitted and the command is redelivered.
 *
 * <h2>Concurrency (M8)</h2>
 *
 * <p>One consumer per partition of {@link Topics#GATEWAY_COMMANDS}, set by
 * {@code dpe.gateway.command-concurrency}. With one thread the gateway was the knee of the whole
 * system: each charge holds its thread for the PSP call, so 50 ms of PSP latency is a ceiling of
 * 20 charges/s however fast everything else gets.
 *
 * <p>Why more threads do not reorder anything: every command about a transfer is keyed by its
 * transfer id, so it lands on one partition, and one partition is only ever read by one thread.
 * Per-transfer order is a property of the key, not of there being one thread (a dead letter
 * replay reuses the original key, so it too lands on the same partition). Two commands for one
 * transfer in flight at once needs a partition to change owner mid-delivery - a consumer evicted
 * past {@code max.poll.interval.ms} while its handler is still running - and that was possible
 * with one thread per instance and two instances just the same. It is settled by the
 * {@code UNIQUE(transfer_id)} on {@code gateway_charges}, never by the thread count.
 *
 * <p>The ceiling is the partition count: a fourth thread would be assigned nothing. And every
 * thread holds a connection for the whole of its transaction, PSP call included, so the value is
 * also a claim on the pool - 3 of 10.
 *
 * <p>The listener is named so a test can find its container, and {@code idIsGroup = false} is
 * NOT optional: without it Spring Kafka uses the id as the {@code group.id}, moving this service
 * to a new group reading from {@code earliest}.
 */
@Component
public class GatewayCommandConsumer {

    public static final String LISTENER_ID = "gateway-commands";

    private static final Logger log = LoggerFactory.getLogger(GatewayCommandConsumer.class);

    private static final TypeReference<EventEnvelope<ChargeGateway>> CHARGE_GATEWAY =
            new TypeReference<>() {
            };
    private static final TypeReference<EventEnvelope<VoidCharge>> VOID_CHARGE =
            new TypeReference<>() {
            };

    private final GatewayCommandHandler handler;
    private final ObjectMapper objectMapper;

    public GatewayCommandConsumer(GatewayCommandHandler handler, ObjectMapper objectMapper) {
        this.handler = handler;
        this.objectMapper = objectMapper;
    }

    // No default in the placeholder: a mis-nested key in application.yml should fail the boot,
    // not quietly fall back to a number nobody chose.
    @KafkaListener(id = LISTENER_ID, idIsGroup = false, topics = Topics.GATEWAY_COMMANDS,
            concurrency = "${dpe.gateway.command-concurrency}")
    public void onCommand(ConsumerRecord<String, String> record, Acknowledgment ack) {

        String eventType = header(record, EventEnvelope.EVENT_TYPE_HEADER);
        String rawMessageId = header(record, EventEnvelope.MESSAGE_ID_HEADER);

        if (rawMessageId == null) {
            log.error("command on {} partition {} offset {} has no {} header; skipping",
                    record.topic(), record.partition(), record.offset(),
                    EventEnvelope.MESSAGE_ID_HEADER);
            ack.acknowledge();
            return;
        }
        UUID messageId = UUID.fromString(rawMessageId);

        Object command = switch (eventType == null ? "" : eventType) {
            case ChargeGateway.TYPE -> objectMapper.readValue(record.value(), CHARGE_GATEWAY).payload();
            case VoidCharge.TYPE    -> objectMapper.readValue(record.value(), VOID_CHARGE).payload();
            default -> null;
        };

        if (command == null) {
            log.warn("ignoring unknown command type '{}' (message {})", eventType, messageId);
            handler.skip(messageId, record.topic(), String.valueOf(eventType));
            ack.acknowledge();
            return;
        }

        boolean applied = handler.handle(messageId, record.topic(), eventType, command);
        ack.acknowledge();

        log.debug("command {} ({}) {}", messageId, eventType, applied ? "applied" : "skipped as duplicate");
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
