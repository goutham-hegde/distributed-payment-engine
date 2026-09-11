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
 */
@Component
public class GatewayCommandConsumer {

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

    @KafkaListener(topics = Topics.GATEWAY_COMMANDS)
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
