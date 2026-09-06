package com.dpe.orchestrator.support;

import com.dpe.events.EventEnvelope;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.redpanda.RedpandaContainer;

/**
 * Base class for orchestrator tests that consume from a real broker.
 *
 * <p>Re-declares {@code @SpringBootTest} to turn the listener container back on - the parent
 * disables it, because most tests here have no broker to talk to.
 */
@SpringBootTest(properties = {
        "spring.kafka.listener.auto-startup=true"
})
public abstract class AbstractKafkaIT extends AbstractPostgresIT {

    @ServiceConnection
    static final RedpandaContainer REDPANDA =
            new RedpandaContainer("redpandadata/redpanda:v25.3.17");

    static {
        REDPANDA.start();
    }

    /**
     * Publishes a record shaped exactly as {@code OutboxRelay} would publish it.
     *
     * <p>Hand-rolled rather than reusing the relay, because account-service is a different module
     * and a different database. Standing in for the producer here is what lets this test assert
     * on duplicate delivery: it can send the same message id twice, which a correct relay would
     * only do by crashing at exactly the wrong moment.
     */
    protected void publish(String topic, UUID messageId, UUID aggregateId, String eventType,
                           String payloadJson) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, aggregateId.toString(), payloadJson);
            record.headers().add(EventEnvelope.MESSAGE_ID_HEADER,
                    messageId.toString().getBytes(StandardCharsets.UTF_8));
            record.headers().add(EventEnvelope.EVENT_TYPE_HEADER,
                    eventType.getBytes(StandardCharsets.UTF_8));
            producer.send(record).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while publishing test record", e);
        } catch (Exception e) {
            throw new IllegalStateException("could not publish test record", e);
        }
    }

    /** The envelope JSON the relay would carry for a FundsTransferred event. */
    protected static String fundsTransferredJson(UUID messageId, UUID transferId, UUID from,
                                                 UUID to, long amountMinor) {
        return """
                {"messageId":"%s","eventType":"FundsTransferred","aggregateId":"%s",\
                "occurredAt":"2026-01-01T00:00:00Z","payload":{"transferId":"%s",\
                "fromAccountId":"%s","toAccountId":"%s","amountMinor":%d,"currency":"INR"}}"""
                .formatted(messageId, transferId, transferId, from, to, amountMinor);
    }
}
