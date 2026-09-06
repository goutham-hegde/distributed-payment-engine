package com.dpe.account.support;

import com.dpe.events.Topics;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.redpanda.RedpandaContainer;

/**
 * Base class for tests that need a real broker as well as a real database.
 *
 * <p>Redpanda rather than Apache Kafka, matching {@code infra/docker-compose.yml}: it speaks the
 * same wire protocol, starts in about a second, and does not need a JVM of its own. The trade is
 * that it is not the implementation production would run, which is why the compose file keeps a
 * {@code kafka} profile - "passes on Redpanda" is evidence, not proof.
 *
 * <p>Started as a JVM-wide singleton for the same reason as the Postgres container: the JUnit
 * {@code @Container} extension would stop it at the end of each test class and pay the startup
 * cost again for the next one.
 *
 * <p>{@code @ServiceConnection} wires {@code spring.kafka.bootstrap-servers} to the container's
 * mapped port. The factory that does it ships in {@code spring-boot-kafka}, which arrives with
 * {@code spring-boot-starter-kafka} - in Boot 4 the connection-detail factories live in the
 * per-technology modules rather than all together in {@code spring-boot-testcontainers}.
 */
public abstract class AbstractKafkaIT extends AbstractPostgresIT {

    @ServiceConnection
    static final RedpandaContainer REDPANDA =
            new RedpandaContainer("redpandadata/redpanda:v25.3.17");

    static {
        REDPANDA.start();
    }

    /**
     * Where each partition of the topic ended before this test started.
     *
     * <p>A Kafka topic is a log, not a queue: nothing is removed by being read, so the container
     * being a per-JVM singleton means every test would otherwise see every record the earlier
     * tests published. Recording the end offsets here and seeking to them in {@link #publishedNow}
     * makes each test's view exactly "what this test caused", without inventing a throwaway topic
     * per test and thereby not testing the real one.
     */
    private final Map<TopicPartition, Long> watermark = new HashMap<>();

    @BeforeEach
    void markTopicPosition() {
        jdbc.execute("TRUNCATE TABLE outbox");
        watermark.clear();
        try (KafkaConsumer<String, String> consumer = probeConsumer()) {
            List<TopicPartition> partitions = partitionsOf(consumer, Topics.ACCOUNT_EVENTS);
            watermark.putAll(consumer.endOffsets(partitions));
        }
    }

    /** Every record published to {@link Topics#ACCOUNT_EVENTS} since this test began. */
    protected List<ConsumerRecord<String, String>> publishedNow() {
        List<ConsumerRecord<String, String>> collected = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = probeConsumer()) {
            List<TopicPartition> partitions = partitionsOf(consumer, Topics.ACCOUNT_EVENTS);
            consumer.assign(partitions);
            for (TopicPartition partition : partitions) {
                consumer.seek(partition, watermark.getOrDefault(partition, 0L));
            }
            // Poll until two consecutive empty polls. One empty poll proves nothing: the first
            // poll of a fresh consumer routinely returns nothing while the connection is still
            // being established, and a test that gave up there would be flaky rather than
            // failing.
            int consecutiveEmpty = 0;
            while (consecutiveEmpty < 2) {
                ConsumerRecords<String, String> batch = consumer.poll(Duration.ofSeconds(2));
                if (batch.isEmpty()) {
                    consecutiveEmpty++;
                } else {
                    consecutiveEmpty = 0;
                    batch.forEach(collected::add);
                }
            }
        }
        return collected;
    }

    /** Header value as a String, or {@code null} if the record does not carry it. */
    protected static String headerOf(ConsumerRecord<String, String> record, String name) {
        var header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value());
    }

    protected Map<String, Object> outboxRow(UUID messageId) {
        return jdbc.queryForMap("SELECT * FROM outbox WHERE id = ?", messageId);
    }

    private static List<TopicPartition> partitionsOf(KafkaConsumer<String, String> consumer,
                                                     String topic) {
        List<PartitionInfo> infos = consumer.partitionsFor(topic);
        List<TopicPartition> partitions = new ArrayList<>();
        if (infos != null) {
            for (PartitionInfo info : infos) {
                partitions.add(new TopicPartition(info.topic(), info.partition()));
            }
        }
        return partitions;
    }

    private static KafkaConsumer<String, String> probeConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
        // A fresh group every time, so a probe never resumes from another probe's offsets and
        // never joins the service's own consumer group.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "probe-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }
}
