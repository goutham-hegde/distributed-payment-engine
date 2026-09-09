package com.dpe.account;

import static org.assertj.core.api.Assertions.assertThat;

import com.dpe.account.support.AbstractPostgresIT;
import com.dpe.events.EventEnvelope;
import com.dpe.events.Topics;
import com.dpe.messaging.deadletter.DeadLetterRecorder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.KafkaHeaders;

/**
 * The specification for {@code DeadLetterRecorder} - the half of the DLQ that turns a record on a
 * dead letter topic into a row an operator can query.
 *
 * <p>No broker here on purpose. What is being verified is header decoding and the dedup rule,
 * both of which are decisions made from a {@code ConsumerRecord} that a test can build by hand.
 * The end-to-end path - listener throws, backoff, recoverer publishes, this runs - is
 * {@code PoisonMessageTest}.
 */
class DeadLetterRecorderTest extends AbstractPostgresIT {

    @Autowired
    DeadLetterRecorder recorder;

    @Test
    @DisplayName("the original coordinates come from the headers, not from the record")
    void recordsTheOriginalTopicNotTheDeadLetterTopic() {
        UUID messageId = UUID.randomUUID();
        UUID transferId = UUID.randomUUID();

        assertThat(recorder.record(deadLetterRecord(messageId, transferId, 2, 41L))).isTrue();

        Map<String, Object> row = onlyRow();
        assertThat(row.get("original_topic"))
                .as("a replay has to go back to the topic that failed, not to the DLT")
                .isEqualTo(Topics.ACCOUNT_COMMANDS);
        assertThat(row.get("original_partition")).isEqualTo(2);
        assertThat(((Number) row.get("original_offset")).longValue()).isEqualTo(41L);
        assertThat(row.get("message_id")).hasToString(messageId.toString());
        assertThat(row.get("message_key")).isEqualTo(transferId.toString());
        assertThat(row.get("event_type")).isEqualTo("ReserveFunds");
        assertThat(row.get("replayed_at")).isNull();
    }

    @Test
    @DisplayName("the exception that killed it is kept, because triage starts there")
    void recordsTheFailureCause() {
        recorder.record(deadLetterRecord(UUID.randomUUID(), UUID.randomUUID(), 0, 1L));

        Map<String, Object> row = onlyRow();
        assertThat((String) row.get("exception_type")).contains("IllegalArgumentException");
        assertThat((String) row.get("exception_message")).contains("Invalid UUID");
    }

    @Test
    @DisplayName("exception_type is the CAUSE, not the wrapper the container caught")
    void prefersTheCauseOverTheListenerWrapper() {
        // Without this, every row in the table reads ListenerExecutionFailedException - Spring
        // stamps DLT_EXCEPTION_FQCN with what it caught, and anything thrown out of a listener is
        // caught wrapped. A column whose whole purpose is "group the failures by kind" would then
        // answer identically for every row, and the one question it exists for could not be asked.
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                Topics.ACCOUNT_COMMANDS + Topics.DLT_SUFFIX, 0, 11L, "key-1", "{}");
        stampOrigin(record, Topics.ACCOUNT_COMMANDS, 0, 11L);
        record.headers().add(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN,
                utf8("org.springframework.dao.DataIntegrityViolationException"));

        recorder.record(record);

        assertThat(onlyRow().get("exception_type"))
                .isEqualTo("org.springframework.dao.DataIntegrityViolationException");
    }

    @Test
    @DisplayName("with no cause header, the outer exception is recorded rather than nothing")
    void fallsBackToTheOuterException() {
        // A failure raised by the container itself, rather than by user code, has no cause. The
        // fallback is what stops the column being null exactly when the failure is unusual.
        recorder.record(deadLetterRecord(UUID.randomUUID(), UUID.randomUUID(), 0, 12L));

        assertThat(onlyRow().get("exception_type")).isEqualTo("java.lang.IllegalArgumentException");
    }

    @Test
    @DisplayName("the same failure delivered twice produces one row")
    void isIdempotentOnTheSourceCoordinates() {
        ConsumerRecord<String, String> record =
                deadLetterRecord(UUID.randomUUID(), UUID.randomUUID(), 1, 7L);

        assertThat(recorder.record(record)).isTrue();
        assertThat(recorder.record(record))
                .as("the dead letter listener is at-least-once like every other consumer here")
                .isFalse();

        assertThat(rowCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("the same MESSAGE failing twice produces two rows")
    void doesNotDedupeOnTheMessageId() {
        // This is the case that decides the dedup key. An operator replays a letter, the fix did
        // not work, and it fails again - at a new offset, because it is a new Kafka record. That
        // is genuinely a second failure and must be visible. Keying on message_id would swallow
        // it, and the operator would be looking at a queue that says "empty" while the same
        // message keeps dying.
        UUID sameMessageId = UUID.randomUUID();
        UUID transferId = UUID.randomUUID();

        recorder.record(deadLetterRecord(sameMessageId, transferId, 1, 7L));
        recorder.record(deadLetterRecord(sameMessageId, transferId, 1, 99L));

        assertThat(rowCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("a record with no message id is still recorded, not dropped")
    void survivesAMissingMessageId() {
        // A missing or malformed message id is one of the reasons a record dead-letters in the
        // first place - AccountCommandConsumer drops those before any handler sees them. A dead
        // letter table that could not hold them would lose exactly the evidence you need.
        ConsumerRecord<String, String> record =
                deadLetterRecord(null, UUID.randomUUID(), 0, 3L);

        assertThat(recorder.record(record)).isTrue();
        assertThat(onlyRow().get("message_id")).isNull();
    }

    @Test
    @DisplayName("a payload that is not valid JSON is stored verbatim")
    void storesUnparseablePayloads() {
        // The column is TEXT rather than jsonb precisely for this. A jsonb column would reject
        // the row, so the INSERT that records the failure would itself fail, and the message
        // would be lost by the machinery built to save it.
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                Topics.ACCOUNT_COMMANDS + Topics.DLT_SUFFIX, 0, 5L, "key-1", "{not json at all");
        stampOrigin(record, Topics.ACCOUNT_COMMANDS, 0, 5L);

        assertThat(recorder.record(record)).isTrue();
        assertThat(onlyRow().get("payload")).isEqualTo("{not json at all");
    }

    // ---- fixtures -----------------------------------------------------------------------

    /** A record shaped exactly as {@code DeadLetterPublishingRecoverer} would publish it. */
    private static ConsumerRecord<String, String> deadLetterRecord(UUID messageId, UUID aggregateId,
                                                                   int partition, long offset) {
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                Topics.ACCOUNT_COMMANDS + Topics.DLT_SUFFIX, partition, offset,
                aggregateId.toString(),
                """
                {"messageId":"%s","eventType":"ReserveFunds","aggregateId":"%s",\
                "occurredAt":"2026-01-01T00:00:00Z","payload":{"transferId":"%s"}}"""
                        .formatted(messageId, aggregateId, aggregateId));

        stampOrigin(record, Topics.ACCOUNT_COMMANDS, partition, offset);
        if (messageId != null) {
            record.headers().add(EventEnvelope.MESSAGE_ID_HEADER, utf8(messageId.toString()));
        }
        record.headers().add(EventEnvelope.EVENT_TYPE_HEADER, utf8("ReserveFunds"));
        return record;
    }

    private static void stampOrigin(ConsumerRecord<String, String> record, String topic,
                                    int partition, long offset) {
        // Spring writes these as BINARY big-endian integers, not as decimal strings. Building
        // them the other way in a fixture is how you end up with a test that passes against a
        // recorder which only parses text - and a partition number of 825373492 in production.
        record.headers()
                .add(KafkaHeaders.DLT_ORIGINAL_TOPIC, utf8(topic))
                .add(KafkaHeaders.DLT_ORIGINAL_PARTITION,
                        ByteBuffer.allocate(Integer.BYTES).putInt(partition).array())
                .add(KafkaHeaders.DLT_ORIGINAL_OFFSET,
                        ByteBuffer.allocate(Long.BYTES).putLong(offset).array())
                .add(KafkaHeaders.DLT_EXCEPTION_FQCN, utf8("java.lang.IllegalArgumentException"))
                .add(KafkaHeaders.DLT_EXCEPTION_MESSAGE, utf8("Invalid UUID string: nope"));
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private Map<String, Object> onlyRow() {
        return jdbc.queryForMap("SELECT * FROM dead_letters");
    }

    private int rowCount() {
        return jdbc.queryForObject("SELECT count(*) FROM dead_letters", Integer.class);
    }
}
