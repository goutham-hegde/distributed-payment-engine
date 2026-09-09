package com.dpe.messaging.deadletter;

import com.dpe.events.EventEnvelope;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Turns a record on a dead letter topic back into a row an operator can query.
 *
 * <p>The transactional boundary for the dead letter listener, mirroring
 * {@code AccountCommandHandler}: the write happens here, so the consumer can acknowledge only
 * after it has committed.
 *
 * <p>What it deliberately does <b>not</b> do is touch the inbox. That is worth stating loudly,
 * because it looks like an omission. A dead letter carries the ORIGINAL business message id, and
 * the inbox is keyed on the message id alone; writing one here would mark the business message as
 * consumed, and the eventual replay - the entire point of the table - would be discarded as a
 * duplicate by a handler that never actually ran. The dead letter listener therefore dedupes on
 * its own key, the source Kafka coordinates, which identify the FAILURE rather than the message.
 * Same rule as always: a dedup key must be unique across everything that shares the table, and
 * these two tables do not share one.
 */
@Component
public class DeadLetterRecorder {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterRecorder.class);

    private final DeadLetterRepository deadLetters;

    public DeadLetterRecorder(DeadLetterRepository deadLetters) {
        this.deadLetters = deadLetters;
    }

    /**
     * Records one dead letter.
     *
     * @return {@code true} if this failure was new, {@code false} if it had already been recorded
     */
    @Transactional
    public boolean record(ConsumerRecord<String, String> record) {

        // The headers DeadLetterPublishingRecoverer stamped on the way out. They are the reason
        // the source topic is knowable at all: by the time the record is on the dead letter
        // topic, record.topic() is the DEAD LETTER topic, and the thing a replay must send back
        // to is gone unless it was carried explicitly.
        String originalTopic = header(record, KafkaHeaders.DLT_ORIGINAL_TOPIC);
        Integer originalPartition = intHeader(record, KafkaHeaders.DLT_ORIGINAL_PARTITION);
        Long originalOffset = longHeader(record, KafkaHeaders.DLT_ORIGINAL_OFFSET);

        if (originalTopic == null || originalPartition == null || originalOffset == null) {
            // Only reachable if something other than our recoverer published here. Recorded
            // against the dead letter topic's own coordinates rather than dropped: an
            // unexplained record on this topic is exactly the kind of thing worth being able to
            // look at later.
            log.warn("record on {} has no DLT origin headers; recording against its own "
                    + "coordinates", record.topic());
            originalTopic = record.topic();
            originalPartition = record.partition();
            originalOffset = record.offset();
        }

        String rawMessageId = header(record, EventEnvelope.MESSAGE_ID_HEADER);
        UUID messageId = parseUuid(rawMessageId);

        int inserted = deadLetters.insertIfAbsent(
                UUID.randomUUID(),
                messageId,
                originalTopic,
                originalPartition,
                originalOffset,
                record.key(),
                header(record, EventEnvelope.EVENT_TYPE_HEADER),
                record.value(),
                failureType(record),
                header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE),
                deliveryAttempts(record));

        if (inserted == 0) {
            log.debug("dead letter for {}-{}@{} already recorded",
                    originalTopic, originalPartition, originalOffset);
            return false;
        }

        log.warn("recorded dead letter: message {} from {}-{}@{} ({})", messageId, originalTopic,
                originalPartition, originalOffset, failureType(record));
        return true;
    }

    /**
     * The exception class worth putting in the {@code exception_type} column.
     *
     * <p>Prefers the CAUSE over the exception the recoverer caught, and the difference is the
     * difference between a useful column and a useless one. Spring stamps
     * {@code DLT_EXCEPTION_FQCN} with what it caught, which for anything thrown out of a listener
     * is {@code ListenerExecutionFailedException} - so a column meant for "group the failures by
     * kind" would read identically on every row in the table, and the one question it exists to
     * answer could not be asked. {@code DLT_EXCEPTION_CAUSE_FQCN} carries what actually went
     * wrong.
     *
     * <p>Falls back to the outer class when there is no cause, which is the case for a failure
     * raised by the container itself rather than by user code.
     */
    private static String failureType(ConsumerRecord<String, String> record) {
        String cause = header(record, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN);
        return cause != null ? cause : header(record, KafkaHeaders.DLT_EXCEPTION_FQCN);
    }

    /**
     * How many times the container delivered the record before giving up.
     *
     * <p>Present only because {@code KafkaErrorHandlingConfig} switches
     * {@code deliveryAttemptHeader} on. Zero when it is absent, which is honest - an invented
     * number here would be indistinguishable from a measured one in the table.
     */
    private static int deliveryAttempts(ConsumerRecord<String, String> record) {
        Integer attempts = intHeader(record, KafkaHeaders.DELIVERY_ATTEMPT);
        return attempts == null ? 0 : attempts;
    }

    private static UUID parseUuid(String value) {
        if (value == null) {
            return null;
        }
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            // A malformed message id is one of the reasons a record ends up here in the first
            // place, so this path is expected rather than exceptional. The raw value survives in
            // the payload; the column simply cannot hold it.
            log.warn("dead letter carries an unparseable message id '{}'", value);
            return null;
        }
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    /**
     * Reads a header the recoverer wrote as a 4-byte big-endian int, falling back to parsing it
     * as text.
     *
     * <p>The two encodings are a real hazard. Spring Kafka writes the DLT origin headers as
     * binary integers; a header set by hand in a test, or by a different client library, is
     * usually the decimal string. Reading only one way works right up until the day it does not,
     * and the failure is a partition number of 825373492 - which is the ASCII bytes of "1234"
     * read as an int - rather than an exception.
     */
    private static Integer intHeader(ConsumerRecord<String, String> record, String name) {
        Header h = record.headers().lastHeader(name);
        if (h == null || h.value() == null) {
            return null;
        }
        byte[] bytes = h.value();
        if (bytes.length == Integer.BYTES) {
            return java.nio.ByteBuffer.wrap(bytes).getInt();
        }
        try {
            return Integer.valueOf(new String(bytes, StandardCharsets.UTF_8).trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Long longHeader(ConsumerRecord<String, String> record, String name) {
        Header h = record.headers().lastHeader(name);
        if (h == null || h.value() == null) {
            return null;
        }
        byte[] bytes = h.value();
        if (bytes.length == Long.BYTES) {
            return java.nio.ByteBuffer.wrap(bytes).getLong();
        }
        if (bytes.length == Integer.BYTES) {
            return (long) java.nio.ByteBuffer.wrap(bytes).getInt();
        }
        try {
            return Long.valueOf(new String(bytes, StandardCharsets.UTF_8).trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
