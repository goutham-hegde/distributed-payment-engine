package com.dpe.account;

import static org.assertj.core.api.Assertions.assertThat;

import com.dpe.account.support.AbstractKafkaIT;
import com.dpe.events.EventEnvelope;
import com.dpe.events.Topics;
import com.dpe.messaging.deadletter.DeadLetterReplayService;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * The specification for {@code DeadLetterReplayService}.
 *
 * <p>Every test fails until it is written, and each fails for its own reason:
 *
 * <ul>
 *   <li>{@link #republishesToTheOriginalTopic()} - the message goes back where it failed, not to
 *       the dead letter topic it was read from
 *   <li>{@link #keepsTheOriginalKeyAndMessageId()} - the two headers that decide partition and
 *       dedup survive, which is the whole reason replay is safe
 *   <li>{@link #marksReplayedOnlyAfterTheBrokerAcknowledges()} - publish-then-mark
 *   <li>{@link #refusesToReplayTwice()} - an operator refreshing a page cannot double-publish
 *   <li>{@link #bulkReplayDrainsThePendingQueue()} - the batch path claims and marks correctly
 * </ul>
 *
 * <p>Rows are seeded straight into {@code dead_letters} rather than produced by failing a real
 * message, so the replay path can be developed independently - and so a bug in the recorder
 * cannot make these tests lie.
 */
@SpringBootTest(properties = {
        // Listeners OFF. A replay publishes a real ReserveFunds onto the real command topic, and
        // this service's own consumer would pick it up, fail on an account that does not exist,
        // and dead-letter it - adding rows to the table these assertions are counting. What is
        // under test is the publish, not what happens to the message afterwards.
        "spring.kafka.listener.auto-startup=false",
        // Repeated from AbstractPostgresIT, and it MUST be: @SpringBootTest properties do not
        // merge down a class hierarchy - Spring takes the first declaration it finds and ignores
        // the rest. Omit this line and the relay timer comes back on and races the assertions.
        "dpe.outbox.scheduled=false"
})
class DeadLetterReplayTest extends AbstractKafkaIT {

    @Autowired
    DeadLetterReplayService replays;

    @Test
    @DisplayName("a replay is published to the topic that failed, not to the dead letter topic")
    void republishesToTheOriginalTopic() {
        UUID id = seed(UUID.randomUUID(), UUID.randomUUID());

        assertThat(replays.replay(id)).isTrue();

        List<ConsumerRecord<String, String>> onCommands = publishedNow(Topics.ACCOUNT_COMMANDS);
        assertThat(onCommands)
                .as("the message has to arrive somewhere a consumer is listening")
                .hasSize(1);

        assertThat(publishedNow(Topics.ACCOUNT_COMMANDS + Topics.DLT_SUFFIX))
                .as("replaying onto the dead letter topic would loop forever")
                .isEmpty();
    }

    @Test
    @DisplayName("the original key and message id survive the round trip")
    void keepsTheOriginalKeyAndMessageId() {
        UUID messageId = UUID.randomUUID();
        UUID transferId = UUID.randomUUID();
        replays.replay(seed(messageId, transferId));

        ConsumerRecord<String, String> record = publishedNow(Topics.ACCOUNT_COMMANDS).get(0);

        assertThat(record.key())
                .as("the key is the partition assignment; a new one can reorder a saga")
                .isEqualTo(transferId.toString());
        assertThat(headerOf(record, EventEnvelope.MESSAGE_ID_HEADER))
                .as("a fresh message id turns a safe replay into a guaranteed double-spend")
                .isEqualTo(messageId.toString());
        assertThat(headerOf(record, EventEnvelope.EVENT_TYPE_HEADER)).isEqualTo("ReserveFunds");
    }

    @Test
    @DisplayName("replayed_at is set only after the send, and the count increments")
    void marksReplayedOnlyAfterTheBrokerAcknowledges() {
        UUID id = seed(UUID.randomUUID(), UUID.randomUUID());

        replays.replay(id);

        Map<String, Object> row = row(id);
        assertThat(row.get("replayed_at"))
                .as("mark-then-send loses the message if the send fails, and this is the last copy")
                .isNotNull();
        assertThat(row.get("replay_count")).isEqualTo(1);
    }

    @Test
    @DisplayName("an already-replayed letter is refused rather than published again")
    void refusesToReplayTwice() {
        UUID id = seed(UUID.randomUUID(), UUID.randomUUID());

        assertThat(replays.replay(id)).isTrue();
        assertThat(replays.replay(id))
                .as("an operator refreshing a page must not be able to send a payment twice")
                .isFalse();

        assertThat(publishedNow(Topics.ACCOUNT_COMMANDS)).hasSize(1);
    }

    @Test
    @DisplayName("an unknown id is a false, not an exception")
    void unknownIdIsFalse() {
        assertThat(replays.replay(UUID.randomUUID())).isFalse();
    }

    @Test
    @DisplayName("a letter with no message id is refused, not replayed and not repaired")
    void refusesALetterWithNothingToDedupeOn() {
        // These rows exist by design - a record with a missing or unparseable message id is
        // dropped by AccountCommandConsumer before any handler sees it, which is precisely how it
        // ends up here. It must be readable and must never be replayed: with no message id the
        // consumer has nothing to dedupe on, and minting a replacement would make the message look
        // new to every inbox in the system, which is a double-spend dressed up as a fix.
        UUID id = seed(null, UUID.randomUUID());

        assertThat(replays.replay(id)).isFalse();
        assertThat(publishedNow(Topics.ACCOUNT_COMMANDS)).isEmpty();
        assertThat(row(id).get("replayed_at"))
                .as("it stays pending and stays visible - a human decides what to do with it")
                .isNull();
    }

    @Test
    @DisplayName("bulk replay skips the unreplayable and still drains the rest")
    void bulkReplaySkipsUnreplayableLetters() {
        seed(UUID.randomUUID(), UUID.randomUUID());
        seed(null, UUID.randomUUID());
        seed(UUID.randomUUID(), UUID.randomUUID());

        assertThat(replays.replayPending())
                .as("one bad row must not stop the batch, nor be counted as replayed")
                .isEqualTo(2);
        assertThat(publishedNow(Topics.ACCOUNT_COMMANDS)).hasSize(2);
        assertThat(pendingCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("bulk replay drains everything pending and marks each one")
    void bulkReplayDrainsThePendingQueue() {
        seed(UUID.randomUUID(), UUID.randomUUID());
        seed(UUID.randomUUID(), UUID.randomUUID());
        seed(UUID.randomUUID(), UUID.randomUUID());

        assertThat(replays.replayPending()).isEqualTo(3);
        assertThat(publishedNow(Topics.ACCOUNT_COMMANDS)).hasSize(3);
        assertThat(pendingCount()).isZero();

        assertThat(replays.replayPending())
                .as("nothing is pending any more, so a second call is a no-op")
                .isZero();
    }

    // ---- fixtures -----------------------------------------------------------------------

    /** A pending dead letter for a ReserveFunds command that failed on the command topic. */
    private UUID seed(UUID messageId, UUID transferId) {
        UUID id = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO dead_letters (
                    id, message_id, original_topic, original_partition, original_offset,
                    message_key, event_type, payload, exception_type, exception_message, attempts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                id, messageId, Topics.ACCOUNT_COMMANDS, 0, offset++, transferId.toString(),
                "ReserveFunds",
                """
                {"messageId":"%s","eventType":"ReserveFunds","aggregateId":"%s",\
                "occurredAt":"2026-01-01T00:00:00Z","payload":{"transferId":"%s"}}"""
                        .formatted(messageId, transferId, transferId),
                "org.springframework.dao.DataAccessResourceFailureException",
                "connection refused", 4);
        return id;
    }

    // The (topic, partition, offset) triple is unique, so seeded rows need distinct offsets.
    private long offset = 1;

    private Map<String, Object> row(UUID id) {
        return jdbc.queryForMap("SELECT * FROM dead_letters WHERE id = ?", id);
    }

    private int pendingCount() {
        return jdbc.queryForObject(
                "SELECT count(*) FROM dead_letters WHERE replayed_at IS NULL", Integer.class);
    }
}
