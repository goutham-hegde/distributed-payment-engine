package com.dpe.account;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.dpe.account.support.AbstractKafkaIT;
import com.dpe.events.Topics;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The end-to-end proof that a message which cannot be processed leaves its partition and lands
 * somewhere an operator can find it.
 *
 * <p>This is the test the milestone exists for, and it exercises the whole chain at once: the
 * listener throws, {@code DefaultErrorHandler} consults {@code RetryClassifier}, the recoverer
 * publishes to {@code dpe.account.commands.v1.dlt}, {@code DeadLetterConsumer} reads it back, and
 * {@code DeadLetterRecorder} writes the row.
 *
 * <p>Listeners are left ON here - unlike {@code DeadLetterReplayTest}, the point is what the
 * container does.
 *
 * <p>Note what would happen without any of it. Spring Boot's default error handler retries ten
 * times with no delay and then <b>logs the exception and commits the offset</b>. Every assertion
 * below would fail with an empty table, and in production the only evidence that a payment
 * command had been discarded would be a stack trace in a log.
 */
class PoisonMessageTest extends AbstractKafkaIT {

    @Test
    @DisplayName("a payload that cannot be parsed ends up in dead_letters, not in a log line")
    void unparseablePayloadIsDeadLettered() {
        UUID messageId = UUID.randomUUID();
        UUID transferId = UUID.randomUUID();

        // Valid headers, so the consumer gets far enough to try to deserialize; a truncated body,
        // so the deserialization fails. This is the shape of a real incident: a producer deployed
        // with a bug, or a message written by a version that no longer agrees with this one.
        publishRaw(Topics.ACCOUNT_COMMANDS, messageId, transferId, "ReserveFunds",
                "{\"messageId\":\"" + messageId + "\",\"eventType\":\"ReserveFunds\",");

        await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> assertThat(deadLettersFor(transferId)).isEqualTo(1));

        Map<String, Object> row = jdbc.queryForMap(
                "SELECT * FROM dead_letters WHERE message_key = ?", transferId.toString());
        assertThat(row.get("original_topic")).isEqualTo(Topics.ACCOUNT_COMMANDS);
        assertThat(row.get("message_id")).hasToString(messageId.toString());
        assertThat(row.get("replayed_at")).isNull();
    }

    @Test
    @DisplayName("the partition keeps moving: a good message behind a poison one is still processed")
    void doesNotBlockThePartition() {
        // The reason a DLQ is worth building at all. Both messages are keyed by the SAME
        // aggregate id, so Kafka guarantees they land on the same partition and are delivered in
        // order - which means the second one is behind the first, and it can only be consumed if
        // the first has genuinely been got out of the way.
        UUID transferId = UUID.randomUUID();
        UUID poisonId = UUID.randomUUID();
        UUID goodId = UUID.randomUUID();

        publishRaw(Topics.ACCOUNT_COMMANDS, poisonId, transferId, "ReserveFunds", "{broken");
        publishRaw(Topics.ACCOUNT_COMMANDS, goodId, transferId, "SomeFutureCommand", "{}");

        // The second message is a type this version does not know, which AccountCommandConsumer
        // records in the inbox and skips - a cheap, observable "I got past the first one".
        await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    assertThat(deadLettersFor(transferId)).isEqualTo(1);
                    assertThat(inboxContains(goodId))
                            .as("the message behind the poison one must still be consumed")
                            .isTrue();
                });
    }

    // ---- fixtures -----------------------------------------------------------------------

    /**
     * Dead letters for THIS test's aggregate, not the whole table.
     *
     * <p>Scoped deliberately. A Kafka topic is a log and the consumer group's offsets outlive a
     * single test class, so a message another class published and never consumed is delivered
     * here, fails, and lands in this table - a global {@code count(*)} then measures the suite's
     * history rather than this test's behaviour. Filtering by the aggregate id, which is unique
     * per test, is what makes the assertion mean what it says.
     */
    private int deadLettersFor(UUID aggregateId) {
        return jdbc.queryForObject("SELECT count(*) FROM dead_letters WHERE message_key = ?",
                Integer.class, aggregateId.toString());
    }

    private boolean inboxContains(UUID messageId) {
        return jdbc.queryForObject("SELECT count(*) FROM inbox WHERE message_id = ?",
                Integer.class, messageId) > 0;
    }
}
