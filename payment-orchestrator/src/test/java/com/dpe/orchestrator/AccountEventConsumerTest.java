package com.dpe.orchestrator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.dpe.events.FundsTransferred;
import com.dpe.events.Topics;
import com.dpe.orchestrator.support.AbstractKafkaIT;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The delivery path, end to end over a real broker: a record on the topic becomes a row in the
 * read model, exactly once, no matter how many times it is delivered.
 *
 * <p>Awaitility rather than a sleep. The listener runs on its own thread and the only honest
 * statement a test can make about it is "this becomes true within N seconds"; a fixed sleep is
 * either flaky or slow, and usually manages both.
 */
class AccountEventConsumerTest extends AbstractKafkaIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    @Test
    @DisplayName("a published event becomes a projection row")
    void consumesAnEvent() {
        UUID messageId = UUID.randomUUID();
        UUID transferId = UUID.randomUUID();
        UUID from = UUID.randomUUID();
        UUID to = UUID.randomUUID();

        publish(Topics.ACCOUNT_EVENTS, messageId, transferId, FundsTransferred.TYPE,
                fundsTransferredJson(messageId, transferId, from, to, 30_000L));

        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(projectionCount(transferId)).isEqualTo(1));

        assertThat(jdbc.queryForObject(
                "SELECT amount_minor FROM transfer_projection WHERE transfer_id = ?",
                Long.class, transferId)).isEqualTo(30_000L);
        assertThat(jdbc.queryForObject(
                "SELECT COUNT(*) FROM inbox WHERE message_id = ?", Integer.class, messageId))
                .as("the inbox row and the projection row are written by the same transaction")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("the same message delivered twice is applied once")
    void duplicateDeliveryIsAbsorbed() {
        UUID messageId = UUID.randomUUID();
        UUID transferId = UUID.randomUUID();
        String json = fundsTransferredJson(messageId, transferId, UUID.randomUUID(),
                UUID.randomUUID(), 42_000L);

        // Exactly what a relay does when it crashes between the broker's ack and the UPDATE
        // that marks the row published: the same message id, sent again.
        publish(Topics.ACCOUNT_EVENTS, messageId, transferId, FundsTransferred.TYPE, json);
        publish(Topics.ACCOUNT_EVENTS, messageId, transferId, FundsTransferred.TYPE, json);

        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(projectionCount(transferId)).isEqualTo(1));

        // Give the second delivery time to be wrongly applied, so this is not merely a race the
        // test happened to win.
        await().during(Duration.ofSeconds(3)).atMost(TIMEOUT).untilAsserted(() ->
                assertThat(applyCount(transferId))
                        .as("at-least-once delivery plus an idempotent consumer is "
                                + "effectively-once PROCESSING; apply_count of 2 means the "
                                + "inbox did not hold")
                        .isEqualTo(1));
    }

    @Test
    @DisplayName("an unrecognised event type is acknowledged rather than blocking the partition")
    void unknownEventTypeDoesNotWedgeTheConsumer() {
        UUID ignoredId = UUID.randomUUID();
        UUID ignoredTransfer = UUID.randomUUID();
        publish(Topics.ACCOUNT_EVENTS, ignoredId, ignoredTransfer, "SomethingFromTheFuture",
                "{\"messageId\":\"" + ignoredId + "\"}");

        UUID messageId = UUID.randomUUID();
        UUID transferId = UUID.randomUUID();
        publish(Topics.ACCOUNT_EVENTS, messageId, transferId, FundsTransferred.TYPE,
                fundsTransferredJson(messageId, transferId, UUID.randomUUID(),
                        UUID.randomUUID(), 7_000L));

        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(projectionCount(transferId))
                        .as("the message behind the unknown one must still be processed - a "
                                + "consumer that stalls on a type it does not recognise dies on "
                                + "the first deploy that introduces one")
                        .isEqualTo(1));

        assertThat(projectionCount(ignoredTransfer)).isZero();
    }

    private int projectionCount(UUID transferId) {
        Integer count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM transfer_projection WHERE transfer_id = ?",
                Integer.class, transferId);
        return count == null ? 0 : count;
    }

    private int applyCount(UUID transferId) {
        Integer count = jdbc.queryForObject(
                "SELECT apply_count FROM transfer_projection WHERE transfer_id = ?",
                Integer.class, transferId);
        return count == null ? 0 : count;
    }
}
