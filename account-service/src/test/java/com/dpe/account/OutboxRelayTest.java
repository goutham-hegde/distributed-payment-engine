package com.dpe.account;

import static org.assertj.core.api.Assertions.assertThat;

import com.dpe.messaging.outbox.OutboxRelay;
import com.dpe.account.support.AbstractKafkaIT;
import com.dpe.account.support.Concurrently;
import com.dpe.events.EventEnvelope;
import com.dpe.events.Topics;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * The specification for {@code OutboxRelay.drainBatch}.
 *
 * <p>Every test fails until that method is written, and each fails for its own reason:
 *
 * <ul>
 *   <li>{@link #publishesAndMarksTheRow()} - the loop exists and the ordering is publish-then-mark
 *   <li>{@link #keyIsTheAggregateIdNotTheMessageId()} - the partition key is right, which is the
 *       whole ordering guarantee
 *   <li>{@link #carriesTheMessageIdHeader()} - the consumer has something to dedupe on
 *   <li>{@link #neverRepublishesAPublishedRow()} - the {@code published_at} filter is honoured
 *   <li>{@link #concurrentRelaysNeverPublishTheSameMessageTwice()} - {@code FOR UPDATE SKIP
 *       LOCKED} is doing its job, and the transaction really is a transaction
 * </ul>
 *
 * <p>Rows are seeded straight into the table rather than through {@code TransferService}, so the
 * relay can be developed and tested before the producer side is wired up - and so a bug in the
 * producer cannot make these tests lie.
 */
class OutboxRelayTest extends AbstractKafkaIT {

    private static final String PAYLOAD = """
            {"messageId":"%s","eventType":"FundsTransferred","aggregateId":"%s",
             "occurredAt":"2026-01-01T00:00:00Z","payload":{"transferId":"%s",
             "fromAccountId":"11111111-1111-1111-1111-111111111111",
             "toAccountId":"22222222-2222-2222-2222-222222222222",
             "amountMinor":30000,"currency":"INR"}}""";

    @Autowired
    OutboxRelay relay;

    @Test
    @DisplayName("draining an empty outbox publishes nothing and costs one query")
    void emptyOutboxIsANoOp() {
        assertThat(relay.drainBatch()).isZero();
    }

    @Test
    @DisplayName("a claimed message reaches Kafka and only then is marked published")
    void publishesAndMarksTheRow() {
        UUID transferId = UUID.randomUUID();
        UUID messageId = seed(transferId);

        assertThat(relay.drainBatch()).isEqualTo(1);

        Map<String, Object> row = outboxRow(messageId);
        assertThat(row.get("published_at"))
                .as("published_at must be set once the broker has acknowledged the send")
                .isNotNull();

        List<ConsumerRecord<String, String>> records = publishedNow();
        assertThat(records).hasSize(1);
        assertThat(records.get(0).value()).contains(transferId.toString());
    }

    @Test
    @DisplayName("the Kafka key is the aggregate id, so one transfer stays on one partition")
    void keyIsTheAggregateIdNotTheMessageId() {
        UUID transferId = UUID.randomUUID();
        UUID messageId = seed(transferId);

        relay.drainBatch();

        ConsumerRecord<String, String> record = publishedNow().get(0);
        assertThat(record.key())
                .as("keying by message id would scatter one transfer's messages across "
                        + "partitions and lose the only ordering guarantee this design offers")
                .isEqualTo(transferId.toString())
                .isNotEqualTo(messageId.toString());
    }

    @Test
    @DisplayName("every record carries the outbox row id as its message-id header")
    void carriesTheMessageIdHeader() {
        UUID transferId = UUID.randomUUID();
        UUID messageId = seed(transferId);

        relay.drainBatch();

        ConsumerRecord<String, String> record = publishedNow().get(0);
        assertThat(headerOf(record, EventEnvelope.MESSAGE_ID_HEADER))
                .as("without this the consumer has no stable key to dedupe on")
                .isEqualTo(messageId.toString());
        assertThat(headerOf(record, EventEnvelope.EVENT_TYPE_HEADER))
                .isEqualTo("FundsTransferred");
    }

    @Test
    @DisplayName("a published row is never picked up again")
    void neverRepublishesAPublishedRow() {
        seed(UUID.randomUUID());

        assertThat(relay.drainBatch()).isEqualTo(1);
        assertThat(relay.drainBatch())
                .as("the claim query filters on published_at IS NULL; a second drain has "
                        + "nothing left to do")
                .isZero();

        assertThat(publishedNow())
                .as("re-publishing on every poll would flood the topic with duplicates that "
                        + "the inbox would have to absorb forever")
                .hasSize(1);
    }

    @Test
    @DisplayName("several relays draining at once publish each message exactly once")
    void concurrentRelaysNeverPublishTheSameMessageTwice() {
        int messages = 12;
        for (int i = 0; i < messages; i++) {
            seed(UUID.randomUUID());
        }

        // Four relay instances, released together. Each drainBatch() runs in its own
        // transaction, so the FOR UPDATE SKIP LOCKED claims must partition the backlog between
        // them. Without SKIP LOCKED three of these block; without FOR UPDATE all four publish
        // everything and the assertion below finds 48 records instead of 12.
        List<Callable<Integer>> relays = List.<Callable<Integer>>of(
                relay::drainBatch, relay::drainBatch, relay::drainBatch, relay::drainBatch);
        List<Concurrently.Outcome<Integer>> outcomes = Concurrently.runAll(relays);

        assertThat(outcomes).allMatch(Concurrently.Outcome::succeeded);
        int totalPublished = outcomes.stream().mapToInt(o -> o.value()).sum();
        assertThat(totalPublished)
                .as("the four relays should have divided the backlog, not duplicated it")
                .isEqualTo(messages);

        Long unpublished = jdbc.queryForObject(
                "SELECT COUNT(*) FROM outbox WHERE published_at IS NULL", Long.class);
        assertThat(unpublished).isZero();

        assertThat(publishedNow())
                .as("exactly one Kafka record per outbox row")
                .hasSize(messages);
    }

    private UUID seed(UUID transferId) {
        UUID messageId = UUID.randomUUID();
        String payload = PAYLOAD.formatted(messageId, transferId, transferId).replace("\n", "");
        jdbc.update("""
                INSERT INTO outbox (id, aggregate_type, aggregate_id, topic, event_type, payload)
                VALUES (?, 'Transfer', ?, ?, 'FundsTransferred', ?::jsonb)
                """, messageId, transferId, Topics.ACCOUNT_EVENTS, payload);
        return messageId;
    }
}
