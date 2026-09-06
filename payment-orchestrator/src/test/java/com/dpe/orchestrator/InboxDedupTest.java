package com.dpe.orchestrator;

import static org.assertj.core.api.Assertions.assertThat;

import com.dpe.events.EventEnvelope;
import com.dpe.events.FundsTransferred;
import com.dpe.events.Topics;
import com.dpe.orchestrator.consumer.AccountEventHandler;
import com.dpe.orchestrator.support.AbstractPostgresIT;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * The specification for {@code AccountEventHandler.handle} - the idempotency gate.
 *
 * <p>No broker: the gate is a database property, and driving it through Kafka would only add
 * timing noise to assertions about a transaction. {@code AccountEventConsumerTest} covers the
 * delivery path.
 *
 * <p>{@link #aSecondMessageIdForTheSameTransferIsNotADuplicate()} is the test that keeps the
 * others honest. It proves the projection write is genuinely not idempotent, so when the other
 * tests find {@code apply_count == 1} that is the inbox's doing and not the primary key
 * quietly absorbing a duplicate on its own.
 */
class InboxDedupTest extends AbstractPostgresIT {

    @Autowired
    AccountEventHandler handler;

    @Test
    @DisplayName("a first delivery is applied and recorded in the inbox")
    void firstDeliveryIsApplied() {
        UUID messageId = UUID.randomUUID();
        UUID transferId = UUID.randomUUID();

        assertThat(handler.handle(messageId, Topics.ACCOUNT_EVENTS, envelope(messageId, transferId)))
                .as("a message never seen before must do the work")
                .isTrue();

        assertThat(count("SELECT COUNT(*) FROM inbox WHERE message_id = ?", messageId))
                .as("the inbox row is the record that this message has been dealt with")
                .isEqualTo(1);

        Map<String, Object> projection = jdbc.queryForMap(
                "SELECT * FROM transfer_projection WHERE transfer_id = ?", transferId);
        assertThat(projection.get("amount_minor")).isEqualTo(30_000L);
        assertThat(projection.get("status")).isEqualTo("COMPLETED");
        assertThat(projection.get("apply_count")).isEqualTo(1);
    }

    @Test
    @DisplayName("a redelivery of the same message id changes nothing")
    void duplicateDeliveryIsANoOp() {
        UUID messageId = UUID.randomUUID();
        UUID transferId = UUID.randomUUID();
        EventEnvelope<FundsTransferred> envelope = envelope(messageId, transferId);

        assertThat(handler.handle(messageId, Topics.ACCOUNT_EVENTS, envelope)).isTrue();
        assertThat(handler.handle(messageId, Topics.ACCOUNT_EVENTS, envelope))
                .as("the second delivery must report that it did nothing - and must not throw. "
                        + "A duplicate is a normal event in an at-least-once system, not an error")
                .isFalse();

        assertThat(applyCount(transferId))
                .as("apply_count of 2 means the business write ran twice: the gate is not working")
                .isEqualTo(1);
        assertThat(count("SELECT COUNT(*) FROM inbox", null)).isEqualTo(1);
    }

    @Test
    @DisplayName("a different message id for the same transfer is NOT a duplicate")
    void aSecondMessageIdForTheSameTransferIsNotADuplicate() {
        UUID transferId = UUID.randomUUID();
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();

        assertThat(handler.handle(first, Topics.ACCOUNT_EVENTS, envelope(first, transferId))).isTrue();
        assertThat(handler.handle(second, Topics.ACCOUNT_EVENTS, envelope(second, transferId)))
                .as("dedup is per MESSAGE, not per aggregate - one transfer legitimately "
                        + "produces several messages, and dropping them would lose real work")
                .isTrue();

        assertThat(applyCount(transferId))
                .as("both messages applied, which is what makes apply_count == 1 in the other "
                        + "tests evidence of dedup rather than of the primary key")
                .isEqualTo(2);
    }

    @Test
    @DisplayName("two threads delivering the same message concurrently apply it once")
    void concurrentDuplicatesApplyOnce() throws Exception {
        UUID messageId = UUID.randomUUID();
        UUID transferId = UUID.randomUUID();
        EventEnvelope<FundsTransferred> envelope = envelope(messageId, transferId);

        // Both threads pass any "have I seen this?" check that is not the insert itself. The
        // second one blocks on the first's uncommitted primary key row and then finds it there.
        // Nothing in Java makes this safe; the row lock does.
        List<Boolean> results = inParallel(2,
                () -> handler.handle(messageId, Topics.ACCOUNT_EVENTS, envelope));

        assertThat(results)
                .as("exactly one of the two deliveries should report that it did the work")
                .containsExactlyInAnyOrder(true, false);
        assertThat(applyCount(transferId)).isEqualTo(1);
    }

    private EventEnvelope<FundsTransferred> envelope(UUID messageId, UUID transferId) {
        return new EventEnvelope<>(
                messageId,
                FundsTransferred.TYPE,
                transferId,
                Instant.parse("2026-01-01T00:00:00Z"),
                new FundsTransferred(transferId, UUID.randomUUID(), UUID.randomUUID(),
                        30_000L, "INR"));
    }

    private int applyCount(UUID transferId) {
        Integer count = jdbc.queryForObject(
                "SELECT apply_count FROM transfer_projection WHERE transfer_id = ?",
                Integer.class, transferId);
        return count == null ? 0 : count;
    }

    private int count(String sql, UUID arg) {
        Integer count = arg == null
                ? jdbc.queryForObject(sql, Integer.class)
                : jdbc.queryForObject(sql, Integer.class, arg);
        return count == null ? 0 : count;
    }

    private static <T> List<T> inParallel(int threads, Callable<T> task) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startingGun = new CountDownLatch(1);
        List<Future<T>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    startingGun.await();
                    return task.call();
                }));
            }
            startingGun.countDown();
            List<T> results = new ArrayList<>();
            for (Future<T> future : futures) {
                results.add(future.get(30, TimeUnit.SECONDS));
            }
            return results;
        } finally {
            pool.shutdownNow();
        }
    }
}
