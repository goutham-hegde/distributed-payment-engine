package com.dpe.orchestrator;

import static org.assertj.core.api.Assertions.assertThat;

import com.dpe.events.Topics;
import com.dpe.messaging.inbox.InboxGate;
import com.dpe.messaging.metrics.MessagingMetrics;
import com.dpe.orchestrator.support.AbstractPostgresIT;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * M6. The messaging spine's gauges and the inbox duplicate counter.
 *
 * <p>The gauges are the ones most worth a test, because their failure mode is a flat line that
 * looks like good news. A backlog gauge stuck at zero and a system with an empty outbox are the
 * same picture.
 */
class MessagingMetricsTest extends AbstractPostgresIT {

    @Autowired
    MessagingMetrics metrics;

    @Autowired
    InboxGate inbox;

    @Autowired
    MeterRegistry registry;

    /**
     * Each claim runs in its OWN transaction, which is both realistic and necessary.
     *
     * <p>Necessary because {@code insertIfAbsent} is a Spring Data {@code @Modifying} query and
     * throws {@code TransactionRequiredException} without one - the same trap the sweeper hit at
     * M4, where "no transaction" is not a third option alongside one-per-batch and one-around-
     * the-loop. Realistic because two deliveries of a message are genuinely two transactions;
     * running both in one would let the second see the first's uncommitted row and prove
     * something weaker than what production does.
     */
    @Autowired
    TransactionTemplate tx;

    @Test
    @DisplayName("outbox backlog counts unpublished rows and drops when they are published")
    void backlogTracksUnpublishedRows() {
        metrics.refresh();
        assertThat(gauge("dpe.outbox.backlog"))
                .as("AbstractPostgresIT truncates the outbox before each test")
                .isEqualTo(0.0);

        insertUnpublished(3);
        metrics.refresh();
        assertThat(gauge("dpe.outbox.backlog")).isEqualTo(3.0);

        jdbc.update("UPDATE outbox SET published_at = now() WHERE published_at IS NULL");
        metrics.refresh();
        assertThat(gauge("dpe.outbox.backlog"))
                .as("published rows stay in the table as an audit trail but must leave the "
                        + "BACKLOG - the partial index is what makes that cheap")
                .isEqualTo(0.0);
    }

    @Test
    @DisplayName("outbox age is zero when drained and non-zero while a row waits")
    void ageDistinguishesDrainedFromWaiting() {
        metrics.refresh();
        assertThat(gauge("dpe.outbox.age"))
                .as("MIN(created_at) over an empty set is NULL, and NULL must read as 0 rather "
                        + "than leaving a gap in the series")
                .isEqualTo(0.0);

        // Backdated, because the value under test is an age and a row created now is zero
        // seconds old - which would pass against a gauge that always returned zero.
        jdbc.update("""
                INSERT INTO outbox (id, aggregate_type, aggregate_id, topic, event_type,
                                    payload, created_at)
                VALUES (?, 'Transfer', ?, ?, 'ReserveFunds', '{}'::jsonb, now() - interval '90 seconds')
                """, UUID.randomUUID(), UUID.randomUUID(), Topics.ACCOUNT_COMMANDS);

        metrics.refresh();
        assertThat(gauge("dpe.outbox.age"))
                .as("age, not depth, is what separates a busy relay from a dead one")
                .isGreaterThanOrEqualTo(89.0);
    }

    @Test
    @DisplayName("dead letter depth counts unreplayed rows only")
    void dlqDepthCountsPendingOnly() {
        metrics.refresh();
        assertThat(gauge("dpe.dlq.depth")).isEqualTo(0.0);

        insertDeadLetter(null);
        insertDeadLetter("now()");
        metrics.refresh();

        assertThat(gauge("dpe.dlq.depth"))
                .as("a replayed letter is finished work and must leave the queue depth, or the "
                        + "number never returns to zero and stops being an alert")
                .isEqualTo(1.0);
    }

    @Test
    @DisplayName("a second delivery of the same message id is counted as a duplicate, not accepted")
    void duplicateDeliveryIsCounted() {
        UUID messageId = UUID.randomUUID();
        double acceptedBefore = counter("dpe.inbox.accepted");
        double duplicatesBefore = counter("dpe.inbox.duplicate");

        assertThat(claimInOwnTransaction(messageId)).isTrue();
        assertThat(claimInOwnTransaction(messageId))
                .as("the primary key decides this, not application logic")
                .isFalse();

        assertThat(counter("dpe.inbox.accepted")).isEqualTo(acceptedBefore + 1);
        assertThat(counter("dpe.inbox.duplicate"))
                .as("this counter rising is at-least-once delivery being absorbed correctly - it "
                        + "is not an error metric. Zero forever is the suspicious reading.")
                .isEqualTo(duplicatesBefore + 1);
    }

    // ------------------------------------------------------------------ helpers

    private void insertUnpublished(int count) {
        for (int i = 0; i < count; i++) {
            jdbc.update("""
                    INSERT INTO outbox (id, aggregate_type, aggregate_id, topic, event_type, payload)
                    VALUES (?, 'Transfer', ?, ?, 'ReserveFunds', '{}'::jsonb)
                    """, UUID.randomUUID(), UUID.randomUUID(), Topics.ACCOUNT_COMMANDS);
        }
    }

    private void insertDeadLetter(String replayedAt) {
        jdbc.update("""
                INSERT INTO dead_letters (id, message_id, message_key, original_topic,
                                          original_partition, original_offset, event_type,
                                          payload, exception_type, exception_message, replayed_at)
                VALUES (?, ?, ?, ?, ?, ?, 'ReserveFunds', '{}', 'java.lang.RuntimeException',
                        'boom', %s)
                """.formatted(replayedAt == null ? "NULL" : replayedAt),
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID().toString(),
                Topics.ACCOUNT_COMMANDS, 0, System.nanoTime());
    }

    private boolean claimInOwnTransaction(UUID messageId) {
        return Boolean.TRUE.equals(tx.execute(status ->
                inbox.claim(messageId, Topics.ACCOUNT_EVENTS, "FundsReserved")));
    }

    private double gauge(String name) {
        return registry.get(name).gauge().value();
    }

    private double counter(String name) {
        try {
            return registry.get(name).counters().stream()
                    .mapToDouble(io.micrometer.core.instrument.Counter::count).sum();
        } catch (MeterNotFoundException e) {
            return 0.0;
        }
    }
}
