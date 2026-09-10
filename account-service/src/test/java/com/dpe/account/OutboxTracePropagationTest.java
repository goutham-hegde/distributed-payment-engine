package com.dpe.account;

import static org.assertj.core.api.Assertions.assertThat;

import com.dpe.account.support.AbstractKafkaIT;
import com.dpe.events.Topics;
import com.dpe.messaging.outbox.OutboxRelay;
import com.dpe.messaging.outbox.OutboxWriter;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * M6 part 2: the specification for carrying a trace context across the outbox.
 *
 * <p>The problem in one sentence: automatic trace propagation assumes the outbound call happens on
 * the thread that is inside the span, and the transactional outbox exists precisely to make that
 * untrue. The write happens on the request thread inside the business transaction; the send happens
 * on {@code OutboxRelayScheduler}'s thread some hundreds of milliseconds later. Nothing links them
 * unless something is stored.
 *
 * <p>So the milestone splits in two, and so does this class:
 *
 * <ul>
 *   <li><b>The capture</b> - {@link #captureStoresTheProducingContextOnTheRow()} and
 *       {@link #noActiveSpanStoresNull()}. Written, and passing.
 *   <li><b>The restore</b> - {@link #relayRestoresTheStoredContextOntoTheRecord()},
 *       {@link #thePublishSpanIsAChildAndNotACopy()} and
 *       {@link #aRowWithNoStoredContextStillPublishes()}. <b>These fail until
 *       {@code OutboxRelay.drainBatch} puts the context back</b> - see the marked block in that
 *       method for the shape and the three ways to get it wrong.
 * </ul>
 *
 * <p>Rows are seeded straight into the table for the restore tests, exactly as
 * {@code OutboxRelayTest} does, so that a bug in the capture cannot make the restore look right.
 * The two halves are asserted separately on purpose: they fail on different days, for different
 * reasons, and a single end-to-end test would only ever say "the trace is broken somewhere".
 */
class OutboxTracePropagationTest extends AbstractKafkaIT {

    private static final String PAYLOAD = """
            {"messageId":"%s","eventType":"FundsTransferred","aggregateId":"%s",
             "occurredAt":"2026-01-01T00:00:00Z","payload":{"transferId":"%s",
             "fromAccountId":"11111111-1111-1111-1111-111111111111",
             "toAccountId":"22222222-2222-2222-2222-222222222222",
             "amountMinor":30000,"currency":"INR"}}""";

    /**
     * A traceparent that no span in this JVM could have produced. Fixed rather than generated so
     * that a failure prints a value that is obviously the seeded one, and so that a relay which
     * quietly substitutes its own context cannot accidentally match.
     */
    private static final String SEEDED_TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";
    private static final String SEEDED_SPAN_ID = "00f067aa0ba902b7";
    private static final String SEEDED_TRACEPARENT =
            "00-" + SEEDED_TRACE_ID + "-" + SEEDED_SPAN_ID + "-01";

    @Autowired
    OutboxRelay relay;

    @Autowired
    OutboxWriter writer;

    @Autowired
    Tracer tracer;

    @Autowired
    TransactionTemplate tx;

    // ---------------------------------------------------------------------------------------
    // The capture: OutboxWriter, on the producing thread.
    // ---------------------------------------------------------------------------------------

    @Test
    @DisplayName("a message written inside a span stores that span's traceparent on the row")
    void captureStoresTheProducingContextOnTheRow() {
        UUID transferId = UUID.randomUUID();
        Span producing = tracer.nextSpan().name("test-producer").start();

        UUID messageId;
        try (Tracer.SpanInScope ignored = tracer.withSpan(producing)) {
            messageId = tx.execute(status -> writer.append(
                    "Transfer", transferId, Topics.ACCOUNT_EVENTS, "FundsTransferred",
                    Map.of("transferId", transferId.toString())));
        } finally {
            producing.end();
        }

        Map<String, Object> row = outboxRow(messageId);
        assertThat((String) row.get("trace_parent"))
                .as("the context has to be captured on the thread that still has it - the relay "
                        + "runs later, on another thread, with no way to find this span")
                .isNotNull()
                .contains(producing.context().traceId());
    }

    @Test
    @DisplayName("a message written with no active span stores NULL, and that is not an error")
    void noActiveSpanStoresNull() {
        UUID transferId = UUID.randomUUID();

        UUID messageId = tx.execute(status -> writer.append(
                "Transfer", transferId, Topics.ACCOUNT_EVENTS, "FundsTransferred",
                Map.of("transferId", transferId.toString())));

        assertThat(outboxRow(messageId).get("trace_parent"))
                .as("the timeout sweeper and every scheduled producer write outside a trace; "
                        + "treating that as a failure would mean a warning per message forever")
                .isNull();
    }

    // ---------------------------------------------------------------------------------------
    // The restore: OutboxRelay, on the scheduler's thread. TO BE WRITTEN.
    // ---------------------------------------------------------------------------------------

    @Test
    @DisplayName("the relay puts the stored context back on the outgoing record")
    void relayRestoresTheStoredContextOntoTheRecord() {
        seed(UUID.randomUUID(), SEEDED_TRACEPARENT);

        assertThat(relay.drainBatch()).isEqualTo(1);

        String traceparent = headerOf(publishedNow().get(0), "traceparent");
        assertThat(traceparent)
                .as("without this header the consumer starts a brand new trace, and one payment "
                        + "appears in Jaeger as four unrelated ones with nothing to join them")
                .isNotNull();
        assertThat(traceIdOf(traceparent))
                .as("the trace id must be the one the producer committed on the row, not one the "
                        + "relay invented")
                .isEqualTo(SEEDED_TRACE_ID);
    }

    @Test
    @DisplayName("the relay's publish is a child span, not a verbatim header copy")
    void thePublishSpanIsAChildAndNotACopy() {
        seed(UUID.randomUUID(), SEEDED_TRACEPARENT);

        relay.drainBatch();

        String traceparent = headerOf(publishedNow().get(0), "traceparent");
        assertThat(traceparent)
                .as("the header has to be there before its shape can be argued about - see "
                        + "relayRestoresTheStoredContextOntoTheRecord, which is the failure to "
                        + "fix first")
                .isNotNull();
        assertThat(spanIdOf(traceparent))
                .as("copying trace_parent straight onto the record is simpler and erases the "
                        + "relay hop - and relay lag is one of the two intervals this milestone "
                        + "exists to make visible. The record must carry the PUBLISH span")
                .isNotEqualTo(SEEDED_SPAN_ID);
    }

    @Test
    @DisplayName("a row with no stored context still publishes, and starts a trace of its own")
    void aRowWithNoStoredContextStillPublishes() {
        UUID transferId = UUID.randomUUID();
        seed(transferId, null);

        assertThat(relay.drainBatch())
                .as("a NULL trace_parent is normal, not exceptional; the relay must not treat "
                        + "the absence of observability as a reason to stop moving money")
                .isEqualTo(1);

        ConsumerRecord<String, String> record = publishedNow().get(0);
        assertThat(record.value()).contains(transferId.toString());
        assertThat(headerOf(record, "traceparent"))
                .as("a root span is the right outcome here - the message is still worth tracing "
                        + "from this point on, it simply has no parent")
                .isNotNull();
    }

    // ---------------------------------------------------------------------------------------

    /** {@code 00-<trace id>-<span id>-<flags>} - the W3C format, split on its hyphens. */
    private static String traceIdOf(String traceparent) {
        return traceparent.split("-")[1];
    }

    private static String spanIdOf(String traceparent) {
        return traceparent.split("-")[2];
    }

    private UUID seed(UUID transferId, String traceParent) {
        UUID messageId = UUID.randomUUID();
        String payload = PAYLOAD.formatted(messageId, transferId, transferId).replace("\n", "");
        jdbc.update("""
                INSERT INTO outbox (id, aggregate_type, aggregate_id, topic, event_type, payload,
                                    trace_parent)
                VALUES (?, 'Transfer', ?, ?, 'FundsTransferred', ?::jsonb, ?)
                """, messageId, transferId, Topics.ACCOUNT_EVENTS, payload, traceParent);
        return messageId;
    }
}
