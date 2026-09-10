package com.dpe.orchestrator.saga;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * One {@code saga_steps} row with the message that caused it already attached.
 *
 * <p>The join is what makes the timeline worth having. {@code saga_steps.message_id} is either an
 * <b>outbox</b> row id in this database - a command this service issued - or an <b>inbox</b> row
 * id, a reply it consumed. Nothing records which, and nothing needs to: a left join to both tables
 * fills in exactly one side, and which side it filled in <i>is</i> the direction of the message.
 *
 * <p>Reading it as two queries and stitching in Java would be an N+1 across two tables per step,
 * on an endpoint a UI polls.
 *
 * @param outboxCreatedAt   when the command row committed
 * @param outboxPublishedAt when the relay actually sent it. The gap between the two is relay lag -
 *                          the same interval M6 part 2 made visible as a span - and having it here
 *                          means the console can show it without a trace backend being up.
 * @param traceParent       the W3C traceparent captured at append time. The console parses the
 *                          trace id out of it to deep-link into Jaeger.
 */
public record SagaStepTrail(
        String stepName,
        StepOutcome outcome,
        SagaStatus toStatus,
        UUID messageId,
        String detail,
        OffsetDateTime stepAt,

        String outboxTopic,
        String outboxEventType,
        OffsetDateTime outboxCreatedAt,
        OffsetDateTime outboxPublishedAt,
        String traceParent,

        String inboxTopic,
        String inboxEventType,
        OffsetDateTime inboxReceivedAt) {

    /** True when {@code message_id} matched a row this service PRODUCED. */
    public boolean isOutbound() {
        return outboxTopic != null;
    }

    /** True when it matched a row this service CONSUMED. */
    public boolean isInbound() {
        return inboxTopic != null;
    }
}
