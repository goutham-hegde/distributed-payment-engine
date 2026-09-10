package com.dpe.orchestrator.web.dto;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Everything known about one transfer's journey: the transfer itself, its saga, the ordered
 * stages, and the messages that carried each one.
 *
 * <p>This is the endpoint the demo console's first screen is built on, and the reason the answer
 * can be assembled at all is a design decision made back at M3: {@code saga_steps} is append-only
 * with <b>two rows per step</b>, one when the command is written and one when the reply lands.
 * A single mutable "current step" column would have made the write simpler and this endpoint
 * impossible - there would be no record that a step had ever started, so no per-step latency, and
 * a stalled saga would look identical to one that had not got there yet.
 *
 * @param traceId the W3C trace id, parsed out of the traceparent stored on the first outbox row of
 *                this saga, or null if tracing was off when the transfer ran. It is here so a UI
 *                can link to Jaeger; it is not load-bearing, and a null means "no deep link", not
 *                an error - the same rule tracing is held to everywhere else in this system.
 */
public record TimelineResponse(
        TransferSummary transfer,
        SagaView saga,
        String traceId,
        List<Stage> stages) {

    /**
     * The saga row. {@code deadlineAt} is included on purpose: it is the difference between "this
     * is taking a while" and "the sweeper is about to compensate this", which is the first thing
     * anyone wants to know about a transfer that has not finished.
     */
    public record SagaView(
            UUID sagaId,
            String status,
            UUID holdId,
            String gatewayChargeId,
            String failureReason,
            OffsetDateTime deadlineAt,
            OffsetDateTime timedOutAt,
            OffsetDateTime completedAt,
            OffsetDateTime createdAt) {
    }

    /**
     * One step, both of its rows folded into one object.
     *
     * @param endedAt   null while the reply has not arrived. A stage with a {@code startedAt} and
     *                  no {@code endedAt} is precisely "where did this saga stall", which was the
     *                  stated purpose of the two-row design.
     * @param latencyMs null for the same reason. Not zero - zero is a latency, and a chart that
     *                  cannot tell "instant" from "never came back" is worse than one with a gap.
     * @param command   the message this service SENT to start the step, or null if the step was
     *                  driven by something other than an outbound command (a timeout sweep).
     * @param reply     the message this service RECEIVED to end it, or null while it is pending.
     */
    public record Stage(
            String name,
            String outcome,
            String toStatus,
            OffsetDateTime startedAt,
            OffsetDateTime endedAt,
            Long latencyMs,
            String detail,
            Outbound command,
            Inbound reply) {
    }

    /**
     * A command this service published.
     *
     * @param relayLagMs {@code publishedAt - queuedAt}: how long the row sat committed in the
     *                   outbox before the relay picked it up. Null while it is still unpublished,
     *                   which is itself the answer to "why has nothing happened" - the row is
     *                   written, the relay has not run or cannot reach the broker.
     */
    public record Outbound(
            UUID messageId,
            String topic,
            String eventType,
            OffsetDateTime queuedAt,
            OffsetDateTime publishedAt,
            Long relayLagMs) {
    }

    /** A reply this service consumed - the inbox row that proves it was processed exactly once. */
    public record Inbound(
            UUID messageId,
            String topic,
            String eventType,
            OffsetDateTime receivedAt) {
    }
}
