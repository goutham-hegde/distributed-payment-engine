package com.dpe.orchestrator.saga;

import com.dpe.orchestrator.transfer.Transfer;
import com.dpe.orchestrator.web.dto.TimelineResponse;
import com.dpe.orchestrator.web.dto.TransferSummary;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Folds the append-only {@code saga_steps} log into the stages a human reads.
 *
 * <p>Pure function, no repository, no Spring. It is separate from {@code TransferService} because
 * the pairing rule below is the only genuinely non-obvious logic on the read path, and it is worth
 * being able to test it against a hand-built list of rows rather than a database.
 *
 * <h2>The pairing rule</h2>
 *
 * <p>Every step writes a {@link StepOutcome#STARTED} row when its command goes out and a second
 * row when its reply lands. The assembler walks the rows in time order, opens a stage on a
 * {@code STARTED} and closes it on the next row with the same step name.
 *
 * <p>Three cases that are not the happy path, and none of them may throw - this is a diagnostic
 * endpoint, and the moment it refuses to render a saga is the moment somebody needed it most:
 *
 * <ul>
 *   <li><b>An open stage that never closes.</b> Left with {@code endedAt} null. This is the useful
 *       one: it is the stalled step, and it renders as a stage still running.
 *   <li><b>A closing row with nothing open.</b> Emitted as a stage with no start. A
 *       {@link StepOutcome#SKIPPED} reply that lost a race can look like this, and inventing a
 *       start time for it would be fabricating a latency.
 *   <li><b>The same step name starting twice.</b> The second {@code STARTED} closes nothing and
 *       opens a new stage; the earlier one stays open and visibly unfinished, which is the truth.
 * </ul>
 */
public final class SagaTimelineAssembler {

    private SagaTimelineAssembler() {
    }

    public static TimelineResponse assemble(Transfer transfer, SagaInstance saga,
                                            List<SagaStepTrail> trail) {
        return new TimelineResponse(
                TransferSummary.of(transfer),
                saga == null ? null : view(saga),
                traceIdOf(trail),
                stages(trail));
    }

    private static TimelineResponse.SagaView view(SagaInstance saga) {
        return new TimelineResponse.SagaView(
                saga.getId(),
                saga.getStatus().name(),
                saga.getHoldId(),
                saga.getGatewayChargeId(),
                saga.getFailureReason(),
                saga.getDeadlineAt(),
                saga.getTimedOutAt(),
                saga.getCompletedAt(),
                saga.getCreatedAt());
    }

    private static List<TimelineResponse.Stage> stages(List<SagaStepTrail> trail) {
        List<MutableStage> stages = new ArrayList<>();
        Map<String, MutableStage> open = new HashMap<>();

        for (SagaStepTrail row : trail) {
            if (row.outcome() == StepOutcome.STARTED) {
                MutableStage stage = new MutableStage(row.stepName(), row.stepAt());
                stage.outcome = StepOutcome.STARTED;
                stage.toStatus = row.toStatus();
                stage.detail = row.detail();
                stage.command = outbound(row);
                stages.add(stage);
                // Replaces any stage of the same name still open. See "started twice" above: the
                // displaced one keeps its null endedAt rather than being closed by a row that
                // belongs to a different attempt.
                open.put(row.stepName(), stage);
                continue;
            }

            MutableStage stage = open.remove(row.stepName());
            if (stage == null) {
                stage = new MutableStage(row.stepName(), null);
                stages.add(stage);
            }
            stage.endedAt = row.stepAt();
            stage.outcome = row.outcome();
            if (row.toStatus() != null) {
                stage.toStatus = row.toStatus();
            }
            if (row.detail() != null) {
                stage.detail = row.detail();
            }
            // A closing row is normally a reply this service consumed. It can also be an outbound
            // row - a compensating command is written under the step that decided to send it - so
            // both sides are read, and whichever the message id actually matched is filled in.
            if (row.isInbound()) {
                stage.reply = new TimelineResponse.Inbound(row.messageId(), row.inboxTopic(),
                        row.inboxEventType(), row.inboxReceivedAt());
            }
            if (stage.command == null && row.isOutbound()) {
                stage.command = outbound(row);
            }
        }

        return stages.stream().map(MutableStage::toStage).toList();
    }

    private static TimelineResponse.Outbound outbound(SagaStepTrail row) {
        if (!row.isOutbound()) {
            return null;
        }
        return new TimelineResponse.Outbound(
                row.messageId(),
                row.outboxTopic(),
                row.outboxEventType(),
                row.outboxCreatedAt(),
                row.outboxPublishedAt(),
                millisBetween(row.outboxCreatedAt(), row.outboxPublishedAt()));
    }

    /**
     * The trace id for the Jaeger deep link, taken from the first outbox row of this saga that
     * carries a traceparent.
     *
     * <p>All of a saga's messages share one trace - that is what M6 part 2 built - so any row
     * would do; the first is chosen because it is the one written by the HTTP request, and if
     * tracing was on at all it is the row most likely to have it.
     *
     * <p>Returns null rather than throwing on anything unexpected. A traceparent is
     * {@code 00-<32 hex trace id>-<16 hex span id>-<flags>}, and a malformed one means the deep
     * link is unavailable - not that the transfer cannot be displayed.
     */
    private static String traceIdOf(List<SagaStepTrail> trail) {
        for (SagaStepTrail row : trail) {
            String traceParent = row.traceParent();
            if (traceParent == null) {
                continue;
            }
            String[] parts = traceParent.split("-");
            if (parts.length == 4 && parts[1].length() == 32) {
                return parts[1];
            }
        }
        return null;
    }

    /** Null-safe, and null-preserving: an absent endpoint means "unknown", never zero. */
    private static Long millisBetween(OffsetDateTime from, OffsetDateTime to) {
        if (from == null || to == null) {
            return null;
        }
        return Duration.between(from, to).toMillis();
    }

    /**
     * A stage under construction. Mutable on purpose - the alternative is rebuilding an immutable
     * record twice per step, which reads worse for no benefit inside a method whose entire scope
     * is this loop.
     */
    private static final class MutableStage {
        private final String name;
        private final OffsetDateTime startedAt;
        private OffsetDateTime endedAt;
        private StepOutcome outcome;
        private SagaStatus toStatus;
        private String detail;
        private TimelineResponse.Outbound command;
        private TimelineResponse.Inbound reply;

        private MutableStage(String name, OffsetDateTime startedAt) {
            this.name = name;
            this.startedAt = startedAt;
        }

        private TimelineResponse.Stage toStage() {
            return new TimelineResponse.Stage(
                    name,
                    outcome == null ? null : outcome.name(),
                    toStatus == null ? null : toStatus.name(),
                    startedAt,
                    endedAt,
                    millisBetween(startedAt, endedAt),
                    detail,
                    command,
                    reply);
        }
    }
}
