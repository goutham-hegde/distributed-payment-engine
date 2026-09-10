package com.dpe.orchestrator;

import static org.assertj.core.api.Assertions.assertThat;

import com.dpe.orchestrator.saga.SagaStatus;
import com.dpe.orchestrator.saga.SagaStepTrail;
import com.dpe.orchestrator.saga.SagaTimelineAssembler;
import com.dpe.orchestrator.saga.StepOutcome;
import com.dpe.orchestrator.transfer.Transfer;
import com.dpe.orchestrator.web.dto.TimelineResponse;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The pairing rule, tested without a database.
 *
 * <p>{@code saga_steps} is append-only with two rows per step, and folding those rows back into
 * stages is the only piece of the read path with real logic in it. Every case here is a shape the
 * table can genuinely hold - an unfinished step, an orphan reply, a step that restarted - and the
 * assembler has to render all of them rather than throwing, because the moment it refuses to
 * render a saga is the moment somebody is trying to work out what went wrong with one.
 *
 * <p>No Spring context, no Postgres. Rows are built by hand, which is the point: an integration
 * test can only produce the shapes the happy path produces, and the interesting shapes here are
 * the ones a broken system produces.
 */
class SagaTimelineAssemblerTest {

    private static final OffsetDateTime T0 =
            OffsetDateTime.of(2026, 9, 10, 12, 0, 0, 0, ZoneOffset.UTC);

    private static final String TRACEPARENT =
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    private final Transfer transfer = new Transfer(UUID.randomUUID(), UUID.randomUUID(),
            UUID.randomUUID(), 30_000L, "INR", "alice");

    @Test
    @DisplayName("two rows for one step become one stage, and the gap between them is the latency")
    void pairsStartedWithItsReply() {
        UUID commandId = UUID.randomUUID();
        UUID replyId = UUID.randomUUID();

        TimelineResponse timeline = SagaTimelineAssembler.assemble(transfer, null, List.of(
                outbound("ReserveFunds", StepOutcome.STARTED, null, commandId, T0,
                        "dpe.account.commands.v1", "ReserveFunds",
                        T0, T0.plusNanos(120_000_000L)),
                inbound("ReserveFunds", StepOutcome.SUCCEEDED, SagaStatus.RESERVED, replyId,
                        T0.plusSeconds(2), "dpe.account.events.v1", "FundsReserved",
                        T0.plusSeconds(2))));

        assertThat(timeline.stages()).hasSize(1);
        TimelineResponse.Stage stage = timeline.stages().get(0);

        assertThat(stage.name()).isEqualTo("ReserveFunds");
        assertThat(stage.outcome())
                .as("the stage carries the outcome of the row that CLOSED it, not STARTED")
                .isEqualTo("SUCCEEDED");
        assertThat(stage.toStatus()).isEqualTo("RESERVED");
        assertThat(stage.latencyMs()).isEqualTo(2_000L);

        assertThat(stage.command().messageId()).isEqualTo(commandId);
        assertThat(stage.command().topic()).isEqualTo("dpe.account.commands.v1");
        assertThat(stage.command().relayLagMs())
                .as("published_at minus created_at - the outbox row sitting committed, waiting "
                        + "for the relay's next poll")
                .isEqualTo(120L);

        assertThat(stage.reply().messageId()).isEqualTo(replyId);
        assertThat(stage.reply().eventType()).isEqualTo("FundsReserved");
    }

    @Test
    @DisplayName("a step whose reply never came stays open - that is the stall, and it must show")
    void unfinishedStepHasNoEndAndNoLatency() {
        TimelineResponse timeline = SagaTimelineAssembler.assemble(transfer, null, List.of(
                outbound("ChargeGateway", StepOutcome.STARTED, null, UUID.randomUUID(), T0,
                        "dpe.gateway.commands.v1", "ChargeGateway", T0, T0)));

        TimelineResponse.Stage stage = timeline.stages().get(0);
        assertThat(stage.startedAt()).isEqualTo(T0);
        assertThat(stage.endedAt()).isNull();
        assertThat(stage.latencyMs())
                .as("null, never zero - zero is a latency, and a chart that cannot tell "
                        + "'instant' from 'never came back' is worse than one with a gap")
                .isNull();
        assertThat(stage.outcome()).isEqualTo("STARTED");
    }

    @Test
    @DisplayName("a closing row with nothing open is rendered with no start rather than dropped")
    void orphanReplyBecomesItsOwnStage() {
        // A SKIPPED reply that lost a race can look exactly like this. Inventing a start time
        // for it would be fabricating a latency; dropping it would hide the duplicate.
        TimelineResponse timeline = SagaTimelineAssembler.assemble(transfer, null, List.of(
                inbound("CommitFunds", StepOutcome.SKIPPED, null, UUID.randomUUID(),
                        T0.plusSeconds(5), "dpe.account.events.v1", "FundsCommitted",
                        T0.plusSeconds(5))));

        assertThat(timeline.stages()).hasSize(1);
        TimelineResponse.Stage stage = timeline.stages().get(0);
        assertThat(stage.startedAt()).isNull();
        assertThat(stage.endedAt()).isEqualTo(T0.plusSeconds(5));
        assertThat(stage.latencyMs()).isNull();
        assertThat(stage.outcome()).isEqualTo("SKIPPED");
    }

    @Test
    @DisplayName("the same step starting twice leaves the first attempt visibly unfinished")
    void aSecondStartOpensASecondStage() {
        TimelineResponse timeline = SagaTimelineAssembler.assemble(transfer, null, List.of(
                outbound("ReserveFunds", StepOutcome.STARTED, null, UUID.randomUUID(), T0,
                        "dpe.account.commands.v1", "ReserveFunds", T0, T0),
                outbound("ReserveFunds", StepOutcome.STARTED, null, UUID.randomUUID(),
                        T0.plusSeconds(30), "dpe.account.commands.v1", "ReserveFunds",
                        T0.plusSeconds(30), T0.plusSeconds(30)),
                inbound("ReserveFunds", StepOutcome.SUCCEEDED, SagaStatus.RESERVED,
                        UUID.randomUUID(), T0.plusSeconds(31), "dpe.account.events.v1",
                        "FundsReserved", T0.plusSeconds(31))));

        assertThat(timeline.stages()).hasSize(2);
        assertThat(timeline.stages().get(0).endedAt())
                .as("the reply closes the attempt that was open when it arrived, and the "
                        + "abandoned one is left unfinished rather than back-filled")
                .isNull();
        assertThat(timeline.stages().get(1).endedAt()).isEqualTo(T0.plusSeconds(31));
    }

    @Test
    @DisplayName("the trace id is parsed out of the traceparent for the Jaeger deep link")
    void extractsTraceId() {
        TimelineResponse timeline = SagaTimelineAssembler.assemble(transfer, null, List.of(
                outbound("ReserveFunds", StepOutcome.STARTED, null, UUID.randomUUID(), T0,
                        "dpe.account.commands.v1", "ReserveFunds", T0, T0)));

        assertThat(timeline.traceId()).isEqualTo("4bf92f3577b34da6a3ce929d0e0e4736");
    }

    @Test
    @DisplayName("a malformed or absent traceparent means no deep link, not a failed request")
    void tracingIsNotLoadBearing() {
        // Same rule Redis is held to. If a broken trace context could stop a transfer being
        // displayed, the instrumentation would have joined the payment path.
        SagaStepTrail malformed = new SagaStepTrail("ReserveFunds", StepOutcome.STARTED, null,
                UUID.randomUUID(), null, T0,
                "dpe.account.commands.v1", "ReserveFunds", T0, T0, "not-a-traceparent",
                null, null, null);

        assertThat(SagaTimelineAssembler.assemble(transfer, null, List.of(malformed)).traceId())
                .isNull();
        assertThat(SagaTimelineAssembler.assemble(transfer, null, List.of()).traceId())
                .isNull();
    }

    @Test
    @DisplayName("an unpublished command reports no relay lag - the row is written, nothing sent")
    void unpublishedCommandHasNoRelayLag() {
        SagaStepTrail queued = new SagaStepTrail("ReserveFunds", StepOutcome.STARTED, null,
                UUID.randomUUID(), null, T0,
                "dpe.account.commands.v1", "ReserveFunds", T0, null, TRACEPARENT,
                null, null, null);

        TimelineResponse.Outbound command =
                SagaTimelineAssembler.assemble(transfer, null, List.of(queued))
                        .stages().get(0).command();

        assertThat(command.publishedAt()).isNull();
        assertThat(command.relayLagMs())
                .as("null is the answer to 'why has nothing happened' - the row is committed and "
                        + "the relay has not run or cannot reach the broker")
                .isNull();
    }

    // ---------------------------------------------------------------- row builders

    private static SagaStepTrail outbound(String step, StepOutcome outcome, SagaStatus toStatus,
                                          UUID messageId, OffsetDateTime stepAt,
                                          String topic, String eventType,
                                          OffsetDateTime queuedAt, OffsetDateTime publishedAt) {
        return new SagaStepTrail(step, outcome, toStatus, messageId, null, stepAt,
                topic, eventType, queuedAt, publishedAt, TRACEPARENT,
                null, null, null);
    }

    private static SagaStepTrail inbound(String step, StepOutcome outcome, SagaStatus toStatus,
                                         UUID messageId, OffsetDateTime stepAt,
                                         String topic, String eventType,
                                         OffsetDateTime receivedAt) {
        return new SagaStepTrail(step, outcome, toStatus, messageId, null, stepAt,
                null, null, null, null, null,
                topic, eventType, receivedAt);
    }
}
