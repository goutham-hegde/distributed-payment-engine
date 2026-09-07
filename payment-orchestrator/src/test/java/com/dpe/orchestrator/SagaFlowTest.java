package com.dpe.orchestrator;

import static org.assertj.core.api.Assertions.assertThat;

import com.dpe.events.ChargeGateway;
import com.dpe.events.CommitFunds;
import com.dpe.events.FundsCommitted;
import com.dpe.events.FundsReleased;
import com.dpe.events.FundsReserved;
import com.dpe.events.GatewayApproved;
import com.dpe.events.GatewayDeclined;
import com.dpe.events.ReleaseFunds;
import com.dpe.events.ReserveFunds;
import com.dpe.events.ReserveRejected;
import com.dpe.events.Topics;
import com.dpe.orchestrator.saga.SagaInstance;
import com.dpe.orchestrator.saga.SagaInstanceRepository;
import com.dpe.orchestrator.saga.SagaOrchestrator;
import com.dpe.orchestrator.saga.SagaStatus;
import com.dpe.orchestrator.support.AbstractPostgresIT;
import com.dpe.orchestrator.transfer.Transfer;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * THE SPECIFICATION for {@link SagaOrchestrator}. Red until you write it.
 *
 * <p>Drives the state machine by calling it directly rather than through Kafka. That is
 * deliberate: these tests are about the transitions and the commands they emit, and a broker in
 * the middle would only add timing noise to assertions about a transaction. The delivery path is
 * proven separately.
 *
 * <p>Read the happy path first, then the two failure branches. The branch that matters most is
 * {@link #rejectedReserveGoesStraightToFailed()} - it is the one people get wrong.
 */
class SagaFlowTest extends AbstractPostgresIT {

    private static final String INR = "INR";

    @Autowired
    SagaOrchestrator orchestrator;

    @Autowired
    SagaInstanceRepository sagas;

    // ------------------------------------------------------------------ the happy path

    @Test
    @DisplayName("start writes the transfer, the saga and the ReserveFunds command in one commit")
    void startEmitsReserveFunds() {
        Transfer transfer = newTransfer(30_000L);

        SagaInstance saga = orchestrator.start(transfer);

        assertThat(saga.getStatus()).isEqualTo(SagaStatus.STARTED);
        assertThat(saga.getDeadlineAt())
                .as("every saga carries a deadline from the moment it starts, or nothing will "
                        + "ever notice it stalling")
                .isNotNull();

        assertThat(transferStatus(transfer.getId())).isEqualTo("PENDING");
        assertThat(outboxCount(transfer.getId(), ReserveFunds.TYPE))
                .as("the command and the saga row commit together - there is no reachable state "
                        + "in which a transfer exists but its first command does not")
                .isEqualTo(1);
        assertThat(outboxTopic(transfer.getId(), ReserveFunds.TYPE))
                .as("commands go to the command topic, not the events topic")
                .isEqualTo(Topics.ACCOUNT_COMMANDS);
        assertThat(stepCount(saga.getId(), "ReserveFunds")).isEqualTo(1);
    }

    @Test
    @DisplayName("FundsReserved stores the hold id and charges the gateway")
    void reservedChargesTheGateway() {
        Transfer transfer = newTransfer(30_000L);
        SagaInstance saga = orchestrator.start(transfer);
        UUID holdId = UUID.randomUUID();

        orchestrator.onFundsReserved(new FundsReserved(transfer.getId(), holdId,
                transfer.getFromAccountId(), transfer.getToAccountId(), 30_000L, INR, 70_000L),
                UUID.randomUUID());

        SagaInstance reloaded = sagas.findById(saga.getId()).orElseThrow();
        assertThat(reloaded.getStatus()).isEqualTo(SagaStatus.RESERVED);
        assertThat(reloaded.getHoldId())
                .as("without the hold id the saga cannot address a commit OR a release later, "
                        + "so a stalled saga would be unrescuable")
                .isEqualTo(holdId);
        assertThat(outboxCount(transfer.getId(), ChargeGateway.TYPE)).isEqualTo(1);
        assertThat(outboxTopic(transfer.getId(), ChargeGateway.TYPE))
                .isEqualTo(Topics.GATEWAY_COMMANDS);
    }

    @Test
    @DisplayName("GatewayApproved commits the funds, and FundsCommitted completes the transfer")
    void approvalCompletesTheTransfer() {
        Transfer transfer = newTransfer(30_000L);
        SagaInstance saga = orchestrator.start(transfer);
        UUID holdId = UUID.randomUUID();
        UUID chargeId = UUID.randomUUID();

        orchestrator.onFundsReserved(reserved(transfer, holdId), UUID.randomUUID());
        orchestrator.onGatewayApproved(
                new GatewayApproved(transfer.getId(), chargeId, 30_000L, INR), UUID.randomUUID());

        SagaInstance charged = sagas.findById(saga.getId()).orElseThrow();
        assertThat(charged.getStatus()).isEqualTo(SagaStatus.CHARGED);
        assertThat(charged.getGatewayChargeId()).isEqualTo(chargeId.toString());
        assertThat(outboxCount(transfer.getId(), CommitFunds.TYPE)).isEqualTo(1);

        orchestrator.onFundsCommitted(
                new FundsCommitted(transfer.getId(), holdId, transfer.getToAccountId(), 30_000L, INR),
                UUID.randomUUID());

        SagaInstance done = sagas.findById(saga.getId()).orElseThrow();
        assertThat(done.getStatus()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(done.getCompletedAt())
                .as("saga_completed_at_iff_terminal makes this non-negotiable: a terminal saga "
                        + "without a completion time will not even flush")
                .isNotNull();
        assertThat(transferStatus(transfer.getId())).isEqualTo("COMPLETED");
    }

    // ------------------------------------------------------------------ the two failure branches

    @Test
    @DisplayName("a REJECTED RESERVE goes straight to FAILED - there is nothing to compensate")
    void rejectedReserveGoesStraightToFailed() {
        Transfer transfer = newTransfer(999_999L);
        SagaInstance saga = orchestrator.start(transfer);

        orchestrator.onReserveRejected(new ReserveRejected(transfer.getId(),
                transfer.getFromAccountId(), ReserveRejected.INSUFFICIENT_FUNDS,
                "balance is 10000, needed 999999"), UUID.randomUUID());

        SagaInstance failed = sagas.findById(saga.getId()).orElseThrow();
        assertThat(failed.getStatus())
                .as("NOT COMPENSATING. No money moved, so there is no hold to release - and "
                        + "emitting one would name a hold that does not exist and strand the "
                        + "saga waiting for a reply nobody will send.")
                .isEqualTo(SagaStatus.FAILED);
        assertThat(failed.getHoldId()).isNull();
        assertThat(outboxCount(transfer.getId(), ReleaseFunds.TYPE))
                .as("no compensation is emitted for something that never happened")
                .isZero();
        assertThat(transferStatus(transfer.getId())).isEqualTo("FAILED");
    }

    @Test
    @DisplayName("a DECLINE compensates: COMPENSATING, ReleaseFunds, then COMPENSATED")
    void declineCompensates() {
        Transfer transfer = newTransfer(30_000L);
        SagaInstance saga = orchestrator.start(transfer);
        UUID holdId = UUID.randomUUID();

        orchestrator.onFundsReserved(reserved(transfer, holdId), UUID.randomUUID());
        orchestrator.onGatewayDeclined(new GatewayDeclined(transfer.getId(), UUID.randomUUID(),
                "do_not_honour", "the PSP refused"), UUID.randomUUID());

        SagaInstance compensating = sagas.findById(saga.getId()).orElseThrow();
        assertThat(compensating.getStatus()).isEqualTo(SagaStatus.COMPENSATING);
        assertThat(compensating.getStatus().isTerminal())
                .as("COMPENSATING is NOT terminal. A compensation that was started and never "
                        + "finished leaves money in a hold, which is as much an I4 violation as "
                        + "never starting one.")
                .isFalse();
        assertThat(outboxCount(transfer.getId(), ReleaseFunds.TYPE)).isEqualTo(1);
        assertThat(outboxField(transfer.getId(), ReleaseFunds.TYPE, "reason"))
                .isEqualTo(ReleaseFunds.GATEWAY_DECLINED);

        orchestrator.onFundsReleased(new FundsReleased(transfer.getId(), holdId,
                transfer.getFromAccountId(), 30_000L, INR, ReleaseFunds.GATEWAY_DECLINED),
                UUID.randomUUID());

        SagaInstance compensated = sagas.findById(saga.getId()).orElseThrow();
        assertThat(compensated.getStatus()).isEqualTo(SagaStatus.COMPENSATED);
        assertThat(transferStatus(transfer.getId()))
                .as("the saga COMPENSATED and the transfer FAILED. Both are true and they are "
                        + "different statements: the system recovered cleanly, and the caller's "
                        + "money did not move.")
                .isEqualTo("FAILED");
    }

    // ------------------------------------------------------------------ late and duplicate replies

    @Test
    @DisplayName("a reply for a saga already finished changes nothing")
    void lateReplyIsIgnored() {
        Transfer transfer = newTransfer(30_000L);
        SagaInstance saga = orchestrator.start(transfer);
        UUID holdId = UUID.randomUUID();

        orchestrator.onFundsReserved(reserved(transfer, holdId), UUID.randomUUID());
        orchestrator.onGatewayDeclined(new GatewayDeclined(transfer.getId(), UUID.randomUUID(),
                "do_not_honour", "refused"), UUID.randomUUID());
        orchestrator.onFundsReleased(new FundsReleased(transfer.getId(), holdId,
                transfer.getFromAccountId(), 30_000L, INR, ReleaseFunds.GATEWAY_DECLINED),
                UUID.randomUUID());

        // The approval was in flight while the sweeper compensated. It is not an error; it is
        // late. Must not throw - throwing rolls back the inbox row and redelivers forever.
        orchestrator.onGatewayApproved(
                new GatewayApproved(transfer.getId(), UUID.randomUUID(), 30_000L, INR),
                UUID.randomUUID());

        SagaInstance saga2 = sagas.findById(saga.getId()).orElseThrow();
        assertThat(saga2.getStatus())
                .as("a terminal saga stays terminal")
                .isEqualTo(SagaStatus.COMPENSATED);
        assertThat(outboxCount(transfer.getId(), CommitFunds.TYPE))
                .as("and above all it must not emit a CommitFunds for a hold that was released")
                .isZero();
    }

    // ------------------------------------------------------------------ helpers

    private FundsReserved reserved(Transfer transfer, UUID holdId) {
        return new FundsReserved(transfer.getId(), holdId, transfer.getFromAccountId(),
                transfer.getToAccountId(), transfer.getAmountMinor(), INR, 70_000L);
    }

    private Transfer newTransfer(long amountMinor) {
        return new Transfer(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                amountMinor, INR);
    }

    private String transferStatus(UUID transferId) {
        return jdbc.queryForObject(
                "SELECT status FROM transfers WHERE id = ?", String.class, transferId);
    }

    private int outboxCount(UUID transferId, String eventType) {
        Integer c = jdbc.queryForObject(
                "SELECT COUNT(*) FROM outbox WHERE event_type = ? AND aggregate_id = ?",
                Integer.class, eventType, transferId);
        return c == null ? 0 : c;
    }

    private String outboxTopic(UUID transferId, String eventType) {
        return jdbc.queryForObject(
                "SELECT topic FROM outbox WHERE event_type = ? AND aggregate_id = ?",
                String.class, eventType, transferId);
    }

    // Read with ->>, never by string-matching payload::text: Postgres re-renders jsonb with its
    // own key order and spacing, so a substring assertion tests Postgres, not the producer.
    private String outboxField(UUID transferId, String eventType, String field) {
        return jdbc.queryForObject(
                "SELECT payload->'payload'->>'" + field + "' FROM outbox "
                        + "WHERE event_type = ? AND aggregate_id = ?",
                String.class, eventType, transferId);
    }

    private int stepCount(UUID sagaId, String stepName) {
        Integer c = jdbc.queryForObject(
                "SELECT COUNT(*) FROM saga_steps WHERE saga_id = ? AND step_name = ?",
                Integer.class, sagaId, stepName);
        return c == null ? 0 : c;
    }
}
