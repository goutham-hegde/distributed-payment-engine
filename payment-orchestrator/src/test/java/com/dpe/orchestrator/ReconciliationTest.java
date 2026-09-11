package com.dpe.orchestrator;

import static org.assertj.core.api.Assertions.assertThat;

import com.dpe.events.FundsCommitted;
import com.dpe.events.FundsReleased;
import com.dpe.events.FundsReserved;
import com.dpe.events.GatewayApproved;
import com.dpe.events.GatewayDeclined;
import com.dpe.events.ReleaseFunds;
import com.dpe.events.ReserveRejected;
import com.dpe.events.VoidCharge;
import com.dpe.orchestrator.saga.ReconcileOutcome;
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
 * {@link SagaOrchestrator#reconcile}: finishing a compensation for a transfer that already failed.
 *
 * <p>The two cases Session 16's chaos run left behind are the first two tests - an ACTIVE hold
 * under a FAILED saga, and an APPROVED charge under a COMPENSATED one. The third is the case the
 * design is sequenced to survive: a hold that turns out to be COMMITTED, where voiding the charge
 * would pay the recipient out of our own books.
 */
class ReconciliationTest extends AbstractPostgresIT {

    private static final String INR = "INR";
    private static final long AMOUNT = 30_000L;

    @Autowired
    SagaOrchestrator orchestrator;

    @Autowired
    SagaInstanceRepository sagas;

    @Test
    @DisplayName("a FAILED saga with a stranded hold: release by transfer id, then void the charge")
    void strandedHoldUnderFailedSaga() {
        Transfer transfer = newTransfer();
        SagaInstance saga = orchestrator.start(transfer);
        orchestrator.onReserveRejected(new ReserveRejected(transfer.getId(),
                transfer.getFromAccountId(), ReserveRejected.INSUFFICIENT_FUNDS, "test"),
                UUID.randomUUID());

        assertThat(orchestrator.reconcile(transfer.getId())).isEqualTo(ReconcileOutcome.REQUESTED);
        assertThat(outboxField(transfer.getId(), ReleaseFunds.TYPE, "reason"))
                .isEqualTo(ReleaseFunds.RECONCILIATION);
        assertThat(outboxCount(transfer.getId(), VoidCharge.TYPE))
                .as("not yet - the orchestrator cannot see the hold, so it waits to be told")
                .isZero();

        orchestrator.onFundsReleased(new FundsReleased(transfer.getId(), UUID.randomUUID(),
                transfer.getFromAccountId(), AMOUNT, INR, ReleaseFunds.RECONCILIATION),
                UUID.randomUUID());

        assertThat(outboxCount(transfer.getId(), VoidCharge.TYPE))
                .as("the sender is whole, so the PSP must hold nothing either")
                .isEqualTo(1);
        assertThat(sagas.findById(saga.getId()).orElseThrow().getStatus())
                .as("a terminal saga stays terminal; reconciliation is recorded as steps")
                .isEqualTo(SagaStatus.FAILED);
        assertThat(stepCount(saga.getId(), "Reconcile", "SUCCEEDED")).isEqualTo(1);
    }

    @Test
    @DisplayName("a COMPENSATED saga whose charge stood: the replayed release answer voids it")
    void orphanedChargeUnderCompensatedSaga() {
        Transfer transfer = newTransfer();
        SagaInstance saga = compensated(transfer);

        orchestrator.reconcile(transfer.getId());
        // account-service finds the hold already RELEASED and says so.
        orchestrator.onFundsReleased(new FundsReleased(transfer.getId(), UUID.randomUUID(),
                transfer.getFromAccountId(), AMOUNT, INR, ReleaseFunds.GATEWAY_DECLINED),
                UUID.randomUUID());

        assertThat(outboxCount(transfer.getId(), VoidCharge.TYPE)).isEqualTo(1);
        assertThat(sagas.findById(saga.getId()).orElseThrow().getStatus())
                .isEqualTo(SagaStatus.COMPENSATED);
    }

    @Test
    @DisplayName("no hold was ever taken: TRANSFER_VOIDED is the answer, and the void still follows")
    void neverReserved() {
        Transfer transfer = newTransfer();
        orchestrator.start(transfer);
        orchestrator.onReserveRejected(new ReserveRejected(transfer.getId(),
                transfer.getFromAccountId(), ReserveRejected.INSUFFICIENT_FUNDS, "test"),
                UUID.randomUUID());

        orchestrator.reconcile(transfer.getId());
        orchestrator.onReserveRejected(new ReserveRejected(transfer.getId(), null,
                ReserveRejected.TRANSFER_VOIDED, "released before any reserve"), UUID.randomUUID());

        assertThat(outboxCount(transfer.getId(), VoidCharge.TYPE)).isEqualTo(1);
    }

    @Test
    @DisplayName("THE CASE IT IS SEQUENCED FOR: a COMMITTED hold means the charge must stand")
    void committedHoldIsNotVoided() {
        Transfer transfer = newTransfer();
        SagaInstance saga = compensated(transfer);

        orchestrator.reconcile(transfer.getId());
        orchestrator.onFundsCommitted(new FundsCommitted(transfer.getId(), UUID.randomUUID(),
                transfer.getToAccountId(), AMOUNT, INR), UUID.randomUUID());

        assertThat(outboxCount(transfer.getId(), VoidCharge.TYPE))
                .as("the recipient has the money. Voiding the charge now would pay them out of our "
                        + "own books - the orchestrator cannot see the hold, which is exactly why it "
                        + "asks before it voids")
                .isZero();
        assertThat(stepCount(saga.getId(), "Reconcile", "FAILED")).isEqualTo(1);
        assertThat(sagas.findById(saga.getId()).orElseThrow().getStatus())
                .isEqualTo(SagaStatus.COMPENSATED);
    }

    @Test
    @DisplayName("a COMPLETED transfer is refused - reversing it would be moving money")
    void completedIsRefused() {
        Transfer transfer = newTransfer();
        orchestrator.start(transfer);
        UUID holdId = UUID.randomUUID();
        orchestrator.onFundsReserved(reserved(transfer, holdId), UUID.randomUUID());
        orchestrator.onGatewayApproved(new GatewayApproved(transfer.getId(), UUID.randomUUID(),
                AMOUNT, INR), UUID.randomUUID());
        orchestrator.onFundsCommitted(new FundsCommitted(transfer.getId(), holdId,
                transfer.getToAccountId(), AMOUNT, INR), UUID.randomUUID());

        assertThat(orchestrator.reconcile(transfer.getId())).isEqualTo(ReconcileOutcome.COMPLETED);
        assertThat(outboxCount(transfer.getId(), ReleaseFunds.TYPE)).isZero();
    }

    @Test
    @DisplayName("a live saga is refused - the sweeper owns it")
    void inFlightIsRefused() {
        Transfer transfer = newTransfer();
        orchestrator.start(transfer);

        assertThat(orchestrator.reconcile(transfer.getId()))
                .isEqualTo(ReconcileOutcome.STILL_IN_FLIGHT);
        assertThat(orchestrator.reconcile(UUID.randomUUID()))
                .isEqualTo(ReconcileOutcome.NO_SUCH_TRANSFER);
    }

    @Test
    @DisplayName("without an outstanding request, a late release reply on a terminal saga changes nothing")
    void noRequestNoVoid() {
        Transfer transfer = newTransfer();
        compensated(transfer);

        orchestrator.onFundsReleased(new FundsReleased(transfer.getId(), UUID.randomUUID(),
                transfer.getFromAccountId(), AMOUNT, INR, ReleaseFunds.GATEWAY_DECLINED),
                UUID.randomUUID());

        assertThat(outboxCount(transfer.getId(), VoidCharge.TYPE))
                .as("a duplicate reply is still only a duplicate; reconciliation is opt-in")
                .isZero();
    }

    // ------------------------------------------------------------------ helpers

    /** Drives a saga through reserve -> decline -> release, ending COMPENSATED. */
    private SagaInstance compensated(Transfer transfer) {
        SagaInstance saga = orchestrator.start(transfer);
        UUID holdId = UUID.randomUUID();
        orchestrator.onFundsReserved(reserved(transfer, holdId), UUID.randomUUID());
        orchestrator.onGatewayDeclined(new GatewayDeclined(transfer.getId(), UUID.randomUUID(),
                "DECLINED", "test"),
                UUID.randomUUID());
        orchestrator.onFundsReleased(new FundsReleased(transfer.getId(), holdId,
                transfer.getFromAccountId(), AMOUNT, INR, ReleaseFunds.GATEWAY_DECLINED),
                UUID.randomUUID());
        assertThat(sagas.findById(saga.getId()).orElseThrow().getStatus())
                .isEqualTo(SagaStatus.COMPENSATED);
        return saga;
    }

    private FundsReserved reserved(Transfer transfer, UUID holdId) {
        return new FundsReserved(transfer.getId(), holdId, transfer.getFromAccountId(),
                transfer.getToAccountId(), AMOUNT, INR, 70_000L);
    }

    private Transfer newTransfer() {
        return new Transfer(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), AMOUNT, INR,
                "test-owner");
    }

    private int stepCount(UUID sagaId, String step, String outcome) {
        Integer c = jdbc.queryForObject("SELECT COUNT(*) FROM saga_steps "
                + "WHERE saga_id = ? AND step_name = ? AND outcome = ?", Integer.class,
                sagaId, step, outcome);
        return c == null ? 0 : c;
    }

    private int outboxCount(UUID transferId, String eventType) {
        Integer c = jdbc.queryForObject(
                "SELECT COUNT(*) FROM outbox WHERE event_type = ? AND aggregate_id = ?",
                Integer.class, eventType, transferId);
        return c == null ? 0 : c;
    }

    private String outboxField(UUID transferId, String eventType, String field) {
        return jdbc.queryForObject(
                "SELECT payload->'payload'->>'" + field + "' FROM outbox "
                        + "WHERE event_type = ? AND aggregate_id = ? ORDER BY created_at DESC LIMIT 1",
                String.class, eventType, transferId);
    }
}
