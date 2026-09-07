package com.dpe.orchestrator;

import static org.assertj.core.api.Assertions.assertThat;

import com.dpe.events.FundsReserved;
import com.dpe.events.ReleaseFunds;
import com.dpe.orchestrator.saga.SagaInstance;
import com.dpe.orchestrator.saga.SagaInstanceRepository;
import com.dpe.orchestrator.saga.SagaOrchestrator;
import com.dpe.orchestrator.saga.SagaStatus;
import com.dpe.orchestrator.saga.SagaTimeoutSweeper;
import com.dpe.orchestrator.support.AbstractPostgresIT;
import com.dpe.orchestrator.transfer.Transfer;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * THE SPECIFICATION for {@link SagaTimeoutSweeper} and {@link SagaOrchestrator#onTimeout}.
 * Red until you write them.
 *
 * <p>This is the milestone's most important test class, because it is the only one that proves
 * the system survives a participant that simply stops answering. Everything else here tests what
 * happens when messages arrive; this tests what happens when they never do - which is the failure
 * an event-driven system cannot otherwise observe at all.
 *
 * <p>Deadlines are moved by writing {@code deadline_at} into the past rather than by sleeping.
 * A test that waited thirty real seconds for a timeout would be thirty seconds slower and no
 * more truthful: the sweeper's logic is "deadline is in the past", and that is exactly what is
 * being set up.
 */
class SagaTimeoutTest extends AbstractPostgresIT {

    private static final String INR = "INR";

    @Autowired
    SagaOrchestrator orchestrator;

    @Autowired
    SagaTimeoutSweeper sweeper;

    @Autowired
    SagaInstanceRepository sagas;

    @Test
    @DisplayName("a saga stuck in RESERVED is compensated, so the hold cannot be stranded")
    void stalledReserveIsCompensated() {
        Transfer transfer = newTransfer();
        SagaInstance saga = orchestrator.start(transfer);
        UUID holdId = UUID.randomUUID();

        // account-service reserved the money, replied, and then died. The gateway never hears
        // anything, so no further reply is ever coming.
        orchestrator.onFundsReserved(new FundsReserved(transfer.getId(), holdId,
                transfer.getFromAccountId(), transfer.getToAccountId(), 30_000L, INR, 70_000L),
                UUID.randomUUID());
        expire(saga.getId());

        int swept = sweeper.sweep();

        assertThat(swept).isEqualTo(1);
        SagaInstance after = sagas.findById(saga.getId()).orElseThrow();
        assertThat(after.getStatus())
                .as("the whole point: the sweeper turns 'we are still waiting' into 'we waited "
                        + "too long', which is the only way to observe something that did not "
                        + "happen")
                .isEqualTo(SagaStatus.COMPENSATING);
        assertThat(after.getTimedOutAt()).isNotNull();
        assertThat(after.getSweepAttempts()).isEqualTo(1);

        assertThat(outboxCount(transfer.getId(), ReleaseFunds.TYPE)).isEqualTo(1);
        assertThat(outboxField(transfer.getId(), ReleaseFunds.TYPE, "reason"))
                .as("SAGA_TIMEOUT, not GATEWAY_DECLINED. 'Was this refunded because we were told "
                        + "no, or because we gave up waiting' is the first question anyone asks "
                        + "about a compensated transfer.")
                .isEqualTo(ReleaseFunds.SAGA_TIMEOUT);
        assertThat(outboxField(transfer.getId(), ReleaseFunds.TYPE, "holdId"))
                .isEqualTo(holdId.toString());
    }

    @Test
    @DisplayName("a saga stuck in STARTED has no hold, so it fails rather than compensating")
    void stalledStartCannotCompensate() {
        Transfer transfer = newTransfer();
        SagaInstance saga = orchestrator.start(transfer);
        expire(saga.getId());

        sweeper.sweep();

        SagaInstance after = sagas.findById(saga.getId()).orElseThrow();
        assertThat(after.getStatus())
                .as("no reserve ever replied, so there is no hold id - a ReleaseFunds here would "
                        + "name nothing and never be answered")
                .isEqualTo(SagaStatus.FAILED);
        assertThat(outboxCount(transfer.getId(), ReleaseFunds.TYPE)).isZero();

        // Worth stating plainly rather than hiding: this outcome can be WRONG. If account-service
        // did reserve and its reply was lost, a hold is now ACTIVE with no live saga to settle
        // it. I3 still balances - the money is accounted for - but it is stranded, and only a
        // reconciliation job that scans for holds with no saga would find it. Knowing where the
        // design leaks is worth more than pretending it does not.
    }

    @Test
    @DisplayName("a saga still in COMPENSATING is swept again - the release is safe to repeat")
    void stalledCompensationIsRetried() {
        Transfer transfer = newTransfer();
        SagaInstance saga = orchestrator.start(transfer);
        orchestrator.onFundsReserved(new FundsReserved(transfer.getId(), UUID.randomUUID(),
                transfer.getFromAccountId(), transfer.getToAccountId(), 30_000L, INR, 70_000L),
                UUID.randomUUID());
        expire(saga.getId());
        sweeper.sweep();
        expire(saga.getId());

        sweeper.sweep();

        SagaInstance after = sagas.findById(saga.getId()).orElseThrow();
        assertThat(after.getStatus()).isEqualTo(SagaStatus.COMPENSATING);
        assertThat(after.getSweepAttempts()).isEqualTo(2);
        assertThat(outboxCount(transfer.getId(), ReleaseFunds.TYPE))
                .as("re-emitting is safe: the participant's inbox absorbs the duplicate, and the "
                        + "UNIQUE ledger constraint absorbs it again if the inbox somehow does not")
                .isEqualTo(2);
    }

    @Test
    @DisplayName("a terminal saga is never swept, however long ago its deadline passed")
    void terminalSagasAreLeftAlone() {
        Transfer transfer = newTransfer();
        SagaInstance saga = orchestrator.start(transfer);
        orchestrator.onReserveRejected(new com.dpe.events.ReserveRejected(transfer.getId(),
                transfer.getFromAccountId(), com.dpe.events.ReserveRejected.INSUFFICIENT_FUNDS,
                "no funds"), UUID.randomUUID());
        expire(saga.getId());

        assertThat(sweeper.sweep())
                .as("a finished saga is not stuck. Sweeping it would compensate a transfer that "
                        + "already resolved.")
                .isZero();
        assertThat(sagas.findById(saga.getId()).orElseThrow().getStatus())
                .isEqualTo(SagaStatus.FAILED);
    }

    @Test
    @DisplayName("the sweeper gives up after max-sweep-attempts instead of looping forever")
    void sweepAttemptsAreCapped() {
        Transfer transfer = newTransfer();
        SagaInstance saga = orchestrator.start(transfer);
        orchestrator.onFundsReserved(new FundsReserved(transfer.getId(), UUID.randomUUID(),
                transfer.getFromAccountId(), transfer.getToAccountId(), 30_000L, INR, 70_000L),
                UUID.randomUUID());

        // Already at the cap configured in application.yml.
        jdbc.update("UPDATE saga_instances SET sweep_attempts = 5 WHERE id = ?", saga.getId());
        expire(saga.getId());

        assertThat(sweeper.sweep())
                .as("without a cap, a saga that can never be compensated is swept forever and "
                        + "fills the log with the same failure until the real one is invisible")
                .isZero();
    }

    @Test
    @DisplayName("I4 holds after the sweep: nothing is left non-terminal but a live compensation")
    void invariantI4() {
        Transfer transfer = newTransfer();
        SagaInstance saga = orchestrator.start(transfer);
        UUID holdId = UUID.randomUUID();
        orchestrator.onFundsReserved(new FundsReserved(transfer.getId(), holdId,
                transfer.getFromAccountId(), transfer.getToAccountId(), 30_000L, INR, 70_000L),
                UUID.randomUUID());
        expire(saga.getId());
        sweeper.sweep();

        assertThat(sagas.countNonTerminal())
                .as("still 1 - COMPENSATING is not terminal, and correctly so: the money is "
                        + "still in a hold until the release is acknowledged")
                .isEqualTo(1);

        orchestrator.onFundsReleased(new com.dpe.events.FundsReleased(transfer.getId(), holdId,
                transfer.getFromAccountId(), 30_000L, INR, ReleaseFunds.SAGA_TIMEOUT),
                UUID.randomUUID());

        assertThat(sagas.countNonTerminal())
                .as("I4: after quiescence, no saga may sit in a non-terminal state")
                .isZero();
    }

    // ------------------------------------------------------------------ helpers

    /** Drags a saga's deadline into the past, so the sweeper considers it expired. */
    private void expire(UUID sagaId) {
        jdbc.update("UPDATE saga_instances SET deadline_at = now() - interval '1 minute' "
                + "WHERE id = ?", sagaId);
    }

    private Transfer newTransfer() {
        return new Transfer(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), 30_000L, INR);
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
                        + "WHERE event_type = ? AND aggregate_id = ? ORDER BY created_at LIMIT 1",
                String.class, eventType, transferId);
    }
}
