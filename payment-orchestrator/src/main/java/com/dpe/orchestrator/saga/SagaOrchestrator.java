package com.dpe.orchestrator.saga;

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
import com.dpe.messaging.outbox.OutboxWriter;
import com.dpe.orchestrator.transfer.Transfer;
import com.dpe.orchestrator.transfer.TransferRepository;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * THE SAGA STATE MACHINE.
 *
 * <h2>The flow</h2>
 *
 * <pre>
 *   start()               transfers(PENDING) + saga(STARTED)  + outbox(ReserveFunds)
 *     |
 *     +-- onFundsReserved()   saga(RESERVED)                  + outbox(ChargeGateway)
 *     |     |
 *     |     +-- onGatewayApproved()  saga(CHARGED)            + outbox(CommitFunds)
 *     |     |     +-- onFundsCommitted()  saga(COMPLETED), transfer(COMPLETED)
 *     |     |
 *     |     +-- onGatewayDeclined()  saga(COMPENSATING)       + outbox(ReleaseFunds)
 *     |           +-- onFundsReleased()   saga(COMPENSATED),  transfer(FAILED)
 *     |
 *     +-- onReserveRejected()  saga(FAILED), transfer(FAILED)     nothing to compensate
 * </pre>
 *
 * <h2>Four rules that hold in every method here</h2>
 *
 * <p><b>1. One transaction covers the state change and the command it emits.</b>
 * {@link SagaReplyHandler} opens the transaction and writes the inbox row before calling in here;
 * the {@code @Transactional} annotations below use the default {@code REQUIRED} propagation and so
 * <i>join</i> it. If the status moved to RESERVED but the ChargeGateway row did not commit with
 * it, the saga would be waiting for a reply to a message that was never sent, and only the sweeper
 * would ever notice.
 *
 * <p><b>2. The saga is always loaded {@code FOR UPDATE}.</b> Every method is a read-modify-write,
 * and the sweeper's scheduled thread can be inside the same saga at the same instant. Kafka's
 * per-partition ordering does not help here: it orders one consumer's view of one partition, and
 * the sweeper is not a consumer at all.
 *
 * <p><b>3. A reply that does not fit the current state is skipped, not rejected.</b> A late
 * approval arriving after the sweeper compensated is normal, not exceptional. It is recorded as a
 * {@link StepOutcome#SKIPPED} step and ignored. Throwing would roll back the inbox row and put the
 * message into an infinite redelivery loop.
 *
 * <p><b>4. Every transition writes a {@link SagaStep}.</b> Two rows per step - STARTED when a
 * command goes out, SUCCEEDED/FAILED/SKIPPED/TIMED_OUT when its reply lands. A step with only the
 * first row is a step whose reply never came, which makes a stalled saga a query rather than an
 * investigation.
 *
 * <h2>The distinction the whole milestone turns on</h2>
 *
 * <p>A <b>rejected reserve</b> goes straight to {@link SagaStatus#FAILED}. A <b>declined
 * gateway</b> goes to {@link SagaStatus#COMPENSATING}. They look similar and are not: in the first
 * case no money ever moved, so there is nothing to undo and a {@link ReleaseFunds} would name a
 * hold that does not exist and strand the saga waiting for a reply nobody will send. The schema
 * agrees - {@code saga_compensation_needs_a_hold} rejects a COMPENSATING row with a null
 * {@code hold_id}.
 */
@Service
public class SagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(SagaOrchestrator.class);

    private static final String STEP_RESERVE = "ReserveFunds";
    private static final String STEP_CHARGE = "ChargeGateway";
    private static final String STEP_COMMIT = "CommitFunds";
    private static final String STEP_RELEASE = "ReleaseFunds";

    private final SagaInstanceRepository sagas;
    private final SagaStepRepository steps;
    private final TransferRepository transfers;
    private final OutboxWriter outbox;
    private final SagaProperties properties;
    private final SagaMetrics metrics;

    public SagaOrchestrator(SagaInstanceRepository sagas, SagaStepRepository steps,
                            TransferRepository transfers, OutboxWriter outbox,
                            SagaProperties properties, SagaMetrics metrics) {
        this.sagas = sagas;
        this.steps = steps;
        this.transfers = transfers;
        this.outbox = outbox;
        this.properties = properties;
        this.metrics = metrics;
    }

    /**
     * Begins a saga: the transfer row, the saga row and the first command, in one commit.
     *
     * <p>The consequence worth stating: it is impossible for a transfer row to exist without its
     * ReserveFunds command, so there is no such thing as a transfer that silently never started.
     */
    @Transactional
    public SagaInstance start(Transfer transfer) {
        transfers.save(transfer);

        SagaInstance saga = new SagaInstance(UUID.randomUUID(), transfer.getId(),
                deadlineFromNow());
        sagas.save(saga);

        UUID messageId = outbox.append("Transfer", transfer.getId(), Topics.ACCOUNT_COMMANDS,
                ReserveFunds.TYPE,
                new ReserveFunds(transfer.getId(), transfer.getFromAccountId(),
                        transfer.getToAccountId(), transfer.getAmountMinor(),
                        transfer.getCurrency(), transfer.getInitiatedBy()));

        recordStep(saga, STEP_RESERVE, StepOutcome.STARTED, messageId, null);
        metrics.sagaStarted();
        return saga;
    }

    /**
     * account-service reserved the funds. Move to RESERVED and charge the gateway.
     *
     * <p>The hold id is stored before anything is emitted. A saga that reached COMPENSATING
     * without it could not be rescued by the sweeper - there would be no hold to name.
     */
    @Transactional
    public void onFundsReserved(FundsReserved event, UUID messageId) {
        SagaInstance saga = load(event.transferId());
        if (saga == null || !expect(saga, SagaStatus.STARTED, STEP_RESERVE, messageId)) {
            return;
        }

        saga.setHoldId(event.holdId());
        saga.transitionTo(SagaStatus.RESERVED);
        recordStep(saga, STEP_RESERVE, StepOutcome.SUCCEEDED, messageId, null);

        UUID commandId = outbox.append("Transfer", saga.getTransferId(), Topics.GATEWAY_COMMANDS,
                ChargeGateway.TYPE,
                new ChargeGateway(event.transferId(), event.fromAccountId(), event.amountMinor(),
                        event.currency()));
        recordStep(saga, STEP_CHARGE, StepOutcome.STARTED, commandId, null);
    }

    /**
     * account-service refused. Terminal FAILED, with nothing to compensate.
     */
    @Transactional
    public void onReserveRejected(ReserveRejected event, UUID messageId) {
        SagaInstance saga = load(event.transferId());
        if (saga == null || !expect(saga, SagaStatus.STARTED, STEP_RESERVE, messageId)) {
            return;
        }

        // The caller is told why in the vocabulary the participant used, not one invented here.
        failTransfer(saga, event.reason());
        finish(saga, SagaStatus.FAILED, event.reason());
        recordStep(saga, STEP_RESERVE, StepOutcome.FAILED, messageId, event.detail());
    }

    /** The PSP approved. Move to CHARGED and settle the hold. */
    @Transactional
    public void onGatewayApproved(GatewayApproved event, UUID messageId) {
        SagaInstance saga = load(event.transferId());
        if (saga == null || !expect(saga, SagaStatus.RESERVED, STEP_CHARGE, messageId)) {
            return;
        }

        saga.setGatewayChargeId(event.chargeId().toString());
        saga.transitionTo(SagaStatus.CHARGED);
        recordStep(saga, STEP_CHARGE, StepOutcome.SUCCEEDED, messageId, null);

        Transfer transfer = transfers.findById(saga.getTransferId()).orElseThrow();
        UUID commandId = outbox.append("Transfer", saga.getTransferId(), Topics.ACCOUNT_COMMANDS,
                CommitFunds.TYPE,
                new CommitFunds(saga.getTransferId(), saga.getHoldId(),
                        transfer.getToAccountId()));
        recordStep(saga, STEP_COMMIT, StepOutcome.STARTED, commandId, null);
    }

    /**
     * The PSP refused. COMPENSATE.
     *
     * <p>The branch the whole project exists to demonstrate: the money has already moved and is
     * sitting in a hold, and this is where the decision to give it back is made.
     */
    @Transactional
    public void onGatewayDeclined(GatewayDeclined event, UUID messageId) {
        SagaInstance saga = load(event.transferId());
        if (saga == null || !expect(saga, SagaStatus.RESERVED, STEP_CHARGE, messageId)) {
            return;
        }

        saga.transitionTo(SagaStatus.COMPENSATING);
        recordStep(saga, STEP_CHARGE, StepOutcome.FAILED, messageId, event.reason());
        emitRelease(saga, ReleaseFunds.GATEWAY_DECLINED, StepOutcome.STARTED);
    }

    /** The hold settled. Terminal COMPLETED. */
    @Transactional
    public void onFundsCommitted(FundsCommitted event, UUID messageId) {
        SagaInstance saga = load(event.transferId());
        if (saga == null || !expect(saga, SagaStatus.CHARGED, STEP_COMMIT, messageId)) {
            return;
        }

        transfers.findById(saga.getTransferId()).ifPresent(Transfer::complete);
        finish(saga, SagaStatus.COMPLETED, null);
        recordStep(saga, STEP_COMMIT, StepOutcome.SUCCEEDED, messageId, null);
    }

    /**
     * The hold was released. Terminal COMPENSATED, and the transfer fails.
     *
     * <p>The saga ends COMPENSATED while the transfer ends FAILED. Not an inconsistency: from the
     * caller's side the transfer did not happen, and from the system's side it recovered cleanly.
     * Both are true and they are different statements.
     */
    @Transactional
    public void onFundsReleased(FundsReleased event, UUID messageId) {
        SagaInstance saga = load(event.transferId());
        if (saga == null || !expect(saga, SagaStatus.COMPENSATING, STEP_RELEASE, messageId)) {
            return;
        }

        failTransfer(saga, event.reason());
        finish(saga, SagaStatus.COMPENSATED, event.reason());
        recordStep(saga, STEP_RELEASE, StepOutcome.SUCCEEDED, messageId, event.reason());
    }

    /**
     * Drives one expired saga towards a terminal state. Called by {@link SagaTimeoutSweeper},
     * which has already claimed and locked the row.
     *
     * <p>Not {@code @Transactional} itself: it runs inside the sweeper's batch transaction, and
     * annotating it would suggest it has a boundary of its own that it could fail within. The
     * sweeper catches per saga so one unrescuable saga does not abandon the batch.
     *
     * <p>"Stuck" means different things in different states, which is why this branches rather
     * than always compensating.
     */
    public void onTimeout(SagaInstance saga) {
        saga.recordSweepAttempt();

        switch (saga.getStatus()) {
            case STARTED -> {
                // The reserve never replied, so there may be no hold at all - a ReleaseFunds here
                // would name nothing and never be answered.
                //
                // AND THIS CAN BE WRONG, which is worth saying out loud rather than hiding.
                // account-service may have reserved successfully and had its reply lost, in which
                // case a hold is now ACTIVE with no live saga to settle it. I3 still balances -
                // the money is accounted for - but it is stranded. The honest fix is a
                // reconciliation job that finds holds with no saga, and it is out of scope here.
                log.warn("saga {} timed out in STARTED; failing it. If the reserve did in fact "
                        + "succeed, its hold is now orphaned and needs reconciliation.",
                        saga.getId());
                failTransfer(saga, ReleaseFunds.SAGA_TIMEOUT);
                finish(saga, SagaStatus.FAILED, "timed out before the reserve replied");
                recordStep(saga, STEP_RESERVE, StepOutcome.TIMED_OUT, null,
                        "no reply before the deadline");
            }
            case RESERVED, CHARGED -> {
                // A hold exists and holds a customer's money. Compensate.
                saga.transitionTo(SagaStatus.COMPENSATING);
                recordStep(saga, STEP_CHARGE, StepOutcome.TIMED_OUT, null,
                        "no reply before the deadline");
                emitRelease(saga, ReleaseFunds.SAGA_TIMEOUT, StepOutcome.STARTED);
            }
            case COMPENSATING -> {
                // A release was already sent and its reply has not come either. Re-emit it: the
                // participant's inbox absorbs the repeat, and the UNIQUE ledger constraint
                // absorbs it again if the inbox somehow does not.
                emitRelease(saga, ReleaseFunds.SAGA_TIMEOUT, StepOutcome.TIMED_OUT);
            }
            default -> log.warn("sweeper reached terminal saga {} in {} - claimExpired should "
                    + "have filtered it", saga.getId(), saga.getStatus());
        }
    }

    // ------------------------------------------------------------------ helpers

    /**
     * Ends a saga, and records that it ended.
     *
     * <p>Every terminal transition goes through here rather than calling
     * {@link SagaInstance#finish} directly, and that is the point: there are four of them, in four
     * branches, and a fifth added later would otherwise be invisible to
     * {@code dpe.saga.terminal} - the compensation rate would quietly under-report and nobody
     * would find out from a test. One door, so the meter cannot be forgotten.
     *
     * <p>{@link SagaMetrics} defers the actual increment to {@code afterCommit}; see the reasoning
     * there. This method being called is not yet an event that happened.
     */
    private void finish(SagaInstance saga, SagaStatus terminal, String failureReason) {
        saga.finish(terminal, failureReason);
        metrics.sagaFinished(terminal, saga.getCreatedAt());
    }

    /** Loads the saga for a transfer with a row lock. See the repository for why it is required. */
    private SagaInstance load(UUID transferId) {
        Optional<SagaInstance> saga = sagas.findByTransferIdForUpdate(transferId);
        if (saga.isEmpty()) {
            // A reply for a transfer this service has never heard of. Dropped rather than
            // retried - redelivery will not make the saga appear.
            log.error("no saga for transfer {}; ignoring its reply", transferId);
        }
        return saga.orElse(null);
    }

    /**
     * Guards a transition. Returns false - having recorded a SKIPPED step - when the saga is not
     * in the state this reply expects.
     *
     * <p>That is the late-reply case, and it must not throw: a terminal saga receiving an approval
     * that was in flight when the sweeper compensated is normal operation, not an error.
     */
    private boolean expect(SagaInstance saga, SagaStatus required, String step, UUID messageId) {
        if (saga.getStatus() == required) {
            return true;
        }
        log.info("saga {} is {} but the reply expected {}; skipping",
                saga.getId(), saga.getStatus(), required);
        recordStep(saga, step, StepOutcome.SKIPPED, messageId,
                "arrived while the saga was " + saga.getStatus());
        return false;
    }

    private void emitRelease(SagaInstance saga, String reason, StepOutcome outcome) {
        UUID commandId = outbox.append("Transfer", saga.getTransferId(), Topics.ACCOUNT_COMMANDS,
                ReleaseFunds.TYPE,
                new ReleaseFunds(saga.getTransferId(), saga.getHoldId(), reason));
        recordStep(saga, STEP_RELEASE, outcome, commandId, reason);
    }

    private void failTransfer(SagaInstance saga, String reason) {
        transfers.findById(saga.getTransferId()).ifPresent(t -> t.fail(reason));
    }

    private void recordStep(SagaInstance saga, String stepName, StepOutcome outcome,
                            UUID messageId, String detail) {
        steps.save(new SagaStep(saga.getId(), stepName, outcome, saga.getStatus(), messageId,
                detail));
    }

    private OffsetDateTime deadlineFromNow() {
        return OffsetDateTime.now().plus(properties.stepTimeout());
    }
}
