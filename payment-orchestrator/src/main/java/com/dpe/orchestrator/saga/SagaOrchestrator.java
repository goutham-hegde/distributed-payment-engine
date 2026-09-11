package com.dpe.orchestrator.saga;

import com.dpe.events.ChargeGateway;
import com.dpe.events.ChargeVoided;
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
import com.dpe.events.VoidCharge;
import com.dpe.messaging.outbox.OutboxWriter;
import com.dpe.orchestrator.transfer.Transfer;
import com.dpe.orchestrator.transfer.TransferRepository;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.Set;
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
 *
 * <h2>What the M7 chaos suite changed</h2>
 *
 * <p><b>The gateway charge is the PIVOT.</b> Before it, recovery runs backward (compensate);
 * after it, only forward (re-send the commit until it lands). A timeout in {@code CHARGED} used to
 * compensate exactly like {@code RESERVED} - a refund out of our own books while the PSP kept the
 * charge. See {@link #onTimeout}.
 *
 * <p><b>Every compensation commutes with the step it compensates.</b> A timeout is decided from a
 * clock, not ordered on the partition, so it can overtake the step it undoes. Compensations are
 * therefore addressed by transfer id, and both participants remember them even when there is
 * nothing yet to undo: a {@code STARTED} timeout sends a {@link ReleaseFunds} with no hold id, and
 * a {@code RESERVED} timeout also sends a {@link VoidCharge} to the gateway.
 *
 * <p><b>A reply is a statement of fact, not an acknowledgement.</b> Participants now answer every
 * command with what actually happened, so {@code FundsCommitted} can arrive for a saga that thought
 * it was compensating, and {@code FundsReleased} for one that thought it had charged. Both are
 * accepted and the saga finishes where the money actually is - see {@link #onFundsCommitted} and
 * {@link #onFundsReleased}.
 */
@Service
public class SagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(SagaOrchestrator.class);

    private static final String STEP_RESERVE = "ReserveFunds";
    private static final String STEP_CHARGE = "ChargeGateway";
    private static final String STEP_COMMIT = "CommitFunds";
    private static final String STEP_RELEASE = "ReleaseFunds";
    private static final String STEP_VOID = "VoidCharge";
    private static final String STEP_RECONCILE = "Reconcile";

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
        if (saga != null && ReserveRejected.TRANSFER_VOIDED.equals(event.reason())
                && settleReconciliation(saga, true, messageId, "no hold was ever taken")) {
            return;
        }
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

        emitCommit(saga, StepOutcome.STARTED);
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

    /**
     * The hold settled. Terminal COMPLETED.
     *
     * <p>Accepted in {@code COMPENSATING} as well as {@code CHARGED} (M7, Fix B). account-service
     * now answers a release that finds the hold already committed with {@code FundsCommitted} - the
     * recipient has the money, and no saga state can make that untrue. Finishing COMPENSATED here
     * would tell the sender "your money came back" about money that went to somebody else. Since
     * Fix A a saga never compensates after the pivot, so this path should only ever be reached by a
     * command this orchestrator did not send in its current state; it is logged as the
     * reconciliation case it is.
     */
    @Transactional
    public void onFundsCommitted(FundsCommitted event, UUID messageId) {
        SagaInstance saga = load(event.transferId());
        if (saga != null && settleReconciliation(saga, false, messageId, null)) {
            return;
        }
        if (saga == null
                || !expectOneOf(saga, Set.of(SagaStatus.CHARGED, SagaStatus.COMPENSATING),
                        STEP_COMMIT, messageId)) {
            return;
        }

        String detail = null;
        if (saga.getStatus() == SagaStatus.COMPENSATING) {
            log.error("saga {} was COMPENSATING but its hold had been COMMITTED - the recipient has "
                    + "the money. Finishing COMPLETED; check the PSP side of transfer {}",
                    saga.getId(), saga.getTransferId());
            detail = "hold was already committed; the release lost the race";
        }
        transfers.findById(saga.getTransferId()).ifPresent(Transfer::complete);
        finish(saga, SagaStatus.COMPLETED, null);
        recordStep(saga, STEP_COMMIT, StepOutcome.SUCCEEDED, messageId, detail);
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
        if (saga != null && settleReconciliation(saga, true, messageId,
                "hold released (" + event.reason() + ")")) {
            return;
        }
        if (saga == null
                || !expectOneOf(saga,
                        Set.of(SagaStatus.COMPENSATING, SagaStatus.RESERVED, SagaStatus.CHARGED),
                        STEP_RELEASE, messageId)) {
            return;
        }

        // M7, Fix B: the release is a fact about the hold, and it may arrive for a saga that never
        // asked for it - a replayed dead letter, a hand-produced command. The sender HAS their money
        // back, so the saga ends COMPENSATED. If it had already reached the gateway, the charge must
        // not stand either, so the PSP-side compensation goes out with it.
        String detail = event.reason();
        if (saga.getStatus() != SagaStatus.COMPENSATING) {
            log.error("saga {} was {} when its hold was RELEASED; compensating the gateway too",
                    saga.getId(), saga.getStatus());
            detail = "released while the saga was " + saga.getStatus() + ": " + event.reason();
            emitVoid(saga, event.reason());
        }
        failTransfer(saga, event.reason());
        finish(saga, SagaStatus.COMPENSATED, event.reason());
        recordStep(saga, STEP_RELEASE, StepOutcome.SUCCEEDED, messageId, detail);
    }

    /**
     * The gateway confirms it holds no money for this transfer. Recorded, and nothing more.
     *
     * <p>The void is sent by a saga that is already compensating or finished, so there is no state
     * left for this reply to move. It is handled at all because a participant always answers, and
     * the timeline should show the answer: "was the charge reversed, or pre-empted, or was there
     * nothing to reverse?" is the first question about a timed-out payment.
     */
    @Transactional
    public void onChargeVoided(ChargeVoided event, UUID messageId) {
        SagaInstance saga = load(event.transferId());
        if (saga == null) {
            return;
        }
        recordStep(saga, STEP_VOID, StepOutcome.SUCCEEDED, messageId, event.outcome());
    }

    /**
     * RECONCILIATION: finish a compensation for a transfer that has already ended FAILED or
     * COMPENSATED, when a participant still holds money the saga's verdict says it should not.
     *
     * <h3>Why this exists</h3>
     *
     * <p>Before M7's fixes, three routes stranded money after a saga had finished: a STARTED
     * timeout that sent nothing and then met a late reserve (an ACTIVE hold under a FAILED saga),
     * and a CHARGED timeout or a replayed dead letter that left the PSP holding a charge for a
     * transfer we had refunded (an APPROVED charge under a COMPENSATED saga). The fixes stop new
     * cases; they do not reach back and repair old ones, because a terminal saga is never swept.
     *
     * <h3>Why it does not break "an operator moves no money"</h3>
     *
     * <p>That rule is about <i>originating</i> a movement - choosing an amount, a source, a
     * destination. This chooses none of them. It can only be applied to a transfer the system has
     * already declared did not happen, and it can only send the compensation that verdict already
     * implies, to the participants the transfer already named, through the same idempotent
     * commands the sweeper sends. A COMPLETED transfer is refused: there, reversing anything would
     * be moving money.
     *
     * <h3>Why it is sequenced, not two commands at once</h3>
     *
     * <p>The orchestrator cannot see holds, so it cannot know whether the hold was released or -
     * the case this must never get wrong - committed. So it asks account-service first, with a
     * transfer-addressed {@link ReleaseFunds}, and lets the reply decide:
     *
     * <ul>
     *   <li>{@code FundsReleased} or {@code ReserveRejected(TRANSFER_VOIDED)} - the sender is
     *       whole, so the PSP must hold nothing either: a {@link VoidCharge} follows.</li>
     *   <li>{@code FundsCommitted} - the recipient has the money. Voiding the charge would then pay
     *       the recipient out of our own books. Nothing more is sent; the step is recorded FAILED
     *       and logged as the human decision it is.</li>
     * </ul>
     *
     * <p>Safe to repeat: every command involved is idempotent at its participant, so a second
     * request for the same transfer produces the same end state and a second pair of steps.
     */
    @Transactional
    public ReconcileOutcome reconcile(UUID transferId) {
        Optional<SagaInstance> maybe = sagas.findByTransferIdForUpdate(transferId);
        if (maybe.isEmpty()) {
            return ReconcileOutcome.NO_SUCH_TRANSFER;
        }
        SagaInstance saga = maybe.get();
        if (saga.getStatus() == SagaStatus.COMPLETED) {
            return ReconcileOutcome.COMPLETED;
        }
        if (!isFailedOrCompensated(saga)) {
            return ReconcileOutcome.STILL_IN_FLIGHT;
        }

        UUID commandId = outbox.append("Transfer", transferId, Topics.ACCOUNT_COMMANDS,
                ReleaseFunds.TYPE,
                new ReleaseFunds(transferId, saga.getHoldId(), ReleaseFunds.RECONCILIATION));
        recordStep(saga, STEP_RECONCILE, StepOutcome.STARTED, commandId,
                "operator requested; asking account-service where the money is");
        log.warn("reconciliation requested for {} saga {} (transfer {})",
                saga.getStatus(), saga.getId(), transferId);
        return ReconcileOutcome.REQUESTED;
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
                // The reserve never replied - but that does not mean it never happened, or never
                // will: the command may be in our outbox, in the topic, or in account-service's dead
                // letter table. So the saga fails AND sends the compensation, addressed by transfer
                // id because there is no hold id to quote. account-service releases the hold if the
                // reserve got there first, or remembers the void and refuses the reserve if it
                // arrives later. Either order ends with the money where the customer is told it is.
                //
                // Before M7 this branch sent nothing and said so in a comment - and chaos scenarios
                // 1, 2 and 3 each stranded money in CLEARING through exactly this gap.
                failTransfer(saga, ReleaseFunds.SAGA_TIMEOUT);
                finish(saga, SagaStatus.FAILED, "timed out before the reserve replied");
                recordStep(saga, STEP_RESERVE, StepOutcome.TIMED_OUT, null,
                        "no reply before the deadline");
                emitRelease(saga, ReleaseFunds.SAGA_TIMEOUT, StepOutcome.STARTED);
            }
            case RESERVED -> {
                // Before the pivot: recovery runs backward. The hold is released - and the gateway
                // is told too, because the ChargeGateway may yet be acted on (it may be sitting in
                // the gateway's dead letter table, which is chaos scenario 5 part B). The void
                // leaves a tombstone there if nothing has been charged yet.
                saga.transitionTo(SagaStatus.COMPENSATING);
                saga.extendDeadline(deadlineFromNow());
                recordStep(saga, STEP_CHARGE, StepOutcome.TIMED_OUT, null,
                        "no reply before the deadline");
                emitRelease(saga, ReleaseFunds.SAGA_TIMEOUT, StepOutcome.STARTED);
                emitVoid(saga, ReleaseFunds.SAGA_TIMEOUT);
            }
            case CHARGED -> {
                // AFTER THE PIVOT: forward only. The PSP has taken the money; releasing the hold
                // now would refund the sender out of our own books while the charge stands - which
                // is exactly what chaos scenario 2 caught this branch doing before M7. The only
                // correct outcome is the one already decided, so re-send the commit. It is safe to
                // repeat: account-service answers a commit on a settled hold with FundsCommitted.
                //
                // Not capped by maxSweepAttempts (see claimExpired): a forward step that must
                // eventually succeed has no alternative to give up in favour of. The deadline is
                // pushed out instead, so a participant that is down for an hour receives one
                // commit per step-timeout rather than one per sweep. Past the cap it is still
                // retried, and it becomes an ERROR - that is the alert.
                saga.extendDeadline(deadlineFromNow());
                if (saga.getSweepAttempts() > properties.maxSweepAttempts()) {
                    log.error("saga {} (transfer {}) is CHARGED and has re-sent CommitFunds {} "
                            + "times; the PSP has the money and account-service is not settling",
                            saga.getId(), saga.getTransferId(), saga.getSweepAttempts());
                }
                emitCommit(saga, StepOutcome.TIMED_OUT);
            }
            case COMPENSATING -> {
                // A release was already sent and its reply has not come either. Re-emit it: the
                // participant's inbox absorbs the repeat, and since M7 a release that finds the hold
                // already settled answers with how it settled. The deadline moves out so the budget
                // is maxSweepAttempts x step-timeout - long enough to outlast a restart - rather than
                // maxSweepAttempts x sweep-interval, which chaos scenario 2 spent entirely while the
                // participant was still booting.
                saga.extendDeadline(deadlineFromNow());
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

    /** {@link #expect}, for a reply that more than one state can truthfully receive. */
    private boolean expectOneOf(SagaInstance saga, Set<SagaStatus> allowed, String step,
                                UUID messageId) {
        if (allowed.contains(saga.getStatus())) {
            return true;
        }
        log.info("saga {} is {} but the reply expected one of {}; skipping",
                saga.getId(), saga.getStatus(), allowed);
        recordStep(saga, step, StepOutcome.SKIPPED, messageId,
                "arrived while the saga was " + saga.getStatus());
        return false;
    }

    /**
     * Handles account-service's answer to a reconciliation's release, if one is outstanding.
     * Returns {@code false} - touching nothing - when this reply is not that answer, so the
     * caller's ordinary handling runs.
     *
     * <p>"Outstanding" is counted from the steps rather than stored in a column: a reconcile
     * STARTED row with no SUCCEEDED/FAILED row to match it. Counting rather than ordering, because
     * the request and its reply can be written in the same microsecond.
     *
     * @param senderWhole true when the reply proves the sender has their money (released, or never
     *                    reserved); false when it proves the recipient does
     */
    private boolean settleReconciliation(SagaInstance saga, boolean senderWhole, UUID messageId,
                                         String detail) {
        if (!isFailedOrCompensated(saga)) {
            return false;
        }
        long asked = steps.countBySagaIdAndStepNameAndOutcome(saga.getId(), STEP_RECONCILE,
                StepOutcome.STARTED);
        long answered = steps.countBySagaIdAndStepNameAndOutcome(saga.getId(), STEP_RECONCILE,
                StepOutcome.SUCCEEDED)
                + steps.countBySagaIdAndStepNameAndOutcome(saga.getId(), STEP_RECONCILE,
                StepOutcome.FAILED);
        if (asked <= answered) {
            return false;
        }

        if (senderWhole) {
            emitVoid(saga, ReleaseFunds.RECONCILIATION);
            recordStep(saga, STEP_RECONCILE, StepOutcome.SUCCEEDED, messageId,
                    detail + "; sender is whole, gateway told to void");
        } else {
            log.error("reconciling {} saga {} (transfer {}) found its hold COMMITTED - the "
                    + "recipient has the money. The PSP charge is left standing; this transfer "
                    + "needs a person, not a compensation", saga.getStatus(), saga.getId(),
                    saga.getTransferId());
            recordStep(saga, STEP_RECONCILE, StepOutcome.FAILED, messageId,
                    "hold was COMMITTED: recipient has the money; charge NOT voided");
        }
        return true;
    }

    private static boolean isFailedOrCompensated(SagaInstance saga) {
        return saga.getStatus() == SagaStatus.FAILED
                || saga.getStatus() == SagaStatus.COMPENSATED;
    }

    private void emitCommit(SagaInstance saga, StepOutcome outcome) {
        Transfer transfer = transfers.findById(saga.getTransferId()).orElseThrow();
        UUID commandId = outbox.append("Transfer", saga.getTransferId(), Topics.ACCOUNT_COMMANDS,
                CommitFunds.TYPE,
                new CommitFunds(saga.getTransferId(), saga.getHoldId(),
                        transfer.getToAccountId()));
        recordStep(saga, STEP_COMMIT, outcome, commandId,
                outcome == StepOutcome.TIMED_OUT ? "no reply before the deadline; re-sent" : null);
    }

    /**
     * The gateway-side compensation. Sent whenever a saga gives up after ChargeGateway may have
     * gone out, whether or not the charge happened - the gateway reverses it, or leaves a tombstone
     * that refuses it later.
     */
    private void emitVoid(SagaInstance saga, String reason) {
        Transfer transfer = transfers.findById(saga.getTransferId()).orElseThrow();
        UUID commandId = outbox.append("Transfer", saga.getTransferId(), Topics.GATEWAY_COMMANDS,
                VoidCharge.TYPE,
                new VoidCharge(saga.getTransferId(), transfer.getAmountMinor(),
                        transfer.getCurrency(), reason));
        recordStep(saga, STEP_VOID, StepOutcome.STARTED, commandId, reason);
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
