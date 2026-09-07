package com.dpe.orchestrator.saga;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

/**
 * Where one transfer is, right now.
 *
 * <p>This row is the answer to "what is happening to transfer X", and having that answer in a
 * single indexed row is the reason this system uses orchestration rather than choreography. In a
 * choreographed design the same question requires reading the logs of every participant and
 * reconstructing the sequence.
 */
@Entity
@Table(name = "saga_instances")
public class SagaInstance {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "transfer_id", nullable = false, updatable = false)
    private UUID transferId;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 12)
    private SagaStatus status;

    /** Learned from FundsReserved. Null until the reserve replies. */
    @Column(name = "hold_id")
    private UUID holdId;

    @Column(name = "gateway_charge_id", length = 64)
    private String gatewayChargeId;

    /**
     * When this saga stops being merely slow and starts being stuck.
     *
     * <p>Set once, at creation, from {@code dpe.saga.step-timeout}. It is not extended on each
     * step, and that is a deliberate simplification worth being able to defend: a per-step
     * deadline would be more precise but needs resetting on every transition, and a missed reset
     * produces a saga that can never time out - the exact failure the deadline exists to prevent.
     * One deadline for the whole saga fails safe.
     */
    @Column(name = "deadline_at", nullable = false)
    private OffsetDateTime deadlineAt;

    @Column(name = "timed_out_at")
    private OffsetDateTime timedOutAt;

    @Column(name = "sweep_attempts", nullable = false)
    private int sweepAttempts;

    @Column(name = "failure_reason")
    private String failureReason;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    /**
     * Set exactly when {@link #status} becomes terminal, and enforced by the
     * {@code saga_completed_at_iff_terminal} CHECK constraint - so a transition that forgets it
     * fails at flush time instead of leaving a finished saga that looks like it is still running.
     */
    @Column(name = "completed_at")
    private OffsetDateTime completedAt;

    protected SagaInstance() {
        // for JPA
    }

    public SagaInstance(UUID id, UUID transferId, OffsetDateTime deadlineAt) {
        this.id = id;
        this.transferId = transferId;
        this.status = SagaStatus.STARTED;
        this.deadlineAt = deadlineAt;
        this.sweepAttempts = 0;
    }

    /**
     * Moves to a non-terminal state.
     *
     * <p>Rejects a terminal target so that a caller cannot finish a saga without recording when.
     * Use {@link #finish} for that.
     */
    public void transitionTo(SagaStatus next) {
        if (next.isTerminal()) {
            throw new IllegalArgumentException(
                    "use finish() for terminal state " + next + " so completedAt is set");
        }
        this.status = next;
    }

    /** Moves to a terminal state and stamps {@code completed_at} with it, in one operation. */
    public void finish(SagaStatus terminal, String failureReason) {
        if (!terminal.isTerminal()) {
            throw new IllegalArgumentException(terminal + " is not a terminal state");
        }
        this.status = terminal;
        this.failureReason = failureReason;
        this.completedAt = OffsetDateTime.now();
    }

    /** True if this saga has run past its deadline and is not already finished. */
    public boolean isExpiredAt(Instant now) {
        return !status.isTerminal() && deadlineAt.toInstant().isBefore(now);
    }

    public void recordSweepAttempt() {
        this.sweepAttempts++;
        if (this.timedOutAt == null) {
            this.timedOutAt = OffsetDateTime.now();
        }
    }

    public void setHoldId(UUID holdId) {
        this.holdId = holdId;
    }

    public void setGatewayChargeId(String gatewayChargeId) {
        this.gatewayChargeId = gatewayChargeId;
    }

    public UUID getId() {
        return id;
    }

    public UUID getTransferId() {
        return transferId;
    }

    public SagaStatus getStatus() {
        return status;
    }

    public UUID getHoldId() {
        return holdId;
    }

    public String getGatewayChargeId() {
        return gatewayChargeId;
    }

    public OffsetDateTime getDeadlineAt() {
        return deadlineAt;
    }

    public OffsetDateTime getTimedOutAt() {
        return timedOutAt;
    }

    public int getSweepAttempts() {
        return sweepAttempts;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public OffsetDateTime getCompletedAt() {
        return completedAt;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    public OffsetDateTime getUpdatedAt() {
        return updatedAt;
    }
}
