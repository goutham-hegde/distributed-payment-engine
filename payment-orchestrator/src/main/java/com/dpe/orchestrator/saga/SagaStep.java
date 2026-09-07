package com.dpe.orchestrator.saga;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.hibernate.annotations.CreationTimestamp;

/**
 * One thing that happened to a saga. Append-only, like {@code ledger_entries}.
 *
 * <p>Two rows per step: one when the command is issued ({@link StepOutcome#STARTED}) and one when
 * its reply lands. The gap between them is the step latency, and a step with only the first row
 * is a step whose reply never came - which makes "where did this saga stall" a query rather than
 * an investigation.
 */
@Entity
@Table(name = "saga_steps")
public class SagaStep {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false, updatable = false)
    private Long id;

    @Column(name = "saga_id", nullable = false, updatable = false)
    private UUID sagaId;

    @Column(name = "step_name", nullable = false, updatable = false, length = 64)
    private String stepName;

    @Enumerated(EnumType.STRING)
    @Column(name = "outcome", nullable = false, updatable = false, length = 12)
    private StepOutcome outcome;

    @Enumerated(EnumType.STRING)
    @Column(name = "to_status", updatable = false, length = 12)
    private SagaStatus toStatus;

    /**
     * The message that caused this step - an outbox row id in this database, or an inbox row id
     * that came from another service's. It is the thread that stitches three services' tables
     * together during an incident.
     */
    @Column(name = "message_id", updatable = false)
    private UUID messageId;

    @Column(name = "detail", updatable = false)
    private String detail;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    protected SagaStep() {
        // for JPA
    }

    public SagaStep(UUID sagaId, String stepName, StepOutcome outcome, SagaStatus toStatus,
                    UUID messageId, String detail) {
        this.sagaId = sagaId;
        this.stepName = stepName;
        this.outcome = outcome;
        this.toStatus = toStatus;
        this.messageId = messageId;
        this.detail = detail;
    }

    public Long getId() {
        return id;
    }

    public UUID getSagaId() {
        return sagaId;
    }

    public String getStepName() {
        return stepName;
    }

    public StepOutcome getOutcome() {
        return outcome;
    }

    public SagaStatus getToStatus() {
        return toStatus;
    }

    public UUID getMessageId() {
        return messageId;
    }

    public String getDetail() {
        return detail;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }
}
