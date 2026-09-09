package com.dpe.orchestrator.transfer;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

/**
 * What the caller asked for, and how it ended.
 *
 * <p>Separate from {@code SagaInstance} even though there is exactly one saga per transfer. The
 * two change for different reasons and are read by different people: this is the API contract,
 * that is coordination machinery. Merging them would leak states like RESERVED and CHARGED into
 * the public response and freeze them there.
 */
@Entity
@Table(name = "transfers")
public class Transfer {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "from_account_id", nullable = false, updatable = false)
    private UUID fromAccountId;

    @Column(name = "to_account_id", nullable = false, updatable = false)
    private UUID toAccountId;

    /** Minor units (paise). Never a float. */
    @Column(name = "amount_minor", nullable = false, updatable = false)
    private long amountMinor;

    @Column(name = "currency", nullable = false, updatable = false, length = 3)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 9)
    private TransferStatus status;

    @Column(name = "failure_reason")
    private String failureReason;

    /**
     * The authenticated subject that asked for this transfer (M5).
     *
     * <p>{@code updatable = false}: who asked is a fact about the request, decided once. A column
     * that can be rewritten is not an audit trail, and this one is read to decide whether a
     * caller may see the transfer at all.
     *
     * <p>Nullable only because rows written before M5 have no identity to record - see
     * {@code V5__authorization.sql}. Every row created from here on has one.
     */
    @Column(name = "initiated_by", updatable = false, length = 64)
    private String initiatedBy;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    protected Transfer() {
        // for JPA
    }

    public Transfer(UUID id, UUID fromAccountId, UUID toAccountId, long amountMinor,
                    String currency, String initiatedBy) {
        this.id = id;
        this.fromAccountId = fromAccountId;
        this.toAccountId = toAccountId;
        this.amountMinor = amountMinor;
        this.currency = currency;
        this.status = TransferStatus.PENDING;
        this.initiatedBy = initiatedBy;
    }

    public void complete() {
        this.status = TransferStatus.COMPLETED;
        this.failureReason = null;
    }

    public void fail(String reason) {
        this.status = TransferStatus.FAILED;
        this.failureReason = reason;
    }

    public UUID getId() {
        return id;
    }

    public UUID getFromAccountId() {
        return fromAccountId;
    }

    public UUID getToAccountId() {
        return toAccountId;
    }

    public long getAmountMinor() {
        return amountMinor;
    }

    public String getCurrency() {
        return currency;
    }

    public TransferStatus getStatus() {
        return status;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public String getInitiatedBy() {
        return initiatedBy;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    public OffsetDateTime getUpdatedAt() {
        return updatedAt;
    }
}
