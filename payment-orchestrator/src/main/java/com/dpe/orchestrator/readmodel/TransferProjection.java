package com.dpe.orchestrator.readmodel;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * What this service believes about a transfer, built from the events account-service published.
 *
 * <p>Read-only from the application's point of view: it is written by the upsert in
 * {@link TransferProjectionRepository#recordCompleted}, never by JPA dirty checking. Mapped as an
 * entity purely so queries and tests can read it back with types.
 */
@Entity
@Table(name = "transfer_projection")
public class TransferProjection {

    @Id
    @Column(name = "transfer_id", nullable = false, updatable = false)
    private UUID transferId;

    @Column(name = "from_account_id", nullable = false)
    private UUID fromAccountId;

    @Column(name = "to_account_id", nullable = false)
    private UUID toAccountId;

    @Column(name = "amount_minor", nullable = false)
    private long amountMinor;

    @Column(name = "currency", nullable = false, length = 3)
    private String currency;

    @Column(name = "status", nullable = false, length = 16)
    private String status;

    /**
     * How many times a message for this transfer has been applied. <b>Must be 1.</b> A 2 means a
     * duplicate delivery slipped past the inbox, and is the assertion that makes the dedup test
     * meaningful rather than vacuous.
     */
    @Column(name = "apply_count", nullable = false)
    private int applyCount;

    @Column(name = "first_seen_at", nullable = false)
    private OffsetDateTime firstSeenAt;

    @Column(name = "last_seen_at", nullable = false)
    private OffsetDateTime lastSeenAt;

    protected TransferProjection() {
        // for JPA
    }

    public UUID getTransferId() {
        return transferId;
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

    public String getStatus() {
        return status;
    }

    public int getApplyCount() {
        return applyCount;
    }

    public OffsetDateTime getFirstSeenAt() {
        return firstSeenAt;
    }

    public OffsetDateTime getLastSeenAt() {
        return lastSeenAt;
    }
}
