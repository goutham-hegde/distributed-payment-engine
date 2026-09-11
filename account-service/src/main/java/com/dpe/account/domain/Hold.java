package com.dpe.account.domain;

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
 * Money set aside for a transfer that has not settled yet - the auth half of auth-then-capture.
 *
 * <p>A hold is not a promise about a balance; the money has genuinely moved already. The reserve
 * debited the sender and credited the CLEARING account, and this row records that the resulting
 * balance in CLEARING belongs to one specific transfer and is owed either forward to the
 * recipient or back to the sender.
 *
 * <p>Unlike {@link LedgerEntry}, a hold IS mutable - its status changes exactly once, from ACTIVE
 * to a terminal state. The ledger entries that settle it are still append-only; this row is the
 * index into them.
 *
 * <p>The id is assigned by the application rather than generated, for the same reason the outbox
 * id is: it has to exist before the INSERT because it travels to the orchestrator in
 * {@code FundsReserved} and comes back in {@code CommitFunds} / {@code ReleaseFunds}.
 */
@Entity
@Table(name = "holds")
public class Hold {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    /** Owned by payment-orchestrator; no foreign key across a service boundary. */
    @Column(name = "transfer_id", nullable = false, updatable = false)
    private UUID transferId;

    /** The account the money came FROM. The money itself is in CLEARING. */
    @Column(name = "account_id", nullable = false, updatable = false)
    private UUID accountId;

    /**
     * A positive magnitude, unlike {@link LedgerEntry#getAmountMinor()} which is signed. A hold
     * has no direction to encode, and invariant I3 sums this column directly - a signed value
     * would make that sum meaningless.
     */
    @Column(name = "amount_minor", nullable = false, updatable = false)
    private long amountMinor;

    @Column(name = "currency", nullable = false, updatable = false, length = 3)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 9)
    private HoldStatus status;

    /**
     * Why a RELEASED hold was released. Set once, with the status. Kept on the row so a repeated
     * or late question about this hold is answered with the original reason, not with whichever
     * one the repeat happened to quote (M7, Fix B).
     */
    @Column(name = "release_reason", length = 32)
    private String releaseReason;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    protected Hold() {
        // for JPA
    }

    public Hold(UUID id, UUID transferId, UUID accountId, long amountMinor, String currency) {
        if (amountMinor <= 0) {
            throw new IllegalArgumentException("hold amount must be positive: " + amountMinor);
        }
        this.id = id;
        this.transferId = transferId;
        this.accountId = accountId;
        this.amountMinor = amountMinor;
        this.currency = currency;
        this.status = HoldStatus.ACTIVE;
    }

    /**
     * Marks this hold settled in favour of the recipient.
     *
     * <p>Throws if the hold is already terminal rather than silently doing nothing. A
     * redelivered {@code CommitFunds} should be stopped by the inbox before it ever reaches
     * here, so arriving at this method with a settled hold means the gate above it did not
     * fire - and a silent no-op would hide that. The definitive protection is still the UNIQUE
     * constraint on {@code (transfer_id, clearing, DEBIT)}, which no in-memory check can race.
     */
    public void commit() {
        requireActive("commit");
        this.status = HoldStatus.COMMITTED;
    }

    /** Marks this hold compensated: the money goes back to {@link #getAccountId()}. */
    public void release(String reason) {
        requireActive("release");
        this.status = HoldStatus.RELEASED;
        this.releaseReason = reason;
    }

    /** True while this hold still counts toward invariant I3. */
    public boolean isActive() {
        return status == HoldStatus.ACTIVE;
    }

    private void requireActive(String action) {
        if (status != HoldStatus.ACTIVE) {
            throw new IllegalStateException(
                    "cannot " + action + " hold " + id + ": status is " + status);
        }
    }

    public UUID getId() {
        return id;
    }

    public UUID getTransferId() {
        return transferId;
    }

    public UUID getAccountId() {
        return accountId;
    }

    public long getAmountMinor() {
        return amountMinor;
    }

    public String getCurrency() {
        return currency;
    }

    public HoldStatus getStatus() {
        return status;
    }

    public String getReleaseReason() {
        return releaseReason;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    public OffsetDateTime getUpdatedAt() {
        return updatedAt;
    }
}
