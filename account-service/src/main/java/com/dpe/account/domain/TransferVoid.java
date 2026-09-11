package com.dpe.account.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.hibernate.annotations.CreationTimestamp;

/**
 * A tombstone: the saga for this transfer gave up before any money was reserved for it, so none
 * may be reserved for it now.
 *
 * <p>The row exists so that a compensation and the step it compensates <b>commute</b>. The
 * orchestrator's timeout is decided from a clock rather than ordered on the partition, so its
 * {@code ReleaseFunds} can arrive here before the {@code ReserveFunds} it is undoing. Without a
 * memory of the release, the late reserve would be obeyed and its money stranded in CLEARING
 * under a saga that has already told the customer "failed". See {@code V7__transfer_voids.sql}.
 */
@Entity
@Table(name = "transfer_voids")
public class TransferVoid {

    @Id
    @Column(name = "transfer_id", nullable = false, updatable = false)
    private UUID transferId;

    @Column(name = "reason", nullable = false, updatable = false, length = 32)
    private String reason;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    protected TransferVoid() {
        // for JPA
    }

    public TransferVoid(UUID transferId, String reason) {
        this.transferId = transferId;
        this.reason = reason;
    }

    public UUID getTransferId() {
        return transferId;
    }

    public String getReason() {
        return reason;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }
}
