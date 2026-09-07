package com.dpe.gateway.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.hibernate.annotations.CreationTimestamp;

/**
 * One charge attempt against the simulated PSP.
 *
 * <p>Immutable once written. A charge is a record of something that happened at an external
 * party, and rewriting it would be rewriting history that this service does not own.
 */
@Entity
@Table(name = "gateway_charges")
public class GatewayCharge {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    /**
     * The saga's correlation id, and UNIQUE in the schema.
     *
     * <p>That constraint is the most important line in this service. An external charge is the
     * one step in the whole system that cannot be undone by writing an opposite row, so the
     * protection against a redelivered command has to be structural rather than a promise made
     * by the caller.
     */
    @Column(name = "transfer_id", nullable = false, updatable = false)
    private UUID transferId;

    @Column(name = "amount_minor", nullable = false, updatable = false)
    private long amountMinor;

    @Column(name = "currency", nullable = false, updatable = false, length = 3)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, updatable = false, length = 8)
    private ChargeStatus status;

    @Column(name = "decline_reason", updatable = false, length = 64)
    private String declineReason;

    @Column(name = "latency_ms", updatable = false)
    private Integer latencyMs;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    protected GatewayCharge() {
        // for JPA
    }

    private GatewayCharge(UUID id, UUID transferId, long amountMinor, String currency,
                          ChargeStatus status, String declineReason, Integer latencyMs) {
        this.id = id;
        this.transferId = transferId;
        this.amountMinor = amountMinor;
        this.currency = currency;
        this.status = status;
        this.declineReason = declineReason;
        this.latencyMs = latencyMs;
    }

    public static GatewayCharge approved(UUID id, UUID transferId, long amountMinor,
                                         String currency, int latencyMs) {
        return new GatewayCharge(id, transferId, amountMinor, currency,
                ChargeStatus.APPROVED, null, latencyMs);
    }

    public static GatewayCharge declined(UUID id, UUID transferId, long amountMinor,
                                         String currency, String reason, int latencyMs) {
        if (reason == null || reason.isBlank()) {
            // Mirrors gateway_charges_decline_has_reason. Failing here gives a stack trace that
            // names the call site; failing at flush time gives a constraint name and nothing else.
            throw new IllegalArgumentException("a decline must carry a reason");
        }
        return new GatewayCharge(id, transferId, amountMinor, currency,
                ChargeStatus.DECLINED, reason, latencyMs);
    }

    public boolean isApproved() {
        return status == ChargeStatus.APPROVED;
    }

    public UUID getId() {
        return id;
    }

    public UUID getTransferId() {
        return transferId;
    }

    public long getAmountMinor() {
        return amountMinor;
    }

    public String getCurrency() {
        return currency;
    }

    public ChargeStatus getStatus() {
        return status;
    }

    public String getDeclineReason() {
        return declineReason;
    }

    public Integer getLatencyMs() {
        return latencyMs;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }
}
