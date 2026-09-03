package com.dpe.account.domain;

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
 * One half of a double-entry posting. Append-only: there are no setters, and nothing in this
 * codebase may UPDATE or DELETE a row of this table.
 */
@Entity
@Table(name = "ledger_entries")
public class LedgerEntry {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false, updatable = false)
    private Long id;

    /** Correlates the two legs. Owned by payment-orchestrator; no foreign key across services. */
    @Column(name = "transfer_id", nullable = false, updatable = false)
    private UUID transferId;

    @Column(name = "account_id", nullable = false, updatable = false)
    private UUID accountId;

    /** Signed minor units: negative for a debit, positive for a credit. */
    @Column(name = "amount_minor", nullable = false, updatable = false)
    private long amountMinor;

    @Enumerated(EnumType.STRING)
    @Column(name = "entry_type", nullable = false, updatable = false, length = 6)
    private EntryType entryType;

    @Column(name = "currency", nullable = false, updatable = false, length = 3)
    private String currency;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    protected LedgerEntry() {
        // for JPA
    }

    private LedgerEntry(UUID transferId, UUID accountId, long amountMinor, EntryType entryType,
                        String currency) {
        this.transferId = transferId;
        this.accountId = accountId;
        this.amountMinor = amountMinor;
        this.entryType = entryType;
        this.currency = currency;
    }

    /**
     * @param amountMinor a positive magnitude; it is stored negated.
     */
    public static LedgerEntry debit(UUID transferId, UUID accountId, long amountMinor,
                                    String currency) {
        requirePositive(amountMinor);
        return new LedgerEntry(transferId, accountId, -amountMinor, EntryType.DEBIT, currency);
    }

    /**
     * @param amountMinor a positive magnitude; it is stored as-is.
     */
    public static LedgerEntry credit(UUID transferId, UUID accountId, long amountMinor,
                                     String currency) {
        requirePositive(amountMinor);
        return new LedgerEntry(transferId, accountId, amountMinor, EntryType.CREDIT, currency);
    }

    private static void requirePositive(long amountMinor) {
        if (amountMinor <= 0) {
            throw new IllegalArgumentException("amount must be a positive magnitude: " + amountMinor);
        }
    }

    public Long getId() {
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

    public EntryType getEntryType() {
        return entryType;
    }

    public String getCurrency() {
        return currency;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }
}
