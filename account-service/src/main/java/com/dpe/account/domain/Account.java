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
 * An account balance.
 *
 * <p>Note what is deliberately absent: there is no {@code @Version} column. We lock
 * pessimistically ({@code SELECT ... FOR UPDATE}) because account rows are genuinely hot, and
 * optimistic retries degrade badly under contention. Adding a version column as well would
 * mask a missing {@code FOR UPDATE} instead of exposing it - and the tests in this milestone
 * exist precisely to expose that.
 */
@Entity
@Table(name = "accounts")
public class Account {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "owner_id", nullable = false, length = 64)
    private String ownerId;

    @Enumerated(EnumType.STRING)
    @Column(name = "account_type", nullable = false, length = 8)
    private AccountType accountType;

    @Column(name = "currency", nullable = false, length = 3)
    private String currency;

    /**
     * Balance in minor units (paise). Denormalized cache of this account's ledger entries;
     * it must only ever be written in the same transaction that writes those entries.
     */
    @Column(name = "balance_minor", nullable = false)
    private long balanceMinor;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    protected Account() {
        // for JPA
    }

    public Account(UUID id, String ownerId, String currency, long balanceMinor) {
        this(id, ownerId, AccountType.CUSTOMER, currency, balanceMinor);
    }

    public Account(UUID id, String ownerId, AccountType accountType, String currency,
                   long balanceMinor) {
        this.id = id;
        this.ownerId = ownerId;
        this.accountType = accountType;
        this.currency = currency;
        this.balanceMinor = balanceMinor;
    }

    public UUID getId() {
        return id;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public AccountType getAccountType() {
        return accountType;
    }

    public String getCurrency() {
        return currency;
    }

    public long getBalanceMinor() {
        return balanceMinor;
    }

    /**
     * Applies a signed delta to the cached balance.
     *
     * <p>Signed, so that the caller uses the same number it puts in the ledger entry and the two
     * cannot drift by a sign error. A negative result is still rejected by the
     * {@code accounts_balance_non_negative} CHECK constraint at flush time.
     */
    public void applyDelta(long deltaMinor) {
        this.balanceMinor += deltaMinor;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    public OffsetDateTime getUpdatedAt() {
        return updatedAt;
    }
}
