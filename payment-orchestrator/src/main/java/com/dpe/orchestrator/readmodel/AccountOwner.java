package com.dpe.orchestrator.readmodel;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * One row of the ownership projection: this account belongs to this subject.
 *
 * <p>A copy of a fact account-service owns, kept locally so the API edge can answer "may you
 * spend from here" without a synchronous call into another service - see
 * {@code V5__authorization.sql} for the full argument, and {@link com.dpe.events.AccountOpened}
 * for why staleness can only fail closed.
 *
 * <p>Read-only in every sense that matters: nothing in this service writes it except the handler
 * that consumes the event, and there are no setters. A projection with mutators invites code that
 * "corrects" it locally, at which point the two systems disagree and the copy is no longer a
 * projection of anything.
 *
 * <p>Note what is deliberately not here: a balance. It would be easy to project one and tempting
 * to check affordability at the edge - and it would be wrong, because a balance is stale the
 * instant it is copied, and the ledger's own constraint is the only thing entitled to decide
 * whether money can be spent.
 */
@Entity
@Table(name = "account_owners")
public class AccountOwner {

    /** CUSTOMER accounts are the only ones an API caller may name as a source. */
    public static final String TYPE_CUSTOMER = "CUSTOMER";

    @Id
    @Column(name = "account_id", nullable = false, updatable = false)
    private UUID accountId;

    @Column(name = "owner_id", nullable = false, updatable = false, length = 64)
    private String ownerId;

    @Column(name = "account_type", nullable = false, updatable = false, length = 16)
    private String accountType;

    @Column(name = "currency", nullable = false, updatable = false, length = 3)
    private String currency;

    @Column(name = "projected_at", nullable = false, updatable = false)
    private OffsetDateTime projectedAt;

    protected AccountOwner() {
        // for JPA
    }

    public UUID getAccountId() {
        return accountId;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public String getAccountType() {
        return accountType;
    }

    public String getCurrency() {
        return currency;
    }

    public OffsetDateTime getProjectedAt() {
        return projectedAt;
    }

    /** True for an ordinary customer account - not the SYSTEM issuance or CLEARING account. */
    public boolean isCustomerAccount() {
        return TYPE_CUSTOMER.equals(accountType);
    }
}
