package com.dpe.account.service;

import java.util.UUID;

/**
 * The debit side does not have enough money. This is an expected business outcome, not a fault -
 * it maps to 422, and from M3 it is what makes the saga compensate rather than retry.
 */
public class InsufficientFundsException extends RuntimeException {

    private final UUID accountId;
    private final long balanceMinor;
    private final long requestedMinor;

    public InsufficientFundsException(UUID accountId, long balanceMinor, long requestedMinor) {
        super("insufficient funds in " + accountId + ": balance=" + balanceMinor
                + " requested=" + requestedMinor);
        this.accountId = accountId;
        this.balanceMinor = balanceMinor;
        this.requestedMinor = requestedMinor;
    }

    public UUID getAccountId() {
        return accountId;
    }

    public long getBalanceMinor() {
        return balanceMinor;
    }

    public long getRequestedMinor() {
        return requestedMinor;
    }
}
