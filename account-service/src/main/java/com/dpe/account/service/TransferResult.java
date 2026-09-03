package com.dpe.account.service;

import java.util.UUID;

/**
 * Balances as of the committing transaction. Returned rather than re-read, so the caller sees
 * the values that were actually written under the lock.
 */
public record TransferResult(
        UUID transferId,
        UUID fromAccountId,
        long fromBalanceMinor,
        UUID toAccountId,
        long toBalanceMinor) {
}
