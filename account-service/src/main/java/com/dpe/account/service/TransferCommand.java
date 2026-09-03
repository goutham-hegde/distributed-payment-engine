package com.dpe.account.service;

import java.util.UUID;

/**
 * A request to move money between two accounts in this service.
 *
 * @param transferId    caller-supplied correlation id. Owned by payment-orchestrator from M3
 *                      onward; it is what the UNIQUE constraint on {@code ledger_entries} keys
 *                      on, so the same id replayed cannot post the same leg twice.
 * @param fromAccountId account to debit
 * @param toAccountId   account to credit
 * @param amountMinor   a POSITIVE magnitude in minor units (paise). The signs are applied when
 *                      the ledger entries are built, so a caller cannot pass a negative amount
 *                      and quietly invert the direction of the transfer.
 * @param currency      ISO-4217 code; must match both accounts
 */
public record TransferCommand(
        UUID transferId,
        UUID fromAccountId,
        UUID toAccountId,
        long amountMinor,
        String currency) {
}
