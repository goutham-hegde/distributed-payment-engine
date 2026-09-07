package com.dpe.events;

import java.util.UUID;

/**
 * Reply to {@link ReserveFunds}: the money has left the sender and is sitting in a hold.
 *
 * <p>Past tense, and it is a fact rather than a promise - by the time this is published the
 * ledger entries and the hold row are committed. That is guaranteed by the outbox: this message
 * was written as a row in the same transaction as the postings it describes, so there is no
 * reachable state in which the orchestrator has been told about a reserve that did not happen.
 *
 * @param holdId the hold just created. The orchestrator stores it on the saga and quotes it back
 *               in {@link CommitFunds} or {@link ReleaseFunds}.
 *               <p>Addressing the hold explicitly, rather than letting account-service look it
 *               up by transfer id, is what makes the later commands unambiguous: the id names
 *               one row, so a command cannot settle a different hold if a transfer ever grows a
 *               second one.
 * @param availableMinor the sender's balance after the debit. Not needed by the saga; carried
 *                       because it makes the event self-describing in a log, and because a read
 *                       model can update from it without a second call.
 */
public record FundsReserved(
        UUID transferId,
        UUID holdId,
        UUID fromAccountId,
        UUID toAccountId,
        long amountMinor,
        String currency,
        long availableMinor) {

    public static final String TYPE = "FundsReserved";
}
