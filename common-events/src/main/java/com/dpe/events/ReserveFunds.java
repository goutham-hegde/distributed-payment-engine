package com.dpe.events;

import java.util.UUID;

/**
 * Step 1 of the saga: take the money from the sender and hold it.
 *
 * <p>A <b>command</b>, not an event, and the grammar carries the meaning. It is imperative
 * ("reserve"), it is addressed to exactly one service, and it may be refused - account-service
 * replies {@link FundsReserved} or {@link ReserveRejected}. An event would be past tense, would
 * state something already true, and could not be refused by anybody.
 *
 * <p>Commands travel on {@link Topics#ACCOUNT_COMMANDS}, separate from the events topic, so that
 * a future subscriber can consume the facts account-service publishes without also receiving
 * instructions it has no business acting on.
 *
 * <h2>Why reserve is the first step, and not the gateway charge</h2>
 *
 * <p>Saga steps must be ordered with the hard-to-undo action LAST. Reserving funds is trivially
 * undoable - {@link ReleaseFunds} writes an opposite pair of ledger entries. Charging an external
 * PSP is not: the money has left the building and only a refund brings it back. So the order is
 * take-then-charge, and a decline compensates a hold rather than chasing a refund.
 *
 * @param transferId   the saga's correlation id, and the Kafka partition key. Every message in
 *                     this saga carries it, so all of them land on one partition and are
 *                     delivered in the order the relay sent them.
 * @param fromAccountId the sender. Debited by the amount; this is the account whose balance must
 *                      cover it and whose I5 constraint enforces that.
 * @param toAccountId   the recipient. Deliberately carried on the RESERVE, not just on the
 *                      commit: account-service validates the recipient exists and the currencies
 *                      agree before it takes anybody's money, so a transfer to a nonexistent
 *                      account fails without ever creating a hold to compensate.
 * @param amountMinor   a positive magnitude in minor units (paise). Direction is carried by the
 *                      from/to fields, never by the sign.
 */
public record ReserveFunds(
        UUID transferId,
        UUID fromAccountId,
        UUID toAccountId,
        long amountMinor,
        String currency) {

    public static final String TYPE = "ReserveFunds";
}
