package com.dpe.events;

import java.util.UUID;

/**
 * THE GATEWAY'S COMPENSATION. Make sure the PSP holds no money for this transfer - whether or not
 * it has charged yet.
 *
 * <p>Issued when a saga compensates from {@code RESERVED} on a timeout: the orchestrator is
 * releasing the hold, so the customer is being given their money back, and the charge it asked
 * for must not stand. It does not know whether the {@link ChargeGateway} was ever acted on - that
 * is what a timeout means - so this command has to be correct in both orders:
 *
 * <ul>
 *   <li><b>The charge already happened.</b> The gateway reverses it (an authorization void) and
 *       the charge row ends {@code VOIDED}.</li>
 *   <li><b>The charge has not happened yet</b> - the command is still in the topic, or sitting in
 *       the dead letter table waiting for an operator to replay it. The gateway writes a
 *       {@code VOIDED} row for the transfer anyway, a tombstone, and the UNIQUE constraint on
 *       {@code gateway_charges.transfer_id} then makes the late charge structurally impossible:
 *       it finds the row, and answers with the outcome already recorded.</li>
 * </ul>
 *
 * <p>That is what it means for a compensation to <b>commute</b> with the step it compensates.
 * The timeout is decided from a clock, not ordered on the partition, so it can overtake the step
 * it undoes; the only safe compensation is one that gives the same answer whichever lands first.
 * M7's chaos scenario 5 part B is the run that proved the need: a dead-lettered charge replayed
 * after the saga had compensated charged ten customers who had already been refunded.
 *
 * <p>Addressed by transfer id, never by charge id, because in the second case there is no charge
 * id to name. The amount and currency are carried so the tombstone can satisfy the same NOT NULL
 * and CHECK constraints as a real charge row.
 */
public record VoidCharge(
        UUID transferId,
        long amountMinor,
        String currency,
        String reason) {

    public static final String TYPE = "VoidCharge";
}
