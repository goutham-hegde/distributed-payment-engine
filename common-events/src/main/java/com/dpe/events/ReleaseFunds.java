package com.dpe.events;

import java.util.UUID;

/**
 * THE COMPENSATING TRANSACTION. Give the sender their money back.
 *
 * <p>This is the message the whole project exists to demonstrate, so it is worth being precise
 * about what it is and is not.
 *
 * <p><b>It is not a rollback.</b> A rollback erases history and is only possible inside one
 * uncommitted transaction. The reserve committed - it is durable, it is visible, and other
 * transactions have already seen it. What this does is write a NEW pair of ledger entries that
 * semantically undo the old ones. The sender's statement ends up showing both the debit and the
 * reversal, which is correct: that is what actually happened to their money.
 *
 * <p><b>It is issued for two quite different reasons</b>, and the saga must handle both:
 *
 * <ol>
 *   <li><b>A decline.</b> The PSP answered no. The outcome is known and compensation is
 *       obviously right.</li>
 *   <li><b>A timeout.</b> Nobody answered at all. The sweeper compensates on the assumption that
 *       the forward path is not coming back - and it may be WRONG. A late
 *       {@link GatewayApproved} can still arrive afterwards.</li>
 * </ol>
 *
 * <p>That second case is the hard one, and the answer is not "make the sweeper cleverer". It is
 * that commit and release both write {@code (transfer_id, clearing, DEBIT)} into a UNIQUE index,
 * so whichever lands first wins and the loser fails on the constraint. The race is resolved by
 * the database, deterministically, rather than by a timing assumption.
 *
 * @param reason why the money is going back. Recorded on the hold and on the saga step, because
 *               "was this refunded because we were told no, or because we gave up waiting" is
 *               the first question anyone asks about a compensated transfer.
 */
public record ReleaseFunds(
        UUID transferId,
        UUID holdId,
        String reason) {

    public static final String TYPE = "ReleaseFunds";

    public static final String GATEWAY_DECLINED = "GATEWAY_DECLINED";
    public static final String SAGA_TIMEOUT     = "SAGA_TIMEOUT";
}
