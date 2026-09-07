package com.dpe.events;

import java.util.UUID;

/**
 * The PSP refused. This is the message that drives the saga into compensation.
 *
 * <p><b>A decline is an ANSWER, not a failure.</b> The distinction is worth being able to state
 * on demand. A decline means the PSP definitively said no: nothing was charged, the outcome is
 * known, and releasing the hold is unambiguously right. A gateway <i>failure</i> - a timeout, a
 * connection reset - means the outcome is unknown, and the charge may in fact have gone through.
 * Treating the two identically is how you refund a customer who was never charged, or fail to
 * refund one who was.
 *
 * <p>This system models the decline explicitly and handles the ambiguous case by making the
 * charge idempotent instead: {@code gateway_charges.transfer_id} is UNIQUE, so a retry after an
 * unknown outcome returns the original charge rather than making a second one. That turns "we do
 * not know" into "ask again and find out", which is the only safe resolution available without a
 * reconciliation feed from the PSP.
 *
 * @param reason the PSP's reason code, e.g. {@code do_not_honour}. Passed through to the
 *               transfer's failure reason, so the explanation a caller eventually sees
 *               originated with the party that actually refused.
 */
public record GatewayDeclined(
        UUID transferId,
        UUID chargeId,
        String reason,
        String detail) {

    public static final String TYPE = "GatewayDeclined";
}
