package com.dpe.events;

import java.util.UUID;

/**
 * Reply to {@link ReserveFunds}: nothing was reserved, and nothing needs undoing.
 *
 * <p>This is the branch that makes the saga's state machine more than a straight line, and the
 * one people get wrong. A rejection is NOT a failure to compensate - <b>no money moved</b>, so
 * there is no hold and no ledger entry to reverse. A saga that routed this into its compensation
 * path would emit a {@link ReleaseFunds} naming a hold that does not exist, get no reply, and
 * strand itself until the timeout sweeper gave up on it.
 *
 * <p>So the saga goes straight to a terminal FAILED state. The schema enforces the distinction:
 * {@code saga_compensation_needs_a_hold} rejects a COMPENSATING saga whose {@code hold_id} is
 * null, which is exactly the row this mistake would try to write.
 *
 * @param reason a stable machine-readable code. A code rather than a sentence because the
 *               orchestrator branches on it, and a message written for a human changes whenever
 *               somebody improves the wording.
 * @param detail the human sentence, for logs and support. Never parsed.
 */
public record ReserveRejected(
        UUID transferId,
        UUID fromAccountId,
        String reason,
        String detail) {

    public static final String TYPE = "ReserveRejected";

    public static final String INSUFFICIENT_FUNDS = "INSUFFICIENT_FUNDS";
    public static final String ACCOUNT_NOT_FOUND  = "ACCOUNT_NOT_FOUND";
    public static final String CURRENCY_MISMATCH  = "CURRENCY_MISMATCH";
    public static final String INVALID_TRANSFER   = "INVALID_TRANSFER";
}
