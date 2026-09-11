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

    /**
     * M5: the subject named in {@link ReserveFunds#initiatedBy()} does not own the account the
     * money would come out of - or no subject was named at all.
     *
     * <p>It is a rejection like any other, which is the point. An authorization failure detected
     * here is a <b>business</b> outcome: it commits, replies, and lets the saga end in FAILED.
     * Throwing would roll back the inbox row and redeliver a command whose ownership will never
     * change, forever - the infinite-retry trap the whole reserve path is written to avoid.
     *
     * <p>A caller should essentially never see this: the orchestrator refuses the same request at
     * the edge with a 403, long before a command is written. A transfer that reaches FAILED with
     * this reason means something bypassed the API - a replayed command, a hand-produced message,
     * a compromised producer - and it is worth alerting on rather than merely counting.
     */
    public static final String NOT_ACCOUNT_OWNER  = "NOT_ACCOUNT_OWNER";

    /**
     * M7: the saga for this transfer has already given up on it, so no money may be reserved for
     * it now or ever.
     *
     * <p>Sent in two situations, and they are the same fact seen from each side of a race. A
     * {@link ReserveFunds} that arrives AFTER the orchestrator's timeout voided the transfer is
     * refused with it. And a {@link ReleaseFunds} that arrives before any reserve - so there is no
     * hold to release - is answered with it, because "nothing was reserved, and nothing will be"
     * is exactly this record's meaning. Either way the saga is already {@code FAILED}; the reply
     * exists so the participant never answers with silence.
     */
    public static final String TRANSFER_VOIDED    = "TRANSFER_VOIDED";
}
