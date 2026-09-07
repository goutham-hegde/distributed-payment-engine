package com.dpe.events;

import java.util.UUID;

/**
 * Step 2 of the saga: ask the external PSP to authorize the payment.
 *
 * <p>The step that justifies the whole pattern. Everything before it happens inside one database
 * and could, in a monolith, have been a single transaction. This one cannot: it is a call into
 * somebody else's system, it can fail in ways that leave the outcome unknown, and it cannot be
 * rolled back. A saga exists precisely because this step exists.
 *
 * <p>It is issued only after {@link FundsReserved}, never in parallel with the reserve. Ordering
 * them the other way would mean charging a customer and only then discovering they had
 * insufficient funds - a refund instead of a released hold.
 */
public record ChargeGateway(
        UUID transferId,
        UUID fromAccountId,
        long amountMinor,
        String currency) {

    public static final String TYPE = "ChargeGateway";
}
