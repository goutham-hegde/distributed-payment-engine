package com.dpe.events;

import java.util.UUID;

/**
 * Reply to {@link CommitFunds}: the recipient has the money and the saga is done.
 *
 * <p>Consuming this is what moves the saga to COMPLETED - a terminal state, which is what
 * invariant I4 looks for. Until it arrives the saga is non-terminal and the timeout sweeper
 * still owns it.
 */
public record FundsCommitted(
        UUID transferId,
        UUID holdId,
        UUID toAccountId,
        long amountMinor,
        String currency) {

    public static final String TYPE = "FundsCommitted";
}
