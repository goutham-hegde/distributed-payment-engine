package com.dpe.events;

import java.util.UUID;

/**
 * Reply to {@link ReleaseFunds}: the sender has their money back and the hold is closed.
 *
 * <p>Moves the saga to COMPENSATED - terminal, and the state that says the system recovered
 * cleanly from a failure rather than merely detecting one. A saga stuck in COMPENSATING is
 * exactly as much of an I4 violation as one stuck in RESERVED: compensation that was started and
 * never finished still leaves money in a hold.
 */
public record FundsReleased(
        UUID transferId,
        UUID holdId,
        UUID fromAccountId,
        long amountMinor,
        String currency,
        String reason) {

    public static final String TYPE = "FundsReleased";
}
