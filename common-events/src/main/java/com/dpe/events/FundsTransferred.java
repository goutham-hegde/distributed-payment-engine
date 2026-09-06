package com.dpe.events;

import java.util.UUID;

/**
 * account-service has posted both legs of a transfer and committed them.
 *
 * <p>An <b>event</b>, not a command: it states a fact that is already true and durable. Nothing
 * in it asks anyone to do anything, which is what lets an arbitrary number of subscribers consume
 * it without coordinating.
 *
 * <p>Past tense is not a naming convention here, it is the contract. A message named
 * {@code TransferFunds} would be a command, would have exactly one legal recipient, and would
 * belong on {@link Topics#ACCOUNT_COMMANDS}. The saga in M3 introduces those.
 *
 * @param amountMinor a positive magnitude in minor units (paise) - the direction is carried by
 *                    the from/to fields, never by the sign, so a consumer cannot invert a
 *                    transfer by mishandling a negative number.
 */
public record FundsTransferred(
        UUID transferId,
        UUID fromAccountId,
        UUID toAccountId,
        long amountMinor,
        String currency) {

    public static final String TYPE = "FundsTransferred";
}
