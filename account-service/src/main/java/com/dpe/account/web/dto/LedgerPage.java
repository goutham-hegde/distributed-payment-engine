package com.dpe.account.web.dto;

import java.util.List;
import java.util.UUID;

/**
 * A page of ledger history, plus the balance it belongs to.
 *
 * <p>The balance is included deliberately, and it is the authoritative
 * {@code accounts.balance_minor} rather than a running total accumulated down the page. A client
 * that adds up the entries it has been shown gets the balance only if it has been shown all of
 * them, which on a paged endpoint it has not - so a "running balance" column computed in the UI
 * would be wrong on every page but the last, in a direction that looks plausible.
 *
 * <p>No total count, for the same reason {@code TransferPage} has none: it is an unbounded
 * {@code COUNT} on every page request, to render a number that is stale as it is drawn.
 */
public record LedgerPage(
        UUID accountId,
        long balanceMinor,
        String currency,
        List<LedgerEntryResponse> entries,
        String nextCursor) {
}
