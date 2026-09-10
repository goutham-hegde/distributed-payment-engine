package com.dpe.account.web.dto;

import com.dpe.account.domain.LedgerEntry;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * One leg of one posting, as the console shows it.
 *
 * <p>{@code amountMinor} is <b>signed</b> and is passed through exactly as stored - negative for a
 * debit, positive for a credit. It is not turned into a magnitude with the sign moved into
 * {@code entryType}, tempting though that is for rendering: the sum of these numbers over an
 * account is its balance and the sum over the whole table is zero, and a response that hands the
 * client absolute values quietly destroys the one property that makes the ledger checkable. A UI
 * that wants to render "-" and a red colour can read the sign.
 *
 * <p>{@code transferId} is included because it is the thread back to the saga: the two legs of a
 * posting share it, and so do the reserve and the commit of the same transfer. It is what lets the
 * console line a ledger row up against a stage of the timeline.
 */
public record LedgerEntryResponse(
        long id,
        UUID transferId,
        UUID accountId,
        long amountMinor,
        String entryType,
        String currency,
        OffsetDateTime createdAt) {

    public static LedgerEntryResponse of(LedgerEntry e) {
        return new LedgerEntryResponse(
                e.getId(),
                e.getTransferId(),
                e.getAccountId(),
                e.getAmountMinor(),
                e.getEntryType().name(),
                e.getCurrency(),
                e.getCreatedAt());
    }
}
