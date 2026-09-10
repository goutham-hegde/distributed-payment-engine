package com.dpe.orchestrator.web.dto;

import com.dpe.orchestrator.transfer.Transfer;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * One row of {@code GET /api/v1/transfers}.
 *
 * <p>Deliberately NOT {@link TransferResponse}. It is missing one field - {@code sagaStatus} - and
 * that single omission is what keeps the list query a single index scan on
 * {@code idx_transfers_initiated_by}. Including it would mean joining {@code saga_instances} for
 * every row of every page, on a read that a UI polls, against the table the saga itself is
 * updating.
 *
 * <p>Which is the general shape of the decision: <b>a list endpoint answers "which ones", a detail
 * endpoint answers "what happened to this one".</b> Serving the detail shape from the list is how
 * a list endpoint quietly becomes the most expensive query in the system. The saga state for a
 * single transfer is one GET away, and its full history is one more.
 */
public record TransferSummary(
        UUID transferId,
        UUID fromAccountId,
        UUID toAccountId,
        long amountMinor,
        String currency,
        String status,
        String failureReason,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt) {

    public static TransferSummary of(Transfer t) {
        return new TransferSummary(
                t.getId(),
                t.getFromAccountId(),
                t.getToAccountId(),
                t.getAmountMinor(),
                t.getCurrency(),
                t.getStatus().name(),
                t.getFailureReason(),
                t.getCreatedAt(),
                t.getUpdatedAt());
    }
}
