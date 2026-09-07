package com.dpe.orchestrator.web.dto;

import com.dpe.orchestrator.saga.SagaInstance;
import com.dpe.orchestrator.transfer.Transfer;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * What a caller sees.
 *
 * <p>Carries the public status and, separately, the saga status. The second is diagnostic - it is
 * what makes "where exactly is this transfer" answerable from the API during a demo or an
 * incident - and it is documented as unstable, because it is the internal state machine and will
 * change when the state machine does.
 */
public record TransferResponse(
        UUID transferId,
        UUID fromAccountId,
        UUID toAccountId,
        long amountMinor,
        String currency,
        String status,
        String failureReason,
        String sagaStatus,
        OffsetDateTime createdAt) {

    public static TransferResponse of(Transfer t, SagaInstance saga) {
        return new TransferResponse(
                t.getId(),
                t.getFromAccountId(),
                t.getToAccountId(),
                t.getAmountMinor(),
                t.getCurrency(),
                t.getStatus().name(),
                t.getFailureReason(),
                saga == null ? null : saga.getStatus().name(),
                t.getCreatedAt());
    }
}
