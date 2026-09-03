package com.dpe.account.web.dto;

import com.dpe.account.service.TransferResult;
import java.util.UUID;

public record TransferResponse(
        UUID transferId,
        UUID fromAccountId,
        long fromBalanceMinor,
        UUID toAccountId,
        long toBalanceMinor) {

    public static TransferResponse from(TransferResult result) {
        return new TransferResponse(
                result.transferId(),
                result.fromAccountId(),
                result.fromBalanceMinor(),
                result.toAccountId(),
                result.toBalanceMinor());
    }
}
