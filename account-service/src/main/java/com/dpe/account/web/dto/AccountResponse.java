package com.dpe.account.web.dto;

import com.dpe.account.domain.Account;
import java.util.UUID;

public record AccountResponse(
        UUID id,
        String ownerId,
        String currency,
        long balanceMinor) {

    public static AccountResponse from(Account account) {
        return new AccountResponse(
                account.getId(),
                account.getOwnerId(),
                account.getCurrency(),
                account.getBalanceMinor());
    }
}
