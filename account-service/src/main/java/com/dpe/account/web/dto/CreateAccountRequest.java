package com.dpe.account.web.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.PositiveOrZero;

/**
 * Opening an account with a non-zero balance writes a ledger entry that does NOT sum to zero
 * against another account - it is money entering the system. See
 * {@link com.dpe.account.service.AccountService} for how that is kept compatible with
 * invariant I1.
 */
public record CreateAccountRequest(
        @NotBlank String ownerId,
        @Pattern(regexp = "[A-Z]{3}") String currency,
        @PositiveOrZero long openingBalanceMinor) {
}
