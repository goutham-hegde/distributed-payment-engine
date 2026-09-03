package com.dpe.account.web.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import java.util.UUID;

/**
 * @param amountMinor MINOR UNITS (paise), not rupees. 300.00 INR is 30000. Named so that nobody
 *                    reading a call site has to guess the unit - the single most common source
 *                    of factor-of-100 money bugs.
 */
public record TransferRequest(
        @NotNull UUID transferId,
        @NotNull UUID fromAccountId,
        @NotNull UUID toAccountId,
        @Positive long amountMinor,
        @NotNull @Pattern(regexp = "[A-Z]{3}") String currency) {
}
