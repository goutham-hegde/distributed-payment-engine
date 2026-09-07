package com.dpe.orchestrator.web.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import java.util.UUID;

/**
 * A request to move money.
 *
 * @param amountMinor minor units (paise), so the wire format cannot carry a fractional amount at
 *                    all. A decimal here would invite a client to send 300.005 and force this
 *                    service to decide how to round somebody else's money.
 */
public record CreateTransferRequest(
        @NotNull UUID fromAccountId,
        @NotNull UUID toAccountId,
        @Positive long amountMinor,
        @NotBlank @Size(min = 3, max = 3) String currency) {
}
