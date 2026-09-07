package com.dpe.events;

import java.util.UUID;

/**
 * The PSP authorized the payment. The saga may now settle the hold.
 *
 * @param chargeId the PSP's own reference. Stored on the saga because a support conversation
 *                 about a real charge starts from this value, not from the transfer id.
 */
public record GatewayApproved(
        UUID transferId,
        UUID chargeId,
        long amountMinor,
        String currency) {

    public static final String TYPE = "GatewayApproved";
}
