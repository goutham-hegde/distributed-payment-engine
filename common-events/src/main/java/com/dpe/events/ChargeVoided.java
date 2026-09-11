package com.dpe.events;

import java.util.UUID;

/**
 * Reply to {@link VoidCharge}: the PSP holds no money for this transfer, and says which way that
 * came about.
 *
 * <p>It moves no saga state - a saga that asked for a void is already compensating or finished -
 * but it is sent anyway, because a participant always answers. Without it the saga's timeline
 * would show a void that went out and nothing coming back, and "did the reversal happen?" would
 * be a question for the gateway's database rather than the orchestrator's.
 *
 * @param chargeId the charge that was reversed, or the tombstone's id if none existed
 * @param outcome  {@link #REVERSED} an approved charge was voided; {@link #PRE_EMPTED} nothing had
 *                 been charged yet and none now can be; {@link #NOTHING_TO_VOID} the PSP had
 *                 already declined, or an earlier void got there first
 */
public record ChargeVoided(
        UUID transferId,
        UUID chargeId,
        String outcome) {

    public static final String TYPE = "ChargeVoided";

    public static final String REVERSED        = "REVERSED";
    public static final String PRE_EMPTED      = "PRE_EMPTED";
    public static final String NOTHING_TO_VOID = "NOTHING_TO_VOID";
}
