package com.dpe.events;

import java.util.UUID;

/**
 * Step 3 of the happy path: turn the hold into a settled payment.
 *
 * <p>The capture half of auth-then-capture. Account-service debits the CLEARING account, credits
 * the recipient, and marks the hold COMMITTED.
 *
 * <p>Note what makes this safe to redeliver. Commit and {@link ReleaseFunds} both write the
 * ledger leg {@code (transfer_id, clearing, DEBIT)}, and that triple is covered by a UNIQUE
 * constraint - so for any one transfer the database permits a commit or a release and never
 * both, and never either one twice. The orchestrator promising to send only one of them is the
 * belt; the constraint is the braces, and the constraint is the part that holds under a race.
 *
 * @param holdId the hold to settle, quoted back from {@link FundsReserved}.
 */
public record CommitFunds(
        UUID transferId,
        UUID holdId,
        UUID toAccountId) {

    public static final String TYPE = "CommitFunds";
}
