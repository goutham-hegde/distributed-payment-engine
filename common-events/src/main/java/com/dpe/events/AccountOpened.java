package com.dpe.events;

import java.util.UUID;

/**
 * account-service has opened an account, and this is who owns it.
 *
 * <p>An <b>event</b>: past tense, already true, addressed to nobody in particular. Added at M5 for
 * one reason - the orchestrator has to answer "may this caller move money out of this account?"
 * at the API edge, and {@code accounts.owner_id} lives in a database it cannot read.
 *
 * <h2>Why this is an event and not an HTTP call</h2>
 *
 * <p>The obvious alternative is for the orchestrator to ask account-service over HTTP while the
 * request is in flight. That works and it is rejected: it puts a hard runtime dependency on the
 * write path, so account-service being down would stop transfers from being <i>accepted</i>, not
 * merely from completing. The saga exists precisely so that a participant can be unavailable
 * without the front door closing; a synchronous authorization lookup would hand that back.
 *
 * <p>So ownership is projected instead. account-service publishes this once, the orchestrator
 * keeps a local table, and the edge check is a local read that cannot fail because another
 * service is down.
 *
 * <h2>What staleness can and cannot do</h2>
 *
 * <p>A projection is always behind, and the honest question is what the lag can cause. Here the
 * answer is bounded by an accident of the domain that is worth saying out loud: <b>owner_id is
 * immutable after opening.</b> An account is opened once, by one owner, and this system has no
 * transfer-of-ownership operation. So the projection can only ever be missing a row, never
 * holding a wrong one - and a missing row means the edge check denies. Stale therefore fails
 * closed: a brand-new account may be refused for the fraction of a second before its event
 * arrives, and no account is ever spendable by the wrong person.
 *
 * <p>If ownership ever became mutable this reasoning collapses, and the edge check would have to
 * become advisory with the authoritative decision moved entirely to account-service. That is why
 * the check is ALSO made there, under the lock that moves the money - see
 * {@link ReserveFunds#initiatedBy()}.
 *
 * @param accountId          the account. Also the aggregate id, so these messages are keyed by
 *                           account rather than by transfer - a different aggregate from every
 *                           other message on this topic, and correctly so.
 * @param ownerId            the JWT subject that owns it. The value an incoming token's
 *                           {@code sub} is compared against.
 * @param accountType        CUSTOMER, SYSTEM or CLEARING. Carried so the orchestrator can refuse
 *                           to let anybody name the SYSTEM or CLEARING account as a source, no
 *                           matter what owner id those internal rows happen to carry.
 * @param openingBalanceMinor minor units, for diagnostics only. The orchestrator never uses a
 *                           projected balance to decide anything - balances change constantly and
 *                           a stale one would authorize a transfer the ledger cannot honour.
 */
public record AccountOpened(
        UUID accountId,
        String ownerId,
        String accountType,
        String currency,
        long openingBalanceMinor) {

    public static final String TYPE = "AccountOpened";
}
