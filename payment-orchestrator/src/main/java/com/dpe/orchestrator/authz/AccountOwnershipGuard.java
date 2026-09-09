package com.dpe.orchestrator.authz;

import com.dpe.orchestrator.readmodel.AccountOwner;
import com.dpe.orchestrator.readmodel.AccountOwnerRepository;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * "May this subject move money out of that account?" - the per-resource authorization check.
 *
 * <p>This is the milestone's acceptance criterion in one class: <i>you cannot move money from an
 * account you do not own</i>. The filter chain cannot do it - the account id arrives in the
 * request body, and by the time it is parsed the filters have long since run. Authorization over
 * a resource named in the payload is always a lookup inside the request.
 *
 * <h2>What {@code requireCanSpendFrom} does</h2>
 *
 * <pre>
 *   look the account up in AccountOwnerRepository (the projection, this database, no network)
 *     |
 *     +-- not there            -> DENY
 *     +-- not a CUSTOMER row   -> DENY      (SYSTEM and CLEARING are not spendable by anyone)
 *     +-- owner != subject     -> DENY
 *     +-- otherwise            -> return
 * </pre>
 *
 * <p>Deny by throwing {@link AccountAccessDeniedException}. It carries a reason for the log and
 * produces a 403 whose body says nothing about which of the three cases it was.
 *
 * <h2>The four things that make this correct</h2>
 *
 * <p><b>1. An unknown account is a denial, not a 404 and not an acceptance.</b> This is the whole
 * argument for putting the check here at all. The projection is fed asynchronously and can be
 * behind, so "I have no row for this account" means "I do not know yet" - and in a payment system
 * the safe reading of "I do not know" is no. The cost is a real one and worth being able to
 * state: an account opened milliseconds ago may have its first transfer refused. That is
 * survivable because the client retries and the row arrives; the opposite failure - accepting
 * because the answer was not yet known - is a transfer out of an account nobody verified.
 *
 * <p><b>2. Missing is the ONLY way this projection can be wrong.</b> Ownership is immutable in
 * this system - an account is opened once, by one owner, and there is no transfer-of-ownership
 * operation - so a row that exists is a row that is right, however old it is. If ownership ever
 * becomes mutable, this check becomes advisory and the real decision has to move entirely to
 * account-service. Know which of those two worlds you are in before trusting a projection for an
 * authorization decision; it is the question that decides whether this design is sound or
 * negligent.
 *
 * <p><b>3. The SYSTEM and CLEARING accounts are not spendable through this API, by anybody.</b>
 * SYSTEM is where money is issued from; a transfer out of it mints currency. CLEARING holds money
 * in flight for every saga in the system at once. Neither is a customer account and neither has a
 * legitimate owner in the token sense - so the account type is checked as well as the owner, and
 * a row whose type is not CUSTOMER is refused whatever owner string it happens to carry. Without
 * this, learning the owner id on the SYSTEM row would be enough to print money through the
 * ordinary front door.
 *
 * <p><b>4. An operator does not get a bypass.</b> Tempting - operators can see everything - and
 * wrong: an operator may inspect, replay and diagnose, but nothing in this system should let one
 * human move another human's money by holding a role. If that capability is ever genuinely
 * needed, it is a separate, named, heavily audited operation, not a quiet {@code ||
 * hasRole(OPERATOR)} in an ownership check.
 *
 * <h2>The second method: reads</h2>
 *
 * <p>{@link #canView} guards {@code GET /api/v1/transfers/{id}} by comparing
 * {@code transfers.initiated_by} with the subject. Transfer ids are UUIDs and effectively
 * unguessable, but "unguessable" is not an authorization model - it is the same argument as a
 * secret URL, and it fails the moment an id appears in a log, a support ticket or a screenshot.
 *
 * <p>A subtlety worth deciding deliberately: what about the RECIPIENT of a transfer? They have a
 * legitimate interest in it, and they are not the initiator. This system answers "no" - the
 * recipient sees the credit in their own ledger history, not the sender's transfer record, which
 * carries the sender's intent and metadata. Different question, different endpoint.
 *
 * <p>And on the response for a transfer that exists but belongs to someone else: <b>404, not
 * 403.</b> The opposite of the spend check, and for the same reason it is a judgement call rather
 * than a rule - here the id itself is the secret, so a 403 confirms "this transfer exists" to
 * somebody who should not know that. On the spend path the account id was supplied by the caller
 * about an account they are asserting a claim to, and 403 is the honest answer to a claim that is
 * refused.
 */
@Component
public class AccountOwnershipGuard {

    private final AccountOwnerRepository owners;

    public AccountOwnershipGuard(AccountOwnerRepository owners) {
        this.owners = owners;
    }

    /**
     * Throws {@link AccountAccessDeniedException} unless {@code subject} owns {@code accountId}
     * and it is a customer account.
     */
    public void requireCanSpendFrom(String subject, UUID accountId) {
        AccountOwner account = owners.findById(accountId)
                // Not "404 account not found": this service does not know whether the account
                // exists. It knows only that its projection has no row - which means either no
                // such account, or one whose AccountOpened event has not landed yet. Both are
                // answered the same way, and the answer is no.
                .orElseThrow(() -> new AccountAccessDeniedException(subject, accountId,
                        "no ownership row projected for this account"));

        if (!account.isCustomerAccount()) {
            // SYSTEM is where money is issued from and CLEARING holds every saga's money in
            // flight. Neither is a customer account, and neither may be named as a source
            // whatever owner string its row happens to carry.
            throw new AccountAccessDeniedException(subject, accountId,
                    "account type " + account.getAccountType() + " is not spendable through "
                            + "this API");
        }

        if (!account.getOwnerId().equals(subject)) {
            throw new AccountAccessDeniedException(subject, accountId,
                    "owned by another subject");
        }
    }

    /**
     * Whether {@code subject} may see a transfer that {@code initiatedBy} created.
     *
     * <p>Returns a boolean rather than throwing, precisely so the caller can turn a refusal into
     * the SAME answer as "no such transfer" - see the 404-not-403 argument above. An exception
     * would push the caller into distinguishing the two cases and then having to remember to
     * flatten them again.
     *
     * @param initiatedBy {@code transfers.initiated_by}, which is NULL for transfers created
     *                    before M5, when the API had no identity at all. Those rows belong to
     *                    nobody, so nobody can read them.
     */
    public boolean canView(String subject, String initiatedBy) {
        // A null initiatedBy is a transfer written before M5, when the API carried no identity at
        // all. Those rows belong to nobody, so nobody may read them - and equality with a null
        // would be the wrong shape of answer anyway: "we do not know who owns this" must never
        // resolve to "you do".
        return initiatedBy != null && initiatedBy.equals(subject);
    }
}
