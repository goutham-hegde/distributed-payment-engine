package com.dpe.orchestrator.authz;

import java.util.UUID;

/**
 * The caller may not use this account.
 *
 * <p>Maps to <b>403</b>, not 404 and not 422. Three things it must never do:
 *
 * <ul>
 *   <li><b>Say why.</b> "Account not found" and "not yours" are the same answer to the caller. A
 *       message that distinguishes them turns this endpoint into an account-existence oracle -
 *       an attacker walks UUIDs and learns which ones are real, which is the first half of most
 *       attacks on a payment API.</li>
 *   <li><b>Echo the account id back.</b> The caller sent it; repeating it in an error body only
 *       helps it end up in a log aggregator or a screenshot.</li>
 *   <li><b>Be thrown for a technical failure.</b> If the projection cannot be read at all, that
 *       is a 500 - the system does not know the answer, which is not the same as knowing the
 *       answer is no.</li>
 * </ul>
 *
 * <p>The account id IS carried on the exception, for the server-side log line. Denials are worth
 * logging with enough detail to investigate: a burst of them against many account ids from one
 * subject is exactly the enumeration attempt described above.
 */
public class AccountAccessDeniedException extends RuntimeException {

    private final String subject;
    private final UUID accountId;

    public AccountAccessDeniedException(String subject, UUID accountId, String reason) {
        super("subject '" + subject + "' may not use account " + accountId + ": " + reason);
        this.subject = subject;
        this.accountId = accountId;
    }

    public String getSubject() {
        return subject;
    }

    public UUID getAccountId() {
        return accountId;
    }
}
