package com.dpe.account.support;

import com.dpe.security.Roles;
import com.dpe.security.TokenIssuer;
import java.time.Duration;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestComponent;

/**
 * Tokens for account-service's HTTP tests.
 *
 * <p>Smaller than the orchestrator's copy on purpose: the malformed-token cases (wrong key,
 * expired, wrong issuer, wrong audience) are properties of the shared decoder in
 * {@code common-security}, and asserting them once, where the decoder is configured, is enough.
 * What this service has to prove for itself is its own authorization rules - which endpoints, for
 * which role - so that is all this helper serves.
 */
@TestComponent
public class TestTokens {

    @Autowired
    private TokenIssuer issuer;

    public String user(String subject) {
        return issuer.issue(subject, List.of(Roles.USER), Duration.ofMinutes(5));
    }

    public String operator() {
        return issuer.issue("operator", List.of(Roles.OPERATOR), Duration.ofMinutes(5));
    }

    public static String bearer(String token) {
        return "Bearer " + token;
    }
}
