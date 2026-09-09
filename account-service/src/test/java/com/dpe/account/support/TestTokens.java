package com.dpe.account.support;

import com.dpe.security.Roles;
import com.dpe.security.SecurityProperties;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestComponent;
import org.springframework.security.oauth2.jose.jws.SignatureAlgorithm;
import org.springframework.security.oauth2.jwt.JwsHeader;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;
import org.springframework.security.oauth2.jwt.NimbusJwtEncoder;

/**
 * Tokens for account-service's HTTP tests, signed with the test key pair this module trusts.
 *
 * <p>Before M5 part 2 this class was four lines: inject {@code TokenIssuer} from the shared library
 * and ask it for a token. It cannot be, any more - the issuer and the private key live in
 * payment-orchestrator now, and no arrangement of imports brings them here. Having to mint with a
 * key of its own is the test suite feeling the same wall an attacker would.
 *
 * <p>Deliberately smaller than the orchestrator's copy: the malformed-token cases (foreign key,
 * expired, wrong issuer, wrong audience) are properties of the shared decoder and are asserted
 * once, where it is configured. What this service has to prove for itself is its own authorization
 * rules - which endpoint, which role.
 */
@TestComponent
public class TestTokens {

    @Autowired
    private SecurityProperties properties;

    public String user(String subject) {
        return issue(subject, List.of(Roles.USER));
    }

    public String operator() {
        return issue("operator", List.of(Roles.OPERATOR));
    }

    public static String bearer(String token) {
        return "Bearer " + token;
    }

    private String issue(String subject, Collection<String> roles) {
        NimbusJwtEncoder encoder = new NimbusJwtEncoder(TestSigningKeys.signingSource());

        Instant now = Instant.now();
        JwtClaimsSet claims = JwtClaimsSet.builder()
                // Read from the application's own configuration rather than hard-coded, so a test
                // token cannot drift away from what the decoder under test demands.
                .issuer(properties.issuer())
                .audience(List.of(properties.audience()))
                .subject(subject)
                .issuedAt(now)
                .expiresAt(now.plus(Duration.ofMinutes(5)))
                .id(UUID.randomUUID().toString())
                .claim(Roles.CLAIM, List.copyOf(roles))
                .build();

        JwsHeader header = JwsHeader.with(SignatureAlgorithm.RS256)
                .keyId(TestSigningKeys.keyId())
                .build();

        return encoder.encode(JwtEncoderParameters.from(header, claims)).getTokenValue();
    }
}
