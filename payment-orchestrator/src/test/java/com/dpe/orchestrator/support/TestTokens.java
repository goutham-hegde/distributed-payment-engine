package com.dpe.orchestrator.support;

import com.dpe.security.Roles;
import com.dpe.security.SecurityProperties;
import com.dpe.security.TokenIssuer;
import com.nimbusds.jose.jwk.source.ImmutableSecret;
import com.nimbusds.jose.proc.SecurityContext;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import javax.crypto.spec.SecretKeySpec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestComponent;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.JwsHeader;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;
import org.springframework.security.oauth2.jwt.NimbusJwtEncoder;

/**
 * Mints tokens for tests - the valid ones through the production issuer, the invalid ones by
 * hand.
 *
 * <p>The hand-built ones matter more than the valid ones. A security test that only ever presents
 * good tokens proves the happy path and nothing else; what has to be demonstrated is that each
 * individual check <b>rejects</b>, and the only way to do that is to produce a token that is
 * perfectly well formed and wrong in exactly one respect. Hence {@link #signedWithWrongKey},
 * {@link #expired}, {@link #wrongIssuer} and {@link #wrongAudience}: four tokens, one broken
 * claim each, so a failing assertion names the validator that stopped doing its job.
 */
@TestComponent
public class TestTokens {

    public static final String ALICE = "alice";
    public static final String BOB = "bob";
    public static final String OPERATOR = "operator";

    @Autowired
    private TokenIssuer issuer;

    @Autowired
    private SecurityProperties properties;

    /** A valid customer token. */
    public String user(String subject) {
        return issuer.issue(subject, List.of(Roles.USER), Duration.ofMinutes(5));
    }

    /** A valid operator token. Note it does NOT carry USER - the roles are disjoint on purpose. */
    public String operator() {
        return issuer.issue(OPERATOR, List.of(Roles.OPERATOR), Duration.ofMinutes(5));
    }

    /** Correct in every claim, signed with a key this system does not trust. */
    public String signedWithWrongKey(String subject) {
        return encode("some-other-systems-signing-key-also-long-enough",
                properties.issuer(), properties.audience(), subject, Duration.ZERO,
                Duration.ofMinutes(5));
    }

    /**
     * Correctly signed, and past its {@code exp} - well beyond the configured clock skew.
     *
     * <p>Built as a token issued an hour ago that lived for thirty minutes, rather than one with
     * a negative lifetime: {@code JwtClaimsSet} refuses to build an {@code exp} that is not after
     * its {@code iat}, which is the library declining to produce a token that was never valid at
     * any instant. An expired token is a different thing from an impossible one.
     */
    public String expired(String subject) {
        return encode(properties.secret(), properties.issuer(), properties.audience(), subject,
                Duration.ofMinutes(-60), Duration.ofMinutes(-30));
    }

    /** Correctly signed by the right key, minted by something claiming to be another system. */
    public String wrongIssuer(String subject) {
        return encode(properties.secret(), "https://not-us.example",
                properties.audience(), subject, Duration.ZERO, Duration.ofMinutes(5));
    }

    /** Correctly signed, intended for a different service - the confused-deputy case. */
    public String wrongAudience(String subject) {
        return encode(properties.secret(), properties.issuer(), "some-other-api", subject,
                Duration.ZERO, Duration.ofMinutes(5));
    }

    /** {@code Authorization} header value. */
    public static String bearer(String token) {
        return "Bearer " + token;
    }

    private static String encode(String secret, String issuer, String audience, String subject,
                                 Duration issuedOffset, Duration expiryOffset) {
        NimbusJwtEncoder encoder = new NimbusJwtEncoder(new ImmutableSecret<SecurityContext>(
                new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256")));

        Instant now = Instant.now();
        JwtClaimsSet claims = JwtClaimsSet.builder()
                .issuer(issuer)
                .audience(List.of(audience))
                .subject(subject)
                .issuedAt(now.plus(issuedOffset))
                .expiresAt(now.plus(expiryOffset))
                .id(UUID.randomUUID().toString())
                .claim(Roles.CLAIM, List.of(Roles.USER))
                .build();

        return encoder.encode(JwtEncoderParameters.from(
                JwsHeader.with(MacAlgorithm.HS256).build(), claims)).getTokenValue();
    }
}
