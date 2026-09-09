package com.dpe.orchestrator.support;

import com.dpe.orchestrator.auth.TokenIssuer;
import com.dpe.security.Roles;
import com.dpe.security.SecurityProperties;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import com.nimbusds.jose.proc.SecurityContext;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Duration;
import java.time.Instant;
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
 * Mints tokens for tests - the valid ones through the production issuer, the invalid ones by hand
 * with a <b>second RSA key pair that this system has never heard of</b>.
 *
 * <p>That second key is what changed at M5 part 2, and it is a better test than what came before.
 * Under HS256 the "forged" token was signed with a different shared secret, which proved the
 * decoder compares secrets. Now it is signed by a real private key belonging to nobody - exactly
 * what an attacker with their own key pair would produce - and the only thing that rejects it is
 * that its public half is not in this system's key set.
 *
 * <p>The other three - expired, wrong issuer, wrong audience - are signed with the SAME key the
 * production issuer uses, so they are cryptographically perfect and must still be refused. Each is
 * wrong in exactly one respect, so a failing assertion names the validator that stopped working.
 */
@TestComponent
public class TestTokens {

    public static final String ALICE = "alice";
    public static final String BOB = "bob";
    public static final String OPERATOR = "operator";

    /**
     * A key pair generated once per JVM, standing in for "somebody else's key". Generating it
     * rather than committing one keeps every private key in this repository ephemeral.
     */
    private static final RSAKey FOREIGN_KEY = generateKey();

    @Autowired
    private TokenIssuer issuer;

    @Autowired
    private SecurityProperties properties;

    /**
     * The signing key of the service under test, reached through the same bean the application
     * uses. Tests therefore exercise the real key path, generated at context startup.
     */
    @Autowired
    private com.dpe.orchestrator.auth.SigningKeys keys;

    /** A valid customer token. */
    public String user(String subject) {
        return issuer.issue(subject, List.of(Roles.USER), Duration.ofMinutes(5));
    }

    /** A valid operator token. Note it does NOT carry USER - the roles are disjoint on purpose. */
    public String operator() {
        return issuer.issue(OPERATOR, List.of(Roles.OPERATOR), Duration.ofMinutes(5));
    }

    /**
     * Correct in every claim, signed with a private key this system has never seen.
     *
     * <p>Under RS256 this is the realistic forgery: anyone can generate a key pair and sign
     * whatever claims they like. The token is well formed, the signature verifies against ITS OWN
     * public key, and it must be rejected because that key is not ours. Note the {@code kid} is
     * the foreign key's own thumbprint - a token that names a key the validator does not have is
     * exactly the shape of a rotation gone wrong, and it must fail closed rather than fall back to
     * "try the keys we do have".
     */
    public String signedWithWrongKey(String subject) {
        return encode(FOREIGN_KEY, properties.issuer(), properties.audience(), subject,
                Duration.ZERO, Duration.ofMinutes(5));
    }

    /**
     * Correctly signed by the real key, and past its {@code exp} - well beyond the clock skew.
     *
     * <p>Built as a token issued an hour ago that lived for thirty minutes, rather than one with a
     * negative lifetime: {@code JwtClaimsSet} refuses to build an {@code exp} that is not after
     * its {@code iat}, which is the library declining to produce a token that was never valid at
     * any instant. An expired token is a different thing from an impossible one.
     */
    public String expired(String subject) {
        return encode(signingKey(), properties.issuer(), properties.audience(), subject,
                Duration.ofMinutes(-60), Duration.ofMinutes(-30));
    }

    /** Correctly signed by the real key, claiming to come from another system. */
    public String wrongIssuer(String subject) {
        return encode(signingKey(), "https://not-us.example", properties.audience(), subject,
                Duration.ZERO, Duration.ofMinutes(5));
    }

    /** Correctly signed by the real key, minted for a different service - confused deputy. */
    public String wrongAudience(String subject) {
        return encode(signingKey(), properties.issuer(), "some-other-api", subject,
                Duration.ZERO, Duration.ofMinutes(5));
    }

    /** {@code Authorization} header value. */
    public static String bearer(String token) {
        return "Bearer " + token;
    }

    /**
     * The production signing key, borrowed so a test can mint a token that is legitimate in every
     * way except the one under test. Only reachable here because this class runs inside the
     * orchestrator's own context - there is no equivalent in account-service, and that is the
     * milestone rather than an inconvenience.
     */
    private RSAKey signingKey() {
        try {
            return (RSAKey) ((ImmutableJWKSet<SecurityContext>) keys.signingSource())
                    .getJWKSet().getKeys().getFirst();
        } catch (RuntimeException e) {
            throw new IllegalStateException("could not read the orchestrator's signing key", e);
        }
    }

    private static String encode(RSAKey key, String issuer, String audience, String subject,
                                 Duration issuedOffset, Duration expiryOffset) {
        NimbusJwtEncoder encoder =
                new NimbusJwtEncoder(new ImmutableJWKSet<SecurityContext>(new JWKSet(key)));

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

        JwsHeader header = JwsHeader.with(SignatureAlgorithm.RS256)
                .keyId(key.getKeyID())
                .build();

        return encoder.encode(JwtEncoderParameters.from(header, claims)).getTokenValue();
    }

    private static RSAKey generateKey() {
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            KeyPair pair = generator.generateKeyPair();
            return new RSAKey.Builder((RSAPublicKey) pair.getPublic())
                    .privateKey((RSAPrivateKey) pair.getPrivate())
                    .keyIDFromThumbprint()
                    .build();
        } catch (Exception e) {
            throw new IllegalStateException("could not generate a foreign test key", e);
        }
    }
}
