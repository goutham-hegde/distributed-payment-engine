package com.dpe.security;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Everything this service needs to VALIDATE a token. Nothing here is about issuing one.
 *
 * <p>Bound from {@code dpe.security.*} in every service. The issuing side lives under
 * {@code dpe.auth.*} and exists only in payment-orchestrator - keeping the two prefixes apart is
 * not tidiness, it is the M5 part 2 migration in advance: when the signature moves to RS256, the
 * orchestrator gains a private key under {@code dpe.auth} and the other two services keep only a
 * public key here, and no configuration has to be untangled to make that split.
 *
 * @param secret    the HS256 shared secret. <b>Every service that can validate with it can also
 *                  mint with it</b> - see {@link TokenIssuer}. That is the defect part 2 removes,
 *                  and it is deliberately visible in the meantime rather than papered over.
 * @param issuer    expected {@code iss} claim. Rejects a validly-signed token from a different
 *                  system that happens to share a secret - which matters the moment the same
 *                  secret is reused for anything else, and it always is eventually.
 * @param audience  expected {@code aud} claim. Rejects a token minted for a DIFFERENT service by
 *                  the same issuer: without it, a token good enough to read a dashboard is also
 *                  good enough to move money, because both are signed by the same key.
 * @param clockSkew leeway applied to {@code exp} and {@code nbf}. Stateless auth makes wall-clock
 *                  agreement between machines a correctness dependency: with zero leeway, a
 *                  validator whose clock runs two seconds fast rejects freshly-minted tokens, and
 *                  the error it reports is "expired", which sends the reader hunting the wrong
 *                  problem. Kept small - every second of leeway is a second a revoked-by-expiry
 *                  token still works.
 */
@ConfigurationProperties("dpe.security")
public record SecurityProperties(
        String secret,
        String issuer,
        String audience,
        Duration clockSkew) {

    /** HS256 is defined over a key of at least 256 bits; a shorter one is not merely weak. */
    private static final int MIN_SECRET_BYTES = 32;

    public SecurityProperties {
        issuer = issuer == null ? "dpe" : issuer;
        audience = audience == null ? "dpe-api" : audience;
        clockSkew = clockSkew == null ? Duration.ofSeconds(30) : clockSkew;

        // Fail at startup, not at the first request. A too-short secret is legal configuration
        // as far as Boot is concerned; Nimbus only objects when it is asked to sign, which in a
        // service that mostly validates could be days later and in someone else's stack trace.
        if (secret == null || secret.getBytes(StandardCharsets.UTF_8).length < MIN_SECRET_BYTES) {
            throw new IllegalStateException(
                    "dpe.security.secret must be at least " + MIN_SECRET_BYTES
                    + " bytes for HS256; got "
                    + (secret == null ? "null" : secret.getBytes(StandardCharsets.UTF_8).length));
        }
    }

    /**
     * The HMAC key. {@code "HmacSHA256"} is the JCA algorithm name, and it must agree with the
     * {@code MacAlgorithm} the decoder and the encoder are built with - a mismatch surfaces as an
     * "Invalid signature" on a token that is perfectly well formed.
     */
    public SecretKey signingKey() {
        return new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
    }
}
