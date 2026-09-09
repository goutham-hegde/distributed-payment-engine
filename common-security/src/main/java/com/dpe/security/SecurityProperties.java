package com.dpe.security;

import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.time.Duration;
import java.util.Base64;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Everything this service needs to VERIFY a token. Nothing here can sign one, and that is the
 * whole difference between M5 part 1 and part 2.
 *
 * <p>Part 1 held {@code secret} - one HS256 value used both to verify and to sign, so every
 * service holding it could mint an operator token for the entire system. That property is gone:
 * with RS256 the key splits, and what is configured here is only ever the <b>public</b> half.
 *
 * <h2>Two ways to obtain that public half, and when each is right</h2>
 *
 * <p><b>{@code jwkSetUri}</b> - fetch it from the issuer's {@code /.well-known/jwks.json}. This is
 * how every real deployment works, because it is what makes <b>key rotation</b> possible: the
 * issuer starts signing with a new key, publishes both in its JWK set, and validators pick the
 * right one up by the token's {@code kid} header with no restart and no coordinated deploy. The
 * cost is a dependency on the issuer being reachable - bounded, because the key set is cached and
 * only re-fetched when an unknown {@code kid} appears.
 *
 * <p><b>{@code publicKey}</b> - a base64 key, configured directly. No network, no rotation story.
 * Right where the verifier already holds the key material for another reason (payment-orchestrator
 * verifies with the key it signs with) and in tests, where there is no HTTP server to fetch from.
 *
 * <p>At most one may be set. Neither being set is legal <i>only</i> for a service that supplies
 * its own {@code JwtDecoder} bean - which the orchestrator does, from its own signing key. Get it
 * wrong and the context fails at startup with a missing {@code JwtDecoder}, which is the right
 * kind of loud.
 *
 * @param jwkSetUri  where to fetch the issuer's public keys, e.g.
 *                   {@code http://payment-orchestrator:8081/.well-known/jwks.json}
 * @param publicKey  an RSA public key: base64 of the X.509 SubjectPublicKeyInfo encoding. PEM
 *                   armour and line breaks are tolerated and stripped, so the same value works in
 *                   a properties file, an environment variable and a Compose entry.
 * @param issuer     expected {@code iss} claim. Rejects a validly-signed token from a different
 *                   system - which matters far less now that only one component can sign, and is
 *                   kept because the check costs nothing and the property it asserts is real.
 * @param audience   expected {@code aud} claim. The confused-deputy check: a token minted for one
 *                   service must not be replayable against another that trusts the same issuer.
 * @param clockSkew  leeway on {@code exp} and {@code nbf}. Stateless auth makes wall-clock
 *                   agreement a correctness dependency; with zero leeway a validator whose clock
 *                   runs two seconds fast rejects freshly-minted tokens and calls them "expired".
 */
@ConfigurationProperties("dpe.security")
public record SecurityProperties(
        String jwkSetUri,
        String publicKey,
        String issuer,
        String audience,
        Duration clockSkew) {

    public SecurityProperties {
        issuer = issuer == null ? "dpe" : issuer;
        audience = audience == null ? "dpe-api" : audience;
        clockSkew = clockSkew == null ? Duration.ofSeconds(30) : clockSkew;

        if (hasText(jwkSetUri) && hasText(publicKey)) {
            // Fail at startup rather than picking one silently. Two sources of truth for a
            // verification key is a configuration that looks fine until the day they disagree,
            // and then rejects every token for a reason nobody can see.
            throw new IllegalStateException(
                    "set dpe.security.jwk-set-uri or dpe.security.public-key, not both");
        }
    }

    public boolean hasJwkSetUri() {
        return hasText(jwkSetUri);
    }

    public boolean hasPublicKey() {
        return hasText(publicKey);
    }

    /**
     * Parses {@link #publicKey}.
     *
     * <p>Note what a caller can do with the result: verify a signature, and nothing else. There is
     * no counterpart to this method on the private side anywhere in this module - the signing key
     * lives in payment-orchestrator and cannot be reached from here. That absence is the milestone.
     */
    public RSAPublicKey parsePublicKey() {
        byte[] der = Base64.getDecoder().decode(stripArmour(publicKey));
        try {
            return (RSAPublicKey) KeyFactory.getInstance("RSA")
                    .generatePublic(new X509EncodedKeySpec(der));
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new IllegalStateException("dpe.security.public-key is not a valid RSA public key",
                    e);
        }
    }

    /**
     * Removes PEM headers and all whitespace.
     *
     * <p>Because the same key gets pasted from three places - an {@code openssl} PEM, a one-line
     * environment variable, a YAML block - and a key that is correct but rejected for a stray
     * newline is an hour nobody gets back.
     */
    static String stripArmour(String key) {
        return key.replaceAll("-----[A-Z ]+-----", "").replaceAll("\\s", "");
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
