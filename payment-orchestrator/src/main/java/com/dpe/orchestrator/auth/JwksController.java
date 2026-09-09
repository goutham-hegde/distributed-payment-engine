package com.dpe.orchestrator.auth;

import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Publishes the public half of the signing key, in the standard place and the standard shape.
 *
 * <p>{@code GET /.well-known/jwks.json} is what every identity provider serves and every resource
 * server knows how to consume, which is the point: account-service and payment-gateway are
 * configured with a URL, not with key material, and the day this endpoint is replaced by
 * Keycloak's the only thing that changes is the URL.
 *
 * <h2>Unauthenticated, and that is correct</h2>
 *
 * <p>The instinct to protect it is worth arguing with rather than following. A JWK set contains
 * public keys - modulus and exponent. They verify signatures and cannot produce them, which is the
 * whole property RS256 was adopted for. Publishing them costs nothing and is required by anything
 * that wants to validate a token, including clients that never authenticate to this service.
 *
 * <p>What it does require is that {@link SigningKeys#publicJwkSet()} really did strip the private
 * half. That single {@code toPublicJWK()} call is the difference between publishing a verification
 * key and publishing the ability to mint tokens - and both would serialise to JSON without
 * complaint, serve with a 200, and pass every test that only checks a token verifies.
 *
 * <h2>Caching</h2>
 *
 * <p>No cache headers, deliberately. Validators cache the key set themselves and re-fetch when
 * they meet an unknown {@code kid}, so a long HTTP cache would delay exactly the moment rotation
 * depends on being fast: the window between the issuer starting to sign with a new key and every
 * validator knowing about it. A key set is small and fetched rarely; the bandwidth is not worth
 * the coupling.
 */
@RestController
public class JwksController {

    private final SigningKeys keys;

    public JwksController(SigningKeys keys) {
        this.keys = keys;
    }

    @GetMapping("/.well-known/jwks.json")
    public Map<String, Object> jwks() {
        return keys.publicJwkSet();
    }
}
