package com.dpe.account.support;

import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import com.nimbusds.jose.proc.SecurityContext;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;

/**
 * A throwaway RSA key pair for tests, generated once per JVM.
 *
 * <h2>Why this exists at all, and why it is awkward on purpose</h2>
 *
 * <p>account-service cannot mint a token any more. {@code TokenIssuer} moved into
 * payment-orchestrator at M5 part 2 along with the private key, and there is no import that brings
 * it back - which is precisely the property the milestone was for. So a test here that needs a
 * valid token has to bring its own key pair and tell the service to trust it, exactly as an
 * operator would when pointing this service at a different identity provider.
 *
 * <p>The public half is handed to the application through {@code dpe.security.public-key} (see
 * {@link AbstractPostgresIT}), so the decoder under test is the production one on its
 * static-key path. The private half never leaves this class, and nothing signs with it outside a
 * test JVM - no key material is committed to the repository.
 *
 * <p>In production these services use {@code dpe.security.jwk-set-uri} instead and fetch the key
 * from the orchestrator. That path is not exercised here for a boring reason worth stating: a
 * {@code MOCK} web environment has no HTTP server, so there is nothing to fetch from. It is
 * covered live on Compose instead, which is where a network dependency deserves to be tested.
 */
public final class TestSigningKeys {

    private static final RSAKey KEY = generate();

    private TestSigningKeys() {
    }

    /** Base64 X.509, the shape {@code dpe.security.public-key} expects. */
    public static String publicKeyBase64() {
        try {
            return Base64.getEncoder().encodeToString(KEY.toRSAPublicKey().getEncoded());
        } catch (Exception e) {
            throw new IllegalStateException("test key has no usable public half", e);
        }
    }

    /** The signing source a test token minter needs. Test scope only, by construction. */
    public static ImmutableJWKSet<SecurityContext> signingSource() {
        return new ImmutableJWKSet<>(new JWKSet(KEY));
    }

    public static String keyId() {
        return KEY.getKeyID();
    }

    private static RSAKey generate() {
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            KeyPair pair = generator.generateKeyPair();
            return new RSAKey.Builder((RSAPublicKey) pair.getPublic())
                    .privateKey((RSAPrivateKey) pair.getPrivate())
                    .keyIDFromThumbprint()
                    .build();
        } catch (Exception e) {
            throw new IllegalStateException("could not generate a test signing key", e);
        }
    }
}
