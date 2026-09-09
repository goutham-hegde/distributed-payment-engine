package com.dpe.security;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.oauth2.jose.jws.SignatureAlgorithm;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;

/**
 * Builds the {@link JwtDecoder} for a service that gets its verification key from configuration.
 *
 * <p>Imported <b>alongside</b> {@link JwtConfig} by account-service and payment-gateway:
 *
 * <pre>
 * &#64;Import({MessagingConfig.class, JwtConfig.class, JwtDecoderConfig.class})
 * </pre>
 *
 * <p>payment-orchestrator imports {@code JwtConfig} and <b>not</b> this one, because it holds the
 * signing key and builds a decoder from the public half in memory - see its {@code AuthConfig}.
 *
 * <h2>Why this is a separate class, and one bean rather than two conditional ones</h2>
 *
 * <p>The obvious shape is a single config with two {@code @ConditionalOnProperty} beans, one per
 * key source. It is wrong in two ways that a passing test suite will not tell you about.
 *
 * <p>First, a service that legitimately supplies its own decoder - the issuer - would still get
 * one from here if either property happened to be set, and <b>two candidates for one bean is a
 * startup failure</b>. Which of them was intended is not something Spring can guess.
 *
 * <p>Second, {@code @ConditionalOnMissingBean} is the usual answer to that and is not safe here:
 * outside auto-configuration it is evaluated in registration order, so whether the issuer's own
 * decoder wins depends on the order configuration classes happen to be processed. Ordering that
 * decides which key verifies your tokens is not something to leave to chance.
 *
 * <p>So the choice is made by an explicit import instead. A service either says "build my decoder
 * from configuration" or supplies its own, and there is no arrangement of properties that produces
 * both or neither by accident.
 */
@Configuration
public class JwtDecoderConfig {

    @Bean
    public JwtDecoder jwtDecoder(SecurityProperties properties) {
        if (properties.hasJwkSetUri()) {
            return jwkSetDecoder(properties);
        }
        if (properties.hasPublicKey()) {
            return publicKeyDecoder(properties);
        }
        // Neither. Fail at startup with a sentence that names the two properties, rather than
        // letting Spring report a missing JwtDecoder bean three frames deeper in the filter chain.
        throw new IllegalStateException(
                "no verification key configured: set dpe.security.jwk-set-uri (normal) or "
                + "dpe.security.public-key (static), or do not import JwtDecoderConfig if this "
                + "service supplies its own JwtDecoder");
    }

    /**
     * The production path: fetch the issuer's public keys over HTTP and cache them.
     *
     * <p>The fetch is <b>lazy</b> - it happens on the first token this service is asked to verify,
     * not at startup - so account-service and payment-gateway boot fine while the orchestrator is
     * still starting. What it does mean is that if the issuer is unreachable when an unknown
     * {@code kid} first appears, verification fails and callers see 401s. That is a real
     * dependency, and the honest description of it is: <i>trusting an issuer means being able to
     * reach it occasionally.</i>
     *
     * <p>It is also what buys key rotation. The issuer publishes old and new keys together, each
     * token names the key that signed it, and validators follow along with no restart. A statically
     * configured key cannot do that without a coordinated deploy at the moment of the rotation.
     */
    private static JwtDecoder jwkSetDecoder(SecurityProperties properties) {
        NimbusJwtDecoder decoder = NimbusJwtDecoder
                .withJwkSetUri(properties.jwkSetUri())
                // Pinned. A verifier that let the token choose its algorithm would accept one
                // HMAC-signed with the public key - which anybody can download from the JWKS
                // endpoint. Under RS256 this line is what stops a public key being a signing key.
                .jwsAlgorithm(SignatureAlgorithm.RS256)
                .build();
        decoder.setJwtValidator(JwtConfig.validators(properties));
        return decoder;
    }

    /**
     * The static path: one configured public key, no network, no rotation story. For tests, where
     * a MOCK web environment has no HTTP server to fetch a key set from, and for any deployment
     * that prefers configuration to a runtime dependency and accepts what that costs.
     */
    private static JwtDecoder publicKeyDecoder(SecurityProperties properties) {
        NimbusJwtDecoder decoder = NimbusJwtDecoder
                .withPublicKey(properties.parsePublicKey())
                .signatureAlgorithm(SignatureAlgorithm.RS256)
                .build();
        decoder.setJwtValidator(JwtConfig.validators(properties));
        return decoder;
    }
}
