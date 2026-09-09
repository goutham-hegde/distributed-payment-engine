package com.dpe.orchestrator.auth;

import com.dpe.security.JwtConfig;
import com.dpe.security.SecurityProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.oauth2.jose.jws.SignatureAlgorithm;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;

/**
 * Binds {@link AuthProperties} and gives this service the one thing {@code JwtConfig} cannot give
 * it: a decoder built from the key it signs with.
 *
 * <p>Deliberately separate from the orchestrator's {@code SecurityConfig}. That class is about who
 * may call what; this one is about issuing credentials and holding the private key, which is the
 * side of the line that account-service and payment-gateway do not have at all.
 */
@Configuration
@EnableConfigurationProperties(AuthProperties.class)
public class AuthConfig {

    /**
     * The orchestrator verifies with its own public key, directly, rather than fetching its own
     * JWKS endpoint over HTTP.
     *
     * <p>Both would work and the self-fetch would even be symmetrical - but it would mean this
     * service could not verify a token until its own HTTP port was serving, which turns a
     * verification failure into a startup-ordering question and makes an obvious operation depend
     * on a network round trip to itself. The key is in memory; use it.
     *
     * <p>The claim checks come from {@link JwtConfig#validators} rather than being rebuilt here.
     * Two services validating the same tokens by two slightly different rules is a bug that only
     * ever shows up as "it works everywhere except there".
     *
     * <p>This bean is also why {@code dpe.security.jwk-set-uri} and
     * {@code dpe.security.public-key} are both unset for this service: those properties each
     * create a decoder, and three candidates for one bean is a startup failure.
     */
    @Bean
    JwtDecoder jwtDecoder(SigningKeys keys, SecurityProperties properties) {
        NimbusJwtDecoder decoder = NimbusJwtDecoder
                .withPublicKey(keys.verificationKey())
                .signatureAlgorithm(SignatureAlgorithm.RS256)
                .build();
        decoder.setJwtValidator(JwtConfig.validators(properties));
        return decoder;
    }
}
