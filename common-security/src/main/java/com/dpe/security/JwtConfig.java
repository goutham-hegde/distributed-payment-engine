package com.dpe.security;

import java.util.List;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtClaimNames;
import org.springframework.security.oauth2.jwt.JwtClaimValidator;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtIssuerValidator;
import org.springframework.security.oauth2.jwt.JwtTimestampValidator;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;

/**
 * Wires token validation into a service. Import it from the service's application class, next to
 * {@code MessagingConfig}.
 *
 * <pre>
 * &#64;SpringBootApplication
 * &#64;Import({MessagingConfig.class, JwtConfig.class})
 * public class AccountServiceApplication { }
 * </pre>
 *
 * <p>Unlike {@code MessagingConfig} this one needs nothing repeated on the application class:
 * there are no entities and no repositories here, so the {@code @EntityScan} /
 * {@code @EnableJpaRepositories} replace-don't-extend trap does not apply.
 *
 * <h2>What this class decides, and what it refuses to decide</h2>
 *
 * <p>It decides how a token is <b>verified</b>: which key, which algorithm, which claims must
 * hold. It decides nothing about who may call what - no filter chain, no matcher, no role. Those
 * live in each service's own {@code SecurityConfig}, because they are statements about that
 * service's resources. A shared filter chain would mean loosening a rule for one endpoint quietly
 * loosens it in all three services, which is exactly the shared-library failure mode
 * {@code docs/adr/0004-shared-messaging-library.md} argues against for domain types.
 *
 * <h2>The three validators, and what each one actually prevents</h2>
 *
 * <p>A signature check alone proves only that <i>somebody holding the key</i> made this token. It
 * says nothing about when, for whom, or for which service - so every claim below is checking a
 * different question that the signature cannot answer.
 *
 * <ol>
 *   <li><b>Timestamp</b> ({@code exp}, {@code nbf}). The only expiry a stateless token has. This
 *       is the entire revocation story: a token cannot be withdrawn, so the window in which a
 *       stolen one is useful is exactly its remaining lifetime. Short TTLs are not tuning here,
 *       they are the mitigation.</li>
 *   <li><b>Issuer</b> ({@code iss}). Rejects a token signed by a different system that shares the
 *       secret. Secrets get reused across environments by accident; without this check a staging
 *       token is a production token.</li>
 *   <li><b>Audience</b> ({@code aud}). Rejects a token minted for a different recipient. This is
 *       the confused-deputy check: it is what stops a token good enough for one service from
 *       being replayed against another that trusts the same issuer.</li>
 * </ol>
 *
 * <p>Not validated, deliberately: the {@code alg} header is pinned by the decoder rather than
 * read from the token. That is what makes the "alg: none" and the RS256-verified-as-HS256
 * confusion attacks structurally impossible here - the decoder is told the algorithm, so the
 * token does not get to choose it.
 */
@Configuration
@EnableConfigurationProperties(SecurityProperties.class)
@ComponentScan(basePackageClasses = JwtConfig.class)
public class JwtConfig {

    /**
     * The decoder every service uses.
     *
     * <p>{@code macAlgorithm(HS256)} is pinned on purpose - see the note above about the token
     * not being allowed to choose its own algorithm.
     */
    @Bean
    public JwtDecoder jwtDecoder(SecurityProperties properties) {
        NimbusJwtDecoder decoder = NimbusJwtDecoder
                .withSecretKey(properties.signingKey())
                .macAlgorithm(MacAlgorithm.HS256)
                .build();

        decoder.setJwtValidator(new DelegatingOAuth2TokenValidator<>(
                new JwtTimestampValidator(properties.clockSkew()),
                new JwtIssuerValidator(properties.issuer()),
                audienceValidator(properties.audience())));

        return decoder;
    }

    /**
     * {@code aud} is a LIST in the JWT spec, even when it carries one value, and Nimbus reports it
     * as one. A validator written against {@code String} does not fail loudly - it fails to match,
     * so every token is rejected as having the wrong audience, which reads like a configuration
     * typo. Same family as the DLT binary-header trap in {@code DeadLetterRecorder}: when a value
     * has two plausible shapes, the wrong one usually parses.
     */
    private static OAuth2TokenValidator<Jwt> audienceValidator(String expected) {
        return new JwtClaimValidator<List<String>>(
                JwtClaimNames.AUD, aud -> aud != null && aud.contains(expected));
    }
}
