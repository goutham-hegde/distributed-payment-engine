package com.dpe.security;

import java.util.List;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtClaimNames;
import org.springframework.security.oauth2.jwt.JwtClaimValidator;
import org.springframework.security.oauth2.jwt.JwtIssuerValidator;
import org.springframework.security.oauth2.jwt.JwtTimestampValidator;

/**
 * Wires token verification into a service. Import it from the service's application class, next to
 * {@code MessagingConfig}.
 *
 * <pre>
 * &#64;SpringBootApplication
 * &#64;Import({MessagingConfig.class, JwtConfig.class, JwtDecoderConfig.class})
 * public class AccountServiceApplication { }
 * </pre>
 *
 * <p>This class binds the properties, registers {@link RolesConverter}, and owns the claim checks.
 * It does <b>not</b> build the decoder - {@link JwtDecoderConfig} does, and a service imports that
 * one only if its verification key comes from configuration. payment-orchestrator does not: it
 * holds the signing key and builds a decoder from the public half in memory.
 *
 * <h2>What this class decides, and what it refuses to decide</h2>
 *
 * <p>It decides how a token is <b>verified</b>: which key, which algorithm, which claims must
 * hold. It decides nothing about who may call what - no filter chain, no matcher, no role. Those
 * live in each service's own {@code SecurityConfig}, because they are statements about that
 * service's resources. A shared filter chain would mean loosening a rule for one endpoint quietly
 * loosens it in all three services.
 *
 * <p><b>M5 part 2 removed something from this module, and the removal is the point:</b>
 * {@code TokenIssuer} used to live here and was therefore a bean in all three services. Under
 * HS256 that was honest - the verification key and the signing key were the same bytes, so every
 * validator really could forge. Under RS256 it would be a lie, so the class moved to
 * payment-orchestrator, where the private key is. Nothing in this module can sign anything.
 *
 * <h2>The three validators, and what each one actually prevents</h2>
 *
 * <p>A signature check alone proves only that <i>the holder of the private key</i> made this
 * token. It says nothing about when, for whom, or for which service - so every claim below is
 * checking a question the signature cannot answer.
 *
 * <ol>
 *   <li><b>Timestamp</b> ({@code exp}, {@code nbf}). The only expiry a stateless token has. This
 *       is the entire revocation story: a token cannot be withdrawn, so the window in which a
 *       stolen one is useful is exactly its remaining lifetime.</li>
 *   <li><b>Issuer</b> ({@code iss}). Rejects a token minted by a different system. Weaker than it
 *       was in part 1 - only one component can sign now - and kept because it costs nothing and
 *       still asserts something true.</li>
 *   <li><b>Audience</b> ({@code aud}). The confused-deputy check: it stops a token good enough for
 *       one service being replayed against another that trusts the same issuer.</li>
 * </ol>
 *
 * <p>Not validated, deliberately: the {@code alg} header is pinned by the decoder rather than read
 * from the token. That is what makes the two classic JWT attacks structurally impossible here -
 * "alg: none", and the RS256-key-presented-as-an-HS256-secret confusion. The second one is worth
 * noticing now that the verification key is <b>public</b>: a verifier that let the token choose
 * its algorithm would accept a token HMAC-signed with the public key, which anybody can download
 * from the JWKS endpoint. Pinning RS256 is not belt and braces, it is the thing that stops the
 * public key from becoming a signing key.
 */
@Configuration
@EnableConfigurationProperties(SecurityProperties.class)
public class JwtConfig {

    /**
     * Declared rather than component-scanned, and the reason is a trap worth remembering.
     *
     * <p>This class used to carry {@code @ComponentScan(basePackageClasses = JwtConfig.class)} to
     * pick up the one component in this module. A {@code @Configuration} class IS a component, so
     * that scan also swept up {@link JwtDecoderConfig} - in <b>every</b> service, including the
     * issuer, which supplies its own decoder. The result was two {@code jwtDecoder} definitions
     * and a {@code BeanDefinitionOverrideException} at startup.
     *
     * <p>Loud, and it could have been worse: had bean overriding been enabled, one decoder would
     * have silently replaced the other and this service would have verified tokens with a key
     * nobody chose. An "import this to opt in" design is not opt-in if a scan can find the class
     * anyway.
     */
    @Bean
    public RolesConverter rolesConverter() {
        return new RolesConverter();
    }

    /**
     * The claim checks, shared by every decoder in the system.
     *
     * <p>Public so payment-orchestrator can build its own decoder around its signing key and still
     * apply exactly these. Two services validating the same tokens by two slightly different rules
     * is a bug that only shows up as "it works everywhere except there".
     */
    public static OAuth2TokenValidator<Jwt> validators(SecurityProperties properties) {
        return new DelegatingOAuth2TokenValidator<>(
                new JwtTimestampValidator(properties.clockSkew()),
                new JwtIssuerValidator(properties.issuer()),
                audienceValidator(properties.audience()));
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
