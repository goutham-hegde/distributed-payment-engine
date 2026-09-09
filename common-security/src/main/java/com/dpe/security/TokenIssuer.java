package com.dpe.security;

import com.nimbusds.jose.jwk.source.ImmutableSecret;
import com.nimbusds.jose.proc.SecurityContext;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.JwsHeader;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;
import org.springframework.security.oauth2.jwt.NimbusJwtEncoder;
import org.springframework.stereotype.Component;

/**
 * Mints a signed token.
 *
 * <h2>Read this bean's existence as a defect report, because it is one</h2>
 *
 * <p>It lives in the shared library and is therefore registered in <b>all three services</b> -
 * account-service and payment-gateway both hold a bean that can issue an operator token for the
 * whole system, and neither has any business doing so. That is not sloppiness in the packaging;
 * it is the unavoidable truth of a symmetric algorithm. <b>With HS256 the verification key and
 * the signing key are the same bytes, so every validator is also a forger.</b> Moving this class
 * into payment-orchestrator would hide that, not fix it: the other two services would still hold
 * the secret, and forging would be a four-line class away.
 *
 * <p>The real fix is asymmetric signing, and it is M5 part 2. RS256 splits the key in half: the
 * orchestrator keeps a private key and can sign; the other two get only a public key and can
 * check a signature but not produce one. At that point this class stops being constructible
 * outside the issuer, and the difference is enforced by mathematics rather than by convention.
 *
 * <h2>The claims, and why each is there</h2>
 *
 * <ul>
 *   <li>{@code sub} - the owner id. It is the value {@code accounts.owner_id} is compared
 *       against, and it replaces the M4 {@code X-Client-Id} header as the idempotency namespace.
 *       That is the whole point of the milestone in one claim: the namespace used to be chosen by
 *       the caller and is now asserted by the issuer.</li>
 *   <li>{@code iss}, {@code aud}, {@code exp}, {@code iat} - checked on the way back in; see
 *       {@link JwtConfig}.</li>
 *   <li>{@code jti} - a unique id per token. Nothing reads it today. It is here because it is the
 *       hook every revocation design needs: a denylist keyed on {@code jti} is the standard way to
 *       claw back a stateless token before it expires, and retrofitting the claim later means the
 *       tokens already in the wild cannot be revoked.</li>
 *   <li>{@link Roles#CLAIM} - the role names. Read on the way in by each service's authorities
 *       converter.</li>
 * </ul>
 *
 * <p>What is NOT in the token: account ids. It would be tempting to list the accounts a subject
 * owns and skip the ownership lookup entirely - and it would be wrong twice over. The token would
 * grow without bound with the account count, and worse, ownership would be frozen at issue time:
 * an account closed a minute ago stays spendable until the token expires. Claims are a snapshot;
 * authorization over mutable state needs a lookup.
 */
@Component
public class TokenIssuer {

    private final JwtEncoder encoder;
    private final SecurityProperties properties;

    public TokenIssuer(SecurityProperties properties) {
        this.properties = properties;
        // ImmutableSecret is the HS256 case of a JWKSource: one key, no rotation, no key id.
        // Part 2 replaces it with an RSA key pair, at which point the `kid` header starts
        // mattering because a validator must be able to tell which key signed what.
        this.encoder = new NimbusJwtEncoder(
                new ImmutableSecret<SecurityContext>(properties.signingKey()));
    }

    /**
     * @param subject the owner id this token speaks for
     * @param roles   role names WITHOUT the {@code ROLE_} prefix - the prefix is a Spring
     *                Security convention on the authority, not part of the claim
     * @param ttl     how long it is valid. The only revocation mechanism this system has.
     */
    public String issue(String subject, Collection<String> roles, Duration ttl) {
        Instant now = Instant.now();

        JwtClaimsSet claims = JwtClaimsSet.builder()
                .issuer(properties.issuer())
                .audience(List.of(properties.audience()))
                .subject(subject)
                .issuedAt(now)
                .expiresAt(now.plus(ttl))
                .id(UUID.randomUUID().toString())
                .claim(Roles.CLAIM, List.copyOf(roles))
                .build();

        return encoder.encode(JwtEncoderParameters.from(
                JwsHeader.with(MacAlgorithm.HS256).build(), claims)).getTokenValue();
    }
}
