package com.dpe.orchestrator.auth;

import com.dpe.security.Roles;
import com.dpe.security.SecurityProperties;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.springframework.security.oauth2.jose.jws.SignatureAlgorithm;
import org.springframework.security.oauth2.jwt.JwsHeader;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;
import org.springframework.security.oauth2.jwt.NimbusJwtEncoder;
import org.springframework.stereotype.Component;

/**
 * Mints a signed token. <b>This class lives in payment-orchestrator and nowhere else.</b>
 *
 * <p>In M5 part 1 it sat in {@code common-security} and was therefore a bean in all three
 * services - a deliberate, uncomfortable piece of honesty, because HS256's verification key and
 * signing key are the same bytes and account-service really could have minted itself an operator
 * token. Moving the class would have hidden that, not fixed it.
 *
 * <p>Part 2 fixed it, and the move follows the fix rather than substituting for it. The
 * constructor needs {@link SigningKeys}, which needs a private key, which exists only here. There
 * is no arrangement of imports that lets another service build one.
 *
 * <h2>The claims, and why each is there</h2>
 *
 * <ul>
 *   <li>{@code sub} - the owner id. It is the value {@code accounts.owner_id} is compared against,
 *       and it is the idempotency namespace. The namespace used to be chosen by the caller
 *       ({@code X-Client-Id}); it is now asserted by the issuer.</li>
 *   <li>{@code iss}, {@code aud}, {@code exp}, {@code iat} - checked on the way back in; see
 *       {@code JwtConfig}.</li>
 *   <li>{@code jti} - unique per token. Nothing reads it. It is the hook every revocation design
 *       needs, and a claim that has to exist <i>before</i> it is needed: tokens already issued
 *       cannot be given one retroactively, so they could never be revoked by id.</li>
 *   <li>{@link Roles#CLAIM} - the role names, read back by each service's authorities converter.
 *       </li>
 * </ul>
 *
 * <p>And in the header, new at part 2: {@code kid}. It names which key signed this token, so a
 * validator holding several can pick the right one - which is what makes rotation possible
 * without a synchronised restart of everything.
 *
 * <p>What is NOT in the token: account ids. Listing the accounts a subject owns would skip the
 * ownership lookup entirely, and it is wrong twice - the token grows with the account count, and
 * ownership freezes at issue time, so an account closed a minute ago stays spendable until expiry.
 * Claims are a snapshot; authorization over mutable state needs a lookup.
 */
@Component
public class TokenIssuer {

    private final JwtEncoder encoder;
    private final SecurityProperties properties;
    private final String keyId;

    public TokenIssuer(SigningKeys keys, SecurityProperties properties) {
        this.properties = properties;
        this.keyId = keys.keyId();
        this.encoder = new NimbusJwtEncoder(keys.signingSource());
    }

    /**
     * @param subject the owner id this token speaks for
     * @param roles   role names WITHOUT the {@code ROLE_} prefix - that prefix is a Spring
     *                Security convention on the authority, not part of the claim
     * @param ttl     how long it is valid. Still the only revocation mechanism this system has:
     *                RS256 changed who can sign, not whether a signed token can be withdrawn.
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

        JwsHeader header = JwsHeader.with(SignatureAlgorithm.RS256)
                .keyId(keyId)
                .build();

        return encoder.encode(JwtEncoderParameters.from(header, claims)).getTokenValue();
    }
}
