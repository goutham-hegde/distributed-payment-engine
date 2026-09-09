package com.dpe.security;

import java.util.Collection;
import java.util.List;
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;

/**
 * Turns a verified token into an {@code Authentication} carrying Spring Security authorities.
 *
 * <p>Registered as a bean by {@link JwtConfig}, not by a stereotype annotation - see the note
 * there about what a component scan in a shared library sweeps up along the way.
 *
 * <p>Ten lines, and the place a role check silently stops working. Every service's filter chain
 * consumes it, so a mistake here is not local to one endpoint: it reads {@link Roles#CLAIM} - a
 * list of strings such as {@code ["USER"]} - and returns a {@link JwtAuthenticationToken} whose
 * authorities are those names with {@link Roles#PREFIX} in front.
 *
 * <h2>The three things worth getting right</h2>
 *
 * <p><b>1. The prefix.</b> {@code hasRole("OPERATOR")} looks for the authority
 * {@code ROLE_OPERATOR}; {@code hasAuthority("OPERATOR")} looks for {@code OPERATOR}. Get the
 * pairing wrong and the rule matches nothing - and a rule that matches nothing does not error, it
 * denies. You will be looking at a 403 for a caller whose token plainly contains the right role.
 *
 * <p><b>2. A missing or empty claim is not an error.</b> It yields a token with no authorities -
 * a perfectly valid authenticated identity allowed to do nothing except reach endpoints requiring
 * only {@code authenticated()}. Throwing here would turn an authorization question into a 500.
 *
 * <p><b>3. Do not invent authorities the token did not carry.</b> No default role, however
 * convenient it seems for the demo. A default is a privilege granted by the validator rather than
 * by the issuer, and it applies to every token that ever reaches this service - including one
 * minted for something else entirely.
 *
 * <p>Written by hand rather than with {@code JwtGrantedAuthoritiesConverter} configured to read
 * {@code roles}. That class exists and works; it also hides the one line - the prefix - that this
 * whole file is about.
 *
 * <h2>Why the roles are in the token at all</h2>
 *
 * <p>Because the alternative is a lookup per request, in every service, against a user store that
 * would then be on the hot path of everything - which is the stateful-session design JWT was
 * chosen to avoid. The cost is that roles are a SNAPSHOT: revoking a role does not reach a token
 * already issued, so the change takes effect only when that token expires. That is the same
 * trade-off as revocation itself, and it is why the TTL is minutes rather than hours.
 */
public class RolesConverter implements Converter<Jwt, AbstractAuthenticationToken> {

    @Override
    public AbstractAuthenticationToken convert(Jwt jwt) {
        return new JwtAuthenticationToken(jwt, authorities(jwt));
    }

    /**
     * Reads the claim defensively rather than with {@code getClaimAsStringList}.
     *
     * <p>That helper throws when the claim is present but not a list of strings, which turns a
     * malformed token - a producer bug, or somebody probing - into a 500 from inside the security
     * filter chain. Here anything unexpected collapses to <b>no authorities</b>: the caller is
     * authenticated, holds nothing, and every role rule denies. Fail closed and answer 403, rather
     * than fail loudly and answer 500.
     */
    private static Collection<GrantedAuthority> authorities(Jwt jwt) {
        Object claim = jwt.getClaim(Roles.CLAIM);
        if (!(claim instanceof Collection<?> names)) {
            // Absent claim, or a single string where a list was expected. Not an error: a token
            // with no roles is a valid identity that may only reach endpoints requiring nothing
            // more than authentication.
            return List.of();
        }
        return names.stream()
                .filter(String.class::isInstance)
                .map(String.class::cast)
                // ROLE_ is the prefix hasRole() looks for. hasRole("OPERATOR") searches for the
                // authority ROLE_OPERATOR; hasAuthority("OPERATOR") searches for OPERATOR. Getting
                // the pairing wrong makes a rule match nothing - and a rule that matches nothing
                // does not error, it denies.
                .map(name -> (GrantedAuthority) new SimpleGrantedAuthority(Roles.PREFIX + name))
                .toList();
    }
}
