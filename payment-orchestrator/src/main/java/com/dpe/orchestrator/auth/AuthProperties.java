package com.dpe.orchestrator.auth;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * The ISSUING side of authentication, and it exists in this service only.
 *
 * <p>Bound from {@code dpe.auth.*}. Kept under a different prefix from {@code dpe.security.*},
 * which every service binds, because these are the settings a real identity provider would own -
 * and at M5 part 2 the split becomes physical: the private key lands here, the public key stays
 * over there.
 *
 * @param tokenTtl how long an issued token is valid. <b>This number is the revocation policy.</b>
 *                 A stateless token cannot be withdrawn, so a stolen one works for exactly this
 *                 long. Fifteen minutes is the usual compromise: short enough that a leaked token
 *                 is a small window, long enough that a normal session is not re-authenticating
 *                 constantly. Making it hours is the mistake that turns a single leaked token
 *                 into a day-long breach; making it seconds means building refresh tokens, which
 *                 is a second mechanism with its own storage and its own revocation problem.
 * @param users    the demo directory. See {@link AuthController} for why this is configuration
 *                 rather than a table, and what that rules out.
 */
@ConfigurationProperties("dpe.auth")
public record AuthProperties(Duration tokenTtl, Map<String, User> users) {

    public AuthProperties {
        tokenTtl = tokenTtl == null ? Duration.ofMinutes(15) : tokenTtl;
        users = users == null ? Map.of() : Map.copyOf(users);
    }

    /**
     * @param password plain text, because this is a development directory in a YAML file - see
     *                 {@link AuthController}. A real user store holds a slow hash (bcrypt/argon2)
     *                 and never the password itself.
     * @param roles    role names without the {@code ROLE_} prefix, e.g. {@code [USER]}
     */
    public record User(String password, List<String> roles) {

        public User {
            roles = roles == null ? List.of() : List.copyOf(roles);
        }
    }
}
