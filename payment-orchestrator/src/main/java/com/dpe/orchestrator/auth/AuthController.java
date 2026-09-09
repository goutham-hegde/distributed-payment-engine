package com.dpe.orchestrator.auth;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Exchanges a username and password for a bearer token.
 *
 * <h2>This is a stand-in for an identity provider, and it should be read as one</h2>
 *
 * <p>In any real deployment this endpoint does not exist: Keycloak, Auth0, Cognito or an internal
 * SSO issues the token, and the three services here validate it exactly as they already do -
 * because that is the property that makes JWT worth the trouble. Validation is local and needs no
 * conversation with the issuer, so swapping this controller for a real IdP changes one
 * configuration key (which key to trust) and nothing else. Nothing downstream of
 * {@code JwtConfig} knows or cares where a token came from.
 *
 * <p>What it deliberately does not have, and what a real one would:
 *
 * <ul>
 *   <li><b>Hashed passwords.</b> These are plain text in {@code application.yml}. A user store
 *       holds a slow hash - bcrypt or argon2 - so that a stolen database does not hand over
 *       everyone's password. Fast hashes (SHA-256) are not an improvement worth making; they are
 *       fast for the attacker too, which is the entire point of the slow ones.</li>
 *   <li><b>Rate limiting.</b> Nothing here stops a thousand password guesses a second. This is
 *       the endpoint an attacker attacks, because it is the one that mints credentials.</li>
 *   <li><b>Refresh tokens.</b> When this token expires the client logs in again. Refresh tokens
 *       exist to keep access-token lifetimes short without asking the user to re-authenticate,
 *       and they need durable storage and their own revocation - a second mechanism, deliberately
 *       out of scope.</li>
 *   <li><b>Lockout, MFA, audit of failed attempts.</b> All of it.</li>
 * </ul>
 *
 * <p>The demo users' subjects are the same strings as {@code accounts.owner_id}, which is what
 * makes {@code sub} usable as an ownership check with no mapping table in between.
 *
 * <p>M5 part 2 changed nothing in this file, which is the interesting part: the tokens it hands
 * out are now RS256, signed with a private key no other service holds, and the controller did not
 * have to know. Issuance is one bean away - see {@link TokenIssuer}.
 */
@RestController
@RequestMapping("/auth")
public class AuthController {

    private static final Logger log = LoggerFactory.getLogger(AuthController.class);

    private final AuthProperties properties;
    private final TokenIssuer tokens;

    public AuthController(AuthProperties properties, TokenIssuer tokens) {
        this.properties = properties;
        this.tokens = tokens;
    }

    /**
     * 200 with a token, or 401. There is no third answer, and in particular no distinction
     * between "no such user" and "wrong password".
     *
     * <p>That is not vagueness for its own sake: a login endpoint that answers the two
     * differently is a user-enumeration oracle - an attacker learns which accounts exist before
     * spending a single guess on a password, which is most of the work.
     */
    @PostMapping("/token")
    public ResponseEntity<TokenResponse> token(@Valid @RequestBody TokenRequest request) {
        AuthProperties.User user = properties.users().get(request.username());

        if (user == null || !passwordMatches(user.password(), request.password())) {
            // Logged at WARN with the username but never the password attempt. A failed login is
            // the signal an operator wants; the string somebody typed is very often their
            // password for another system, and putting it in a log file spreads a credential.
            log.warn("failed token request for '{}'", request.username());
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }

        String token = tokens.issue(request.username(), user.roles(), properties.tokenTtl());
        log.info("issued token for '{}' with roles {} (ttl {})",
                request.username(), user.roles(), properties.tokenTtl());

        return ResponseEntity.ok(new TokenResponse(token, "Bearer",
                properties.tokenTtl().toSeconds(), request.username(), user.roles()));
    }

    /**
     * Constant-time comparison.
     *
     * <p>{@code String.equals} returns as soon as two bytes differ, so the time it takes leaks how
     * much of the password was right - enough, over many attempts, to recover it one character at
     * a time. The concern is largely theoretical over a network, and it is one line to remove
     * anyway. {@code MessageDigest.isEqual} is the JDK's constant-time byte comparison; the name
     * is a historical accident and it hashes nothing.
     */
    private static boolean passwordMatches(String expected, String presented) {
        return MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.UTF_8),
                presented.getBytes(StandardCharsets.UTF_8));
    }

    public record TokenRequest(@NotBlank String username, @NotBlank String password) {
    }

    /**
     * @param expiresInSeconds returned so a client can refresh before expiry rather than
     *                         discovering it through a 401 in the middle of something. The token
     *                         carries {@code exp} too, but a client should not have to parse a
     *                         credential to use it.
     */
    public record TokenResponse(String accessToken,
                                String tokenType,
                                long expiresInSeconds,
                                String subject,
                                List<String> roles) {
    }
}
