package com.dpe.orchestrator.security;

import com.dpe.security.Roles;
import com.dpe.security.RolesConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;

/**
 * THE FILTER CHAIN. Every HTTP request into the orchestrator passes through it.
 *
 * <p>account-service and payment-gateway have simpler versions of this - deny everything, open
 * health, put {@code /admin/**} behind the operator role. This is the only service with a public
 * API, a login endpoint and two different kinds of caller, so it is the only one where the order
 * of the rules carries an argument.
 *
 * <h2>The rules</h2>
 *
 * <pre>
 *   POST /auth/token          permitAll        the login endpoint cannot require a login
 *   GET  /actuator/health**   permitAll        probes have no credentials
 *   GET  /actuator/info       permitAll
 *        /actuator/**         OPERATOR         metrics, env, mappings
 *        /admin/**            OPERATOR         dead letters, and one endpoint that REPUBLISHES
 *                                              payment commands
 *        /api/v1/transfers**  USER             move money / read your own transfers
 *   everything else           denyAll
 * </pre>
 *
 * <h2>Six decisions inside those seven lines</h2>
 *
 * <p><b>1. {@code denyAll()}, not {@code authenticated()}, as the catch-all.</b> They differ only
 * for endpoints nobody wrote a rule for - which is precisely the endpoint someone will add next
 * month. {@code authenticated()} exposes it to every customer holding any valid token;
 * {@code denyAll()} exposes it to nobody and shows up as a 403 the first time it is called.
 *
 * <p><b>2. Order is first-match-wins.</b> A broad matcher placed above a narrow one swallows it
 * silently - no warning, no error, just a rule that never runs. Hence {@code /actuator/health}
 * above {@code /actuator/**}: reversed, the health probe would need an operator token and the
 * container would never report healthy.
 *
 * <p><b>3. {@code /auth/token} is {@code permitAll} and it is the attack surface.</b> It is the one
 * endpoint that mints credentials, so it is where password guessing happens. Nothing here rate
 * limits it. It is also pinned to POST: a GET rule would leave the path open to a browser
 * navigation, and a credential should never be reachable through a URL.
 *
 * <p><b>4. CSRF disabled - because of what the credential is, not because it is inconvenient.</b>
 * CSRF exploits credentials the browser attaches on its own: cookies, basic auth. A bearer token
 * has to be put into a header by code that already holds it, so a cross-site form post cannot
 * carry it. If this API ever authenticates with a cookie, CSRF protection comes straight back.
 *
 * <p><b>5. Stateless sessions.</b> No {@code HttpSession}, ever. A session would make the
 * orchestrator stateful and its instances non-interchangeable, which is the thing a token buys.
 *
 * <p><b>6. The role check is NOT the ownership check.</b> {@code hasRole(USER)} says the caller is
 * a customer. It says nothing about <i>whose</i> account they named in the body, and no filter
 * chain can: the account id is in the request payload, not the URL. That decision belongs to
 * {@link com.dpe.orchestrator.authz.AccountOwnershipGuard}, called from inside the request.
 * Authentication is a filter; per-resource authorization is a lookup.
 *
 * <h2>401 versus 403</h2>
 *
 * <p>401 is "I do not know who you are" - no token, bad signature, expired. 403 is "I know who you
 * are and the answer is no". Returning 403 for a missing token would tell an attacker the endpoint
 * exists and their credential merely lacked a role; returning 401 for a valid token with the wrong
 * role sends a client into a pointless re-login loop. Spring gets this right by default, and
 * nothing here overrides it.
 */
@Configuration
public class SecurityConfig {

    @Bean
    SecurityFilterChain filterChain(HttpSecurity http, RolesConverter roles) throws Exception {
        http
                .csrf(AbstractHttpConfigurer::disable)
                .sessionManagement(session ->
                        session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))

                .authorizeHttpRequests(auth -> auth
                        // The one endpoint that hands out credentials, and therefore the one that
                        // cannot require one. POST only.
                        .requestMatchers(HttpMethod.POST, "/auth/token").permitAll()

                        // Probes. Unauthenticated because the Compose healthcheck and (at M10) the
                        // kubelet have no credentials, and an expiring secret must not be able to
                        // turn into a rolling restart.
                        .requestMatchers("/actuator/health", "/actuator/health/**",
                                "/actuator/info").permitAll()
                        .requestMatchers("/actuator/**").hasRole(Roles.OPERATOR)

                        // The dead letter console. One of its endpoints republishes payment
                        // commands, which is the single strongest reason this milestone exists.
                        .requestMatchers("/admin/**").hasRole(Roles.OPERATOR)

                        // The public API. Both patterns are listed rather than relying on `/**`
                        // to match the bare path - it does, under both matcher implementations,
                        // but a security rule that depends on knowing that is a security rule
                        // waiting for someone to change the matcher.
                        //
                        // OPERATOR is deliberately absent here: an operator sees everything and
                        // moves nothing.
                        .requestMatchers("/api/v1/transfers", "/api/v1/transfers/**")
                                .hasRole(Roles.USER)

                        .anyRequest().denyAll())

                // Without this the role rules above match nothing - the default converter reads
                // `scope`/`scp` into SCOPE_ authorities and never looks at `roles`. The symptom is
                // a 403 for a caller whose token visibly contains the right role.
                .oauth2ResourceServer(oauth2 ->
                        oauth2.jwt(jwt -> jwt.jwtAuthenticationConverter(roles)));

        return http.build();
    }
}
