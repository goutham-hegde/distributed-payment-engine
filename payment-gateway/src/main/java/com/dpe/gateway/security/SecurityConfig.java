package com.dpe.gateway.security;

import com.dpe.security.ManagementPortMatcher;
import com.dpe.security.Roles;
import com.dpe.security.RolesConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;

/**
 * The simulated PSP's HTTP rules.
 *
 * <p>Same shape as account-service's: deny by default, health open for probes, {@code /admin/**}
 * behind the operator role - with one deliberate exception that is worth defending rather than
 * hiding.
 *
 * <h2>The chaos knobs stay reachable by an operator, and only an operator</h2>
 *
 * <p>{@code POST /admin/simulation} makes this gateway start declining or timing out. Before M5
 * it was open to anybody, with a javadoc arguing it was "a test affordance on a simulated third
 * party". That was true and it is no longer a good enough reason: it is now the only unprotected
 * endpoint in the system, it can take down the payment path for every customer at once, and the
 * M6.5 console will call it from a browser. An availability control is a security control.
 *
 * <p>It falls under {@code /admin/**} and therefore needs the operator role like everything else
 * there. The chaos scripts at M7 obtain a token the same way any other client does - one extra
 * line in each script, and it keeps the demo honest: the console has a login, so its chaos
 * buttons should need one too.
 *
 * <p>What has not changed: this service exists only to be a failure generator. If it were ever
 * deployed anywhere real, the controller behind those knobs would be deleted rather than
 * protected.
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
                        .requestMatchers("/actuator/health", "/actuator/health/**",
                                "/actuator/info").permitAll()
                        .requestMatchers("/actuator/**").hasRole(Roles.OPERATOR)

                        // Both the dead letter console and the simulation knobs.
                        .requestMatchers("/admin/**").hasRole(Roles.OPERATOR)

                        .anyRequest().denyAll())

                .oauth2ResourceServer(oauth2 ->
                        oauth2.jwt(jwt -> jwt.jwtAuthenticationConverter(roles)));

        return http.build();
    }

    /**
     * Recognises the management connector. Fails closed - see {@link ManagementPortMatcher}.
     */
    @Bean
    ManagementPortMatcher managementPortMatcher(
            @Value("${server.port:8080}") int applicationPort,
            @Value("${management.server.port:-1}") int managementPort) {
        return new ManagementPortMatcher(applicationPort, managementPort);
    }

    /**
     * <b>M6: the scrape path.</b> Requests arriving on the management connector are permitted;
     * every rule above still governs the application connector unchanged.
     *
     * <p>Port 9091 is not published in {@code infra/docker-compose.yml}, so this is reachable from
     * the Compose network and nowhere else. The trust boundary is the network - the same answer
     * this service already gives for {@code dpe.account.commands.v1}.
     *
     * <p>It has to be a separate chain, not a {@code requestMatchers} rule, because the question is
     * which socket the request arrived on and {@code requestMatchers} only sees a path. And it has
     * to exist at all because a separate management port is <b>not</b> unprotected by default: Boot
     * registers this context's own filter chain on the management connector. The full reasoning,
     * with the autoconfiguration that does it, is in
     * {@code payment-orchestrator}'s {@code SecurityConfig#managementChain} and in
     * {@link ManagementPortMatcher}.
     */
    @Bean
    @Order(0)
    SecurityFilterChain managementChain(HttpSecurity http, ManagementPortMatcher managementPort)
            throws Exception {
        http
                .securityMatcher(managementPort)
                .csrf(AbstractHttpConfigurer::disable)
                .sessionManagement(session ->
                        session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth.anyRequest().permitAll());

        return http.build();
    }
}
