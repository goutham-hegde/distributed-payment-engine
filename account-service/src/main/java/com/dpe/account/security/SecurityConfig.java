package com.dpe.account.security;

import com.dpe.security.Roles;
import com.dpe.security.RolesConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Who may call this service over HTTP. The answer is "almost nobody", and that is the point.
 *
 * <h2>account-service is not a public API</h2>
 *
 * <p>Its real interface is Kafka: the orchestrator sends it commands and it replies with events.
 * The HTTP surface is administrative - open an account, read one, inspect the dead letter queue -
 * and none of it is anything a customer's browser should be able to reach. So the chain below
 * denies by default and names the few exceptions, rather than protecting the endpoints somebody
 * remembered to think about.
 *
 * <p><b>{@code anyRequest().denyAll()} rather than {@code authenticated()}.</b> The difference
 * shows up the day someone adds a controller and forgets to add a rule: with
 * {@code authenticated()} the new endpoint is reachable by anyone holding any valid token, which
 * in this system is every customer. With {@code denyAll()} it is reachable by nobody until a rule
 * is written for it, and the failure is a 403 in a test rather than an exposure in production.
 *
 * <h2>Why the Kafka path is not authenticated, and why that is not a hole</h2>
 *
 * <p>Commands arriving on {@code dpe.account.commands.v1} carry no token. The trust boundary
 * there is the broker's - who may produce to that topic - not a per-message credential, which is
 * how every message-driven system of this shape works. What the handler does <i>not</i> do is
 * assume the command was authorized because it arrived: {@code ReserveFunds} carries the subject
 * that asked for it, and {@code ReservationService} checks ownership itself before moving money.
 * That check is the one that matters, because it is inside the transaction that touches the
 * balance.
 */
@Configuration
public class SecurityConfig {

    @Bean
    SecurityFilterChain filterChain(HttpSecurity http, RolesConverter roles) throws Exception {
        http
                // No cookies, no browser-driven state, so there is no session for a cross-site
                // request to ride on. CSRF protection defends a credential the browser attaches
                // automatically; a bearer token in an Authorization header is not one - it has to
                // be put there by script that already has it.
                .csrf(AbstractHttpConfigurer::disable)

                // Never create an HttpSession. Not a performance note: a session would make this
                // service stateful, so a request could succeed on one instance and fail on
                // another, and horizontal scaling would need sticky routing or a shared session
                // store. Statelessness is the property being bought with a token.
                .sessionManagement(session ->
                        session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))

                .authorizeHttpRequests(auth -> auth
                        // Liveness and readiness, unauthenticated. The Compose healthcheck and
                        // (at M10) the kubelet call these with no credentials, and a probe that
                        // needs a token turns an expiring secret into a rolling restart. They
                        // expose no business data.
                        .requestMatchers("/actuator/health", "/actuator/health/**",
                                "/actuator/info").permitAll()

                        // Everything else under /actuator is operational detail - metrics
                        // cardinality, environment, mappings - and is worth a role even though
                        // none of it moves money.
                        //
                        // NOTE for M6: Prometheus scrapes /actuator/prometheus and will need a
                        // credential of its own, or an allowance for the scrape network. Left
                        // closed here rather than pre-opened, so that decision is made when the
                        // scraper actually exists.
                        .requestMatchers("/actuator/**").hasRole(Roles.OPERATOR)

                        // The dead letter console from common-messaging. Operator work by
                        // definition, and one of its endpoints republishes payment commands -
                        // which is exactly why it is the endpoint M5 existed to close.
                        .requestMatchers("/admin/**").hasRole(Roles.OPERATOR)

                        // Onboarding. An operator opens accounts; a customer does not open their
                        // own in this system, and the request body names the owner, so leaving it
                        // to any authenticated caller would let one customer create an account in
                        // another's name.
                        .requestMatchers("/accounts/**").hasRole(Roles.OPERATOR)

                        // /transfers - the direct ledger endpoint from M1 - is deliberately
                        // absent, and therefore denied by the rule below.
                        //
                        // It writes ledger entries without a saga, without an idempotency key and
                        // without an ownership check: a second door into the money that bypasses
                        // every mechanism M3, M4 and M5 built. There is no role that should have
                        // it. Not deleted, because TransferService behind it is still the ledger
                        // core and is exercised directly by the M1 concurrency tests - only its
                        // HTTP exposure is closed.
                        .anyRequest().denyAll())

                .oauth2ResourceServer(oauth2 ->
                        oauth2.jwt(jwt -> jwt.jwtAuthenticationConverter(roles)));

        return http.build();
    }
}
