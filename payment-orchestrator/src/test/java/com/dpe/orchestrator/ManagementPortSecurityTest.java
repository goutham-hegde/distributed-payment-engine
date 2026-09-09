package com.dpe.orchestrator;

import static org.assertj.core.api.Assertions.assertThat;

import com.dpe.orchestrator.support.AbstractPostgresIT;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.springframework.boot.micrometer.metrics.test.autoconfigure.AutoConfigureMetrics;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.util.TestSocketUtils;

/**
 * M6. The scrape path: what the management connector exposes, and - more importantly - what it
 * does not.
 *
 * <p>The rest of the suite runs with the actuator collapsed onto the application port (see
 * {@code src/test/resources/application.properties}), which exercises the fail-closed branch of
 * {@link com.dpe.security.ManagementPortMatcher}. This class is the only place the real
 * two-connector deployment posture is asserted, so everything it checks is checked nowhere else.
 *
 * <p><b>Named {@code ...Test}, not {@code ...IT}, and that is not cosmetic.</b> This build
 * configures no Failsafe plugin, and Surefire's default includes are {@code *Test}, {@code Test*}
 * and {@code *Tests}. A class called {@code ManagementPortSecurityTest} compiles, passes when run
 * explicitly with {@code -Dtest=}, and is silently skipped by {@code ./mvnw verify} - the test
 * count simply does not go up, which is the one signal nobody checks. Every test class in this
 * repository ends in {@code Test} for that reason.
 *
 * <h2>The belief this exists to falsify</h2>
 *
 * <p>"Put the actuator on another port and it is unauthenticated" is <b>false</b> in Spring Boot,
 * and it fails in the safe direction, which is why it survives review: the scraper gets a 401 and
 * somebody adds a credential rather than finding out why. Boot's
 * {@code ServletManagementChildContextConfiguration} pulls the <i>parent</i> context's
 * {@code springSecurityFilterChain} onto the management connector, so every M5 rule applies there
 * too. {@code managementChain} is what actually opens it, and if that bean is ever deleted this
 * test fails rather than the scrape quietly going stale.
 *
 * <h2>Why a random port, and why not port 0</h2>
 *
 * <p>The port is picked at runtime so a developer running the suite while Compose is up does not
 * collide with a real 9091. It cannot be {@code management.server.port=0} - Boot would allocate
 * one, but {@code ManagementPortMatcher} is constructed from configuration and would see 0, treat
 * it as "no separate connector", and match nothing. That is the deliberate fail-closed reading of
 * a port it cannot identify, so the test names a real port instead.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                // TRAP 10, from M4, and this class is exactly where it bites: @SpringBootTest
                // properties do NOT merge down a class hierarchy. Spring takes the first
                // declaration it finds, so re-declaring the annotation here DISCARDS every
                // property AbstractPostgresIT sets. All five are repeated deliberately - drop one
                // and a background timer comes back on and races this test.
                "spring.kafka.listener.auto-startup=false",
                "dpe.outbox.scheduled=false",
                "dpe.saga.scheduled=false",
                "dpe.idempotency.cache=false",
                "dpe.idempotency.scheduled=false",
        })
@AutoConfigureMetrics
@DisplayName("M6: the management connector is open, and serves only the actuator")
class ManagementPortSecurityTest extends AbstractPostgresIT {

    /**
     * Chosen ONCE, into a constant.
     *
     * <p>This looks like it could be inlined as a method reference below. It cannot, and the
     * failure is silent. {@link DynamicPropertyRegistry#add(String, java.util.function.Supplier)}
     * takes a supplier that is invoked on <b>every</b> resolution of the property, not once - and
     * {@code TestSocketUtils::findAvailableTcpPort} returns a DIFFERENT free port each time it is
     * called. Passed directly, the web server binds one port and
     * {@link com.dpe.security.ManagementPortMatcher} is constructed with another, so the matcher
     * compares the request's real port against a port nothing is listening on, matches nothing,
     * and every request falls through to the M5 chain. The symptom is a 401 from a connector that
     * is supposed to be open, which reads exactly like the security rule being wrong.
     *
     * <p>A dynamic property supplier must be idempotent. This one is now a constant, so it is.
     */
    private static final int MANAGEMENT_PORT = TestSocketUtils.findAvailableTcpPort();

    /**
     * Overrides the {@code management.server.port=8081} set for the rest of the suite in
     * {@code src/test/resources/application.properties}. A {@code @DynamicPropertySource} wins over
     * a config file, which is the only reason one class can run a different posture from the other
     * sixty-odd.
     */
    @DynamicPropertySource
    static void managementPort(DynamicPropertyRegistry registry) {
        registry.add("management.server.port", () -> MANAGEMENT_PORT);
    }

    @LocalServerPort
    int applicationPort;

    @LocalManagementPort
    int managementPort;

    /**
     * The JDK's own client, deliberately, rather than {@code TestRestTemplate}. Boot 4 moved that
     * class into {@code spring-boot-resttestclient}, made the bean opt-in behind
     * {@code @AutoConfigureTestRestTemplate}, and left it needing {@code spring-boot-restclient}
     * for {@code RestTemplateBuilder} - three modules and an annotation to issue a GET and read a
     * status code. This test needs neither connection pooling nor message conversion, and it must
     * NOT throw on a 4xx, which is the one thing a bare {@code RestTemplate} would do.
     */
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    @Test
    @DisplayName("two distinct connectors are actually listening")
    void twoConnectors() {
        assertThat(managementPort)
                .as("the whole design depends on these being different sockets")
                .isNotEqualTo(applicationPort)
                .isPositive();
    }

    @Test
    @DisplayName("Prometheus scrapes the management port with no credential at all")
    void scrapeNeedsNoCredential() throws Exception {
        HttpResponse<String> response = get(managementPort, "/actuator/prometheus");

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body())
                .as("a registry that serialises nothing means micrometer-registry-prometheus is missing")
                .contains("jvm_memory_used_bytes")
                // The common tag from management.metrics.tags. Dashboards group on it, so its
                // absence would leave every panel empty while the scrape looked healthy.
                .contains("application=\"payment-orchestrator\"");
    }

    @Test
    @DisplayName("the application port still refuses the actuator, exactly as M5 left it")
    void applicationPortStillClosed() throws Exception {
        // 401, not 403: no token was presented, so the answer is "I do not know who you are".
        // This is the assertion that would fail if someone "simplified" the two chains into one
        // permitAll rule on /actuator/**.
        assertThat(get(applicationPort, "/actuator/prometheus").statusCode()).isEqualTo(401);
    }

    @Test
    @DisplayName("the management connector serves ONLY the actuator - the business API is not on it")
    void businessApiIsNotOnTheManagementPort() throws Exception {
        // This is what bounds the permitAll. The management child context has its own
        // DispatcherServlet with only the actuator's handler mappings, so the payments API is not
        // merely denied there - it is not mapped at all, and a 404 is the proof.
        //
        // Without this, "permit everything on 9091" would be a second, unauthenticated door onto
        // POST /api/v1/transfers, which is the one thing this whole milestone must not do.
        // 404, not 403 and not 401: the handler is not there to deny. Contrast the application
        // port in the next assertion, which cannot produce a 404 at all.
        assertThat(get(managementPort, "/api/v1/transfers").statusCode()).isEqualTo(404);
        assertThat(get(managementPort, "/auth/token").statusCode()).isEqualTo(404);

        // And the same paths on the application port ARE mapped, so they answer with a security
        // decision instead. Without this half, the assertions above would also pass against a
        // service that had no transfer API at all.
        assertThat(get(applicationPort, "/api/v1/transfers").statusCode()).isEqualTo(401);
    }

    @Test
    @DisplayName("health moved with the actuator - which is why the Compose probe had to move too")
    void healthIsOnTheManagementPortOnly() throws Exception {
        HttpResponse<String> onManagementPort = get(managementPort, "/actuator/health");

        // 200 UP or 503 DOWN - either one proves the endpoint is SERVED here, which is all this
        // test claims. It is 503 in this class because AbstractPostgresIT starts Postgres and no
        // broker, so the Kafka health indicator reports DOWN. Asserting a bare 200 would make this
        // test a statement about health indicators instead of about routing.
        assertThat(onManagementPort.statusCode()).isIn(200, 503);
        assertThat(onManagementPort.body()).contains("\"status\"");

        // 401 on the application port, and the reason is worth knowing because it is NOT "health
        // is protected there" - /actuator/health is permitAll on that chain too.
        //
        // The endpoint moved, so there is no handler; the 404 becomes a servlet ERROR dispatch to
        // /error; and /error matches nothing in the M5 chain, so `anyRequest().denyAll()` denies
        // it. Every unmapped path on the application port therefore answers 401 to an anonymous
        // caller - it cannot return a 404 at all.
        //
        // That is a real property of `denyAll()` and a desirable one: an anonymous caller cannot
        // distinguish "no such endpoint" from "not allowed", so the application port cannot be
        // walked for its routes. It is the same reasoning as answering 404 on the transfer read
        // path, one layer down.
        //
        // For the Compose probe the practical consequence is what matters: pointed at the old
        // port it gets 401 forever, and the container is reported unhealthy with nothing to say
        // the endpoint merely moved.
        assertThat(get(applicationPort, "/actuator/health").statusCode()).isEqualTo(401);
    }

    private static HttpResponse<String> get(int port, String path)
            throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + path))
                .GET()
                .build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
