package com.dpe.gateway.support;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Base class for gateway tests. Real Postgres, no broker.
 *
 * <p>No broker because everything worth testing here is a database property: the UNIQUE
 * constraint on {@code transfer_id}, and the fact that the charge row and the outbox reply share
 * a transaction. Kafka in the middle would test the delivery path, which is proven elsewhere.
 */
@SpringBootTest(properties = {
        // The listener must not start with no broker present - it would retry a connection it
        // will never get and slow every test down behind its backoff.
        "spring.kafka.listener.auto-startup=false",
        // No background relay draining the outbox while a test asserts on it.
        "dpe.outbox.scheduled=false",
        // Deterministic simulation by default: always approve, no latency. Each test that cares
        // sets the knobs it needs.
        "gateway.simulation.failure-rate=0.0",
        "gateway.simulation.latency-ms=0",
        "gateway.simulation.timeout-rate=0.0",
        "gateway.simulation.duplicate-callback-rate=0.0"
})
public abstract class AbstractPostgresIT {

    /**
     * M5 part 2: hand the application the public half of the test key pair.
     *
     * <p>A dynamic property rather than a line in {@code application.properties}, because the key
     * is generated per JVM and never committed - see {@link TestSigningKeys}. It puts the
     * production decoder on its static-key path; the {@code jwk-set-uri} path these services
     * actually use in production needs an HTTP server to fetch from, which a MOCK web environment
     * has not got, so it is verified live on Compose instead.
     */
    @DynamicPropertySource
    static void trustTheTestSigningKey(DynamicPropertyRegistry registry) {
        // Blank out the JWKS URI application.yml configures for production. Not a tidiness
        // measure: SecurityProperties refuses to start with both sources set, because two
        // sources of truth for a verification key is a configuration that looks fine until the
        // day they disagree.
        registry.add("dpe.security.jwk-set-uri", () -> "");
        registry.add("dpe.security.public-key", TestSigningKeys::publicKeyBase64);
    }


    // Testcontainers 2.x: PostgreSQLContainer is NOT generic, so new PostgreSQLContainer<>(...)
    // does not compile. Singleton rather than @Container so the suite pays startup once.
    @ServiceConnection
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    static {
        POSTGRES.start();
    }

    @Autowired
    protected JdbcTemplate jdbc;

    @BeforeEach
    void resetGateway() {
        jdbc.execute("TRUNCATE TABLE gateway_charges");
        jdbc.execute("TRUNCATE TABLE outbox");
        jdbc.execute("TRUNCATE TABLE inbox");
    }
}
