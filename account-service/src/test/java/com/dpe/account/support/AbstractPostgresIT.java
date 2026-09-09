package com.dpe.account.support;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Base class for tests that need a real PostgreSQL.
 *
 * <p>Real Postgres, not H2. Every mechanism this milestone is about - {@code SELECT ... FOR
 * UPDATE} blocking semantics, deadlock detection, CHECK and UNIQUE constraint behaviour, the
 * READ COMMITTED default - is database-specific. A test against an in-memory database would pass
 * while the production behaviour was wrong, which is worse than having no test.
 *
 * <p>The container is a <b>singleton started once per JVM</b> rather than a JUnit
 * {@code @Container}. The JUnit extension stops a static container at the end of each test class,
 * so a five-class suite would pay the Postgres startup cost five times. Started here, it is
 * reused; Docker reaps it via Ryuk when the JVM exits.
 *
 * <p>{@code @ServiceConnection} replaces the {@code @DynamicPropertySource} boilerplate of Boot
 * 3.0 and earlier: Boot reads the container and wires the datasource URL, user and password
 * itself. Flyway then runs {@code V1__ledger_core.sql} against it on context start, so the
 * migration is exercised by every test rather than only in production.
 */
@SpringBootTest(properties = {
        // The relay's timer is off for every test by default. A background thread draining the
        // outbox while a test is asserting on it makes failures depend on scheduling, and the
        // tests that do exercise the relay call drainBatch() directly so their assertions are
        // deterministic. AbstractKafkaIT is where a broker actually exists.
        "dpe.outbox.scheduled=false"
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


    // Testcontainers 2.x: PostgreSQLContainer is NOT generic. The old self-typed
    // PostgreSQLContainer<SELF> of the 1.x org.testcontainers.containers package is gone along
    // with the package, so the familiar new PostgreSQLContainer<>(...) does not compile.
    @ServiceConnection
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    static {
        POSTGRES.start();
    }

    @Autowired
    protected JdbcTemplate jdbc;

    /**
     * Returns the database to the state the migrations leave it in: an empty ledger, an empty
     * outbox, no customer accounts, and a SYSTEM account at zero.
     *
     * <p>Deliberately not {@code @Transactional} rollback on the test method. These tests run
     * work on several threads, and a rollback-scoped test transaction is bound to one thread -
     * the other threads would not see the seed data, and nothing being tested would be
     * committed. Cleaning explicitly is the only honest way to test concurrency.
     */
    @BeforeEach
    void resetLedger() {
        // Order matters: holds carries a foreign key to accounts, so it has to go before the
        // customer accounts it references or the DELETE below fails on the constraint.
        jdbc.execute("TRUNCATE TABLE holds");
        jdbc.execute("TRUNCATE TABLE ledger_entries RESTART IDENTITY");
        jdbc.execute("TRUNCATE TABLE outbox");
        jdbc.execute("TRUNCATE TABLE inbox");
        jdbc.execute("TRUNCATE TABLE dead_letters");
        jdbc.update("DELETE FROM accounts WHERE account_type = 'CUSTOMER'");
        // Both non-customer accounts go back to zero. Missing the CLEARING account here would
        // leave a balance behind from a previous test with no entries to justify it, and every
        // subsequent I2 assertion would fail against a fixture bug rather than a real one.
        jdbc.update("UPDATE accounts SET balance_minor = 0 WHERE account_type <> 'CUSTOMER'");
    }
}
