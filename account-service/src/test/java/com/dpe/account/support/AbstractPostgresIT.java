package com.dpe.account.support;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
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
@SpringBootTest
public abstract class AbstractPostgresIT {

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
     * Returns the database to the state V1 leaves it in: an empty ledger, no customer accounts,
     * and a SYSTEM account at zero.
     *
     * <p>Deliberately not {@code @Transactional} rollback on the test method. These tests run
     * work on several threads, and a rollback-scoped test transaction is bound to one thread -
     * the other threads would not see the seed data, and nothing being tested would be
     * committed. Cleaning explicitly is the only honest way to test concurrency.
     */
    @BeforeEach
    void resetLedger() {
        jdbc.execute("TRUNCATE TABLE ledger_entries RESTART IDENTITY");
        jdbc.update("DELETE FROM accounts WHERE account_type = 'CUSTOMER'");
        jdbc.update("UPDATE accounts SET balance_minor = 0 WHERE account_type = 'SYSTEM'");
    }
}
