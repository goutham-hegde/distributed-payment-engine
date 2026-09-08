package com.dpe.orchestrator.support;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Base class for orchestrator tests that need a real PostgreSQL and no broker.
 *
 * <p>Real Postgres, not H2, for the same reason as in account-service: the behaviour under test
 * here is {@code INSERT ... ON CONFLICT DO NOTHING} against a primary key, which is a Postgres
 * feature with Postgres semantics. A test that passed against an in-memory database while the
 * production dedup was broken would be worse than no test at all.
 *
 * <p>The container is a JVM-wide singleton rather than a JUnit {@code @Container}: the extension
 * stops a static container at the end of each test class, so a multi-class suite would pay the
 * startup cost repeatedly.
 */
@SpringBootTest(properties = {
        // No broker in these tests, so the listener container must not start. Left on, it
        // retries a connection it will never get, floods the log, and slows every test in the
        // class down behind its backoff. AbstractKafkaIT turns it back on.
        "spring.kafka.listener.auto-startup=false",
        // Both background timers off. A relay draining the outbox, or a sweeper compensating a
        // saga, while a test is asserting on exactly those rows makes failures depend on thread
        // scheduling. The tests that exercise them call drainBatch() and sweep() directly, so
        // their assertions are deterministic.
        "dpe.outbox.scheduled=false",
        "dpe.saga.scheduled=false",
        // M4: no Redis container in these tests. The fast path is an optimization, so switching
        // it off must change nothing they assert - which is itself asserted, by
        // IdempotencyCacheTest running the same gate WITH Redis and IdempotencyWithoutRedisTest
        // running it with the cache enabled and pointed at nothing.
        "dpe.idempotency.cache=false"
})
public abstract class AbstractPostgresIT {

    // Testcontainers 2.x: PostgreSQLContainer is NOT generic - the self-typed
    // PostgreSQLContainer<SELF> from the 1.x org.testcontainers.containers package is gone, and
    // new PostgreSQLContainer<>(...) does not compile.
    @ServiceConnection
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    static {
        POSTGRES.start();
    }

    @Autowired
    protected JdbcTemplate jdbc;

    /**
     * Empties everything the consumer and the saga write.
     *
     * <p>Deliberately not a rollback-scoped test transaction: the code under test manages its own
     * transaction boundary, and that boundary is the thing being verified. Wrapping it in an outer
     * transaction that never commits would test a different program.
     */
    @BeforeEach
    void resetConsumerState() {
        jdbc.execute("TRUNCATE TABLE inbox");
        jdbc.execute("TRUNCATE TABLE transfer_projection");
        jdbc.execute("TRUNCATE TABLE outbox");
        // saga_steps references saga_instances which references transfers, and idempotency_records
        // references transfers as well. Truncating them separately fails on a foreign key no
        // matter which order you pick.
        jdbc.execute("TRUNCATE TABLE saga_steps, saga_instances, idempotency_records, transfers");
    }
}
