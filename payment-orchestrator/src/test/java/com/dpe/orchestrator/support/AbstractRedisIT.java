package com.dpe.orchestrator.support;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;

/**
 * Base class for the tests that exercise the Redis fast path itself.
 *
 * <p>Only two test classes need this. Everything else runs against
 * {@link AbstractPostgresIT} with the cache switched off - which is not a shortcut but the
 * point: if turning Redis off changed what those tests observe, the cache would be part of the
 * mechanism rather than in front of it.
 *
 * <h2>Why a GenericContainer and not a RedisContainer</h2>
 *
 * <p>There is no {@code org.testcontainers:testcontainers-redis} module - Redis was never an
 * official Testcontainers module, and the widely-used {@code com.redis:testcontainers-redis} is a
 * third-party artifact this project has no other reason to depend on.
 *
 * <p>It is not needed. Boot's {@code RedisContainerConnectionDetailsFactory} accepts any
 * container whose image is {@code redis} (or redis-stack), so {@code @ServiceConnection} on a
 * plain {@code GenericContainer} wires {@code spring.data.redis.*} exactly as it would for a
 * dedicated container class. The exposed port has to be declared explicitly, because a
 * GenericContainer knows nothing about what runs inside it.
 *
 * <p>Note the container class here IS still generic, unlike {@code PostgreSQLContainer} - the
 * self-typed {@code GenericContainer<SELF>} survived the Testcontainers 2.x rewrite, so the
 * diamond compiles here and does not there.
 */
@SpringBootTest(properties = {
        // Repeated in full rather than inherited. Spring resolves @SpringBootTest by finding the
        // first one in the class hierarchy and using it ALONE - the parent's properties are not
        // merged in - so anything omitted here silently reverts to its production default, and
        // a background relay or sweeper would start racing these tests.
        "spring.kafka.listener.auto-startup=false",
        "dpe.outbox.scheduled=false",
        "dpe.saga.scheduled=false",
        // The one difference from the parent, and the reason this class exists.
        "dpe.idempotency.cache=true"
})
public abstract class AbstractRedisIT extends AbstractPostgresIT {

    @ServiceConnection
    static final GenericContainer<?> REDIS =
            new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    static {
        REDIS.start();
    }

    @Autowired
    protected StringRedisTemplate redis;

    /**
     * A cache that survived the previous test is a cache that can make this one pass for the
     * wrong reason - or fail for one. Emptied rather than key-prefixed per test, because part of
     * what is under test is which keys the gate writes.
     */
    @BeforeEach
    protected void flushRedis() {
        try (RedisConnection connection = redis.getRequiredConnectionFactory().getConnection()) {
            connection.serverCommands().flushAll();
        }
    }
}
