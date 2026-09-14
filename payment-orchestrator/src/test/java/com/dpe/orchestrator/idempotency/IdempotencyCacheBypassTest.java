package com.dpe.orchestrator.idempotency;

import static org.assertj.core.api.Assertions.assertThat;

import com.dpe.orchestrator.support.Concurrently;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.RedisScript;
import tools.jackson.databind.ObjectMapper;

/**
 * M10 (S25): after Redis fails, the cache stops asking it for a cooldown, then sends ONE probe.
 *
 * <p>The number that matters is how many calls reach Redis, so Redis here is a fake that counts
 * them and can be switched off, and time is a clock the test moves. A real unreachable Redis
 * cannot show this: {@link com.dpe.orchestrator.IdempotencyWithoutRedisTest} points at port 1,
 * which refuses at once - the expensive outage is the one that TIMES OUT, 200 ms per call, three
 * calls per request, inside a bulkhead permit. Counting calls is that cost without the waiting.
 */
class IdempotencyCacheBypassTest {

    private static final Duration COOLDOWN = Duration.ofSeconds(5);
    private static final CachedResponse RESPONSE = new CachedResponse("fp", 202, "{}");

    private FakeRedis redis;
    private AtomicLong now;
    private SimpleMeterRegistry registry;
    private IdempotencyCache cache;

    @BeforeEach
    void setUp() {
        redis = new FakeRedis();
        now = new AtomicLong(1_000_000_000L);
        registry = new SimpleMeterRegistry();
        IdempotencyProperties properties =
                new IdempotencyProperties(null, null, true, null, null, null, 0, null, COOLDOWN);
        cache = new IdempotencyCache(redis, new ObjectMapper(), properties, registry, now::get);
    }

    @Test
    @DisplayName("one failure, and Redis is not called again for the cooldown")
    void aFailureStopsTheCalls() {
        redis.up = false;

        assertThat(cache.lookup("c", "k")).isEmpty();
        assertThat(redis.calls).hasValue(1);
        assertThat(bypassed()).isEqualTo(1.0);

        now.addAndGet(COOLDOWN.toNanos() - 1);
        for (int i = 0; i < 10; i++) {
            assertThat(cache.lookup("c", "k" + i)).isEmpty();
            assertThat(cache.acquireLock("c", "k" + i))
                    .as("Unavailable, never HeldByAnother - only that one makes the gate wait")
                    .isInstanceOf(IdempotencyCache.LockOutcome.Unavailable.class);
            cache.store("c", "k" + i, RESPONSE);
            cache.releaseLock("c", "k" + i, "token");
        }
        assertThat(redis.calls)
                .as("forty calls inside the cooldown, none of which may pay the Redis timeout")
                .hasValue(1);
    }

    @Test
    @DisplayName("after the cooldown one caller probes; if Redis is still down, the bypass re-arms")
    void aFailedProbeRearms() {
        redis.up = false;
        cache.lookup("c", "k");
        now.addAndGet(COOLDOWN.toNanos());

        cache.lookup("c", "k");
        assertThat(redis.calls).as("the probe").hasValue(2);
        cache.lookup("c", "k");
        assertThat(redis.calls).as("re-armed by the failed probe").hasValue(2);
        assertThat(bypassed()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("when the probe is answered the bypass ends, and the gauge goes back to zero")
    void anAnsweredProbeRecovers() {
        redis.up = false;
        cache.lookup("c", "k");
        now.addAndGet(COOLDOWN.toNanos());
        redis.up = true;

        assertThat(cache.lookup("c", "k")).as("a miss is an answer").isEmpty();
        assertThat(bypassed()).isEqualTo(0.0);
        cache.store("c", "k", RESPONSE);
        assertThat(cache.lookup("c", "k")).contains(RESPONSE);
        assertThat(redis.calls).hasValue(4);
    }

    @Test
    @DisplayName("a burst arriving as the cooldown ends sends one probe, not one per request")
    void oneProbeNotAHundred() {
        redis.up = false;
        cache.lookup("c", "k");
        now.addAndGet(COOLDOWN.toNanos());

        List<Concurrently.Outcome<Boolean>> outcomes =
                Concurrently.run(100, () -> cache.lookup("c", "k").isEmpty());

        assertThat(outcomes).allMatch(Concurrently.Outcome::succeeded);
        assertThat(redis.calls).as("the first failure plus exactly one probe").hasValue(2);
    }

    @Test
    @DisplayName("a miss, a held lock and an unparseable value are answers, not failures")
    void answersDoNotTrip() {
        assertThat(cache.lookup("c", "k")).isEmpty();
        assertThat(cache.acquireLock("c", "k")).isInstanceOf(IdempotencyCache.LockOutcome.Acquired.class);
        assertThat(cache.acquireLock("c", "k")).isInstanceOf(IdempotencyCache.LockOutcome.HeldByAnother.class);
        redis.values.put(IdempotencyCache.RESPONSE_PREFIX + "c:bad", "not json");
        assertThat(cache.lookup("c", "bad")).isEmpty();

        int before = redis.calls.get();
        cache.lookup("c", "k");
        assertThat(redis.calls)
                .as("tripping on one bad value would switch the fast path off for every key")
                .hasValue(before + 1);
        assertThat(bypassed()).isEqualTo(0.0);
    }

    private double bypassed() {
        return registry.get("dpe.idempotency.cache.bypassed").gauge().value();
    }

    /**
     * Redis as a map, with a switch and a call counter. The value operations are a JDK proxy so
     * the fake implements only the three calls the cache makes; anything else fails loudly.
     */
    static final class FakeRedis extends StringRedisTemplate {
        final AtomicInteger calls = new AtomicInteger();
        final Map<String, String> values = new ConcurrentHashMap<>();
        volatile boolean up = true;

        @Override
        @SuppressWarnings("unchecked")
        public ValueOperations<String, String> opsForValue() {
            return (ValueOperations<String, String>) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[] {ValueOperations.class},
                    (proxy, method, args) -> {
                        calls.incrementAndGet();
                        if (!up) {
                            throw new RedisConnectionFailureException("fake Redis is down");
                        }
                        return switch (method.getName()) {
                            case "get" -> values.get((String) args[0]);
                            case "set" -> {
                                values.put((String) args[0], (String) args[1]);
                                yield null;
                            }
                            case "setIfAbsent" -> values.putIfAbsent((String) args[0], (String) args[1]) == null;
                            default -> throw new UnsupportedOperationException(method.getName());
                        };
                    });
        }

        @Override
        public <T> T execute(RedisScript<T> script, List<String> keys, Object... args) {
            calls.incrementAndGet();
            if (!up) {
                throw new RedisConnectionFailureException("fake Redis is down");
            }
            return null;
        }
    }
}
