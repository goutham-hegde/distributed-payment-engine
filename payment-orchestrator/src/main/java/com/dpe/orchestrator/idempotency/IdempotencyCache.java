package com.dpe.orchestrator.idempotency;
import java.util.List;
import java.util.UUID;
import org.springframework.data.redis.core.script.RedisScript;
import java.util.Optional;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/**
 * The Redis fast path, and the distributed lock that is deliberately not load-bearing.
 *
 * <h2>YOU WRITE THIS ONE (M4)</h2>
 *
 * <h2>What it is for</h2>
 *
 * <p>Under a retry storm - a mobile client on a bad connection sending the same request eight
 * times in four seconds, times ten thousand users - every one of those requests would otherwise
 * open a database transaction, take a lock on an index tuple, and wait. The database will get the
 * answer right. It will also be the bottleneck, and it is the one component here that cannot be
 * scaled by adding another one.
 *
 * <p>So: keep a copy of the answer somewhere cheap, and stop the herd before it arrives. Both are
 * throughput measures. <b>Deleting Redis entirely must leave this system correct and only
 * slower</b> - that is the property the tests assert, and it is what lets the design claim
 * Kleppmann-proof idempotency while still using a Redis lock.
 *
 * <h2>The four methods</h2>
 *
 * <p><b>lookup</b> - {@code GET dpe:idem:v1:{clientId}:{key}}, deserialize to
 * {@link CachedResponse}. The version segment in the key prefix is not decoration: change the
 * shape of what is stored and every old value in a live Redis becomes unparseable, so the prefix
 * has to change with it.
 *
 * <p><b>store</b> - {@code SET key value EX cacheTtl}. Always with the TTL, never without. A
 * cache entry with no expiry outlives the {@code idempotency_records} row it copies, and then
 * answers, from memory, a request the database has already forgotten - at which point Redis is
 * the source of truth for how long a key is honoured, and the retention policy is a lie.
 *
 * <p><b>acquireLock</b> - {@code SET dpe:idem:lock:v1:{clientId}:{key} <token> NX PX lockTtl},
 * returning the token when it was set. {@code NX} is the whole lock: set-if-absent is one atomic
 * operation, where {@code EXISTS} followed by {@code SET} is two and lets two callers pass. The
 * token is a fresh UUID per attempt and it exists for releaseLock.
 *
 * <p><b>releaseLock</b> - and this one is the interesting one, because {@code DEL key} is wrong.
 * Consider: A takes the lock with a 3 second TTL, stalls for 4 seconds, Redis expires the lock, B
 * takes it, A wakes up and calls {@code DEL} - deleting <b>B's</b> lock, which A never held. Now
 * C takes it too and the mutual exclusion the lock claimed to provide has quietly stopped
 * existing. The release must therefore be conditional on the token, and the check and the delete
 * must be one operation:
 *
 * <pre>
 *   if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end
 * </pre>
 *
 * <p>Run it with {@code DefaultRedisScript<Long>} on the injected template. Lua on Redis is
 * atomic because Redis is single threaded - the script runs to completion with nothing
 * interleaved, which is what makes compare-and-delete a compare-and-delete.
 *
 * <h2>The thing that must be true of every method here</h2>
 *
 * <p><b>Nothing in this class may throw, and nothing may block for long.</b> Redis being down,
 * slow, full, or returning something unparseable must all degrade to "no answer" - a miss, or a
 * lock not acquired - and let the caller reach Postgres. Catch broadly and log at DEBUG or WARN,
 * not ERROR: a cache miss is not an incident.
 *
 * <p>Note the shape of the failure that this rules out, because it is the one that catches
 * people. If {@code lookup} propagated a {@code RedisConnectionFailureException}, then losing
 * Redis would turn every write request into a 500 - a system that is <i>less</i> available than
 * the one with no cache at all. Adding a cache must not add a dependency.
 *
 * <p>The other half of the same discipline lives in the caller: {@link IdempotencyGate} treats a
 * lock it failed to acquire as advice, waits briefly, and goes to the database regardless.
 */
@Component
public class IdempotencyCache {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyCache.class);

    public static final String RESPONSE_PREFIX = "dpe:idem:v1:";
    public static final String LOCK_PREFIX = "dpe:idem:lock:v1:";
    
    private static final RedisScript<Long> RELEASE_SCRIPT = RedisScript.of(
        "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end",
        Long.class);

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final IdempotencyProperties properties;

    /**
     * M6. Three outcomes, and the whole point of the metric is that they are three and not two.
     *
     * <p>{@code hit} and {@code miss} are the cache doing its job. {@code unavailable} is Redis
     * switched off or Redis throwing - and it is counted separately because the system is
     * REQUIRED to stay correct in that state, only slower. That claim is meant to be falsifiable:
     * {@code docker compose stop redis} should drive this to {@code unavailable} and change
     * nothing on any other panel except latency. A cache that lumps "miss" and "broken" together
     * cannot show you that, and it is the same distinction the M4 LockOutcome refactor existed to
     * make - {@code Optional.empty()} carrying two causes the caller has to tell apart.
     */
    private final Counter hits;
    private final Counter misses;
    private final Counter unavailable;

    public IdempotencyCache(StringRedisTemplate redis, ObjectMapper objectMapper,
                            IdempotencyProperties properties, MeterRegistry registry) {
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.hits = counter(registry, "hit");
        this.misses = counter(registry, "miss");
        this.unavailable = counter(registry, "unavailable");
    }

    private static Counter counter(MeterRegistry registry, String result) {
        return Counter.builder("dpe.idempotency.cache")
                .tag("result", result)
                .description("Idempotency fast-path lookups by outcome")
                .register(registry);
    }
    private static String responseKey(String clientId, String key) {
        return RESPONSE_PREFIX + clientId + ":" + key;
    }

    private static String lockKey(String clientId, String key) {
        return LOCK_PREFIX + clientId + ":" + key;
    }

    /**
     * @return the stored response for this key, or empty on a miss, a parse failure, or any
     *         Redis problem at all. Never throws.
     */
    public Optional<CachedResponse> lookup(String clientId, String key) {
        if (!properties.cache()) {
            // Switched off is not a miss. A miss says "the cache answered and had nothing";
            // this says "there is no cache", and an operator reading a 0% hit ratio needs to
            // know which of those they are looking at.
            unavailable.increment();
            return Optional.empty();
        }
        try {
            String json = redis.opsForValue().get(responseKey(clientId, key));
            if (json == null) {
                misses.increment();
                return Optional.empty();
            }
            // Parse BEFORE counting the hit. Counted first, a stored value that no longer
            // deserialises - an old shape left over across a deploy - would increment `hit` and
            // then fall into the catch below and increment `unavailable` as well, so one lookup
            // would appear twice and the hit ratio would read above what actually happened.
            CachedResponse response = objectMapper.readValue(json, CachedResponse.class);
            hits.increment();
            return Optional.of(response);
        } catch (Exception e) {
            log.debug("Idempotency cache lookup failed for {}:{}", clientId, key, e);
            // Redis threw, or the stored JSON no longer parses. Both mean the fast path is not
            // available for this request, and neither means the request is in trouble: the
            // caller falls through to the Postgres claim, which is where the guarantee lives.
            unavailable.increment();
            return Optional.empty();
        }
    }

    /** Publishes a committed response for retries to find. Never throws. */
    public void store(String clientId, String key, CachedResponse response) {
        if (!properties.cache()) {
            return;
        }
        try {
            String json = objectMapper.writeValueAsString(response);
            redis.opsForValue().set(responseKey(clientId, key), json, properties.cacheTtl());
        } catch (Exception e) {
            log.debug("Idempotency cache store failed for {}:{}", clientId, key, e);
        }
    }

    /**
     * @return the lock token if this caller took the lock, empty if someone else holds it or
     *         Redis could not be reached. Empty is advice to slow down, never a refusal.
     */
    public sealed interface LockOutcome {
    record Acquired(String token) implements LockOutcome {}
    record HeldByAnother() implements LockOutcome {}
    record Unavailable() implements LockOutcome {}
}

public LockOutcome acquireLock(String clientId, String key) {
    if (!properties.cache()) {
        return new LockOutcome.Unavailable();
    }
    try {
        String token = UUID.randomUUID().toString();
        Boolean acquired = redis.opsForValue().setIfAbsent(lockKey(clientId, key), token, properties.lockTtl());
        return Boolean.TRUE.equals(acquired)
                ? new LockOutcome.Acquired(token)
                : new LockOutcome.HeldByAnother();
    } catch (Exception e) {
        log.debug("Idempotency lock acquire failed for {}:{}", clientId, key, e);
        return new LockOutcome.Unavailable();
    }
}

    /** Releases the lock only if this caller still holds it. Never throws. */
    public void releaseLock(String clientId, String key, String token) {
        if (!properties.cache()) {
            return;
        }
        try {
            redis.execute(RELEASE_SCRIPT, List.of(lockKey(clientId, key)), token);
        } catch (Exception e) {
            log.debug("Idempotency lock release failed for {}:{}", clientId, key, e);
        }
    }
}
