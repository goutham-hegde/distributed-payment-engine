package com.dpe.orchestrator.idempotency;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.script.RedisScript;
import java.util.Optional;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
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
 *
 * <h2>"Not for long" had to be enforced across requests too (M10, S25)</h2>
 *
 * <p>Each call is bounded by the client timeout (200 ms), and that was the whole defence - per
 * call. A request makes three (lookup, lock, store), so with Redis down EVERY request, new or
 * replay, paid ~0.6 s: 0.85 s against 0.23 s, measured. It paid it inside a request-bulkhead
 * permit, so six permits drained 3.5x slower, and chaos scenario 06 with Redis stopped refused 46
 * of 100 callers with 503 BUSY. Correct throughout - one transfer, bob paid once - but a Redis
 * outage was costing capacity, which is load-bearing by another name.
 *
 * <p>So after any call to Redis fails, this class stops calling it for
 * {@code dpe.idempotency.failure-cooldown} and answers as if the cache were switched off:
 * {@code lookup} misses, {@code acquireLock} is {@link LockOutcome.Unavailable}, the other two do
 * nothing. When the cooldown ends, exactly ONE caller asks Redis again - whoever wins a
 * compare-and-set - while the rest keep bypassing until that probe reports. Redis answering
 * anything at all ends the bypass; failing again re-arms it. One request per cooldown pays the
 * timeout, instead of all of them.
 *
 * <p>What does NOT trip it: a miss, a lock held by someone else, or a stored value that no longer
 * parses. All three are Redis answering. Tripping on a bad value would switch off the fast path
 * for every key because of one.
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

    /** True while Redis is being bypassed after a failure. Read by a gauge, so it is a field. */
    private final AtomicBoolean bypassing = new AtomicBoolean(false);
    /** nanoClock value before which Redis is not asked. Meaningful only while bypassing. */
    private final AtomicLong retryAt = new AtomicLong();
    private final LongSupplier nanoClock;
    private final long cooldownNanos;

    @Autowired
    public IdempotencyCache(StringRedisTemplate redis, ObjectMapper objectMapper,
                            IdempotencyProperties properties, MeterRegistry registry) {
        this(redis, objectMapper, properties, registry, System::nanoTime);
    }

    /** With a clock the test can move, so a cooldown is asserted without sleeping through it. */
    IdempotencyCache(StringRedisTemplate redis, ObjectMapper objectMapper,
                     IdempotencyProperties properties, MeterRegistry registry,
                     LongSupplier nanoClock) {
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.hits = counter(registry, "hit");
        this.misses = counter(registry, "miss");
        this.unavailable = counter(registry, "unavailable");
        this.nanoClock = nanoClock;
        this.cooldownNanos = properties.failureCooldown().toNanos();
        // Registered at zero from the start: a gauge that only appears once something is wrong is
        // a gap on the graph, not a line at 0. `this` is the object held, and it is a bean, so the
        // weak reference Micrometer keeps to it cannot be collected out from under the gauge.
        Gauge.builder("dpe.idempotency.cache.bypassed", this, IdempotencyCache::bypassedNow)
                .description("1 while inside a Redis-failure cooldown, 0 otherwise")
                .register(registry);
    }

    /**
     * 1 only while a cooldown started by a real failure is running - not merely while the flag is
     * set. Only a request that reaches Redis can clear the flag, so on an idle system after an
     * outage it would stay set indefinitely, and an alert on it would report a Redis outage that
     * may have ended hours ago: the drained-gauge trap. When a cooldown ends with nobody to probe,
     * the true answer is "not bypassing" - the next request will ask Redis. Under traffic against
     * a Redis that is still down, every probe re-arms the cooldown, so the gauge reads 1 except in
     * the gap between a cooldown ending and the next request; the alert averages over that.
     */
    private double bypassedNow() {
        return bypassing.get() && nanoClock.getAsLong() - retryAt.get() < 0 ? 1 : 0;
    }

    /**
     * May this call go to Redis? Yes if nothing has failed; no while cooling down; and when the
     * cooldown has ended, yes for exactly one caller - the one whose compare-and-set pushes the
     * next retry out - so a burst arriving at that instant sends one probe, not a hundred.
     */
    private boolean mayAsk() {
        if (!bypassing.get()) {
            return true;
        }
        long due = retryAt.get();
        long now = nanoClock.getAsLong();
        if (now - due < 0) {
            return false;
        }
        return retryAt.compareAndSet(due, now + cooldownNanos);
    }

    /** Redis replied - whatever it said. */
    private void answered() {
        if (bypassing.compareAndSet(true, false)) {
            log.info("Redis is answering again; idempotency fast path back on");
        }
    }

    /** Redis did not reply. Logged once per outage, not once per request. */
    private void failed(Exception e) {
        retryAt.set(nanoClock.getAsLong() + cooldownNanos);
        if (bypassing.compareAndSet(false, true)) {
            log.warn("Redis did not answer ({}); bypassing the idempotency fast path for {} - "
                    + "requests go straight to Postgres, which is where the guarantee is",
                    e.toString(), properties.failureCooldown());
        }
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
        if (!mayAsk()) {
            // Bypassing after a recent failure. Counted as unavailable, because it is: the fast
            // path is not there for this request, which is all the metric has ever claimed.
            unavailable.increment();
            return Optional.empty();
        }
        String json;
        try {
            json = redis.opsForValue().get(responseKey(clientId, key));
        } catch (Exception e) {
            log.debug("Idempotency cache lookup failed for {}:{}", clientId, key, e);
            failed(e);
            // Neither this nor the parse failure below means the request is in trouble: the
            // caller falls through to the Postgres claim, which is where the guarantee lives.
            unavailable.increment();
            return Optional.empty();
        }
        answered();
        if (json == null) {
            misses.increment();
            return Optional.empty();
        }
        try {
            // Parse BEFORE counting the hit. Counted first, a stored value that no longer
            // deserialises - an old shape left over across a deploy - would increment `hit` and
            // then fall into the catch below and increment `unavailable` as well, so one lookup
            // would appear twice and the hit ratio would read above what actually happened.
            CachedResponse response = objectMapper.readValue(json, CachedResponse.class);
            hits.increment();
            return Optional.of(response);
        } catch (Exception e) {
            // Redis answered; the VALUE is what failed. Not a reason to stop asking Redis - that
            // would switch the fast path off for every key because of one.
            log.debug("Idempotency cache entry for {}:{} does not parse", clientId, key, e);
            unavailable.increment();
            return Optional.empty();
        }
    }

    /** Publishes a committed response for retries to find. Never throws. */
    public void store(String clientId, String key, CachedResponse response) {
        if (!properties.cache()) {
            return;
        }
        String json;
        try {
            json = objectMapper.writeValueAsString(response);
        } catch (Exception e) {
            log.debug("Idempotency cache entry for {}:{} did not serialise", clientId, key, e);
            return;
        }
        if (!mayAsk()) {
            return;
        }
        try {
            redis.opsForValue().set(responseKey(clientId, key), json, properties.cacheTtl());
            answered();
        } catch (Exception e) {
            log.debug("Idempotency cache store failed for {}:{}", clientId, key, e);
            failed(e);
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
    if (!properties.cache() || !mayAsk()) {
        // Switched off, or bypassing after a failure: the same thing to the caller, which goes
        // straight to Postgres. Unavailable, never HeldByAnother - only that one earns a wait.
        return new LockOutcome.Unavailable();
    }
    try {
        String token = UUID.randomUUID().toString();
        Boolean acquired = redis.opsForValue().setIfAbsent(lockKey(clientId, key), token, properties.lockTtl());
        answered();
        return Boolean.TRUE.equals(acquired)
                ? new LockOutcome.Acquired(token)
                : new LockOutcome.HeldByAnother();
    } catch (Exception e) {
        log.debug("Idempotency lock acquire failed for {}:{}", clientId, key, e);
        failed(e);
        return new LockOutcome.Unavailable();
    }
}

    /** Releases the lock only if this caller still holds it. Never throws. */
    public void releaseLock(String clientId, String key, String token) {
        // Skipped while bypassing: a lock taken just before Redis failed then expires on its own
        // lockTtl, which is exactly what a failed release would have left behind anyway.
        if (!properties.cache() || !mayAsk()) {
            return;
        }
        try {
            redis.execute(RELEASE_SCRIPT, List.of(lockKey(clientId, key)), token);
            answered();
        } catch (Exception e) {
            log.debug("Idempotency lock release failed for {}:{}", clientId, key, e);
            failed(e);
        }
    }
}
