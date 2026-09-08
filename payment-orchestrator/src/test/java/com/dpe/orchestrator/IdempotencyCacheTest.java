package com.dpe.orchestrator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dpe.orchestrator.idempotency.IdempotencyCache;
import com.dpe.orchestrator.idempotency.IdempotencyConflictException;
import com.dpe.orchestrator.idempotency.IdempotencyGate;
import com.dpe.orchestrator.idempotency.IdempotentOutcome;
import com.dpe.orchestrator.support.AbstractRedisIT;
import com.dpe.orchestrator.web.dto.CreateTransferRequest;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * The Redis fast path, and the proof that it is only a fast path.
 *
 * <p>Two of these tests are about what Redis does; the rest are about what happens when it does
 * not. The second group is the one that matters - a cache nobody has tried to remove is a
 * dependency nobody has noticed.
 */
class IdempotencyCacheTest extends AbstractRedisIT {

    private static final String CLIENT = "acme";

    @Autowired
    IdempotencyGate gate;

    @Autowired
    IdempotencyCache cache;

    @Test
    @DisplayName("a retry is answered from Redis without consulting the database")
    void theFastPathSkipsTheDatabase() {
        String key = newKey();
        CreateTransferRequest request = transferOf(30_000);
        IdempotentOutcome first = gate.execute(CLIENT, key, request);

        // Removing the row is how a test proves the answer did not come from it. Nothing in
        // production ever does this - which is the point: after this line the database can no
        // longer answer, so an answer can only have come from the cache.
        jdbc.update("DELETE FROM idempotency_records WHERE client_id = ? AND idempotency_key = ?",
                CLIENT, key);

        IdempotentOutcome retry = gate.execute(CLIENT, key, request);

        assertThat(retry.replayed()).isTrue();
        assertThat(retry.bodyJson()).isEqualTo(first.bodyJson());
        assertThat(transferCount())
                .as("the fast path must reach the same decision as the slow one, not skip it")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("the cached entry expires, so Redis never outlives the row it copies")
    void cachedEntriesCarryATtl() {
        String key = newKey();
        gate.execute(CLIENT, key, transferOf(30_000));

        Long ttl = redis.getExpire(IdempotencyCache.RESPONSE_PREFIX + CLIENT + ":" + key,
                TimeUnit.SECONDS);

        assertThat(ttl)
                .as("-1 means no expiry, which would make Redis the authority on how long a key "
                        + "is honoured - a promise the database has already stopped keeping")
                .isNotNull()
                .isPositive();
    }

    @Test
    @DisplayName("flushing Redis mid-flight changes nothing except speed")
    void redisIsNotLoadBearing() {
        String key = newKey();
        CreateTransferRequest request = transferOf(30_000);
        IdempotentOutcome first = gate.execute(CLIENT, key, request);

        flushRedis();

        IdempotentOutcome retry = gate.execute(CLIENT, key, request);

        assertThat(retry.replayed())
                .as("with the cache empty the gate falls back to Postgres, which is where the "
                        + "guarantee lives")
                .isTrue();
        assertThat(retry.bodyJson()).isEqualTo(first.bodyJson());
        assertThat(transferCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("a cache hit still refuses a key reused with a different body")
    void theFastPathAlsoChecksTheFingerprint() {
        String key = newKey();
        gate.execute(CLIENT, key, transferOf(30_000));

        assertThatThrownBy(() -> gate.execute(CLIENT, key, transferOf(500_000)))
                .as("two paths to the same decision means two places to forget the check, and "
                        + "this is the one that runs under load")
                .isInstanceOf(IdempotencyConflictException.class);
    }

    @Test
    @DisplayName("the lock excludes a second holder, and says so specifically")
    void theLockIsExclusive() {
        String key = newKey();

        IdempotencyCache.LockOutcome mine = cache.acquireLock(CLIENT, key);
        IdempotencyCache.LockOutcome theirs = cache.acquireLock(CLIENT, key);

        assertThat(mine).isInstanceOf(IdempotencyCache.LockOutcome.Acquired.class);
        assertThat(theirs)
                .as("SET NX is one operation. EXISTS-then-SET is two, and both callers pass")
                .isInstanceOf(IdempotencyCache.LockOutcome.HeldByAnother.class);

        // The distinction that Optional.empty() could not carry. HeldByAnother means a winner
        // exists and will publish an answer, so waiting for it is useful; Unavailable means
        // there is no lock service at all, and waiting is a fixed cost paid for nothing. The
        // gate branches on exactly this, so the test names it rather than asserting "not
        // acquired".
        assertThat(theirs)
                .as("a held lock is not the same event as an absent Redis")
                .isNotInstanceOf(IdempotencyCache.LockOutcome.Unavailable.class);
    }

    @Test
    @DisplayName("a lock is released only by the caller that holds it")
    void releasingIsConditionalOnTheToken() {
        String key = newKey();
        String token = acquiredToken(key);

        cache.releaseLock(CLIENT, key, "some-other-token");

        assertThat(cache.acquireLock(CLIENT, key))
                .as("a plain DEL would have freed a lock this caller does not hold - which is "
                        + "exactly how a stalled holder deletes its successor's lock and two "
                        + "requests run at once")
                .isInstanceOf(IdempotencyCache.LockOutcome.HeldByAnother.class);

        cache.releaseLock(CLIENT, key, token);

        assertThat(cache.acquireLock(CLIENT, key))
                .as("and the real holder can still release it")
                .isInstanceOf(IdempotencyCache.LockOutcome.Acquired.class);
    }

    private String acquiredToken(String key) {
        IdempotencyCache.LockOutcome outcome = cache.acquireLock(CLIENT, key);
        assertThat(outcome).isInstanceOf(IdempotencyCache.LockOutcome.Acquired.class);
        return ((IdempotencyCache.LockOutcome.Acquired) outcome).token();
    }

    private static CreateTransferRequest transferOf(long amountMinor) {
        return new CreateTransferRequest(UUID.randomUUID(), UUID.randomUUID(), amountMinor, "INR");
    }

    private static String newKey() {
        return UUID.randomUUID().toString();
    }

    private long transferCount() {
        Long count = jdbc.queryForObject("SELECT COUNT(*) FROM transfers", Long.class);
        return count == null ? 0 : count;
    }
}
