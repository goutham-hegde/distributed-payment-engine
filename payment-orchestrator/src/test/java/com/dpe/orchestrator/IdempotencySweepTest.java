package com.dpe.orchestrator;

import static org.assertj.core.api.Assertions.assertThat;

import com.dpe.orchestrator.idempotency.IdempotencySweeper;
import com.dpe.orchestrator.support.AbstractPostgresIT;
import java.time.OffsetDateTime;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * The specification for {@code IdempotencySweeper.sweep()}.
 *
 * <p>The batch size is turned down to 10 so that {@link #loopsPastTheFirstBatch()} can prove the
 * loop exists without seeding thousands of rows. That test is the one that matters: a sweeper
 * that deletes exactly one batch per interval is a leak the moment arrivals outpace it, which is
 * precisely the load at which anyone looks at this table.
 */
@SpringBootTest(properties = {
        // Every property from AbstractPostgresIT, repeated. @SpringBootTest properties do NOT
        // merge down a class hierarchy - Spring uses the first declaration it finds and ignores
        // the rest - so omitting one here silently switches a production default back on.
        "spring.kafka.listener.auto-startup=false",
        "dpe.outbox.scheduled=false",
        "dpe.saga.scheduled=false",
        "dpe.idempotency.cache=false",
        "dpe.idempotency.scheduled=false",
        "dpe.idempotency.sweep-batch-size=10"
})
class IdempotencySweepTest extends AbstractPostgresIT {

    @Autowired
    IdempotencySweeper sweeper;

    @Value("${dpe.idempotency.sweep-batch-size}")
    int batchSize;

    @Test
    @DisplayName("an empty table costs one statement and deletes nothing")
    void emptyTableIsANoOp() {
        assertThat(sweeper.sweep()).isZero();
    }

    @Test
    @DisplayName("expired keys are deleted")
    void deletesExpiredKeys() {
        seed("acme", "key-1", OffsetDateTime.now().minusHours(1));
        seed("acme", "key-2", OffsetDateTime.now().minusDays(2));

        assertThat(sweeper.sweep()).isEqualTo(2);
        assertThat(rowCount()).isZero();
    }

    @Test
    @DisplayName("a key still inside its retention window is left alone")
    void keepsLiveKeys() {
        // The one thing this class must never do. A live key deleted early means the next retry
        // of that intent is treated as a new payment, and the client is charged twice by the
        // component whose entire job is to prevent exactly that.
        seed("acme", "live", OffsetDateTime.now().plusHours(23));
        seed("acme", "dead", OffsetDateTime.now().minusMinutes(1));

        assertThat(sweeper.sweep()).isEqualTo(1);
        assertThat(rowCount()).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                "SELECT idempotency_key FROM idempotency_records", String.class))
                .isEqualTo("live");
    }

    @Test
    @DisplayName("more expired keys than one batch are all removed in a single sweep")
    void loopsPastTheFirstBatch() {
        int rows = batchSize * 3 + 4;
        for (int i = 0; i < rows; i++) {
            seed("acme", "key-" + i, OffsetDateTime.now().minusHours(1));
        }

        assertThat(sweeper.sweep())
                .as("one call must drain the backlog, not shave one batch off it")
                .isEqualTo(rows);
        assertThat(rowCount()).isZero();
    }

    @Test
    @DisplayName("the loop is bounded, so a sweep always returns")
    void terminates() {
        // Nothing here asserts a number; the assertion is that the call returns at all. Under a
        // backlog that keeps growing, an unbounded loop never does - and it is holding a
        // transaction while it does not.
        for (int i = 0; i < batchSize * 2; i++) {
            seed("acme", "bounded-" + i, OffsetDateTime.now().minusHours(1));
        }

        assertThat(sweeper.sweep()).isPositive();
    }

    /**
     * Seeds a key that expires at {@code expiresAt}.
     *
     * <p>{@code created_at} has to be set explicitly, and it is not fixture noise: the table
     * carries {@code CHECK (expires_at > created_at)}, and {@code created_at} defaults to
     * {@code now()}. So a row that is ALREADY expired cannot be inserted with the default - which
     * is the constraint working exactly as intended, since in production the pair is always
     * written together as {@code now()} and {@code now() + retention}. Backdating both keeps the
     * fixture honest about that relationship rather than working around it.
     */
    private void seed(String clientId, String key, OffsetDateTime expiresAt) {
        jdbc.update("""
                INSERT INTO idempotency_records
                    (client_id, idempotency_key, request_fingerprint, created_at, expires_at)
                VALUES (?, ?, ?, ?, ?)
                """, clientId, key, "fingerprint", expiresAt.minusHours(24), expiresAt);
    }

    private int rowCount() {
        return jdbc.queryForObject("SELECT count(*) FROM idempotency_records", Integer.class);
    }
}
