package com.dpe.orchestrator;

import static org.assertj.core.api.Assertions.assertThat;

import com.dpe.orchestrator.idempotency.IdempotencyGate;
import com.dpe.orchestrator.idempotency.IdempotentOutcome;
import com.dpe.orchestrator.support.AbstractPostgresIT;
import com.dpe.orchestrator.support.Concurrently;
import com.dpe.orchestrator.web.dto.CreateTransferRequest;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * The cache is switched ON and pointed at nothing. Everything must still work.
 *
 * <p>This is the test that turns "Redis is not load-bearing" from a claim in an ADR into a fact
 * about the code, and it is deliberately harsher than pulling the container: here the client is
 * configured, enabled, and connecting on every single request - to a port with nothing behind it.
 * Every {@code lookup}, {@code store}, {@code acquireLock} and {@code releaseLock} fails, on the
 * request path, under concurrency.
 *
 * <p>The failure it is looking for is a specific and very common one. A cache added in front of a
 * database, with its exceptions left to propagate, does not degrade - it <b>amplifies</b>: the
 * system now fails whenever <i>either</i> component is down, so adding a component to make things
 * faster has made them less available. If {@link IdempotencyGate} ever answers 500 because Redis
 * is unreachable, this test fails, and it should.
 *
 * <p>Two consequences worth being explicit about, because they are what make the milestone's
 * headline claim defensible:
 *
 * <ul>
 *   <li>the anti-stampede lock is never acquired here, and 100 concurrent duplicates still
 *       produce exactly one transfer. The mutual exclusion was never Redis's to provide - it is
 *       Postgres blocking the second inserter on an uncommitted index tuple;</li>
 *   <li>nothing in the request path waits on a Redis timeout for long, because the client
 *       timeouts in {@code application.yml} are 200ms. A cache with a long timeout is a
 *       dependency with extra steps.</li>
 * </ul>
 */
@SpringBootTest(properties = {
        "spring.kafka.listener.auto-startup=false",
        "dpe.outbox.scheduled=false",
        "dpe.saga.scheduled=false",
        // The cache is ON - this is not the no-cache configuration the parent class uses.
        "dpe.idempotency.cache=true",
        // ...and pointed at a port nothing is listening on. Port 1 is reserved and unbindable on
        // every platform, so this cannot accidentally find a real Redis on a developer machine
        // the way a plausible-looking 6380 could.
        "spring.data.redis.host=127.0.0.1",
        "spring.data.redis.port=1"
})
class IdempotencyWithoutRedisTest extends AbstractPostgresIT {

    private static final String CLIENT = "acme";

    @Autowired
    IdempotencyGate gate;

    @Test
    @DisplayName("requests succeed with Redis unreachable")
    void theApiKeepsWorking() {
        IdempotentOutcome outcome = gate.execute(CLIENT, newKey(), transferOf(30_000));

        assertThat(outcome.status()).isEqualTo(202);
        assertThat(outcome.replayed()).isFalse();
        assertThat(transferCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("retries are still deduplicated with Redis unreachable")
    void deduplicationSurvivesTheCache() {
        String key = newKey();
        CreateTransferRequest request = transferOf(30_000);

        IdempotentOutcome first = gate.execute(CLIENT, key, request);
        IdempotentOutcome retry = gate.execute(CLIENT, key, request);

        assertThat(retry.replayed()).isTrue();
        assertThat(retry.bodyJson()).isEqualTo(first.bodyJson());
        assertThat(transferCount())
                .as("the guarantee is the primary key on idempotency_records, and it is still "
                        + "there when the cache is not")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("100 concurrent duplicates still produce one transfer with no lock available")
    void theLockWasNeverTheGuarantee() {
        String key = newKey();
        CreateTransferRequest request = transferOf(30_000);

        List<Concurrently.Outcome<IdempotentOutcome>> outcomes =
                Concurrently.run(100, () -> gate.execute(CLIENT, key, request));

        assertThat(outcomes).allMatch(Concurrently.Outcome::succeeded,
                "an unreachable cache must not turn a duplicate into an error");
        assertThat(outcomes.stream().map(o -> o.value().bodyJson()).collect(Collectors.toSet()))
                .hasSize(1);
        assertThat(transferCount()).isEqualTo(1);
        assertThat(reserveCommandCount()).isEqualTo(1);
    }

    private static CreateTransferRequest transferOf(long amountMinor) {
        return new CreateTransferRequest(UUID.randomUUID(), UUID.randomUUID(), amountMinor, "INR");
    }

    private static String newKey() {
        return UUID.randomUUID().toString();
    }

    private long transferCount() {
        return count("SELECT COUNT(*) FROM transfers");
    }

    private long reserveCommandCount() {
        return count("SELECT COUNT(*) FROM outbox WHERE event_type = 'ReserveFunds'");
    }

    private long count(String sql) {
        Long count = jdbc.queryForObject(sql, Long.class);
        return count == null ? 0 : count;
    }
}
