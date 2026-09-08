package com.dpe.orchestrator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dpe.orchestrator.idempotency.IdempotencyConflictException;
import com.dpe.orchestrator.idempotency.IdempotencyGate;
import com.dpe.orchestrator.idempotency.IdempotentOutcome;
import com.dpe.orchestrator.support.AbstractPostgresIT;
import com.dpe.orchestrator.support.Concurrently;
import com.dpe.orchestrator.transfer.InvalidTransferException;
import com.dpe.orchestrator.web.dto.CreateTransferRequest;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * The specification for {@link IdempotencyGate}.
 *
 * <p>No Redis here - the parent switches the cache off. That is deliberate and it is half the
 * argument of the milestone: <b>every guarantee in this file must hold with Redis absent.</b>
 * {@code IdempotencyCacheTest} then runs the same gate with a real Redis and asserts it observes
 * the same things, faster. If a test here only passed with the cache on, the cache would have
 * become part of the mechanism.
 *
 * <p>{@link #oneHundredConcurrentRetriesProduceExactlyOneTransfer()} is the milestone's stated
 * done-when. It is also the only test here capable of catching a {@code SELECT}-then-{@code
 * INSERT} gate: single-threaded, that broken implementation passes everything else in this class.
 */
class IdempotencyGateTest extends AbstractPostgresIT {

    private static final String CLIENT = "acme";

    @Autowired
    IdempotencyGate gate;

    @Test
    @DisplayName("a first request does the work and stores what it answered")
    void firstRequestIsExecuted() {
        String key = newKey();
        IdempotentOutcome outcome = gate.execute(CLIENT, key, transferOf(30_000));

        assertThat(outcome.status())
                .as("202 Accepted: the saga has started, the money has not moved")
                .isEqualTo(202);
        assertThat(outcome.replayed())
                .as("the first execution of an intent is not a replay")
                .isFalse();

        assertThat(transferCount()).isEqualTo(1);

        Map<String, Object> record = jdbc.queryForMap(
                "SELECT * FROM idempotency_records WHERE client_id = ? AND idempotency_key = ?",
                CLIENT, key);
        assertThat(record.get("response_status")).isEqualTo(202);
        assertThat(record.get("response_body"))
                .as("the stored response is what makes the retry answerable")
                .isNotNull();
        assertThat(record.get("transfer_id"))
                .as("denormalized out of the body so the key can be traced to its transfer")
                .isEqualTo(onlyTransferId());
    }

    @Test
    @DisplayName("a retry of the same intent returns the original response and does nothing else")
    void retryReplaysTheStoredResponse() {
        String key = newKey();
        CreateTransferRequest request = transferOf(30_000);

        IdempotentOutcome first = gate.execute(CLIENT, key, request);
        IdempotentOutcome retry = gate.execute(CLIENT, key, request);

        assertThat(retry.bodyJson())
                .as("byte for byte, not a re-rendering: a retry must not see a status that has "
                        + "moved on since the original answered")
                .isEqualTo(first.bodyJson());
        assertThat(retry.status()).isEqualTo(first.status());
        assertThat(retry.replayed()).isTrue();

        assertThat(transferCount())
                .as("a second transfer here is the double-payment bug this milestone exists for")
                .isEqualTo(1);
        assertThat(sagaCount()).isEqualTo(1);
        assertThat(reserveCommandCount())
                .as("a second ReserveFunds would debit the sender twice, whatever the API "
                        + "answered")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("the same key with a different body is refused, not replayed")
    void reusingAKeyForADifferentRequestIsAConflict() {
        String key = newKey();
        gate.execute(CLIENT, key, transferOf(30_000));

        assertThatThrownBy(() -> gate.execute(CLIENT, key, transferOf(500_000)))
                .as("replaying here would answer a 5000 rupee request with the receipt for a "
                        + "300 rupee one - silently, and in the client's favour to believe")
                .isInstanceOf(IdempotencyConflictException.class);

        assertThat(transferCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("two keys are two intents")
    void differentKeysAreNotDuplicates() {
        CreateTransferRequest request = transferOf(30_000);

        gate.execute(CLIENT, newKey(), request);
        gate.execute(CLIENT, newKey(), request);

        assertThat(transferCount())
                .as("identical requests under different keys are two deliberate payments. The "
                        + "key is the client's statement of intent, and the body is not")
                .isEqualTo(2);
    }

    @Test
    @DisplayName("one client's key cannot collide with another's")
    void keysAreScopedToTheClient() {
        String key = newKey();

        gate.execute("acme", key, transferOf(30_000));
        gate.execute("globex", key, transferOf(70_000));

        assertThat(transferCount())
                .as("without client_id in the primary key, globex would be handed acme's "
                        + "response body - which is another customer's transfer")
                .isEqualTo(2);
    }

    @Test
    @DisplayName("100 concurrent identical requests produce exactly one transfer")
    void oneHundredConcurrentRetriesProduceExactlyOneTransfer() {
        String key = newKey();
        CreateTransferRequest request = transferOf(30_000);

        List<Concurrently.Outcome<IdempotentOutcome>> outcomes =
                Concurrently.run(100, () -> gate.execute(CLIENT, key, request));

        assertThat(outcomes).allMatch(Concurrently.Outcome::succeeded,
                "every caller gets an answer - a duplicate is a normal event, never an error");

        assertThat(outcomes.stream().map(o -> o.value().bodyJson()).collect(Collectors.toSet()))
                .as("all 100 callers must be told about the same transfer")
                .hasSize(1);

        assertThat(outcomes.stream().filter(o -> !o.value().replayed()).count())
                .as("exactly one of them did the work")
                .isEqualTo(1);

        assertThat(transferCount()).isEqualTo(1);
        assertThat(sagaCount()).isEqualTo(1);
        assertThat(reserveCommandCount()).isEqualTo(1);
        assertThat(idempotencyRecordCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("a rejected request leaves the key free to be retried")
    void aFailedRequestDoesNotBurnTheKey() {
        String key = newKey();
        UUID account = UUID.randomUUID();

        assertThatThrownBy(() -> gate.execute(CLIENT, key,
                new CreateTransferRequest(account, account, 30_000, "INR")))
                .isInstanceOf(InvalidTransferException.class);

        assertThat(idempotencyRecordCount())
                .as("the claim was made in the same transaction as the work, so the rollback "
                        + "took it with it")
                .isZero();

        IdempotentOutcome retry = gate.execute(CLIENT, key, transferOf(30_000));

        assertThat(retry.replayed())
                .as("the corrected request under the same key is new work, not a replay of a "
                        + "failure")
                .isFalse();
        assertThat(transferCount()).isEqualTo(1);
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

    private long sagaCount() {
        return count("SELECT COUNT(*) FROM saga_instances");
    }

    private long idempotencyRecordCount() {
        return count("SELECT COUNT(*) FROM idempotency_records");
    }

    /**
     * The assertion that survives a gate which returns the right HTTP body for the wrong reason.
     * The API answer is a claim; an outbox row is a command that will actually move money.
     */
    private long reserveCommandCount() {
        return count("SELECT COUNT(*) FROM outbox WHERE event_type = 'ReserveFunds'");
    }

    private UUID onlyTransferId() {
        return jdbc.queryForObject("SELECT id FROM transfers", UUID.class);
    }

    private long count(String sql) {
        Long count = jdbc.queryForObject(sql, Long.class);
        return count == null ? 0 : count;
    }
}
