package com.dpe.orchestrator;

import static com.dpe.orchestrator.support.TestTokens.ALICE;
import static com.dpe.orchestrator.support.TestTokens.BOB;
import static com.dpe.orchestrator.support.TestTokens.bearer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

import com.dpe.orchestrator.admission.AdmissionRefusedException;
import com.dpe.orchestrator.idempotency.IdempotencyGate;
import com.dpe.orchestrator.idempotency.IdempotentOutcome;
import com.dpe.orchestrator.support.AbstractPostgresIT;
import com.dpe.orchestrator.support.TestTokens;
import com.dpe.orchestrator.web.dto.CreateTransferRequest;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

/**
 * M8: admission control, against a real Postgres.
 *
 * <p>The relay and the sweeper are off in this suite, so every accepted transfer stays in flight -
 * which makes "three accepted" exactly "three in flight". The limit is three, and the count is
 * retaken on every admission ({@code count-interval=0s}) so each assertion sees the database as it
 * is, not an estimate from before it.
 *
 * <p>@TestPropertySource rather than @SpringBootTest properties: the latter would REPLACE
 * AbstractPostgresIT's list (the M4 trap) and quietly turn the relay and sweeper back on.
 */
@AutoConfigureMockMvc
@Import(TestTokens.class)
@TestPropertySource(properties = {
        "dpe.admission.max-in-flight=3",
        "dpe.admission.count-interval=0s",
        "dpe.admission.retry-after=2s"
})
class AdmissionControlTest extends AbstractPostgresIT {

    private static final String CLIENT = "acme";

    @Autowired
    IdempotencyGate gate;

    @Autowired
    MockMvc mvc;

    @Autowired
    TestTokens tokens;

    @Autowired
    MeterRegistry registry;

    @Test
    @DisplayName("at the limit, a new payment is refused and leaves nothing behind")
    void refusedAtTheLimitWritesNothing() {
        fillToTheLimit();

        assertThatThrownBy(() -> gate.execute(CLIENT, newKey(), transfer()))
                .isInstanceOf(AdmissionRefusedException.class);

        assertThat(count("SELECT COUNT(*) FROM transfers")).isEqualTo(3);
        assertThat(count("SELECT COUNT(*) FROM saga_instances")).isEqualTo(3);
        assertThat(count("SELECT COUNT(*) FROM outbox"))
                .as("no ReserveFunds for a payment that was refused")
                .isEqualTo(3);
        assertThat(count("SELECT COUNT(*) FROM idempotency_records"))
                .as("the claim rolled back with the refusal - the key was not consumed")
                .isEqualTo(3);
    }

    @Test
    @DisplayName("a retry of an ACCEPTED payment is replayed at the limit, never refused")
    void replayIsNeverRefused() {
        String accepted = newKey();
        CreateTransferRequest request = transfer();
        IdempotentOutcome first = gate.execute(CLIENT, accepted, request);
        gate.execute(CLIENT, newKey(), transfer());
        gate.execute(CLIENT, newKey(), transfer());

        // A 503 here would tell the client its payment was not accepted - and it was. The client
        // could reasonably conclude the money had not moved and pay again with a new key.
        IdempotentOutcome retry = gate.execute(CLIENT, accepted, request);

        assertThat(retry.replayed()).isTrue();
        assertThat(retry.status()).isEqualTo(202);
        assertThat(retry.bodyJson()).isEqualTo(first.bodyJson());
    }

    @Test
    @DisplayName("a refused key is still unused: the same key is accepted once capacity frees")
    void refusedKeyCanBeRetried() {
        UUID oldest = fillToTheLimit();
        String key = newKey();
        CreateTransferRequest request = transfer();

        assertThatThrownBy(() -> gate.execute(CLIENT, key, request))
                .isInstanceOf(AdmissionRefusedException.class);

        finish(oldest);

        IdempotentOutcome retry = gate.execute(CLIENT, key, request);
        assertThat(retry.replayed())
                .as("a first execution, not a replay - the refusal left no record of the key")
                .isFalse();
        assertThat(retry.status()).isEqualTo(202);
        assertThat(count("SELECT COUNT(*) FROM transfers")).isEqualTo(4);
    }

    @Test
    @DisplayName("the API answers 503 with Retry-After, says the payment was not accepted, and counts it")
    void apiAnswers503WithRetryAfter() throws Exception {
        UUID from = projectAccount(ALICE);
        UUID to = projectAccount(BOB);
        fillToTheLimit();
        double refusedBefore = refusedCount();

        MvcResult result = mvc.perform(post("/api/v1/transfers")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Idempotency-Key", newKey())
                        .header(HttpHeaders.AUTHORIZATION, bearer(tokens.user(ALICE)))
                        .content("""
                                {"fromAccountId":"%s","toAccountId":"%s","amountMinor":30000,\
                                "currency":"INR"}""".formatted(from, to)))
                .andReturn();

        assertThat(result.getResponse().getStatus()).isEqualTo(503);
        assertThat(result.getResponse().getHeader(HttpHeaders.RETRY_AFTER)).isEqualTo("2");
        assertThat(result.getResponse().getContentAsString())
                .contains("\"code\":\"AT_CAPACITY\"")
                .contains("not accepted")
                .contains("same Idempotency-Key");
        // A delta, not an absolute: the registry is shared by every class in this context (M6).
        assertThat(refusedCount() - refusedBefore).isEqualTo(1.0);
    }

    // ---------------------------------------------------------------------------------- helpers

    /** Three accepted transfers, all in flight. Returns the first one's transfer id. */
    private UUID fillToTheLimit() {
        gate.execute(CLIENT, newKey(), transfer());
        UUID first = jdbc.queryForObject("SELECT id FROM transfers", UUID.class);
        gate.execute(CLIENT, newKey(), transfer());
        gate.execute(CLIENT, newKey(), transfer());
        return first;
    }

    /** Moves one saga to a terminal state, as the pipeline would have. */
    private void finish(UUID transferId) {
        jdbc.update("""
                UPDATE saga_instances SET status = 'FAILED', completed_at = now(),
                       failure_reason = 'finished by the test'
                WHERE transfer_id = ?""", transferId);
    }

    private UUID projectAccount(String owner) {
        jdbc.execute("DELETE FROM account_owners WHERE owner_id = '" + owner + "'");
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO account_owners (account_id, owner_id, account_type, currency) "
                + "VALUES (?, ?, 'CUSTOMER', 'INR')", id, owner);
        return id;
    }

    private double refusedCount() {
        return registry.get("dpe.admission.refused").counter().count();
    }

    private long count(String sql) {
        Long n = jdbc.queryForObject(sql, Long.class);
        return n == null ? 0 : n;
    }

    private static CreateTransferRequest transfer() {
        return new CreateTransferRequest(UUID.randomUUID(), UUID.randomUUID(), 30_000, "INR");
    }

    private static String newKey() {
        return UUID.randomUUID().toString();
    }
}
