package com.dpe.orchestrator;

import static com.dpe.orchestrator.support.TestTokens.ALICE;
import static com.dpe.orchestrator.support.TestTokens.bearer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

import com.dpe.orchestrator.support.AbstractPostgresIT;
import com.dpe.orchestrator.support.TestTokens;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.OffsetDateTime;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

/**
 * <b>M6 part 3.</b> {@code GET /admin/invariants} - I4, and only I4.
 *
 * <p>The interesting assertion is {@link #inFlightIsNotReportedAsAViolation()}. I4 is the one
 * invariant that is only meaningful at quiescence: a saga in {@code RESERVED} is a saga working
 * correctly right now and a violation ten minutes from now, and an endpoint that flattens that
 * into {@code holds: false} teaches every operator who watches it under load to ignore the light.
 */
@AutoConfigureMockMvc
@Import(TestTokens.class)
class InvariantsEndpointTest extends AbstractPostgresIT {

    private static final ObjectMapper JSON = new ObjectMapper();

    private static final Set<String> TERMINAL = Set.of("COMPLETED", "COMPENSATED", "FAILED");

    private static final Set<String> COMPENSATING_OR_COMPENSATED =
            Set.of("COMPENSATING", "COMPENSATED");

    @Autowired
    MockMvc mvc;

    @Autowired
    TestTokens tokens;

    @Test
    @DisplayName("with nothing in flight, I4 holds and the breakdown is empty")
    void quiescentSystemPasses() throws Exception {
        insertSaga("COMPLETED");
        insertSaga("COMPENSATED");
        insertSaga("FAILED");

        JsonNode body = invariants();

        assertThat(body.get("service").asText()).isEqualTo("payment-orchestrator");
        assertThat(body.get("database").asText()).isEqualTo("payments_db");
        assertThat(i4(body).get("holds").asBoolean())
                .as("all three terminal states must be recognised - this is one of the six "
                        + "places the terminal set is written down, and they change together")
                .isTrue();
        assertThat(body.get("inFlight")).isEmpty();
    }

    @Test
    @DisplayName("a saga in flight is reported with its state, and flagged as a quiescence check")
    void inFlightIsNotReportedAsAViolation() throws Exception {
        insertSaga("RESERVED");
        insertSaga("RESERVED");
        insertSaga("STARTED");

        JsonNode body = invariants();

        assertThat(i4(body).get("holds").asBoolean()).isFalse();
        assertThat(i4(body).get("requiresQuiescence").asBoolean())
                .as("the flag is how a console knows to say 'in flight' rather than 'violated'")
                .isTrue();
        assertThat(i4(body).get("detail").asText()).contains("3");

        // The total says something is stuck; the SHAPE says which hop. STARTED piling up means
        // account-service is not replying, RESERVED means the gateway is not answering.
        assertThat(body.get("inFlight").get("RESERVED").asLong()).isEqualTo(2L);
        assertThat(body.get("inFlight").get("STARTED").asLong()).isEqualTo(1L);
    }

    @Test
    @DisplayName("the endpoint is operator-only, like everything else under /admin")
    void requiresOperator() throws Exception {
        mvc.perform(get("/admin/invariants"))
                .andExpect(r -> assertThat(r.getResponse().getStatus()).isEqualTo(401));

        mvc.perform(get("/admin/invariants")
                        .header(HttpHeaders.AUTHORIZATION, bearer(tokens.user(ALICE))))
                .andExpect(r -> assertThat(r.getResponse().getStatus()).isEqualTo(403));
    }

    // ---------------------------------------------------------------- helpers

    private JsonNode invariants() throws Exception {
        MvcResult result = mvc.perform(get("/admin/invariants")
                        .header(HttpHeaders.AUTHORIZATION, bearer(tokens.operator())))
                .andReturn();
        assertThat(result.getResponse().getStatus()).isEqualTo(200);
        return JSON.readTree(result.getResponse().getContentAsString());
    }

    private static JsonNode i4(JsonNode body) {
        return body.get("checks").get(0);
    }

    private void insertSaga(String status) {
        UUID transferId = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO transfers (id, from_account_id, to_account_id, amount_minor, currency,
                                       status, initiated_by)
                VALUES (?, ?, ?, 30000, 'INR', 'PENDING', ?)
                """, transferId, UUID.randomUUID(), UUID.randomUUID(), ALICE);
        // Two constraints this fixture had to learn about, and both are the schema refusing to
        // hold a state that cannot be true:
        //
        //   saga_completed_at_iff_terminal   completed_at is set exactly when the status is
        //                                    terminal. A seventh place the terminal set is
        //                                    written down.
        //   saga_compensation_needs_a_hold   a saga that compensated must name the hold it
        //                                    released - COMPENSATING with a NULL hold_id is a
        //                                    saga with nothing to release.
        //
        // Both refused an earlier version of this helper. That is the point of putting
        // correctness in constraints: a test fixture cannot fabricate a state the system could
        // never reach, so an assertion cannot accidentally be about an impossible row.
        OffsetDateTime completedAt = TERMINAL.contains(status) ? OffsetDateTime.now() : null;
        UUID holdId = COMPENSATING_OR_COMPENSATED.contains(status) ? UUID.randomUUID() : null;
        jdbc.update("""
                INSERT INTO saga_instances (id, transfer_id, status, deadline_at, completed_at,
                                            hold_id)
                VALUES (?, ?, ?, ?, ?, ?)
                """, UUID.randomUUID(), transferId, status,
                OffsetDateTime.now().plusMinutes(5), completedAt, holdId);
    }
}
