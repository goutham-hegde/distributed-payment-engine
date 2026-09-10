package com.dpe.orchestrator;

import static com.dpe.orchestrator.support.TestTokens.ALICE;
import static com.dpe.orchestrator.support.TestTokens.BOB;
import static com.dpe.orchestrator.support.TestTokens.bearer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

import com.dpe.orchestrator.support.AbstractPostgresIT;
import com.dpe.orchestrator.support.TestTokens;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
 * <b>M6 part 3.</b> The read endpoints the demo console is built on: the keyset-paged list and the
 * timeline.
 *
 * <p>Rows are inserted straight into the tables rather than driven through the API, deliberately.
 * The list's interesting properties are about ORDERING under concurrent insertion and about two
 * rows sharing a {@code created_at} to the microsecond - neither of which the write path will
 * produce on request, and both of which are exactly where an offset-paged endpoint breaks. The
 * write path has its own tests.
 */
@AutoConfigureMockMvc
@Import(TestTokens.class)
class TransferReadApiTest extends AbstractPostgresIT {

    private static final OffsetDateTime T0 =
            OffsetDateTime.of(2026, 9, 10, 9, 0, 0, 0, ZoneOffset.UTC);

    private static final ObjectMapper JSON = new ObjectMapper();

    @Autowired
    MockMvc mvc;

    @Autowired
    TestTokens tokens;

    // ---------------------------------------------------------------- scoping

    @Test
    @DisplayName("the list contains the caller's transfers and nobody else's")
    void listIsScopedToTheSubject() throws Exception {
        UUID mine = insertTransfer(ALICE, T0);
        insertTransfer(BOB, T0.plusSeconds(1));
        insertTransfer(BOB, T0.plusSeconds(2));

        JsonNode page = getJson("/api/v1/transfers", ALICE);

        assertThat(ids(page))
                .as("scoping is in the WHERE clause, not a filter after the LIMIT - a filtered "
                        + "page would be short by exactly the number of somebody else's rows, "
                        + "which is itself information")
                .containsExactly(mine);
    }

    @Test
    @DisplayName("a subject with no transfers gets an empty page, not an error")
    void emptyListIsAPage() throws Exception {
        JsonNode page = getJson("/api/v1/transfers", ALICE);

        assertThat(page.get("items")).isEmpty();
        assertThat(page.get("nextCursor").isNull())
                .as("null cursor is the only 'no more pages' signal a keyset API has")
                .isTrue();
    }

    @Test
    @DisplayName("no token is 401 - the list is not more public than the transfer it lists")
    void anonymousIsRejected() throws Exception {
        mvc.perform(get("/api/v1/transfers"))
                .andExpect(r -> assertThat(r.getResponse().getStatus()).isEqualTo(401));
    }

    // ---------------------------------------------------------------- paging

    @Test
    @DisplayName("paging one row at a time visits every transfer exactly once, newest first")
    void keysetWalksTheWholeListWithoutRepeatingOrSkipping() throws Exception {
        List<UUID> expected = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            expected.add(0, insertTransfer(ALICE, T0.plusSeconds(i)));
        }

        List<UUID> seen = new ArrayList<>();
        String cursor = null;
        for (int page = 0; page < 10; page++) {
            JsonNode json = getJson("/api/v1/transfers?size=1"
                    + (cursor == null ? "" : "&cursor=" + cursor), ALICE);
            seen.addAll(ids(json));
            cursor = json.get("nextCursor").isNull() ? null : json.get("nextCursor").asText();
            if (cursor == null) {
                break;
            }
        }

        assertThat(seen).containsExactlyElementsOf(expected);
        assertThat(new HashSet<>(seen)).hasSize(expected.size());
    }

    @Test
    @DisplayName("two transfers sharing a created_at are still ordered - the id is the tiebreak")
    void identicalTimestampsDoNotBreakTheBoundary() throws Exception {
        // The case that makes created_at alone an insufficient cursor. Without the id in both the
        // ORDER BY and the comparison, a page boundary falling between these two returns one of
        // them twice and loses the other, depending on which way the planner emitted them.
        UUID a = insertTransfer(ALICE, T0);
        UUID b = insertTransfer(ALICE, T0);
        UUID c = insertTransfer(ALICE, T0);

        Set<UUID> seen = new HashSet<>();
        String cursor = null;
        for (int page = 0; page < 5; page++) {
            JsonNode json = getJson("/api/v1/transfers?size=1"
                    + (cursor == null ? "" : "&cursor=" + cursor), ALICE);
            List<UUID> ids = ids(json);
            assertThat(seen.addAll(ids))
                    .as("a row must never be returned on two different pages")
                    .isTrue();
            cursor = json.get("nextCursor").isNull() ? null : json.get("nextCursor").asText();
            if (cursor == null) {
                break;
            }
        }

        assertThat(seen).containsExactlyInAnyOrder(a, b, c);
    }

    @Test
    @DisplayName("a transfer created between two pages does not shift the second page")
    void concurrentInsertDoesNotDuplicateARow() throws Exception {
        // THE reason this endpoint is not OFFSET-paged. With ?page=2 every row below the new one
        // shifts down by a position, so the last row of page 1 reappears as the first row of
        // page 2 and one row is lost for every page after it. A cursor names a place in the data
        // rather than a count of rows, so a row appearing above it changes nothing.
        insertTransfer(ALICE, T0.plusSeconds(1));
        insertTransfer(ALICE, T0.plusSeconds(2));
        insertTransfer(ALICE, T0.plusSeconds(3));
        insertTransfer(ALICE, T0.plusSeconds(4));

        JsonNode first = getJson("/api/v1/transfers?size=2", ALICE);
        List<UUID> firstPage = ids(first);
        String cursor = first.get("nextCursor").asText();

        insertTransfer(ALICE, T0.plusSeconds(99));

        List<UUID> secondPage = ids(getJson("/api/v1/transfers?size=2&cursor=" + cursor, ALICE));

        assertThat(secondPage).doesNotContainAnyElementsOf(firstPage);
        assertThat(secondPage).hasSize(2);
    }

    @Test
    @DisplayName("an oversized page is clamped, not refused")
    void pageSizeIsCapped() throws Exception {
        for (int i = 0; i < 3; i++) {
            insertTransfer(ALICE, T0.plusSeconds(i));
        }

        // ?size=100000 is a denial-of-service parameter with a friendly name. Clamped rather than
        // rejected so an over-eager client still gets data and a cursor.
        JsonNode page = getJson("/api/v1/transfers?size=100000", ALICE);
        assertThat(page.get("items")).hasSize(3);
    }

    @Test
    @DisplayName("a cursor this API did not issue is 400, not a silent restart from the top")
    void malformedCursorIsRefused() throws Exception {
        mvc.perform(get("/api/v1/transfers?cursor=not-a-real-cursor")
                        .header(HttpHeaders.AUTHORIZATION, bearer(tokens.user(ALICE))))
                .andExpect(r -> assertThat(r.getResponse().getStatus()).isEqualTo(400));
    }

    // ---------------------------------------------------------------- timeline

    @Test
    @DisplayName("the timeline pairs each step's two rows and reports relay lag and the trace id")
    void timelineFoldsStepsIntoStages() throws Exception {
        UUID transferId = insertTransfer(ALICE, T0);
        UUID sagaId = insertSaga(transferId, "RESERVED");

        UUID commandId = insertOutbox(transferId, "dpe.account.commands.v1", "ReserveFunds",
                T0, T0.plusNanos(150_000_000L));
        UUID replyId = insertInbox("dpe.account.events.v1", "FundsReserved");

        insertStep(sagaId, "ReserveFunds", "STARTED", null, commandId, T0);
        insertStep(sagaId, "ReserveFunds", "SUCCEEDED", "RESERVED", replyId, T0.plusSeconds(1));

        JsonNode timeline = getJson("/api/v1/transfers/" + transferId + "/timeline", ALICE);

        assertThat(timeline.get("saga").get("status").asText()).isEqualTo("RESERVED");
        assertThat(timeline.get("traceId").asText())
                .as("parsed off the outbox row so the console can deep-link into Jaeger")
                .isEqualTo("4bf92f3577b34da6a3ce929d0e0e4736");

        assertThat(timeline.get("stages")).hasSize(1);
        JsonNode stage = timeline.get("stages").get(0);
        assertThat(stage.get("name").asText()).isEqualTo("ReserveFunds");
        assertThat(stage.get("outcome").asText()).isEqualTo("SUCCEEDED");
        assertThat(stage.get("latencyMs").asLong()).isEqualTo(1_000L);
        assertThat(stage.get("command").get("relayLagMs").asLong()).isEqualTo(150L);
        assertThat(stage.get("command").get("topic").asText())
                .isEqualTo("dpe.account.commands.v1");
        assertThat(stage.get("reply").get("eventType").asText()).isEqualTo("FundsReserved");
    }

    @Test
    @DisplayName("somebody else's timeline is 404, exactly like one that does not exist")
    void timelineIsScopedAndDoesNotConfirmExistence() throws Exception {
        UUID bobs = insertTransfer(BOB, T0);

        // Stronger case than the polling GET: this response names topics, message ids and the
        // trace id. A 403 would confirm to a caller walking uuids that the transfer is real, and
        // therefore worth attacking.
        expectStatus(404, "/api/v1/transfers/" + bobs + "/timeline", ALICE);
        expectStatus(404, "/api/v1/transfers/" + UUID.randomUUID() + "/timeline", ALICE);
        expectStatus(200, "/api/v1/transfers/" + bobs + "/timeline", BOB);
    }

    // ---------------------------------------------------------------- helpers

    private JsonNode getJson(String uri, String subject) throws Exception {
        MvcResult result = mvc.perform(get(uri)
                        .header(HttpHeaders.AUTHORIZATION, bearer(tokens.user(subject))))
                .andReturn();
        assertThat(result.getResponse().getStatus()).isEqualTo(200);
        return JSON.readTree(result.getResponse().getContentAsString());
    }

    private void expectStatus(int expected, String uri, String subject) throws Exception {
        mvc.perform(get(uri).header(HttpHeaders.AUTHORIZATION, bearer(tokens.user(subject))))
                .andExpect(r -> assertThat(r.getResponse().getStatus()).isEqualTo(expected));
    }

    private static List<UUID> ids(JsonNode page) {
        List<UUID> ids = new ArrayList<>();
        page.get("items").forEach(item -> ids.add(UUID.fromString(item.get("transferId").asText())));
        return ids;
    }

    private UUID insertTransfer(String subject, OffsetDateTime createdAt) {
        UUID id = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO transfers (id, from_account_id, to_account_id, amount_minor, currency,
                                       status, initiated_by, created_at, updated_at)
                VALUES (?, ?, ?, 30000, 'INR', 'PENDING', ?, ?, ?)
                """, id, UUID.randomUUID(), UUID.randomUUID(), subject, createdAt, createdAt);
        return id;
    }

    private UUID insertSaga(UUID transferId, String status) {
        UUID id = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO saga_instances (id, transfer_id, status, deadline_at)
                VALUES (?, ?, ?, ?)
                """, id, transferId, status, T0.plusMinutes(5));
        return id;
    }

    private void insertStep(UUID sagaId, String name, String outcome, String toStatus,
                            UUID messageId, OffsetDateTime at) {
        jdbc.update("""
                INSERT INTO saga_steps (saga_id, step_name, outcome, to_status, message_id,
                                        created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """, sagaId, name, outcome, toStatus, messageId, at);
    }

    private UUID insertOutbox(UUID aggregateId, String topic, String eventType,
                              OffsetDateTime createdAt, OffsetDateTime publishedAt) {
        UUID id = UUID.randomUUID();
        jdbc.update("""
                INSERT INTO outbox (id, aggregate_type, aggregate_id, topic, event_type, payload,
                                    created_at, published_at, trace_parent)
                VALUES (?, 'Transfer', ?, ?, ?, '{}'::jsonb, ?, ?,
                        '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01')
                """, id, aggregateId, topic, eventType, createdAt, publishedAt);
        return id;
    }

    private UUID insertInbox(String topic, String eventType) {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO inbox (message_id, topic, event_type) VALUES (?, ?, ?)",
                id, topic, eventType);
        return id;
    }
}
