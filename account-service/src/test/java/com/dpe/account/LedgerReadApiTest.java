package com.dpe.account;

import static com.dpe.account.support.TestTokens.bearer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

import com.dpe.account.support.AbstractPostgresIT;
import com.dpe.account.support.TestTokens;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

/**
 * <b>M6 part 3.</b> Reading an account and its ledger history.
 *
 * <p>Two things are being asserted and they pull in opposite directions. The endpoint has to be
 * <i>open enough</i> that a customer can see their own money, which is new - before this milestone
 * {@code /accounts/**} was operator-only. And it has to be <i>closed enough</i> that opening it did
 * not hand every token holder every account, which is the failure mode of exactly this kind of
 * change: the role rule is relaxed, and the per-resource check that was supposed to replace it is
 * either forgotten or written in the controller where a second caller of the service can miss it.
 */
@AutoConfigureMockMvc
@Import(TestTokens.class)
class LedgerReadApiTest extends AbstractPostgresIT {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Autowired
    MockMvc mvc;

    @Autowired
    TestTokens tokens;

    // ---------------------------------------------------------------- authorization

    @Test
    @DisplayName("a customer reads their own account, and nobody else's")
    void accountReadIsScopedToItsOwner() throws Exception {
        UUID aliceAccount = openAccount("alice", 100_000);

        expectStatus(200, "/accounts/" + aliceAccount, tokens.user("alice"));

        // 404 rather than 403: an account that exists but is not yours is answered exactly like
        // one that does not exist, so there is no code path that can tell them apart and none
        // that can leak the difference later.
        expectStatus(404, "/accounts/" + aliceAccount, tokens.user("bob"));
        expectStatus(404, "/accounts/" + UUID.randomUUID(), tokens.user("bob"));
    }

    @Test
    @DisplayName("the ledger is scoped by the same check that fetches the account")
    void ledgerIsScopedToItsOwner() throws Exception {
        UUID aliceAccount = openAccount("alice", 100_000);

        expectStatus(200, "/accounts/" + aliceAccount + "/ledger", tokens.user("alice"));
        expectStatus(404, "/accounts/" + aliceAccount + "/ledger", tokens.user("bob"));
    }

    @Test
    @DisplayName("an operator reads any account - an operator sees everything and moves nothing")
    void operatorMayReadAnyAccount() throws Exception {
        UUID aliceAccount = openAccount("alice", 100_000);

        expectStatus(200, "/accounts/" + aliceAccount, tokens.operator());
        expectStatus(200, "/accounts/" + aliceAccount + "/ledger", tokens.operator());
    }

    @Test
    @DisplayName("no token is 401 on both reads")
    void anonymousIsRejected() throws Exception {
        UUID aliceAccount = openAccount("alice", 100_000);

        mvc.perform(get("/accounts/" + aliceAccount))
                .andExpect(r -> assertThat(r.getResponse().getStatus()).isEqualTo(401));
        mvc.perform(get("/accounts/" + aliceAccount + "/ledger"))
                .andExpect(r -> assertThat(r.getResponse().getStatus()).isEqualTo(401));
    }

    @Test
    @DisplayName("opening an account is still operator-only - relaxing the reads did not open it")
    void openingIsStillOperatorOnly() throws Exception {
        // The rule for POST was split out of the /accounts/** matcher so the GETs could fall
        // through to a wider one. This asserts the split did not take the write with it.
        mvc.perform(post("/accounts").contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"ownerId":"bob","currency":"INR","openingBalanceMinor":1000}""")
                        .header(HttpHeaders.AUTHORIZATION, bearer(tokens.user("alice"))))
                .andExpect(r -> assertThat(r.getResponse().getStatus()).isEqualTo(403));
    }

    // ---------------------------------------------------------------- content

    @Test
    @DisplayName("the opening credit is visible, signed, and carries its transfer id")
    void ledgerShowsSignedEntries() throws Exception {
        UUID aliceAccount = openAccount("alice", 100_000);

        JsonNode page = getJson("/accounts/" + aliceAccount + "/ledger", tokens.user("alice"));

        assertThat(page.get("balanceMinor").asLong()).isEqualTo(100_000L);
        assertThat(page.get("entries")).hasSize(1);

        JsonNode entry = page.get("entries").get(0);
        assertThat(entry.get("amountMinor").asLong())
                .as("signed and passed through as stored - the sum of these numbers IS the "
                        + "balance, and a response of magnitudes would destroy that")
                .isEqualTo(100_000L);
        assertThat(entry.get("entryType").asText()).isEqualTo("CREDIT");
        assertThat(entry.get("transferId").isNull())
                .as("the thread back to the saga - the two legs of a posting share it")
                .isFalse();
    }

    @Test
    @DisplayName("the balance shown is the authoritative column, not a total of the page")
    void balanceIsNotDerivedFromThePage() throws Exception {
        UUID aliceAccount = openAccount("alice", 100_000);
        // Two more postings, so a single-entry page cannot possibly add up to the balance.
        transferOut(aliceAccount, 10_000);
        transferOut(aliceAccount, 20_000);

        JsonNode page = getJson("/accounts/" + aliceAccount + "/ledger?size=1",
                tokens.user("alice"));

        assertThat(page.get("entries")).hasSize(1);
        assertThat(page.get("balanceMinor").asLong())
                .as("a UI accumulating the entries it has been shown would be wrong on every "
                        + "page but the last, in a direction that looks plausible")
                .isEqualTo(70_000L);
    }

    @Test
    @DisplayName("paging one entry at a time visits every entry exactly once, newest first")
    void keysetWalksTheHistory() throws Exception {
        UUID aliceAccount = openAccount("alice", 100_000);
        transferOut(aliceAccount, 1_000);
        transferOut(aliceAccount, 2_000);
        transferOut(aliceAccount, 3_000);

        List<Long> seen = new ArrayList<>();
        String cursor = null;
        for (int page = 0; page < 10; page++) {
            JsonNode json = getJson("/accounts/" + aliceAccount + "/ledger?size=1"
                    + (cursor == null ? "" : "&cursor=" + cursor), tokens.user("alice"));
            json.get("entries").forEach(e -> seen.add(e.get("id").asLong()));
            cursor = json.get("nextCursor").isNull() ? null : json.get("nextCursor").asText();
            if (cursor == null) {
                break;
            }
        }

        assertThat(seen).hasSize(4);
        assertThat(new HashSet<>(seen)).hasSize(4);
        assertThat(seen).isSortedAccordingTo((a, b) -> Long.compare(b, a));
    }

    @Test
    @DisplayName("a cursor this API did not issue is 400")
    void malformedCursorIsRefused() throws Exception {
        UUID aliceAccount = openAccount("alice", 100_000);

        // Valid base64url that does not decode to a number - the case a hand-edited cursor
        // actually produces, and the one a naive decoder returns 0 for.
        expectStatus(400, "/accounts/" + aliceAccount + "/ledger?cursor=zzzz",
                tokens.user("alice"));
    }

    // ---------------------------------------------------------------- helpers

    private UUID openAccount(String owner, long openingBalanceMinor) throws Exception {
        MvcResult result = mvc.perform(post("/accounts")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"ownerId":"%s","currency":"INR","openingBalanceMinor":%d}"""
                                .formatted(owner, openingBalanceMinor))
                        .header(HttpHeaders.AUTHORIZATION, bearer(tokens.operator())))
                .andReturn();
        assertThat(result.getResponse().getStatus()).isEqualTo(201);
        return UUID.fromString(
                JSON.readTree(result.getResponse().getContentAsString()).get("id").asText());
    }

    /**
     * Writes a posting out of the account by hand.
     *
     * <p>Straight into the tables rather than through {@code TransferService}, because this class
     * is about the READ path and needs history with known amounts; the write path has its own
     * tests. Both legs and the balance are written together so I1 and I2 still hold - a helper
     * that broke the invariants to set up a read test would make every later assertion suspect.
     */
    private void transferOut(UUID from, long amountMinor) {
        UUID transferId = UUID.randomUUID();
        UUID counterparty = UUID.fromString("00000000-0000-0000-0000-000000000002");
        jdbc.update("""
                INSERT INTO ledger_entries (transfer_id, account_id, amount_minor, entry_type,
                                            currency)
                VALUES (?, ?, ?, 'DEBIT', 'INR'), (?, ?, ?, 'CREDIT', 'INR')
                """, transferId, from, -amountMinor, transferId, counterparty, amountMinor);
        jdbc.update("UPDATE accounts SET balance_minor = balance_minor - ? WHERE id = ?",
                amountMinor, from);
        jdbc.update("UPDATE accounts SET balance_minor = balance_minor + ? WHERE id = ?",
                amountMinor, counterparty);
    }

    private JsonNode getJson(String uri, String token) throws Exception {
        MvcResult result = mvc.perform(get(uri).header(HttpHeaders.AUTHORIZATION, bearer(token)))
                .andReturn();
        assertThat(result.getResponse().getStatus()).isEqualTo(200);
        return JSON.readTree(result.getResponse().getContentAsString());
    }

    private void expectStatus(int expected, String uri, String token) throws Exception {
        mvc.perform(get(uri).header(HttpHeaders.AUTHORIZATION, bearer(token)))
                .andExpect(r -> assertThat(r.getResponse().getStatus()).isEqualTo(expected));
    }
}
