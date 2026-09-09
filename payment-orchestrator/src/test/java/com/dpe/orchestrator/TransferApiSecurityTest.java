package com.dpe.orchestrator;

import static com.dpe.orchestrator.support.TestTokens.ALICE;
import static com.dpe.orchestrator.support.TestTokens.BOB;
import static com.dpe.orchestrator.support.TestTokens.bearer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

import com.dpe.orchestrator.support.AbstractPostgresIT;
import com.dpe.orchestrator.support.TestTokens;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
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
 * The milestone's acceptance criterion, as tests: <b>you cannot move money from an account you do
 * not own.</b>
 *
 * <p>Two halves, and both have to hold.
 *
 * <p><b>Authentication</b> - four tokens that are each wrong in exactly one way, so a regression
 * names the validator that stopped working rather than "security is broken". A token signed with
 * the wrong key, an expired one, one from another issuer, one for another audience: every one is
 * a well-formed JWT that parses cleanly and must still be refused.
 *
 * <p><b>Authorization</b> - a perfectly valid token used against somebody else's account. This is
 * the case a filter chain cannot catch, because the account id is in the request body. It is also
 * the one that shows up in real breach reports.
 *
 * <p>Note what the 401s and 403s mean here, because the distinction is asserted deliberately:
 * <b>401 is "I do not know who you are"</b> (missing, expired, forged) and <b>403 is "I know, and
 * no"</b>. A 403 for a missing token would tell an attacker their credential merely lacked a
 * role; a 401 for a valid token with the wrong role sends a client into a re-login loop that
 * cannot help it.
 */
@AutoConfigureMockMvc
@Import(TestTokens.class)
class TransferApiSecurityTest extends AbstractPostgresIT {

    private static final String INR = "INR";

    @Autowired
    MockMvc mvc;

    @Autowired
    TestTokens tokens;

    private UUID aliceAccount;
    private UUID bobAccount;

    @BeforeEach
    void seedOwnership() {
        jdbc.execute("TRUNCATE TABLE account_owners");
        aliceAccount = projectAccount(ALICE, "CUSTOMER");
        bobAccount = projectAccount(BOB, "CUSTOMER");
    }

    // ---------------------------------------------------------------- authentication

    @Test
    @DisplayName("no token at all is 401, and nothing is written")
    void anonymousIsRejected() throws Exception {
        mvc.perform(transfer(aliceAccount, bobAccount).header("Idempotency-Key", newKey()))
                .andExpect(result -> assertThat(result.getResponse().getStatus()).isEqualTo(401));

        assertThat(transferCount())
                .as("an unauthenticated request must not reach the database at all")
                .isZero();
    }

    @Test
    @DisplayName("a token signed with another key is 401 - the signature is the whole point")
    void forgedSignatureIsRejected() throws Exception {
        expectStatus(401, tokens.signedWithWrongKey(ALICE));
    }

    @Test
    @DisplayName("an expired token is 401 - expiry is the only revocation a stateless token has")
    void expiredTokenIsRejected() throws Exception {
        expectStatus(401, tokens.expired(ALICE));
    }

    @Test
    @DisplayName("a token from another issuer is 401, even correctly signed")
    void wrongIssuerIsRejected() throws Exception {
        // Correctly signed by OUR key, which is the point: this is what a shared secret reused
        // across environments looks like, and only the iss check catches it.
        expectStatus(401, tokens.wrongIssuer(ALICE));
    }

    @Test
    @DisplayName("a token minted for another service is 401 - the confused deputy case")
    void wrongAudienceIsRejected() throws Exception {
        expectStatus(401, tokens.wrongAudience(ALICE));
    }

    // ---------------------------------------------------------------- authorization

    @Test
    @DisplayName("alice cannot move money out of bob's account")
    void cannotSpendFromAnotherAccount() throws Exception {
        mvc.perform(transfer(bobAccount, aliceAccount)
                        .header("Idempotency-Key", newKey())
                        .header(HttpHeaders.AUTHORIZATION, bearer(tokens.user(ALICE))))
                .andExpect(result -> assertThat(result.getResponse().getStatus()).isEqualTo(403));

        assertThat(transferCount())
                .as("a refused request must not create a transfer")
                .isZero();
        assertThat(idempotencyRecordCount())
                .as("nor burn the idempotency key - the client must be able to retry the "
                        + "corrected request with the same key")
                .isZero();
    }

    @Test
    @DisplayName("alice can move money out of her own account")
    void canSpendFromOwnAccount() throws Exception {
        MvcResult result = mvc.perform(transfer(aliceAccount, bobAccount)
                        .header("Idempotency-Key", newKey())
                        .header(HttpHeaders.AUTHORIZATION, bearer(tokens.user(ALICE))))
                .andReturn();

        assertThat(result.getResponse().getStatus()).isEqualTo(202);
        assertThat(transferCount()).isEqualTo(1);
        assertThat(jdbc.queryForObject(
                "SELECT initiated_by FROM transfers LIMIT 1", String.class))
                .as("the subject is recorded on the transfer, and travels on the command")
                .isEqualTo(ALICE);
    }

    @Test
    @DisplayName("an account the projection has never heard of is refused, not accepted")
    void unknownAccountFailsClosed() throws Exception {
        // The staleness case, and the reason the edge check is safe: the projection is fed
        // asynchronously, so "no row" means "I do not know yet" - which in a payment system is
        // answered no. The opposite reading would authorize a transfer out of an account nobody
        // has verified.
        mvc.perform(transfer(UUID.randomUUID(), bobAccount)
                        .header("Idempotency-Key", newKey())
                        .header(HttpHeaders.AUTHORIZATION, bearer(tokens.user(ALICE))))
                .andExpect(result -> assertThat(result.getResponse().getStatus()).isEqualTo(403));
    }

    @Test
    @DisplayName("the CLEARING account is not spendable, whoever appears to own it")
    void internalAccountsAreNotSpendable() throws Exception {
        // Money in flight for every saga in the system sits here. A transfer out of it would be
        // spending other people's held funds; a transfer out of SYSTEM would be minting currency.
        // The owner string on those rows must not be enough to unlock them.
        UUID clearing = projectAccountWithId(
                UUID.fromString("00000000-0000-0000-0000-000000000002"), ALICE, "CLEARING");

        mvc.perform(transfer(clearing, bobAccount)
                        .header("Idempotency-Key", newKey())
                        .header(HttpHeaders.AUTHORIZATION, bearer(tokens.user(ALICE))))
                .andExpect(result -> assertThat(result.getResponse().getStatus()).isEqualTo(403));
    }

    @Test
    @DisplayName("an operator holds no USER role and cannot move money")
    void operatorCannotMoveMoney() throws Exception {
        // Not an oversight in the role list - a deliberate line. An operator inspects and
        // replays; nothing in this system should let one human move another human's money by
        // holding a role.
        mvc.perform(transfer(aliceAccount, bobAccount)
                        .header("Idempotency-Key", newKey())
                        .header(HttpHeaders.AUTHORIZATION, bearer(tokens.operator())))
                .andExpect(result -> assertThat(result.getResponse().getStatus()).isEqualTo(403));
    }

    // ---------------------------------------------------------------- reads

    @Test
    @DisplayName("a transfer belonging to somebody else is 404, not 403")
    void othersTransferIsIndistinguishableFromAbsent() throws Exception {
        MvcResult created = mvc.perform(transfer(aliceAccount, bobAccount)
                        .header("Idempotency-Key", newKey())
                        .header(HttpHeaders.AUTHORIZATION, bearer(tokens.user(ALICE))))
                .andReturn();
        assertThat(created.getResponse().getStatus()).isEqualTo(202);
        UUID transferId = UUID.fromString(jdbc.queryForObject(
                "SELECT id::text FROM transfers LIMIT 1", String.class));

        // 404 rather than 403 because here the ID ITSELF is the secret: a 403 would confirm that
        // this transfer exists to somebody walking uuids. On the write path the caller supplied
        // the account id and 403 is the honest refusal of a claim. Same system, opposite call,
        // and the difference is which value the attacker is fishing for.
        mvc.perform(get("/api/v1/transfers/{id}", transferId)
                        .header(HttpHeaders.AUTHORIZATION, bearer(tokens.user(BOB))))
                .andExpect(result -> assertThat(result.getResponse().getStatus()).isEqualTo(404));

        mvc.perform(get("/api/v1/transfers/{id}", transferId)
                        .header(HttpHeaders.AUTHORIZATION, bearer(tokens.user(ALICE))))
                .andExpect(result -> assertThat(result.getResponse().getStatus()).isEqualTo(200));
    }

    @Test
    @DisplayName("health stays open - a probe has no credentials")
    void healthIsUnauthenticated() throws Exception {
        mvc.perform(get("/actuator/health"))
                .andExpect(result -> assertThat(result.getResponse().getStatus()).isEqualTo(200));
    }

    // ---------------------------------------------------------------- helpers

    private void expectStatus(int expected, String token) throws Exception {
        mvc.perform(transfer(aliceAccount, bobAccount)
                        .header("Idempotency-Key", newKey())
                        .header(HttpHeaders.AUTHORIZATION, bearer(token)))
                .andExpect(result ->
                        assertThat(result.getResponse().getStatus()).isEqualTo(expected));
    }

    private org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder transfer(
            UUID from, UUID to) {
        return post("/api/v1/transfers")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                        {"fromAccountId":"%s","toAccountId":"%s","amountMinor":30000,\
                        "currency":"%s"}""".formatted(from, to, INR));
    }

    private UUID projectAccount(String owner, String type) {
        return projectAccountWithId(UUID.randomUUID(), owner, type);
    }

    /**
     * Writes the projection row directly rather than publishing an AccountOpened event: this
     * class is about the authorization decision, and the delivery path that feeds the table has
     * its own test in {@code AccountOwnerProjectionTest}.
     */
    private UUID projectAccountWithId(UUID accountId, String owner, String type) {
        jdbc.update("""
                INSERT INTO account_owners (account_id, owner_id, account_type, currency)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (account_id) DO NOTHING
                """, accountId, owner, type, INR);
        return accountId;
    }

    private String newKey() {
        return UUID.randomUUID().toString();
    }

    private int transferCount() {
        Integer count = jdbc.queryForObject("SELECT count(*) FROM transfers", Integer.class);
        return count == null ? 0 : count;
    }

    private int idempotencyRecordCount() {
        Integer count = jdbc.queryForObject(
                "SELECT count(*) FROM idempotency_records", Integer.class);
        return count == null ? 0 : count;
    }
}
