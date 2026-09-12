package com.dpe.account;

import static com.dpe.account.support.TestTokens.bearer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

import com.dpe.account.support.AbstractPostgresIT;
import com.dpe.account.support.TestTokens;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

/**
 * <b>M6 part 3.</b> {@code GET /admin/invariants} - the checks {@code accounts_db} can answer.
 *
 * <p>The test that earns its place is {@link #i1FailsWhenTheLedgerIsBroken()}. Asserting that the
 * endpoint reports green on a healthy system proves almost nothing - a method returning
 * {@code holds: true} unconditionally passes that. So the ledger is deliberately corrupted with a
 * single unbalanced row and the endpoint has to notice. <b>A check that has never been seen to
 * fail is not known to be a check.</b>
 */
@AutoConfigureMockMvc
@Import(TestTokens.class)
class InvariantsEndpointTest extends AbstractPostgresIT {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Autowired
    MockMvc mvc;

    @Autowired
    TestTokens tokens;

    @Test
    @DisplayName("a healthy ledger reports I1, I2 and I5 holding, each with its number")
    void healthyLedgerPasses() throws Exception {
        openAccount("alice", 100_000);

        JsonNode body = invariants();

        assertThat(body.get("service").asText()).isEqualTo("account-service");
        assertThat(body.get("database").asText())
                .as("named explicitly, because the whole point of this endpoint being "
                        + "per-service is that its answer is only ever about one database")
                .isEqualTo("accounts_db");

        assertThat(check(body, "I1").get("holds").asBoolean()).isTrue();
        assertThat(check(body, "I2").get("holds").asBoolean()).isTrue();
        assertThat(check(body, "I5").get("holds").asBoolean()).isTrue();

        assertThat(check(body, "I1").get("detail").asText())
                .as("a passing check still carries evidence that it ran - 'I2 holds' and 'I2 "
                        + "holds and 3 accounts were compared' are different amounts of proof")
                .contains("entries");
    }

    @Test
    @DisplayName("I4 is not answered here - it lives in the other service's database")
    void i4IsAbsentByDesign() throws Exception {
        JsonNode body = invariants();

        assertThat(body.get("checks").findValuesAsText("id")).doesNotContain("I4");
        // Joining the two on the server would need one process holding credentials to both
        // databases - a shared-database architecture reintroduced through the monitoring door.
    }

    @Test
    @DisplayName("I3 is reported as a total, not a verdict - conservation needs two instants")
    void i3IsATotalNotACheck() throws Exception {
        openAccount("alice", 100_000);

        JsonNode conservation = invariants().get("conservation");

        assertThat(conservation.get("customerBalanceMinor").asLong()).isEqualTo(100_000L);
        assertThat(conservation.get("activeHoldsMinor").asLong()).isZero();
        assertThat(conservation.get("totalMinor")).isNotNull();
        assertThat(conservation.get("totalMinor").asLong()).isEqualTo(100_000L);

        // Returning holds:true for I3 would show five green lights instead of four and a number,
        // and would be a lie in the one place this system claims to prove something.
        assertThat(invariants().get("checks").findValuesAsText("id")).doesNotContain("I3");
    }

    @Test
    @DisplayName("money in flight counts toward the conservation total")
    void activeHoldsAreAddedBack() throws Exception {
        UUID account = openAccount("alice", 100_000);

        // A reserve debits the sender and credits CLEARING, so the customer balance total drops
        // while a saga runs. Without adding the hold back, the 'constant' would dip and recover
        // on every transfer and the invariant would be unusable under load.
        jdbc.update("""
                INSERT INTO holds (id, transfer_id, account_id, clearing_account_id, amount_minor,
                                   currency, status)
                VALUES (?, ?, ?, '00000000-0000-0000-0000-000000000002', 30000, 'INR', 'ACTIVE')
                """, UUID.randomUUID(), UUID.randomUUID(), account);
        jdbc.update("UPDATE accounts SET balance_minor = balance_minor - 30000 WHERE id = ?",
                account);

        JsonNode conservation = invariants().get("conservation");
        assertThat(conservation.get("customerBalanceMinor").asLong()).isEqualTo(70_000L);
        assertThat(conservation.get("activeHoldsMinor").asLong()).isEqualTo(30_000L);
        assertThat(conservation.get("totalMinor").asLong()).isEqualTo(100_000L);
    }

    @Test
    @DisplayName("a single unbalanced ledger row makes I1 and I2 fail - the check really checks")
    void i1FailsWhenTheLedgerIsBroken() throws Exception {
        UUID account = openAccount("alice", 100_000);

        // One credit with no counterpart: money created. This is precisely what I1 exists to make
        // structurally impossible, and it can only be written here by going around the service.
        jdbc.update("""
                INSERT INTO ledger_entries (transfer_id, account_id, amount_minor, entry_type,
                                            currency)
                VALUES (?, ?, 500, 'CREDIT', 'INR')
                """, UUID.randomUUID(), account);

        JsonNode body = invariants();

        assertThat(check(body, "I1").get("holds").asBoolean()).isFalse();
        assertThat(check(body, "I1").get("detail").asText()).contains("500");
        assertThat(check(body, "I2").get("holds").asBoolean())
                .as("the balance column no longer agrees with the entries behind it either")
                .isFalse();
    }

    @Test
    @DisplayName("I5 holds even though the SYSTEM account is negative - the scoping is the point")
    void i5IsScopedToCustomerAccounts() throws Exception {
        UUID account = openAccount("alice", 100_000);

        // Funding an account debits SYSTEM, so its balance is now minus the total ever issued.
        // That is by design: money enters the ledger by being debited from it.
        assertThat(systemBalance())
                .as("the row that a check written without the account_type predicate would trip on")
                .isNegative();

        assertThat(check(invariants(), "I5").get("holds").asBoolean())
                .as("scoped to CUSTOMER, so a healthy system is green from the first funded "
                        + "account - a check that is always red is a check that gets switched off")
                .isTrue();

        // And this one cannot be driven red from here at all, which is worth stating rather than
        // working around: `accounts_customer_balance_non_negative` refuses a negative customer
        // balance in the DATABASE, so the overdraft I5 looks for is structurally impossible
        // rather than merely absent. The endpoint is a second, independent read of a fact the
        // constraint already enforces - which is exactly what it should be. Unlike I1 and I2,
        // whose enforcement is the discipline of writing balanced pairs and can therefore be
        // broken by going around the service, as the test above does.
        assertThatThrownBy(() ->
                jdbc.update("UPDATE accounts SET balance_minor = -1 WHERE id = ?", account))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("accounts_customer_balance_non_negative");
    }

    private long systemBalance() {
        Long balance = jdbc.queryForObject(
                "SELECT balance_minor FROM accounts WHERE account_type = 'SYSTEM'", Long.class);
        return balance == null ? 0L : balance;
    }

    @Test
    @DisplayName("the endpoint is operator-only - it is under /admin/**, and stays there")
    void requiresOperator() throws Exception {
        mvc.perform(get("/admin/invariants"))
                .andExpect(r -> assertThat(r.getResponse().getStatus()).isEqualTo(401));

        mvc.perform(get("/admin/invariants")
                        .header(HttpHeaders.AUTHORIZATION, bearer(tokens.user("alice"))))
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

    private static JsonNode check(JsonNode body, String id) {
        for (JsonNode check : body.get("checks")) {
            if (id.equals(check.get("id").asText())) {
                return check;
            }
        }
        throw new AssertionError("no check with id " + id + " in " + body.get("checks"));
    }

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
}
