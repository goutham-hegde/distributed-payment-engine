package com.dpe.account;

import static com.dpe.account.support.TestTokens.bearer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

import com.dpe.account.support.AbstractPostgresIT;
import com.dpe.account.support.TestTokens;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

/**
 * account-service's HTTP surface after M5: administrative, and mostly shut.
 *
 * <p>The test that matters most is {@link #directLedgerEndpointIsClosedToEveryone()}. M1's
 * {@code POST /transfers} writes ledger entries with no saga, no idempotency key and no ownership
 * check - a second door into the money that bypasses every mechanism the last three milestones
 * built. There is no role that should hold it open, so it is denied for everyone, including an
 * operator.
 */
@AutoConfigureMockMvc
@Import(TestTokens.class)
class AccountApiSecurityTest extends AbstractPostgresIT {

    @Autowired
    MockMvc mvc;

    @Autowired
    TestTokens tokens;

    @Test
    @DisplayName("opening an account needs an operator, not merely a token")
    void openingRequiresOperator() throws Exception {
        String body = """
                {"ownerId":"alice","currency":"INR","openingBalanceMinor":100000}""";

        // No token at all.
        mvc.perform(post("/accounts").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(r -> assertThat(r.getResponse().getStatus()).isEqualTo(401));

        // A real customer's token. The request body names the owner, so leaving this to any
        // authenticated caller would let one customer open an account in another's name.
        mvc.perform(post("/accounts").contentType(MediaType.APPLICATION_JSON).content(body)
                        .header(HttpHeaders.AUTHORIZATION, bearer(tokens.user("alice"))))
                .andExpect(r -> assertThat(r.getResponse().getStatus()).isEqualTo(403));

        mvc.perform(post("/accounts").contentType(MediaType.APPLICATION_JSON).content(body)
                        .header(HttpHeaders.AUTHORIZATION, bearer(tokens.operator())))
                .andExpect(r -> assertThat(r.getResponse().getStatus()).isEqualTo(201));
    }

    @Test
    @DisplayName("the direct ledger endpoint is closed to everyone, operators included")
    void directLedgerEndpointIsClosedToEveryone() throws Exception {
        String body = """
                {"transferId":"%s","fromAccountId":"%s","toAccountId":"%s",\
                "amountMinor":1000,"currency":"INR"}"""
                .formatted(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());

        mvc.perform(post("/transfers").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(r -> assertThat(r.getResponse().getStatus()).isEqualTo(401));

        // The point of the test: holding the highest role in the system does not reopen a path
        // that skips the saga, the idempotency gate and the ownership check.
        mvc.perform(post("/transfers").contentType(MediaType.APPLICATION_JSON).content(body)
                        .header(HttpHeaders.AUTHORIZATION, bearer(tokens.operator())))
                .andExpect(r -> assertThat(r.getResponse().getStatus()).isEqualTo(403));
    }

    @Test
    @DisplayName("the dead letter console is operator-only here too")
    void deadLettersRequireOperator() throws Exception {
        mvc.perform(get("/admin/dead-letters/depth"))
                .andExpect(r -> assertThat(r.getResponse().getStatus()).isEqualTo(401));
        mvc.perform(get("/admin/dead-letters/depth")
                        .header(HttpHeaders.AUTHORIZATION, bearer(tokens.user("alice"))))
                .andExpect(r -> assertThat(r.getResponse().getStatus()).isEqualTo(403));
        mvc.perform(get("/admin/dead-letters/depth")
                        .header(HttpHeaders.AUTHORIZATION, bearer(tokens.operator())))
                .andExpect(r -> assertThat(r.getResponse().getStatus()).isEqualTo(200));
    }

    @Test
    @DisplayName("health stays open so the container probe keeps working")
    void healthIsUnauthenticated() throws Exception {
        // Not a formality: a probe that needs a credential turns an expiring secret into a
        // rolling restart, and this endpoint exposes nothing.
        mvc.perform(get("/actuator/health"))
                .andExpect(r -> assertThat(r.getResponse().getStatus()).isEqualTo(200));
    }
}
