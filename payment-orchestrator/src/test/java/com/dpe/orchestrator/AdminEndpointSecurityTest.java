package com.dpe.orchestrator;

import static com.dpe.orchestrator.support.TestTokens.ALICE;
import static com.dpe.orchestrator.support.TestTokens.bearer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

import com.dpe.orchestrator.support.AbstractPostgresIT;
import com.dpe.orchestrator.support.TestTokens;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.test.web.servlet.MockMvc;

/**
 * The dead letter console, which M4 shipped wide open.
 *
 * <p>{@code POST /admin/dead-letters/{id}/replay} <b>republishes a payment command</b>. Until this
 * milestone anyone who could reach the port could call it, and the M4 log says so in as many
 * words. These tests are the closing of that hole, and they are written against the three answers
 * that matter: no token, a customer's token, an operator's token.
 *
 * <p>The middle one is the interesting case. A valid, unexpired, correctly signed token belonging
 * to a real customer must not open an operator surface - authentication is not authorization, and
 * an endpoint guarded by {@code authenticated()} rather than a role is guarded by nothing in a
 * system where every customer holds a token.
 */
@AutoConfigureMockMvc
@Import(TestTokens.class)
class AdminEndpointSecurityTest extends AbstractPostgresIT {

    @Autowired
    MockMvc mvc;

    @Autowired
    TestTokens tokens;

    @Test
    @DisplayName("no token: 401 on every dead letter endpoint")
    void anonymousIsRejected() throws Exception {
        expect(401, get("/admin/dead-letters"), null);
        expect(401, get("/admin/dead-letters/depth"), null);
        expect(401, post("/admin/dead-letters/{id}/replay", UUID.randomUUID()), null);
        expect(401, post("/admin/dead-letters/replay"), null);
    }

    @Test
    @DisplayName("a customer's valid token: 403 - authentication is not authorization")
    void customerIsForbidden() throws Exception {
        String customer = tokens.user(ALICE);
        expect(403, get("/admin/dead-letters"), customer);
        expect(403, get("/admin/dead-letters/depth"), customer);

        // The one that would have mattered: replay republishes a payment command onto the
        // account-service topic. A customer reaching it is a customer able to re-drive somebody
        // else's money movement.
        expect(403, post("/admin/dead-letters/{id}/replay", UUID.randomUUID()), customer);
        expect(403, post("/admin/dead-letters/replay"), customer);
    }

    @Test
    @DisplayName("an operator's token: through, and a replay of nothing is a 404 rather than a 403")
    void operatorIsAllowed() throws Exception {
        String operator = tokens.operator();
        expect(200, get("/admin/dead-letters/depth"), operator);
        expect(200, get("/admin/dead-letters"), operator);

        // Reaching the controller and being told the letter does not exist is the proof the role
        // check passed - a 403 here would mean the operator never got in.
        expect(404, post("/admin/dead-letters/{id}/replay", UUID.randomUUID()), operator);
    }

    @Test
    @DisplayName("actuator: health open, the rest operator-only")
    void actuatorIsSplit() throws Exception {
        expect(200, get("/actuator/health"), null);
        expect(401, get("/actuator/metrics"), null);
        expect(403, get("/actuator/metrics"), tokens.user(ALICE));
        expect(200, get("/actuator/metrics"), tokens.operator());
    }

    private void expect(int status,
                        org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder
                                request,
                        String token) throws Exception {
        if (token != null) {
            request.header(HttpHeaders.AUTHORIZATION, bearer(token));
        }
        mvc.perform(request).andExpect(result ->
                assertThat(result.getResponse().getStatus()).isEqualTo(status));
    }
}
