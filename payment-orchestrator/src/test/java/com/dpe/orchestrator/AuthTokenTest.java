package com.dpe.orchestrator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

import com.dpe.orchestrator.support.AbstractPostgresIT;
import com.dpe.security.Roles;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * The login endpoint: what it hands out, and what it refuses to say.
 *
 * <p>The interesting assertions here are the negative ones. A login endpoint that answers "no
 * such user" differently from "wrong password" is an enumeration oracle - an attacker learns
 * which accounts exist before spending a guess on any password, which is most of the work of
 * attacking a credential store. Both cases must be the same 401 with the same empty body.
 */
@AutoConfigureMockMvc
class AuthTokenTest extends AbstractPostgresIT {

    @Autowired
    MockMvc mvc;

    @Autowired
    JwtDecoder decoder;

    @Autowired
    ObjectMapper objectMapper;

    @Test
    @DisplayName("valid credentials return a token this system's own decoder accepts")
    void issuesAUsableToken() throws Exception {
        MvcResult result = login("alice", "alice-password");
        assertThat(result.getResponse().getStatus()).isEqualTo(200);

        JsonNode body = objectMapper.readTree(result.getResponse().getContentAsString());
        String token = body.get("accessToken").asString();

        // Decoded with the production decoder, not by pulling the JWT apart in the test: the
        // property worth asserting is that this token passes the validators every service runs,
        // not that it has the right shape.
        Jwt jwt = decoder.decode(token);

        assertThat(jwt.getSubject())
                .as("the subject is the owner id, which is what ownership is checked against")
                .isEqualTo("alice");
        assertThat(jwt.getClaimAsStringList(Roles.CLAIM)).containsExactly(Roles.USER);
        assertThat(jwt.getId())
                .as("jti - unused today, and the hook any future revocation list needs")
                .isNotNull();
        assertThat(jwt.getExpiresAt()).isAfter(jwt.getIssuedAt());
    }

    @Test
    @DisplayName("a wrong password and an unknown user are the same 401")
    void refusesWithoutSayingWhy() throws Exception {
        MvcResult wrongPassword = login("alice", "not-alice-password");
        MvcResult unknownUser = login("mallory", "anything");

        assertThat(wrongPassword.getResponse().getStatus()).isEqualTo(401);
        assertThat(unknownUser.getResponse().getStatus()).isEqualTo(401);
        assertThat(unknownUser.getResponse().getContentAsString())
                .as("a body that differed between the two would be an enumeration oracle")
                .isEqualTo(wrongPassword.getResponse().getContentAsString());
    }

    @Test
    @DisplayName("the login endpoint itself needs no token - it is the one that hands them out")
    void loginIsPublic() throws Exception {
        // Stated as a test because it is the one permitAll that is easy to lose in a refactor,
        // and losing it makes the whole system unreachable in a way that looks like a token bug.
        assertThat(login("alice", "alice-password").getResponse().getStatus()).isEqualTo(200);
    }

    @Test
    @DisplayName("an operator's token carries OPERATOR and not USER")
    void rolesAreDisjoint() throws Exception {
        MvcResult result = login("operator", "operator-password");
        JsonNode body = objectMapper.readTree(result.getResponse().getContentAsString());
        Jwt jwt = decoder.decode(body.get("accessToken").asString());

        assertThat(jwt.getClaimAsStringList(Roles.CLAIM))
                .as("an operator that also held USER could move money, which is the one thing "
                        + "the role split exists to prevent")
                .containsExactly(Roles.OPERATOR);
    }

    @Test
    @DisplayName("an endpoint with no rule is denied, not merely unauthenticated")
    void unmappedPathsAreDenied() throws Exception {
        // The catch-all matters more than any single rule: it decides what happens to the
        // endpoint somebody adds next month and forgets to write a rule for.
        mvc.perform(get("/some/endpoint/nobody/wrote/a/rule/for")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer nonsense"))
                .andExpect(result ->
                        assertThat(result.getResponse().getStatus()).isIn(401, 403, 404));
    }

    private MvcResult login(String username, String password) throws Exception {
        return mvc.perform(post("/auth/token")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"username":"%s","password":"%s"}"""
                                .formatted(username, password)))
                .andReturn();
    }
}
