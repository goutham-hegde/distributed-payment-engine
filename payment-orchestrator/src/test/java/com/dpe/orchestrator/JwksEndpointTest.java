package com.dpe.orchestrator;

import static com.dpe.orchestrator.support.TestTokens.ALICE;
import static com.dpe.orchestrator.support.TestTokens.bearer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

import com.dpe.orchestrator.support.AbstractPostgresIT;
import com.dpe.orchestrator.support.TestTokens;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.util.Date;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

/**
 * The JWKS endpoint, and the two properties M5 part 2 exists for.
 *
 * <p><b>What is published verifies and cannot sign.</b> That is the whole difference from part 1's
 * shared secret, and it rests on a single {@code toPublicJWK()} call in {@code SigningKeys}. Get
 * that wrong and the endpoint serves the private key: it would still be valid JSON, still return
 * 200, and every other test in this repository would still pass. Only an assertion that looks for
 * the private fields by name catches it - hence {@link #jwkSetContainsNoPrivateKeyMaterial()}.
 *
 * <p><b>A public key must not become a signing key.</b> Anyone can download this key set, so if a
 * verifier let a token choose its own algorithm, an attacker could HMAC-sign a token using the
 * published key bytes as the secret and be believed. That attack is the reason
 * {@code NimbusJwtDecoder} is pinned to RS256 here, and
 * {@link #tokenHmacSignedWithThePublicKeyIsRejected()} is what proves the pin is real rather than
 * assumed.
 */
@AutoConfigureMockMvc
@Import(TestTokens.class)
class JwksEndpointTest extends AbstractPostgresIT {

    @Autowired
    MockMvc mvc;

    @Autowired
    TestTokens tokens;

    @Autowired
    JwtDecoder decoder;

    @Test
    @DisplayName("the key set is readable without a token - public keys are meant to be public")
    void jwkSetIsPublic() throws Exception {
        MvcResult result = mvc.perform(get("/.well-known/jwks.json")).andReturn();

        assertThat(result.getResponse().getStatus()).isEqualTo(200);
        assertThat(result.getResponse().getContentAsString()).contains("\"kty\":\"RSA\"");
    }

    @Test
    @DisplayName("the key set contains modulus and exponent, and no private material at all")
    void jwkSetContainsNoPrivateKeyMaterial() throws Exception {
        String body = jwkSetJson();

        assertThat(body)
                .as("n and e are what a verifier needs")
                .contains("\"n\"")
                .contains("\"e\"");

        // The private RSA fields, by their JWK names. Serving any one of them would hand every
        // reader the ability to mint tokens for any identity in the system.
        assertThat(body)
                .as("d is the private exponent - publishing it publishes the ability to sign")
                .doesNotContain("\"d\"")
                .doesNotContain("\"p\"")
                .doesNotContain("\"q\"")
                .doesNotContain("\"dp\"")
                .doesNotContain("\"dq\"")
                .doesNotContain("\"qi\"");
    }

    @Test
    @DisplayName("the published key verifies a real token, and the token names it by kid")
    void publishedKeyVerifiesRealTokens() throws Exception {
        RSAKey published = (RSAKey) JWKSet.parse(jwkSetJson()).getKeys().getFirst();
        String token = tokens.user(ALICE);

        Jwt decoded = decoder.decode(token);
        assertThat(decoded.getSubject()).isEqualTo(ALICE);
        assertThat(decoded.getHeaders().get("kid"))
                .as("kid names the key that signed this token - it is what lets a validator hold "
                        + "several keys at once, which is what makes rotation possible")
                .isEqualTo(published.getKeyID());

        assertThat(published.isPrivate())
                .as("parsed straight from the wire: what was served has no private half")
                .isFalse();
    }

    @Test
    @DisplayName("a token HMAC-signed with the PUBLIC key is rejected - the algorithm is pinned")
    void tokenHmacSignedWithThePublicKeyIsRejected() throws Exception {
        RSAKey published = (RSAKey) JWKSet.parse(jwkSetJson()).getKeys().getFirst();

        // The classic RS256/HS256 confusion attack: take the public key everyone can download,
        // use its bytes as an HMAC secret, and sign whatever you like. A verifier that reads the
        // algorithm from the token's own header would compute the same HMAC and accept it.
        byte[] publicKeyBytes = published.toRSAPublicKey().getEncoded();
        SignedJWT forged = new SignedJWT(
                new JWSHeader.Builder(JWSAlgorithm.HS256).keyID(published.getKeyID()).build(),
                new JWTClaimsSet.Builder()
                        .issuer("dpe")
                        .audience(List.of("dpe-api"))
                        .subject(ALICE)
                        .issueTime(new Date())
                        .expirationTime(new Date(System.currentTimeMillis() + 300_000))
                        .claim("roles", List.of("USER"))
                        .build());
        forged.sign(new MACSigner(publicKeyBytes));

        mvc.perform(get("/api/v1/transfers/{id}", java.util.UUID.randomUUID())
                        .header(HttpHeaders.AUTHORIZATION, bearer(forged.serialize())))
                .andExpect(result -> assertThat(result.getResponse().getStatus())
                        .as("the decoder was told the algorithm; the token does not get to choose")
                        .isEqualTo(401));
    }

    private String jwkSetJson() throws Exception {
        return mvc.perform(get("/.well-known/jwks.json"))
                .andReturn().getResponse().getContentAsString();
    }
}
