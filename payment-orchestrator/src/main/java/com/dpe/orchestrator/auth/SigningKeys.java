package com.dpe.orchestrator.auth;

import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.KeyUse;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.proc.SecurityContext;
import com.dpe.security.SecurityProperties;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * The system's signing key. This class exists in payment-orchestrator and nowhere else, and that
 * is the entire content of M5 part 2.
 *
 * <p>Under HS256 there was one secret, held by all three services, and it both signed and
 * verified - so account-service could have minted itself an operator token, and only convention
 * stopped it. RS256 splits the key in half. The private half lives here; account-service and
 * payment-gateway get the public half and can check a signature but not produce one. The
 * difference stopped being a rule people follow and became arithmetic they cannot do.
 *
 * <h2>Where the key comes from</h2>
 *
 * <p><b>Configured</b>, if {@code dpe.auth.private-key} is set - that is the production path, and
 * the only one that survives a restart or a second instance.
 *
 * <p><b>Generated at startup</b> otherwise, with a loud log line. That keeps
 * {@code git clone && docker compose up} working without a key ceremony, and it is safe in a way
 * a committed default key would not be: nothing to leak, because it never existed before the
 * process started and does not outlive it.
 *
 * <p>The two consequences of the generated key are worth stating, because they are the reason it
 * is not the production answer:
 *
 * <ul>
 *   <li><b>A restart invalidates every token in flight.</b> Bounded by the 15-minute TTL and
 *       tolerable for a demo; a client just logs in again.</li>
 *   <li><b>Two orchestrator instances would sign with different keys.</b> Each publishes only its
 *       own key at its own JWKS endpoint, so a validator that fetched from instance A rejects a
 *       token minted by instance B - intermittently, by load balancer luck, which is the worst
 *       way for anything to fail. Horizontal scaling requires a configured key. That is the same
 *       shape of problem as a session store, arriving from the opposite direction: state that
 *       must be shared, hiding in a component that looks stateless.</li>
 * </ul>
 *
 * <h2>The key id</h2>
 *
 * <p>{@code kid} is the RFC 7638 thumbprint of the public key - derived from the key itself, so
 * two processes given the same key compute the same id without coordinating. It is what makes
 * rotation work: the issuer publishes old and new keys together, each token names the key that
 * signed it, and validators follow along with no restart and no synchronised deploy.
 */
@Component
public class SigningKeys {

    private static final Logger log = LoggerFactory.getLogger(SigningKeys.class);

    private static final int KEY_SIZE = 2048;

    private final RSAKey rsaKey;
    private final boolean generated;

    public SigningKeys(AuthProperties properties) {
        if (properties.hasKeyPair()) {
            this.rsaKey = fromConfiguration(properties);
            this.generated = false;
            log.info("signing with the configured RSA key (kid {})", rsaKey.getKeyID());
        } else {
            this.rsaKey = generate();
            this.generated = true;
            // WARN, not INFO. It is a correct configuration for a laptop and a wrong one for
            // anything with two instances or an uptime requirement, and the difference must not
            // be discovered from a support ticket.
            log.warn("no dpe.auth.private-key configured - generated an ephemeral RSA key "
                    + "(kid {}). Tokens will not survive a restart, and a second instance would "
                    + "sign with a different key. Configure a key pair for anything real.",
                    rsaKey.getKeyID());
        }
    }

    /** The private half, for {@link TokenIssuer}. Nothing else in the system can reach it. */
    public JWKSource<SecurityContext> signingSource() {
        return new ImmutableJWKSet<>(new JWKSet(rsaKey));
    }

    /** The public half, so this service can verify the tokens it issued without an HTTP hop. */
    public RSAPublicKey verificationKey() {
        try {
            return rsaKey.toRSAPublicKey();
        } catch (Exception e) {
            throw new IllegalStateException("signing key has no usable public half", e);
        }
    }

    public String keyId() {
        return rsaKey.getKeyID();
    }

    public boolean isGenerated() {
        return generated;
    }

    /**
     * The JWK set every other service fetches.
     *
     * <p>{@code toPublicJWK()} is the load-bearing call in this class. It strips the private
     * exponent and the primes, leaving modulus and exponent - so what is served can verify a
     * signature and can never produce one. Serving {@code rsaKey} itself would publish the
     * private key to anything that can reach the port, and it would look exactly like this, work
     * exactly like this, and be the worst bug in the repository.
     */
    public Map<String, Object> publicJwkSet() {
        return new JWKSet(rsaKey.toPublicJWK()).toJSONObject();
    }

    private static RSAKey fromConfiguration(AuthProperties properties) {
        try {
            KeyFactory factory = KeyFactory.getInstance("RSA");
            RSAPrivateKey privateKey = (RSAPrivateKey) factory.generatePrivate(
                    new PKCS8EncodedKeySpec(decode(properties.privateKey())));
            RSAPublicKey publicKey = (RSAPublicKey) factory.generatePublic(
                    new X509EncodedKeySpec(decode(properties.publicKey())));
            return build(publicKey, privateKey);
        } catch (Exception e) {
            // Startup failure, deliberately. A service that could not read its signing key and
            // carried on would either mint nothing or mint with something else.
            throw new IllegalStateException(
                    "dpe.auth.private-key / dpe.auth.public-key are not a valid RSA key pair", e);
        }
    }

    private static RSAKey generate() {
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(KEY_SIZE);
            KeyPair pair = generator.generateKeyPair();
            return build((RSAPublicKey) pair.getPublic(), (RSAPrivateKey) pair.getPrivate());
        } catch (Exception e) {
            throw new IllegalStateException("could not generate an RSA signing key", e);
        }
    }

    private static RSAKey build(RSAPublicKey publicKey, RSAPrivateKey privateKey) {
        try {
            return new RSAKey.Builder(publicKey)
                    .privateKey(privateKey)
                    .keyUse(KeyUse.SIGNATURE)
                    // Derived from the key material itself (RFC 7638), not random: two processes
                    // handed the same key agree on its id without ever talking to each other.
                    .keyIDFromThumbprint()
                    .build();
        } catch (Exception e) {
            throw new IllegalStateException("could not compute the key id", e);
        }
    }

    /** Tolerates PEM armour and line breaks, so the same value works in YAML, env and Compose. */
    private static byte[] decode(String key) {
        return Base64.getDecoder().decode(
                key.replaceAll("-----[A-Z ]+-----", "").replaceAll("\\s", ""));
    }
}
