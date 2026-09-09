package com.dpe.security;

/**
 * The role vocabulary, and the claim it travels in.
 *
 * <p>Two roles, and the line between them is the one that matters in a payment system:
 * <b>an operator can see everything and move nothing.</b> Operator endpoints are diagnostic and
 * remedial - queue depth, dead letters, replay - and none of them may become a way to move money
 * out of an account that is not the operator's. Moving money is {@link #USER}'s alone, and even
 * then only out of accounts that user owns.
 *
 * <p>Deliberately absent: a SERVICE role. Nothing in this system authenticates service to service
 * over HTTP - the services talk over Kafka, where the trust boundary is the broker's, not a
 * token's. Adding a role for callers that do not exist would be an unused key with production
 * privileges, which is how the interesting breaches start.
 */
public final class Roles {

    private Roles() {
    }

    /**
     * The claim carrying role names. Not {@code scope}: Spring's default converter maps
     * {@code scope}/{@code scp} to authorities prefixed {@code SCOPE_}, and OAuth2 scopes are
     * what a CLIENT was delegated, which is a different question from what a USER is. This system
     * has no third-party clients, so roles are the honest model.
     */
    public static final String CLAIM = "roles";

    /** Owns accounts. May move money out of the accounts they own, and no others. */
    public static final String USER = "USER";

    /** Reads operational surfaces and replays dead letters. Cannot move money. */
    public static final String OPERATOR = "OPERATOR";

    /**
     * Prefix Spring Security requires on an authority for {@code hasRole("X")} to match it.
     * {@code hasRole("OPERATOR")} looks for the authority {@code ROLE_OPERATOR};
     * {@code hasAuthority("OPERATOR")} looks for {@code OPERATOR}. Mixing the two is the single
     * most common way a rule silently matches nothing, and a rule that matches nothing after
     * {@code anyRequest().authenticated()} denies rather than errors.
     */
    public static final String PREFIX = "ROLE_";
}
