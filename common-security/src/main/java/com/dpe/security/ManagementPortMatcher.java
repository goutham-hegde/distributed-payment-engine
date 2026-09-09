package com.dpe.security;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.security.web.util.matcher.RequestMatcher;

/**
 * Answers one question: <b>did this request arrive on the management connector?</b>
 *
 * <h2>Why this exists, and why it is not optional</h2>
 *
 * <p>M6 moves the actuator to its own port ({@code management.server.port}) which Compose does not
 * publish, so Prometheus can scrape {@code /actuator/prometheus} over the Docker network without a
 * credential. The obvious assumption behind that design is <b>wrong</b>, and it is worth stating
 * plainly because it is the kind of wrong that looks like it is working:
 *
 * <p><b>A separate management port is not unprotected.</b> Spring Boot's
 * {@code ServletManagementChildContextConfiguration$ServletManagementContextSecurityConfiguration}
 * reaches into the <i>parent</i> bean factory and registers the parent's
 * {@code springSecurityFilterChain} on the management connector:
 *
 * <pre>
 * springSecurityFilterChain(HierarchicalBeanFactory bf) {
 *     return bf.getParentBeanFactory().getBean("springSecurityFilterChain", Filter.class);
 * }
 * </pre>
 *
 * <p>So every rule written for port 8081 applies unchanged to port 9091, including
 * {@code /actuator/**  hasRole(OPERATOR)}. Without an explicit rule that can tell the two
 * connectors apart, the scraper gets a 401 from a port that "has no security on it".
 *
 * <h2>Why this class is here and the rule is not</h2>
 *
 * <p>This module holds token-validation <i>mechanics</i> and no authorization rule - see the POM.
 * "Which connector did this arrive on" is mechanics: it is a fact about a socket, identical in all
 * three services, with no opinion attached. "Therefore permit it" is a rule, and lives in each
 * service's own {@code SecurityConfig} where every other rule is, so a {@code permitAll} can never
 * arrive by surprise from a library.
 *
 * <h2>Fail closed</h2>
 *
 * <p>The port is read from configuration once, at construction. If {@code management.server.port}
 * is unset, zero, negative, or the same as the application port, there is no separate connector to
 * recognise and this matcher <b>never matches</b> - so the ordinary operator-only rule applies and
 * the actuator stays shut. The failure mode of a misconfiguration is a scraper that gets 401, not
 * an actuator open to the internet.
 *
 * <p>The check is {@link HttpServletRequest#getLocalPort()} - the port the connector actually
 * accepted the connection on. Not a header, not a hostname, not {@code X-Forwarded-Port}: nothing
 * a client can set. That is the whole reason a port is usable as a trust boundary at all.
 */
public final class ManagementPortMatcher implements RequestMatcher {

    /** Sentinel for "there is no distinct management connector"; matches nothing. */
    private static final int NONE = -1;

    private final int managementPort;

    /**
     * @param applicationPort value of {@code server.port}
     * @param managementPort  value of {@code management.server.port}, or -1 when unset
     */
    public ManagementPortMatcher(int applicationPort, int managementPort) {
        this.managementPort =
                (managementPort > 0 && managementPort != applicationPort) ? managementPort : NONE;
    }

    /** True when there is a distinct management connector to match on at all. */
    public boolean isSeparateConnector() {
        return managementPort != NONE;
    }

    public int port() {
        return managementPort;
    }

    @Override
    public boolean matches(HttpServletRequest request) {
        return managementPort != NONE && request.getLocalPort() == managementPort;
    }

    @Override
    public String toString() {
        return isSeparateConnector()
                ? "ManagementPort [" + managementPort + "]"
                : "ManagementPort [none - no separate connector configured]";
    }
}
