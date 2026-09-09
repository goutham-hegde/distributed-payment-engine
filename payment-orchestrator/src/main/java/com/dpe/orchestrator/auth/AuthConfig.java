package com.dpe.orchestrator.auth;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Binds {@link AuthProperties}. Nothing else - issuing a token needs no beans beyond the
 * {@code TokenIssuer} that {@code common-security} already provides.
 *
 * <p>Separate from the orchestrator's {@code SecurityConfig} on purpose: that class is about who
 * may call what, this one is about handing out credentials, and at M5 part 2 (RS256) this is the
 * side that gains a private key. Keeping them apart means the split does not require untangling
 * one class into two under time pressure.
 */
@Configuration
@EnableConfigurationProperties(AuthProperties.class)
public class AuthConfig {
}
