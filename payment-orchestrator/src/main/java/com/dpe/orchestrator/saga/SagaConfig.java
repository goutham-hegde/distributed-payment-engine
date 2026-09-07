package com.dpe.orchestrator.saga;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Binds {@link SagaProperties}.
 *
 * <p>{@code @EnableScheduling} is deliberately NOT here - {@link com.dpe.messaging.MessagingConfig}
 * already brings it for the outbox relay, and declaring it twice is harmless but misleading about
 * where the scheduler comes from.
 */
@Configuration
@EnableConfigurationProperties(SagaProperties.class)
public class SagaConfig {
}
