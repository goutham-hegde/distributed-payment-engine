package com.dpe.orchestrator.idempotency;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Binds {@link IdempotencyProperties} and provides the transaction boundary the gate runs its
 * database work inside.
 *
 * <h2>Why a TransactionTemplate and not another {@code @Transactional} method</h2>
 *
 * <p>The gate has to do three things in a specific order: check Redis, then run a database
 * transaction, then write to Redis. Only the middle part is transactional, and the outer parts
 * must not be inside it - a Redis round trip with a database transaction held open is a
 * connection pinned to a network call, and the cache write in particular MUST happen after the
 * commit, since caching a response that then rolls back is the dual-write bug in a new outfit.
 *
 * <p>The obvious shape - a public {@code execute()} calling a {@code @Transactional}
 * {@code runOnce()} on the same bean - <b>silently does nothing.</b> Spring implements
 * {@code @Transactional} with a proxy that wraps the bean, and a call from one of its own methods
 * to another goes straight down the {@code this} reference, never touching the proxy. No
 * transaction is started, no error is raised, and the claim and the transfer then commit as two
 * separate auto-committed statements - which is precisely the atomicity this whole design turns
 * on. It would pass every single-threaded test.
 *
 * <p>An injected {@link TransactionTemplate} makes the boundary explicit and impossible to lose
 * to a refactor: the transaction starts where {@code execute(...)} is called and ends where the
 * callback returns. Being able to see the boundary is worth more here than the annotation is.
 */
@Configuration
@EnableConfigurationProperties(IdempotencyProperties.class)
public class IdempotencyConfig {

    @Bean
    TransactionTemplate idempotencyTransactionTemplate(PlatformTransactionManager transactionManager) {
        return new TransactionTemplate(transactionManager);
    }
}
