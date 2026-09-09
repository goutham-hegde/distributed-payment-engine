package com.dpe.messaging;

import com.dpe.messaging.deadletter.DeadLetterProperties;
import com.dpe.messaging.outbox.OutboxProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Wires the outbox and inbox into a service. Import it from the service's application class.
 *
 * <pre>
 * &#64;SpringBootApplication
 * &#64;Import(MessagingConfig.class)
 * &#64;EntityScan({"com.dpe.account", "com.dpe.messaging"})
 * &#64;EnableJpaRepositories({"com.dpe.account", "com.dpe.messaging"})
 * public class AccountServiceApplication { }
 * </pre>
 *
 * <h2>Why the service repeats those package lists instead of this class declaring them</h2>
 *
 * <p>This is the one genuinely awkward part of shipping JPA components in a library, and it is
 * worth understanding rather than copying.
 *
 * <p>Spring Boot finds entities and repositories by scanning <b>the package of the class annotated
 * {@code @SpringBootApplication}, and its subpackages</b>. {@code com.dpe.messaging} is not under
 * {@code com.dpe.account}, so nothing in this module is found by default: the relay starts, claims
 * from a repository that does not exist, and the context fails at startup with a missing-bean
 * error that points at the library rather than at the service that forgot to scan it.
 *
 * <p>The trap is what happens next. {@code @EntityScan} and {@code @EnableJpaRepositories} do not
 * <i>add</i> packages to the default scan - they <b>replace</b> it. Boot's
 * {@code JpaRepositoriesAutoConfiguration} backs off the moment {@code @EnableJpaRepositories}
 * appears anywhere in the context, and {@code @EntityScan} overwrites the auto-configured
 * {@code EntityScanPackages} outright. So if this class declared
 * {@code @EnableJpaRepositories("com.dpe.messaging")} on the service's behalf, the service's own
 * repositories would silently stop being registered - the library would work and the application
 * would break.
 *
 * <p>Both packages therefore have to be named together, in one place, and the only place that
 * knows both is the service. Naming them on the application class also puts the fact where a
 * reader looks first, rather than hiding it in a dependency.
 *
 * <p>The alternative - having this class scan the common ancestor {@code "com.dpe"} - works, and
 * is rejected: it makes the library quietly responsible for scanning packages it does not own, so
 * a service that later moves out from under {@code com.dpe} breaks with the same invisible
 * missing-repository error. Explicit lists fail at the point of the mistake.
 *
 * <h2>What this class does bring</h2>
 *
 * <p>{@code @ComponentScan} registers the writer, the relay and its scheduler. {@code @Scheduled}
 * needs {@link EnableScheduling} to be honoured at all - without it the relay's poll method is
 * simply never called, the outbox fills up, and nothing anywhere logs a complaint. And
 * {@link OutboxProperties} needs binding, which {@link EnableConfigurationProperties} does; it
 * carries its own defaults, so a service that sets no {@code dpe.outbox.*} keys still gets a
 * working relay.
 *
 * <p>What it deliberately does <b>not</b> bring is any topic declaration. Which topics exist, with
 * how many partitions, is a decision each service makes about the contracts it participates in;
 * see {@code Topics} and the per-service {@code KafkaTopicsConfig}.
 */
@Configuration
@EnableScheduling
@ComponentScan(basePackageClasses = MessagingConfig.class)
@EnableConfigurationProperties({OutboxProperties.class, DeadLetterProperties.class})
public class MessagingConfig {
}
