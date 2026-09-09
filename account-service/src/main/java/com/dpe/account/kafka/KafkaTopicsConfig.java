package com.dpe.account.kafka;

import com.dpe.events.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * The Kafka topics this service participates in.
 *
 * <p>Renamed from {@code OutboxConfig} at M3. {@code @EnableScheduling} and the relay's
 * {@code @ConfigurationProperties} binding moved to {@link com.dpe.messaging.MessagingConfig}
 * along with the relay itself, leaving this class doing exactly one thing: declaring topics.
 *
 * <p>That split is deliberate. Which topics a service declares is a statement about which
 * contracts it participates in, and the shared messaging library has no business making that
 * decision on a service's behalf - it moves rows to whatever topic the row names.
 */
@Configuration
public class KafkaTopicsConfig {

    /**
     * Declares the topic this service owns, rather than relying on broker-side auto-creation.
     *
     * <p>Auto-created topics get the broker's defaults, which on a single-node dev broker means
     * <b>one partition</b> - and a single-partition topic is totally ordered, which would hide
     * every ordering bug this design is supposed to survive until the day the topic is scaled up
     * in production. Three partitions means the code is exercised against the reordering it
     * actually has to tolerate.
     *
     * <p>The consumer declares the same topic, with the same constant. That is not redundancy:
     * whichever service boots first must create it correctly, because a consumer that subscribes
     * to a topic nobody has declared yet will have the broker auto-create it at one partition and
     * then be assigned only that one - permanently, as far as any test is concerned.
     *
     * <p>Replication factor 1 because there is one broker. In production this is 3 with
     * {@code min.insync.replicas=2}, which is what makes the producer's {@code acks=all}
     * meaningful: with a single replica, "all replicas acknowledged" is one machine's word, and
     * an acknowledged write is still lost if that machine's disk dies.
     */
    @Bean
    NewTopic accountEventsTopic() {
        return TopicBuilder.name(Topics.ACCOUNT_EVENTS)
                .partitions(Topics.ACCOUNT_EVENTS_PARTITIONS)
                .replicas(1)
                .build();
    }

    /**
     * The command topic this service consumes from, declared here as well as by the orchestrator
     * that produces to it.
     *
     * <p>Declaring a topic you only consume is exactly the lesson of M2, and it is worth
     * restating because the failure is invisible: a consumer that subscribes to an undeclared
     * topic has the broker create it with the broker's default of one partition, is assigned only
     * that partition, and then never sees the commands the orchestrator keys onto the other two.
     * No exception, no error log, and it heals itself five minutes later at
     * {@code metadata.max.age.ms} - so it disappears precisely when you go looking for it.
     *
     * <p>Two thirds of every saga would stall in STARTED until the timeout sweeper compensated
     * them, and the symptom would read as "account-service is slow" rather than "the topic has
     * the wrong shape".
     */
    @Bean
    NewTopic accountCommandsTopic() {
        return TopicBuilder.name(Topics.ACCOUNT_COMMANDS)
                .partitions(Topics.ACCOUNT_COMMANDS_PARTITIONS)
                .replicas(1)
                .build();
    }

    /**
     * The dead letter topic for the command topic this service consumes.
     *
     * <p>Declared for exactly the same reason as every other topic here, and the failure it
     * prevents is nastier than usual. A dead letter topic is written to only when something has
     * already gone wrong, so an undeclared one is discovered on the worst day rather than the
     * first day - and with broker auto-creation off, the recoverer's own publish fails, which
     * {@code DefaultErrorHandler} logs and swallows. The message that was being rescued is then
     * lost by the machinery built to rescue it.
     *
     * <p>Same partition count as the source topic. Not required - the recoverer lets the
     * partitioner choose, keying on the unchanged aggregate id - but a dead letter topic
     * narrower than its source would serialise the failures of unrelated aggregates behind each
     * other for no reason.
     */
    @Bean
    NewTopic accountCommandsDltTopic() {
        return TopicBuilder.name(Topics.ACCOUNT_COMMANDS + Topics.DLT_SUFFIX)
                .partitions(Topics.ACCOUNT_COMMANDS_PARTITIONS)
                .replicas(1)
                .build();
    }

    /**
     * The dead letter topics {@code DeadLetterConsumer} subscribes to, resolved by name from its
     * {@code "#{@dltTopics}"} SpEL expression.
     *
     * <p>Derived from the same constants the {@code NewTopic} beans above use, so the list the
     * listener subscribes to and the list the admin client creates cannot disagree. A topic name
     * repeated as a literal in {@code application.yml} would drift the first time one of them was
     * edited, and the symptom - a listener quietly subscribed to a topic nobody writes to - looks
     * exactly like "nothing has failed yet".
     */
    @Bean
    String[] dltTopics() {
        return new String[]{Topics.ACCOUNT_COMMANDS + Topics.DLT_SUFFIX};
    }
}
