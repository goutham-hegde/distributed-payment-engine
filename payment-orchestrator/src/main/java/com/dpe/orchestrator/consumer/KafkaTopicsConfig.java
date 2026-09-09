package com.dpe.orchestrator.consumer;

import com.dpe.events.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the topic this service consumes, with the same shape account-service declares it.
 *
 * <h2>Why the consumer declares a topic it does not own</h2>
 *
 * <p>Because otherwise the first thing it does is create the topic <i>wrongly</i>. A Kafka
 * consumer subscribing to an unknown topic asks the broker for its metadata, and a broker with
 * auto-creation enabled obliges - using its own defaults, which on a single-node dev broker is
 * <b>one partition</b>. If the orchestrator wins that race against account-service's declaration,
 * the topic is born with one partition, the consumer is assigned that one partition, and
 * account-service's {@code NewTopic} then grows the topic to three behind its back.
 *
 * <p>The failure that follows is genuinely nasty, because nothing errors. The producer keys
 * messages across all three partitions exactly as designed; the consumer holds a stale partition
 * count and is assigned only partition 0, so roughly two thirds of the traffic is simply never
 * delivered. No exception, no lag alert on the partition being read, no log line above DEBUG -
 * it looks like a system with less traffic than it has. It resolves itself, silently, whenever
 * {@code metadata.max.age.ms} (five minutes, by default) triggers a refresh and a rebalance.
 *
 * <p>{@code KafkaAdmin} applies this declaration at startup, before the listener container
 * subscribes, and creating a topic that already exists with the same shape is a no-op. So both
 * services declaring it is idempotent, and whichever boots first gets it right.
 *
 * <p>Belt and braces: {@code allow.auto.create.topics=false} on the consumer stops it requesting
 * creation at all, and the broker in {@code infra/docker-compose.yml} has auto-creation disabled
 * outright, so a topic nobody declared fails loudly instead of appearing with wrong defaults.
 */
@Configuration
public class KafkaTopicsConfig {

    @Bean
    NewTopic accountEventsTopic() {
        return TopicBuilder.name(Topics.ACCOUNT_EVENTS)
                .partitions(Topics.ACCOUNT_EVENTS_PARTITIONS)
                .replicas(1)
                .build();
    }

    /**
     * M3: the orchestrator now produces to both command topics and consumes the gateway's event
     * topic, so all four topics it touches are declared here.
     *
     * <p>Declaring the ones it produces to is not optional either. A producer that reaches an
     * undeclared topic first creates it with the broker's defaults - one partition - and the
     * consumer on the other side inherits that shape permanently. Every saga's messages would
     * then be totally ordered across all transfers, which looks like it works and quietly
     * serialises the whole system behind the slowest participant.
     */
    @Bean
    NewTopic accountCommandsTopic() {
        return TopicBuilder.name(Topics.ACCOUNT_COMMANDS)
                .partitions(Topics.ACCOUNT_COMMANDS_PARTITIONS)
                .replicas(1)
                .build();
    }

    @Bean
    NewTopic gatewayCommandsTopic() {
        return TopicBuilder.name(Topics.GATEWAY_COMMANDS)
                .partitions(Topics.GATEWAY_COMMANDS_PARTITIONS)
                .replicas(1)
                .build();
    }

    @Bean
    NewTopic gatewayEventsTopic() {
        return TopicBuilder.name(Topics.GATEWAY_EVENTS)
                .partitions(Topics.GATEWAY_EVENTS_PARTITIONS)
                .replicas(1)
                .build();
    }

    /**
     * Dead letter topics for the two event topics this service consumes.
     *
     * <p>Declared rather than left to auto-creation, and the failure that prevents is the
     * unkindest one in the system: with broker auto-creation off, an undeclared dead letter topic
     * makes the RECOVERER's publish fail, {@code DefaultErrorHandler} logs and swallows that, and
     * the message the DLQ existed to save is lost by the machinery built to save it. Discovered
     * on the worst day rather than the first.
     */
    @Bean
    NewTopic accountEventsDltTopic() {
        return TopicBuilder.name(Topics.ACCOUNT_EVENTS + Topics.DLT_SUFFIX)
                .partitions(Topics.ACCOUNT_EVENTS_PARTITIONS)
                .replicas(1)
                .build();
    }

    @Bean
    NewTopic gatewayEventsDltTopic() {
        return TopicBuilder.name(Topics.GATEWAY_EVENTS + Topics.DLT_SUFFIX)
                .partitions(Topics.GATEWAY_EVENTS_PARTITIONS)
                .replicas(1)
                .build();
    }

    /**
     * The dead letter topics {@code DeadLetterConsumer} subscribes to, resolved by name from its
     * {@code "#{@dltTopics}"} SpEL expression.
     *
     * <p>Both inbound topics, because this service consumes both. Derived from the same constants
     * the {@code NewTopic} beans use so the two lists cannot drift; a literal list in
     * {@code application.yml} would, and a listener subscribed to a mistyped topic never fires
     * and never complains.
     */
    @Bean
    String[] dltTopics() {
        return new String[]{
                Topics.ACCOUNT_EVENTS + Topics.DLT_SUFFIX,
                Topics.GATEWAY_EVENTS + Topics.DLT_SUFFIX
        };
    }
}
