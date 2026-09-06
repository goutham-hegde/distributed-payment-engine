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
}
