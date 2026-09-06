package com.dpe.account.outbox;

import com.dpe.events.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
@EnableConfigurationProperties(OutboxProperties.class)
public class OutboxConfig {

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
}
