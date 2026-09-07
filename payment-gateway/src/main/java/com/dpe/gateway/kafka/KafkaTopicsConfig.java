package com.dpe.gateway.kafka;

import com.dpe.events.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * The topics this service participates in: it consumes commands and produces events.
 *
 * <p>Both are declared, including the one it only consumes. See account-service's
 * {@code KafkaTopicsConfig} for the silent one-partition stall that a missing declaration causes.
 */
@Configuration
public class KafkaTopicsConfig {

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
}
