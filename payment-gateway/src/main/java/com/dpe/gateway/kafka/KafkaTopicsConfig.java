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

    /**
     * The dead letter topic for the command topic this service consumes.
     *
     * <p>Declared, not auto-created: with broker auto-creation off, an undeclared dead letter
     * topic makes the recoverer's own publish fail, and the message the DLQ existed to save is
     * lost by the machinery built to save it.
     */
    @Bean
    NewTopic gatewayCommandsDltTopic() {
        return TopicBuilder.name(Topics.GATEWAY_COMMANDS + Topics.DLT_SUFFIX)
                .partitions(Topics.GATEWAY_COMMANDS_PARTITIONS)
                .replicas(1)
                .build();
    }

    /**
     * The dead letter topics {@code DeadLetterConsumer} subscribes to, resolved by name from its
     * {@code "#{@dltTopics}"} SpEL expression. Built from the same constants as the
     * {@code NewTopic} beans so the two cannot drift apart.
     */
    @Bean
    String[] dltTopics() {
        return new String[]{Topics.GATEWAY_COMMANDS + Topics.DLT_SUFFIX};
    }
}
