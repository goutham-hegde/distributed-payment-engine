package com.dpe.orchestrator.saga;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.dpe.events.Topics;
import com.dpe.orchestrator.support.AbstractKafkaIT;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;

/**
 * M8: the reply listener runs three consumers in the service's one group, and between them they
 * hold every reply partition.
 *
 * <p>Against a real broker, because the claim is about what the group coordinator actually hands
 * out, not about what the annotation says. Each half fails silently in production: a concurrency
 * that does not bind leaves the listener at one thread and the knee where it was; a named listener
 * without {@code idIsGroup = false} becomes its own group and re-reads every reply from
 * {@code earliest}, absorbed by the inbox; and a consumer beyond what the assignor can use holds
 * nothing while looking like capacity.
 */
class ReplyListenerConcurrencyTest extends AbstractKafkaIT {

    @Autowired
    KafkaListenerEndpointRegistry listeners;

    @Autowired
    ReplyListenerReadiness readiness;

    @Test
    @DisplayName("three reply consumers in the payment-orchestrator group, one partition of each topic apiece")
    void threeConsumersShareEveryReplyPartition() {
        MessageListenerContainer container =
                listeners.getListenerContainer(SagaReplyConsumer.LISTENER_ID);
        assertThat(container).isInstanceOf(ConcurrentMessageListenerContainer.class);
        ConcurrentMessageListenerContainer<?, ?> concurrent =
                (ConcurrentMessageListenerContainer<?, ?>) container;

        assertThat(concurrent.getConcurrency()).isEqualTo(3);
        assertThat(container.getGroupId()).isEqualTo("payment-orchestrator");

        // Every child consumer holds exactly one partition of each reply topic - the range
        // assignor divides each topic on its own - so none is idle and none holds two of one topic.
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            List<? extends MessageListenerContainer> children = concurrent.getContainers();
            assertThat(children).hasSize(3);
            for (MessageListenerContainer child : children) {
                Collection<TopicPartition> held = child.getAssignedPartitions();
                assertThat(held).isNotNull();
                assertThat(held).extracting(TopicPartition::topic)
                        .containsExactlyInAnyOrder(Topics.ACCOUNT_EVENTS, Topics.GATEWAY_EVENTS);
            }
        });

        Set<TopicPartition> all = new HashSet<>(container.getAssignedPartitions());
        assertThat(all).hasSize(Topics.ACCOUNT_EVENTS_PARTITIONS + Topics.GATEWAY_EVENTS_PARTITIONS);

        // The sweeper's gate reads the union, so it still opens once the instance holds partitions -
        // after listen-grace, counted from the first call that sees them.
        await().atMost(Duration.ofSeconds(30)).until(readiness::canHear);
    }
}
