package com.dpe.orchestrator.saga;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

/**
 * M7, Fix E: the sweeper must not time sagas out while this service cannot hear their replies.
 *
 * <p>No broker and no Spring context - the gate is a decision over "which partitions do I hold, and
 * since when", so the test hands it a container whose assignment it controls and a clock it moves.
 * The live proof is chaos scenario 3, which kills the orchestrator with replies waiting.
 */
class ReplyListenerReadinessTest {

    private static final Duration GRACE = Duration.ofSeconds(10);
    private static final Instant T0 = Instant.parse("2026-09-11T10:00:00Z");

    private final StubContainer container = new StubContainer();
    private final ReplyListenerReadiness readiness = new ReplyListenerReadiness(
            new KafkaListenerEndpointRegistry() {
                @Override
                public MessageListenerContainer getListenerContainer(String id) {
                    return SagaReplyConsumer.LISTENER_ID.equals(id) ? container : null;
                }
            },
            new SagaProperties(null, null, 0, 0, GRACE));

    @Test
    @DisplayName("no partitions assigned: deaf, so no timeouts - the replies may already be waiting")
    void unassignedIsDeaf() {
        container.assigned = List.of();
        assertThat(readiness.canHear(T0)).isFalse();
    }

    @Test
    @DisplayName("just assigned: still deaf until the grace period has drained the backlog")
    void graceAfterAssignment() {
        container.assigned = List.of(new TopicPartition("dpe.account.events.v1", 0));

        assertThat(readiness.canHear(T0))
                .as("assigned is not the same as caught up")
                .isFalse();
        assertThat(readiness.canHear(T0.plusSeconds(9))).isFalse();
        assertThat(readiness.canHear(T0.plus(GRACE))).isTrue();
    }

    @Test
    @DisplayName("losing the assignment closes the gate, and the grace starts again from zero")
    void revocationRestartsTheGrace() {
        container.assigned = List.of(new TopicPartition("dpe.account.events.v1", 0));
        readiness.canHear(T0);
        assertThat(readiness.canHear(T0.plusSeconds(20))).isTrue();

        container.assigned = List.of();
        assertThat(readiness.canHear(T0.plusSeconds(25))).isFalse();

        container.assigned = List.of(new TopicPartition("dpe.account.events.v1", 1));
        assertThat(readiness.canHear(T0.plusSeconds(30))).isFalse();
        assertThat(readiness.canHear(T0.plusSeconds(40))).isTrue();
    }

    @Test
    @DisplayName("a stopped container is deaf whatever it last reported")
    void stoppedContainerIsDeaf() {
        container.assigned = List.of(new TopicPartition("dpe.account.events.v1", 0));
        container.running = false;
        assertThat(readiness.canHear(T0.plusSeconds(60))).isFalse();
    }

    /** Just enough of a container to report an assignment. */
    private static final class StubContainer implements MessageListenerContainer {
        Collection<TopicPartition> assigned = List.of();
        boolean running = true;

        @Override
        public Collection<TopicPartition> getAssignedPartitions() {
            return assigned;
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public void setupMessageListener(Object messageListener) {
        }

        @Override
        public Map<String, Map<MetricName, ? extends Metric>> metrics() {
            return Map.of();
        }

        @Override
        public void start() {
        }

        @Override
        public void stop() {
        }
    }
}
