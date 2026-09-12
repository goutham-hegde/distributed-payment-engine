package com.dpe.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import com.dpe.events.Topics;
import com.dpe.gateway.saga.GatewayCommandConsumer;
import com.dpe.gateway.support.AbstractPostgresIT;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;

/**
 * M8: the command listener runs one consumer per partition, in the service's own group.
 *
 * <p>Asserted on the container rather than trusted from the annotation because both halves fail
 * silently. A concurrency that does not bind leaves the gateway at one thread and the knee where it
 * was, with nothing wrong in any log. And naming the listener - which is how this test finds it -
 * moves it to a group named after the listener unless {@code idIsGroup = false}; the new group reads
 * from {@code earliest}, the inbox absorbs every re-read, and nothing fails at all.
 */
class GatewayCommandConcurrencyTest extends AbstractPostgresIT {

    @Autowired
    KafkaListenerEndpointRegistry listeners;

    @Test
    @DisplayName("the command listener runs one consumer per partition, in the payment-gateway group")
    void onePerPartitionInTheServiceGroup() {
        MessageListenerContainer container =
                listeners.getListenerContainer(GatewayCommandConsumer.LISTENER_ID);

        assertThat(container).isInstanceOf(ConcurrentMessageListenerContainer.class);
        int concurrency = ((ConcurrentMessageListenerContainer<?, ?>) container).getConcurrency();

        assertThat(concurrency).isEqualTo(3);
        // Above the partition count the extra consumers are assigned nothing - a number that looks
        // like capacity and is not.
        assertThat(concurrency).isLessThanOrEqualTo(Topics.GATEWAY_COMMANDS_PARTITIONS);
        assertThat(container.getGroupId()).isEqualTo("payment-gateway");
    }
}
