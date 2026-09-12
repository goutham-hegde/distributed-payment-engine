package com.dpe.account;

import static org.assertj.core.api.Assertions.assertThat;

import com.dpe.account.saga.AccountCommandConsumer;
import com.dpe.account.support.AbstractPostgresIT;
import com.dpe.events.Topics;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;

/**
 * M8: the command listener runs one consumer per partition, in the service's own group.
 *
 * <p>Asserted on the container because both halves fail silently: a concurrency that does not bind
 * leaves one thread and the knee where it was, and a named listener without
 * {@code idIsGroup = false} becomes a group of its own that re-reads from {@code earliest} while
 * the inbox hides it. The parallelism itself - and that it cannot deadlock - is
 * {@link ClearingShardTest}'s job.
 */
class AccountCommandConcurrencyTest extends AbstractPostgresIT {

    @Autowired
    KafkaListenerEndpointRegistry listeners;

    @Test
    @DisplayName("the command listener runs one consumer per partition, in the account-service group")
    void onePerPartitionInTheServiceGroup() {
        MessageListenerContainer container =
                listeners.getListenerContainer(AccountCommandConsumer.LISTENER_ID);

        assertThat(container).isInstanceOf(ConcurrentMessageListenerContainer.class);
        int concurrency = ((ConcurrentMessageListenerContainer<?, ?>) container).getConcurrency();

        assertThat(concurrency).isEqualTo(3);
        assertThat(concurrency).isLessThanOrEqualTo(Topics.ACCOUNT_COMMANDS_PARTITIONS);
        assertThat(container.getGroupId()).isEqualTo("account-service");
    }
}
