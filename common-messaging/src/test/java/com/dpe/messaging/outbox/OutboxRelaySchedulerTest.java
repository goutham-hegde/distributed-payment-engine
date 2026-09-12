package com.dpe.messaging.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * M8: the scheduler drains again while batches come back full - and only a bounded number of times,
 * because the relay shares its scheduler thread with the saga sweeper and the metrics refresh.
 */
class OutboxRelaySchedulerTest {

    private static final OutboxProperties PROPERTIES = new OutboxProperties(100, null, null, null, 3);

    @Test
    @DisplayName("a full batch is followed by another drain; a short one ends the tick")
    void drainsAgainOnlyWhileBatchesAreFull() {
        ScriptedRelay relay = new ScriptedRelay(100, 100, 40, 100);

        new OutboxRelayScheduler(relay, PROPERTIES).poll();

        assertThat(relay.calls).as("the 40 means the outbox is drained").isEqualTo(3);
    }

    @Test
    @DisplayName("a batch with any failure ends the tick, so a failing broker costs one batch per tick")
    void aPartlyFailedBatchEndsTheTick() {
        ScriptedRelay relay = new ScriptedRelay(99, 100, 100);

        new OutboxRelayScheduler(relay, PROPERTIES).poll();

        assertThat(relay.calls).isEqualTo(1);
    }

    @Test
    @DisplayName("a backlog that never runs out still yields the thread after maxBatchesPerPoll")
    void boundedUnderSustainedLoad() {
        ScriptedRelay relay = new ScriptedRelay(100, 100, 100, 100, 100, 100);

        new OutboxRelayScheduler(relay, PROPERTIES).poll();

        assertThat(relay.calls)
                .as("unbounded, this thread never returns to the sweeper or the metrics refresh")
                .isEqualTo(PROPERTIES.maxBatchesPerPoll());
    }

    @Test
    @DisplayName("an absent max-batches-per-poll defaults to more than one")
    void defaultsAreSane() {
        assertThat(new OutboxProperties(0, null, null, null, 0).maxBatchesPerPoll()).isEqualTo(5);
    }

    /** Returns a scripted published-count per call; no database, no broker. */
    private static final class ScriptedRelay extends OutboxRelay {

        private final Deque<Integer> results;
        private int calls;

        ScriptedRelay(Integer... results) {
            super(null, null, PROPERTIES, null);
            this.results = new ArrayDeque<>(List.of(results));
        }

        @Override
        public int drainBatch() {
            calls++;
            return results.isEmpty() ? 0 : results.poll();
        }
    }
}
