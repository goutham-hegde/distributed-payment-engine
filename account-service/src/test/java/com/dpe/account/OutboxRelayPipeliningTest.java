package com.dpe.account;

import static org.assertj.core.api.Assertions.assertThat;

import com.dpe.account.support.AbstractPostgresIT;
import com.dpe.events.EventEnvelope;
import com.dpe.events.Topics;
import com.dpe.messaging.outbox.OutboxProperties;
import com.dpe.messaging.outbox.OutboxRelay;
import com.dpe.messaging.outbox.OutboxRepository;
import com.dpe.messaging.tracing.OutboxTracing;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.mock.MockProducerFactory;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * M8: the specification for a PIPELINED {@code OutboxRelay.drainBatch}.
 *
 * <p>Until M8 the relay sent one message and waited for its ack before sending the next, so a
 * batch cost one broker round trip (plus {@code linger.ms}) per message. Pipelining hands the
 * producer many records and waits for their acks together. The speed-up is the easy half; the
 * other half is keeping every guarantee the serial loop got for free from doing one thing at a
 * time. This class pins both halves, and the tests are split accordingly:
 *
 * <ul>
 *   <li><b>Fail against the serial relay - the new behaviour:</b>
 *     <ul>
 *       <li>{@link #sendsTheWholeBatchBeforeWaitingForAnyAck()} - the relay is actually pipelined
 *       <li>{@link #oneDeadlineForTheWholeWaitNotOnePerMessage()} - waiting for N acks is bounded
 *           by {@code sendTimeout}, not by N x {@code sendTimeout}
 *     </ul>
 *   <li><b>Pass against the serial relay - the guards a rewrite must not break:</b>
 *     <ul>
 *       <li>{@link #neverTwoUnackedSendsForOneAggregate()} - fails for "send everything, then wait"
 *       <li>{@link #aFailedSendHoldsBackTheRestOfItsAggregate()} - the M2 blocked-aggregate rule
 *       <li>{@link #marksOnlyWhatTheBrokerAcknowledged()} - publish-then-mark, per message
 *       <li>{@link #theBudgetIsCheckedBeforeEverySend()} - fails for a relay that checks the
 *           budget only between waits, because {@code send()} itself can block
 *     </ul>
 * </ul>
 *
 * <p>No broker: the relay is built by hand over a {@link ScriptedProducer} that decides when (and
 * whether) each send is acknowledged and records what was in flight at every send. Postgres is
 * real, because "marked published" is only true once the claim transaction commits.
 */
class OutboxRelayPipeliningTest extends AbstractPostgresIT {

    private static final String PAYLOAD = """
            {"messageId":"%s","eventType":"FundsTransferred","aggregateId":"%s",
             "occurredAt":"2026-01-01T00:00:00Z","payload":{"transferId":"%s"}}""";

    @Autowired
    OutboxRepository outboxRepository;

    @Autowired
    OutboxTracing tracing;

    @Autowired
    TransactionTemplate tx;

    private final AtomicInteger seedOrder = new AtomicInteger();
    private ScriptedProducer producer;

    @AfterEach
    void stopProducer() {
        if (producer != null) {
            producer.shutdown();
        }
    }

    @Test
    @DisplayName("every aggregate in a batch is sent before the relay waits for any acknowledgement")
    void sendsTheWholeBatchBeforeWaitingForAnyAck() {
        producer = new ScriptedProducer().ackAfter(Duration.ofMillis(300));
        for (int i = 0; i < 5; i++) {
            seed(UUID.randomUUID());
        }

        assertThat(drain(relay(Duration.ofSeconds(5), Duration.ofSeconds(10)))).isEqualTo(5);

        assertThat(producer.maxInFlight())
                .as("five messages about five different transfers share no ordering constraint, "
                        + "so all five should be with the producer at once. 1 means the relay "
                        + "still waits out a round trip per message")
                .isEqualTo(5);
        assertThat(unpublished()).isZero();
    }

    @Test
    @DisplayName("a message is never sent while an earlier one for the same aggregate is unacknowledged")
    void neverTwoUnackedSendsForOneAggregate() {
        producer = new ScriptedProducer().ackAfter(Duration.ofMillis(30));
        UUID a = UUID.randomUUID();
        UUID b = UUID.randomUUID();
        // Interleaved, as a real outbox is: two transfers' messages alternating by created_at.
        UUID a1 = seed(a);
        UUID b1 = seed(b);
        UUID a2 = seed(a);
        UUID b2 = seed(b);
        UUID a3 = seed(a);

        drainUntilEmpty(relay(Duration.ofSeconds(5), Duration.ofSeconds(10)));

        assertThat(producer.overlappingKeys())
                .as("two unacked sends for one aggregate are ordered only by the producer's "
                        + "retry behaviour, and not at all by the relay: if the first fails, the "
                        + "second is already in Kafka and the first's retry lands after it")
                .isEmpty();
        assertThat(sentMessageIds(a)).containsExactly(a1, a2, a3);
        assertThat(sentMessageIds(b)).containsExactly(b1, b2);
    }

    @Test
    @DisplayName("a failed send holds back the rest of its aggregate and nothing else")
    void aFailedSendHoldsBackTheRestOfItsAggregate() {
        UUID a = UUID.randomUUID();
        UUID b = UUID.randomUUID();
        producer = new ScriptedProducer().ackAfter(Duration.ofMillis(30)).failFirstSendOf(a);
        UUID a1 = seed(a);
        UUID b1 = seed(b);
        UUID a2 = seed(a);
        OutboxRelay relay = relay(Duration.ofSeconds(5), Duration.ofSeconds(10));

        assertThat(drain(relay)).as("only b1 was acknowledged").isEqualTo(1);

        assertThat(sentMessageIds(a))
                .as("a2 must not be sent in the drain where a1 failed - wherever the relay "
                        + "puts it, it would reach the broker before a1's retry")
                .containsExactly(a1);
        assertThat(row(a1)).containsEntry("attempts", 1).containsEntry("published_at", null);
        assertThat(row(a2)).containsEntry("attempts", 0).containsEntry("published_at", null);
        assertThat(row(b1).get("published_at"))
                .as("the failure is scoped to aggregate a; b is unaffected")
                .isNotNull();

        drainUntilEmpty(relay);

        assertThat(sentMessageIds(a))
                .as("a1 twice (the failure, then the retry), and only then a2")
                .containsExactly(a1, a1, a2);
    }

    @Test
    @DisplayName("only the messages the broker acknowledged are marked published")
    void marksOnlyWhatTheBrokerAcknowledged() {
        UUID b = UUID.randomUUID();
        producer = new ScriptedProducer().ackAfter(Duration.ofMillis(30)).failFirstSendOf(b);
        UUID a1 = seed(UUID.randomUUID());
        UUID b1 = seed(b);
        UUID c1 = seed(UUID.randomUUID());

        assertThat(drain(relay(Duration.ofSeconds(5), Duration.ofSeconds(10)))).isEqualTo(2);

        assertThat(row(a1).get("published_at")).isNotNull();
        assertThat(row(c1).get("published_at")).isNotNull();
        assertThat(row(b1))
                .as("marked only after its own ack - an ack for a different record says nothing "
                        + "about this one")
                .containsEntry("published_at", null)
                .containsEntry("attempts", 1);
        assertThat(row(b1).get("last_error")).isNotNull();
    }

    @Test
    @DisplayName("waiting for a batch's acknowledgements is bounded by one sendTimeout, not one per message")
    void oneDeadlineForTheWholeWaitNotOnePerMessage() {
        // The first drain in a JVM pays ~1 s of class loading and span setup, which a timing
        // assertion would charge to the relay. Measured: 2.05 s for what should be 1.2 s.
        warmUp();

        producer = new ScriptedProducer().neverAck();
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            ids.add(seed(UUID.randomUUID()));
        }
        Duration sendTimeout = Duration.ofMillis(500);

        long started = System.nanoTime();
        int published = drain(relay(sendTimeout, Duration.ofSeconds(10)));
        Duration took = Duration.ofNanos(System.nanoTime() - started);

        assertThat(published).isZero();
        assertThat(took)
                .as("five sends in flight together should be given up on together: ~1 x 500 ms. "
                        + "Waiting sendTimeout on each future in turn is 5 x 500 ms here, and "
                        + "100 x 5 s with a real batch - the claim transaction open for all of it, "
                        + "against an idle_in_transaction_session_timeout of 30 s")
                .isLessThan(sendTimeout.multipliedBy(3));
        for (UUID id : ids) {
            assertThat(row(id))
                    .as("an unanswered send is a failure: it may have landed, and the retry's "
                            + "duplicate is the inbox's job")
                    .containsEntry("published_at", null)
                    .containsEntry("attempts", 1);
        }
    }

    @Test
    @DisplayName("the batch budget is checked before every send, because send() itself can block")
    void theBudgetIsCheckedBeforeEverySend() {
        // Models send() blocking on topic metadata or a full buffer - up to max.block.ms (3 s) in
        // production - before it has even returned a future.
        producer = new ScriptedProducer().ackAfter(Duration.ofMillis(10))
                .sendBlocksFor(Duration.ofMillis(150));
        for (int i = 0; i < 10; i++) {
            seed(UUID.randomUUID());
        }

        int published = drain(relay(Duration.ofSeconds(5), Duration.ofMillis(400)));
        int sent = producer.history().size();

        assertThat(sent)
                .as("150 ms per send against a 400 ms budget: about three sends. All ten means "
                        + "the budget is only consulted between waits - and with max.block.ms at "
                        + "3 s, a batch of 100 is 300 s of open transaction")
                .isBetween(1, 4);
        assertThat(published).as("every send that was made was acknowledged").isEqualTo(sent);
        assertThat(unpublished()).isEqualTo(10L - sent);
    }

    // ------------------------------------------------------------------------------------------

    private OutboxRelay relay(Duration sendTimeout, Duration batchBudget) {
        KafkaTemplate<String, String> kafka =
                new KafkaTemplate<>(new MockProducerFactory<String, String>(() -> producer));
        return new OutboxRelay(outboxRepository, kafka,
                new OutboxProperties(100, null, sendTimeout, batchBudget, 0), tracing);
    }

    /** A hand-built relay has no transactional proxy, so the drain is given a transaction here. */
    private int drain(OutboxRelay relay) {
        Integer n = tx.execute(s -> relay.drainBatch());
        return n == null ? 0 : n;
    }

    /** One acknowledged publish, so a timed drain afterwards measures the relay and not the JVM. */
    private void warmUp() {
        producer = new ScriptedProducer();
        seed(UUID.randomUUID());
        assertThat(drain(relay(Duration.ofSeconds(5), Duration.ofSeconds(10)))).isEqualTo(1);
        producer.shutdown();
    }

    private void drainUntilEmpty(OutboxRelay relay) {
        for (int i = 0; i < 10 && unpublished() > 0; i++) {
            drain(relay);
        }
        assertThat(unpublished()).as("the outbox should drain within ten polls").isZero();
    }

    private long unpublished() {
        Long n = jdbc.queryForObject("SELECT COUNT(*) FROM outbox WHERE published_at IS NULL",
                Long.class);
        return n == null ? 0 : n;
    }

    private Map<String, Object> row(UUID messageId) {
        return jdbc.queryForMap(
                "SELECT published_at, attempts, last_error FROM outbox WHERE id = ?", messageId);
    }

    /** The message ids the producer was handed for one aggregate, in the order it was handed them. */
    private List<UUID> sentMessageIds(UUID aggregateId) {
        return producer.history().stream()
                .filter(r -> aggregateId.toString().equals(r.key()))
                .map(r -> UUID.fromString(new String(
                        r.headers().lastHeader(EventEnvelope.MESSAGE_ID_HEADER).value(),
                        StandardCharsets.UTF_8)))
                .toList();
    }

    /**
     * Seeds one unpublished row. {@code created_at} is set explicitly and strictly increasing: the
     * claim orders by {@code (created_at, id)}, and two rows inserted in the same microsecond would
     * otherwise be ordered by a random UUID - which would make "a1 before a2" a coin toss.
     */
    private UUID seed(UUID aggregateId) {
        UUID messageId = UUID.randomUUID();
        String payload = PAYLOAD.formatted(messageId, aggregateId, aggregateId).replace("\n", "");
        jdbc.update("""
                INSERT INTO outbox (id, aggregate_type, aggregate_id, topic, event_type, payload,
                                    created_at)
                VALUES (?, 'Transfer', ?, ?, 'FundsTransferred', ?::jsonb,
                        TIMESTAMPTZ '2026-01-01 00:00:00+00' + ? * INTERVAL '1 millisecond')
                """, messageId, aggregateId, Topics.ACCOUNT_EVENTS, payload,
                seedOrder.incrementAndGet());
        return messageId;
    }

    /**
     * A producer whose acknowledgements are scripted, and which records what was in flight.
     *
     * <p>Acks are delivered by one scheduler thread, each {@code ackAfter} after its send, so they
     * complete in send order - {@link MockProducer#completeNext()} always completes the OLDEST
     * outstanding send, and a task scheduled per send with equal delays fires in the same order.
     *
     * <p>Two traps this class exists to step around, both found writing it:
     * <ul>
     *   <li>{@code KafkaTemplate} calls {@code producer.close()} after every non-transactional send
     *       (a real factory hands out a close-safe wrapper), and a closed {@link MockProducer}
     *       refuses the next send with "MockProducer is already closed". So close is a no-op here.
     *   <li>With no partitioner, {@link MockProducer} takes partition 0 from its {@link Cluster},
     *       and the default cluster is empty - an {@code IndexOutOfBoundsException} on the first
     *       send. So it is given one that knows the topic.
     * </ul>
     */
    static final class ScriptedProducer extends MockProducer<String, String> {

        private final ScheduledExecutorService acks = Executors.newSingleThreadScheduledExecutor();
        private final Map<String, Integer> inFlightByKey = new HashMap<>();
        private final List<String> overlapping = new ArrayList<>();
        private final Set<String> failFirstSendOf = new HashSet<>();
        private int inFlight;
        private int maxInFlight;
        private long ackAfterMs = 20;
        private boolean neverAck;
        private long sendBlocksMs;

        ScriptedProducer() {
            super(clusterWith(Topics.ACCOUNT_EVENTS), false, null,
                    new StringSerializer(), new StringSerializer());
        }

        ScriptedProducer ackAfter(Duration delay) {
            this.ackAfterMs = delay.toMillis();
            return this;
        }

        ScriptedProducer neverAck() {
            this.neverAck = true;
            return this;
        }

        ScriptedProducer sendBlocksFor(Duration blocked) {
            this.sendBlocksMs = blocked.toMillis();
            return this;
        }

        /** The first send for this aggregate fails; later ones (the retry) succeed. */
        ScriptedProducer failFirstSendOf(UUID aggregateId) {
            this.failFirstSendOf.add(aggregateId.toString());
            return this;
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<String, String> record, Callback callback) {
            if (sendBlocksMs > 0) {
                // Outside the monitor, as a real send() blocked on metadata would be.
                sleep(sendBlocksMs);
            }
            synchronized (this) {
                String key = record.key();
                if (inFlightByKey.merge(key, 1, Integer::sum) > 1) {
                    overlapping.add(key);
                }
                maxInFlight = Math.max(maxInFlight, ++inFlight);
                boolean fail = failFirstSendOf.remove(key);

                Future<RecordMetadata> future = super.send(record, (metadata, e) -> {
                    // Invoked from completeNext/errorNext, which already hold this monitor.
                    inFlightByKey.merge(key, -1, Integer::sum);
                    inFlight--;
                    if (callback != null) {
                        callback.onCompletion(metadata, e);
                    }
                });
                if (!neverAck) {
                    acks.schedule(() -> fail
                                    ? errorNext(new IllegalStateException("scripted failure for " + key))
                                    : completeNext(),
                            ackAfterMs, TimeUnit.MILLISECONDS);
                }
                return future;
            }
        }

        @Override
        public void close() {
            // See the class javadoc: KafkaTemplate closes after every send.
        }

        @Override
        public void close(Duration timeout) {
        }

        synchronized int maxInFlight() {
            return maxInFlight;
        }

        synchronized List<String> overlappingKeys() {
            return List.copyOf(overlapping);
        }

        void shutdown() {
            acks.shutdownNow();
        }

        private static Cluster clusterWith(String topic) {
            Node node = new Node(0, "localhost", 9092);
            return new Cluster("test", List.of(node),
                    List.of(new PartitionInfo(topic, 0, node, new Node[] {node}, new Node[] {node})),
                    Set.of(), Set.of());
        }

        private static void sleep(long millis) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
