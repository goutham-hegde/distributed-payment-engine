package com.dpe.messaging.outbox;

import com.dpe.events.EventEnvelope;
import com.dpe.messaging.tracing.OutboxTracing;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Drains the outbox into Kafka.
 *
 * <p>The relay is the half of the outbox pattern that turns rows back into messages. It is
 * deliberately dumb: it never deserializes a payload, never knows an event class, and routes
 * purely on what the row says, so a new message type is added without touching this file.
 *
 * <h2>What one drain does</h2>
 *
 * <ol>
 *   <li><b>Claims a batch</b> via {@link OutboxRepository#claimUnpublished}, which locks the rows
 *       {@code FOR UPDATE SKIP LOCKED}. That lock is the only thing preventing a second relay
 *       instance from publishing the same rows. An empty claim returns immediately, so an idle
 *       system costs one indexed lookup per poll.</li>
 *
 *   <li><b>Keys each record by {@code aggregateId}</b>, never by the message id and never null.
 *       Kafka orders within a partition only, so the key is the entire ordering guarantee: two
 *       messages about the same transfer land on the same partition and cannot overtake each
 *       other. A null key round-robins; the message id is unique per message and would scatter
 *       one transfer's messages across partitions. The message id and event type ride in headers
 *       instead, so a consumer can dedupe and route without deserializing the body.</li>
 *
 *   <li><b>Waits for the broker's acknowledgement, with a bounded timeout.</b>
 *       {@code kafka.send()} returns a future immediately - the send has not happened yet. The
 *       wait is bounded by {@link OutboxProperties#sendTimeout} because an unbounded {@code get()}
 *       against a wedged broker would hold the claim transaction open indefinitely and turn a
 *       broker outage into a database incident.</li>
 *
 *   <li><b>Marks the row published only after that ack.</b> This ordering is the whole design.
 *       Publish-then-mark can duplicate: a crash in between republishes on restart. Mark-then-
 *       publish can lose: the row is retired for a send that never happened. Duplicates are
 *       absorbed by the consumer's inbox; losses are absorbed by nobody, so the recoverable
 *       failure is the one to choose.</li>
 *
 *   <li><b>On a failed send, records the failure and leaves the row unpublished.</b> The
 *       exception is swallowed rather than rethrown: an exception escaping this method rolls the
 *       transaction back, which would discard the {@code markPublished} of every message in the
 *       batch that <i>did</i> succeed and guarantee they were all published a second time.
 *       {@code attempts} accumulates instead, which is what M4 turns into a DLQ decision.</li>
 * </ol>
 *
 * <h2>Why a failure blocks the rest of its aggregate</h2>
 *
 * Step 5 has a hole that swallowing the exception creates. If two messages in one batch share an
 * {@code aggregateId} and the first fails while the second succeeds, the second reaches Kafka
 * first and the retry of the first arrives after it - <b>reordered within a single aggregate</b>,
 * which is exactly the guarantee the partition key was bought to provide.
 *
 * <p>So the failing aggregate id is remembered for the rest of the batch and its later messages
 * are left for the next poll. Order within the aggregate is preserved because nothing later ever
 * overtakes something earlier that has not been sent.
 *
 * <p>The blunter alternative - abandon the whole batch on the first failure - also preserves
 * order, and is worse. A single poison message would then stall every unrelated transfer behind
 * it in the batch, converting one undeliverable row into a total outage of the relay. Blocking
 * only the affected aggregate keeps the blast radius equal to the thing that actually broke.
 * That is the same principle as keying by aggregate in the first place: guarantees are scoped to
 * an aggregate, and so are failures.
 *
 * <h2>Why the transaction boundary is where it is</h2>
 *
 * The claim's row locks last until this method's transaction commits, and those locks are what
 * stop a second relay instance from publishing the same rows. So the Kafka sends happen
 * <i>inside</i> the transaction - network I/O with database locks held, which is normally an
 * anti-pattern.
 *
 * <p>It is acceptable here for a specific reason: nothing else ever touches these rows. Business
 * transactions only INSERT new outbox rows; they never read or update the ones being drained. So
 * the lock blocks no user-facing work. What it does cost is a <b>long-running transaction</b>,
 * which pins the vacuum horizon in Postgres and bloats the table if the broker is slow - which is
 * why the batch is small and the send timeout is bounded.
 *
 * <p>The alternative shape - claim and release, publish outside any transaction, then mark in a
 * second transaction - avoids the long transaction and is worse. Releasing the lock reopens the
 * window for another relay to claim the same row, so it needs a {@code claimed_by} /
 * {@code claimed_at} column and a reaper for rows claimed by a relay that then died. More
 * machinery, same at-least-once guarantee.
 *
 * <h2>Why this class has to know about tracing at all</h2>
 *
 * Every other component in this system gets distributed tracing for free, because the
 * instrumentation reads the current span off a thread local at the moment of the outbound call.
 * This one cannot: the call it makes was decided by a transaction that committed on a different
 * thread and has already finished. The context was persisted on the row by
 * {@link OutboxWriter#append}, and it is this method's job to put it back before the send - see
 * {@link com.dpe.messaging.tracing.OutboxTracing}. Skip it and every payment becomes one trace per
 * hop, joined by nothing.
 *
 * @see OutboxRelayScheduler for why the {@code @Scheduled} trigger lives in a different class
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository outbox;
    private final KafkaTemplate<String, String> kafka;
    private final OutboxProperties properties;
    private final OutboxTracing tracing;

    public OutboxRelay(OutboxRepository outbox, KafkaTemplate<String, String> kafka,
                       OutboxProperties properties, OutboxTracing tracing) {
        this.outbox = outbox;
        this.kafka = kafka;
        this.properties = properties;
        this.tracing = tracing;
    }

    /**
     * Publishes one batch and returns how many messages the broker acknowledged.
     *
     * <p>The return value is not decoration: it is what lets a test drive the relay
     * deterministically instead of sleeping and hoping, and it becomes a counter at M6.
     */
    @Transactional
    public int drainBatch() {
        List<OutboxMessage> batch = outbox.claimUnpublished(properties.batchSize());
        if (batch.isEmpty()) {
            return 0;
        }

        // Aggregates that have already had a message fail in this batch. Their later messages
        // wait for the next poll rather than overtaking the one that did not get through.
        Set<UUID> blocked = new HashSet<>();
        int published = 0;

        // M7: the batch has a wall-clock budget as well as a size. See OutboxProperties#batchBudget
        // - it is what lets Postgres reap an orphaned transaction without reaping this one.
        long deadline = System.nanoTime() + properties.batchBudget().toNanos();
        int attempted = 0;

        for (OutboxMessage message : batch) {
            // Never before the first send: a budget shorter than one send would otherwise stop
            // every drain before it started, and a relay that never publishes is an outage that
            // looks exactly like an idle system.
            if (attempted > 0 && System.nanoTime() - deadline > 0) {
                // Stopping is safe for the same reason a blocked aggregate is: every row not
                // reached is still unpublished and still in order, and the next poll claims it.
                log.warn("outbox drain spent its {} budget having published {} of {} claimed "
                        + "messages; the rest wait for the next poll", properties.batchBudget(),
                        published, batch.size());
                break;
            }
            if (blocked.contains(message.getAggregateId())) {
                continue;
            }
            attempted++;
            ProducerRecord<String, String> record = recordFor(message);

            // Restores the trace context this message was produced with. The row carries what
            // OutboxWriter captured on the producing thread - message.getTraceParent() /
            // getTraceState() - and beginPublish() turns that into a child span (or a root span
            // when the row has no stored context, e.g. the timeout sweeper). One span per
            // message, scoped to this loop iteration, closed via try-with-resources so a failure
            // below can never leak the scope onto the next message on this thread.
            try (var publish = tracing.beginPublish(message.getTraceParent(),
                    message.getTraceState(), message.getTopic(), message.getEventType())) {

                // Must happen before send(): the producer serializes the record on this thread,
                // so a header added after send() has already been handed a copy the broker
                // never sees.
                publish.injectInto(record.headers());

                try {
                    // Blocks for at most sendTimeout. The producer's own delivery.timeout.ms is
                    // set below this value in application.yml, so in practice the future
                    // completes on its own - exceptionally if it must - rather than being
                    // abandoned here while the producer is still retrying in the background and
                    // may yet succeed.
                    kafka.send(record)
                            .get(properties.sendTimeout().toMillis(), TimeUnit.MILLISECONDS);

                    // Dirty checking flushes this UPDATE at commit; the row is durably published-
                    // marked only if the whole batch's transaction commits, which it does because
                    // nothing below rethrows.
                    message.markPublished(OffsetDateTime.now());
                    published++;
                } catch (InterruptedException e) {
                    // The JVM is shutting down. Restore the flag the catch cleared, stop claiming
                    // more work, and let the remaining rows be picked up by whoever runs next -
                    // they are still unpublished, which is the safe state.
                    Thread.currentThread().interrupt();
                    message.recordFailure("interrupted while awaiting broker acknowledgement");
                    blocked.add(message.getAggregateId());
                    // The method swallows this exception, so a span left to infer success from a
                    // normal return would report a green publish for a message that never left.
                    publish.error(e);
                    break;
                } catch (ExecutionException | TimeoutException | RuntimeException e) {
                    // Not rethrown on purpose - see the class javadoc, step 5. A timeout is the
                    // ambiguous case and is treated as a failure: the message may in fact have
                    // reached the broker, so the retry may duplicate it. That is precisely the
                    // duplicate the consumer's inbox exists to absorb, and it is the right way to
                    // be wrong.
                    log.warn("outbox message {} ({}) failed to publish to {}; leaving it unpublished "
                                    + "for retry (attempt {})", message.getId(), message.getEventType(),
                            message.getTopic(), message.getAttempts() + 1, e);
                    message.recordFailure(describe(e));
                    blocked.add(message.getAggregateId());
                    publish.error(e);
                }
            }
        }

        return published;
    }

    private static ProducerRecord<String, String> recordFor(OutboxMessage message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                message.getTopic(),
                message.getAggregateId().toString(),
                message.getPayload());
        record.headers()
                .add(EventEnvelope.MESSAGE_ID_HEADER, utf8(message.getId().toString()))
                .add(EventEnvelope.EVENT_TYPE_HEADER, utf8(message.getEventType()));
        return record;
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * A one-line cause for the {@code last_error} column. An {@link ExecutionException} is
     * unwrapped because its own message is only ever the wrapped class name, which says nothing
     * about why the broker refused.
     */
    private static String describe(Exception e) {
        Throwable cause = e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
        return cause.getClass().getSimpleName() + ": " + cause.getMessage();
    }
}
