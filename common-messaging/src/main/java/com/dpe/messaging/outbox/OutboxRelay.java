package com.dpe.messaging.outbox;

import com.dpe.events.EventEnvelope;
import com.dpe.messaging.tracing.OutboxTracing;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
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
 *   <li><b>Sends in waves, and waits for each wave's acknowledgements together</b> (M8). A wave is
 *       the next message of every aggregate that has one; see "Pipelining" below.</li>
 *
 *   <li><b>Marks a row published only after its own ack.</b> This ordering is the whole design.
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
 * <h2>Pipelining, and the one rule it must not break</h2>
 *
 * Until M8 the relay sent one record and blocked on its ack before sending the next, so a batch of
 * 100 cost 100 broker round trips back to back - and {@code linger.ms} on every one of them, since
 * the producer waited for company that the relay would never send until the ack came back. Now the
 * producer is handed many records at once and batches them into a few requests, which is what it
 * is built for.
 *
 * <p>The rule: <b>at most one unacknowledged message per aggregate.</b> The idempotent producer
 * keeps order across its OWN retries, and knows nothing about this class's retry, which is a brand
 * new record sent by a later poll. Send A1 and A2 together, let A1 fail terminally while A2
 * succeeds, and A2 is in the log before A1's retry - one transfer reordered, which is exactly the
 * guarantee the partition key was bought to provide. So a wave carries one message per aggregate,
 * and the next wave only carries an aggregate's next message if the previous one was acked.
 *
 * <p>In the orchestrator this costs nothing measurable: a transfer's commands are almost always one
 * per batch, because each waits for a reply. The waves exist for the case where they are not.
 *
 * <h2>Why a failure blocks the rest of its aggregate</h2>
 *
 * Step 5 has a hole that swallowing the exception creates. If A1 fails, A2 must not go in a later
 * wave of the same drain either, or it reaches Kafka before A1's retry. So the failing aggregate id
 * is remembered for the rest of the batch and its later messages are left for the next poll.
 *
 * <p>The blunter alternative - abandon the whole batch on the first failure - also preserves
 * order, and is worse. A single poison message would then stall every unrelated transfer behind
 * it in the batch, converting one undeliverable row into a total outage of the relay. Blocking
 * only the affected aggregate keeps the blast radius equal to the thing that actually broke.
 * That is the same principle as keying by aggregate in the first place: guarantees are scoped to
 * an aggregate, and so are failures.
 *
 * <h2>The two time bounds, and why each is where it is</h2>
 *
 * The claim transaction is open from the claim to the commit with no SQL in between, so to Postgres
 * the whole drain is one "idle in transaction" stretch, and {@code idle_in_transaction_session_timeout}
 * (30 s) is sized against its ceiling: batchBudget + max.block.ms + sendTimeout.
 * <ul>
 *   <li><b>The batch budget is checked before every {@code send()}</b>, not only between waves:
 *       {@code send()} itself can block for {@code max.block.ms} on metadata or a full buffer
 *       before it returns a future, so 100 sends against a broker with no metadata would otherwise
 *       be 300 s. Never before the first send, or a budget shorter than one send publishes nothing
 *       forever.</li>
 *   <li><b>A wave's acks share one deadline</b>, {@code sendTimeout} from when the wait starts.
 *       Waiting {@code sendTimeout} on each future in turn is N x {@code sendTimeout} against a
 *       wedged broker. The producer's own {@code delivery.timeout.ms} is set below it, so in
 *       practice the futures complete - exceptionally if they must - before the deadline; the
 *       relay's bound does not rely on that.</li>
 * </ul>
 *
 * <h2>Why a row is marked here and not in a send callback</h2>
 *
 * The ack future completes on the producer's network thread. The {@link OutboxMessage} belongs to
 * this thread's persistence context and this transaction, and an entity manager is not thread
 * safe: a callback that marks the row races the commit's dirty-check flush and, when it loses,
 * silently drops the mark. So the futures are collected and resolved here, on the relay thread.
 * The network thread serves every partition in the process and must never touch a database.
 *
 * <h2>Why the transaction boundary is where it is</h2>
 *
 * The claim's row locks last until this method's transaction commits, and those locks are what
 * stop a second relay instance from publishing the same rows. So the Kafka sends happen
 * <i>inside</i> the transaction - network I/O with database locks held, which is normally an
 * anti-pattern. It is acceptable here because nothing else ever touches these rows: business
 * transactions only INSERT new outbox rows. What it does cost is a long-running transaction, which
 * is what the two bounds above are for.
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
 * {@link com.dpe.messaging.tracing.OutboxTracing}. Each publish span runs from its send to its ack;
 * it is made current only for the {@code send()} call, because a wave has many spans open on this
 * thread at once and they end in whatever order the broker answers.
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

    /** One send awaiting the broker's answer. */
    private record InFlight(OutboxMessage message, OutboxTracing.PublishSpan span,
                            CompletableFuture<SendResult<String, String>> ack) {
    }

    /**
     * Publishes one batch and returns how many messages the broker acknowledged.
     *
     * <p>The return value is not decoration: it is what lets a test drive the relay
     * deterministically instead of sleeping and hoping, and it is how the scheduler knows the
     * batch came back full and there is more to drain.
     */
    @Transactional
    public int drainBatch() {
        List<OutboxMessage> batch = outbox.claimUnpublished(properties.batchSize());
        if (batch.isEmpty()) {
            return 0;
        }

        // Each aggregate's messages in claim order, which is created_at order. A wave takes the
        // head of every queue; an aggregate's next message waits for the wave after its
        // predecessor was acked.
        Map<UUID, Deque<OutboxMessage>> queues = new LinkedHashMap<>();
        for (OutboxMessage message : batch) {
            queues.computeIfAbsent(message.getAggregateId(), id -> new ArrayDeque<>()).add(message);
        }

        // Aggregates that have had a message fail in this batch. Their later messages wait for
        // the next poll rather than overtaking the one that did not get through.
        Set<UUID> blocked = new HashSet<>();
        long budgetEnds = System.nanoTime() + properties.batchBudget().toNanos();
        int attempted = 0;
        int published = 0;
        boolean outOfBudget = false;

        while (!outOfBudget && !Thread.currentThread().isInterrupted()) {
            List<InFlight> wave = new ArrayList<>();
            for (Map.Entry<UUID, Deque<OutboxMessage>> queue : queues.entrySet()) {
                if (blocked.contains(queue.getKey()) || queue.getValue().isEmpty()) {
                    continue;
                }
                // Before every send, because send() itself can block. Stopping is safe for the
                // same reason a blocked aggregate is: every row not reached is still unpublished
                // and still in order, and the next poll claims it.
                if (attempted > 0 && System.nanoTime() - budgetEnds > 0) {
                    outOfBudget = true;
                    break;
                }
                attempted++;
                InFlight sent = send(queue.getValue().poll(), blocked);
                if (sent != null) {
                    wave.add(sent);
                }
            }
            if (wave.isEmpty()) {
                // Every queue is empty or blocked - or every send in this wave failed before it
                // returned a future, which blocked its aggregate. Either way nothing is left.
                break;
            }
            published += awaitWave(wave, blocked);
        }

        if (outOfBudget) {
            log.warn("outbox drain spent its {} budget having published {} of {} claimed "
                    + "messages; the rest wait for the next poll", properties.batchBudget(),
                    published, batch.size());
        }
        return published;
    }

    /**
     * Hands one record to the producer. Returns null if {@code send()} failed before returning a
     * future - a serialization error, or {@code max.block.ms} spent waiting for metadata or buffer
     * space - in which case the failure is already recorded and the aggregate blocked.
     */
    private InFlight send(OutboxMessage message, Set<UUID> blocked) {
        ProducerRecord<String, String> record = recordFor(message);

        // Restores the trace context this message was produced with: a child of the row's stored
        // traceparent, or a root span when there is none (e.g. the timeout sweeper). The span is
        // ended when the ack resolves, in awaitWave.
        OutboxTracing.PublishSpan span = tracing.beginPublish(message.getTraceParent(),
                message.getTraceState(), message.getTopic(), message.getEventType());
        try {
            // Must happen before send(): the producer serializes the record on this thread, so a
            // header added after send() has already been handed a copy the broker never sees.
            span.injectInto(record.headers());
            return new InFlight(message, span, span.inScope(() -> kafka.send(record)));
        } catch (RuntimeException e) {
            fail(message, span, blocked, e);
            span.close();
            return null;
        }
    }

    /**
     * Resolves one wave on this thread and returns how many were acknowledged. Every future gets
     * an answer - acked, failed, or given up on at the shared deadline - and every span is ended.
     */
    private int awaitWave(List<InFlight> wave, Set<UUID> blocked) {
        long deadline = System.nanoTime() + properties.sendTimeout().toNanos();
        int acked = 0;
        for (InFlight inFlight : wave) {
            OutboxMessage message = inFlight.message();
            try (OutboxTracing.PublishSpan span = inFlight.span()) {
                try {
                    // Once interrupted, or past the deadline, this is a zero wait: a future that
                    // has already completed still returns its result, so an ack that arrived is
                    // never thrown away just because an earlier one in the wave timed out.
                    long remaining = Thread.currentThread().isInterrupted()
                            ? 0 : Math.max(0, deadline - System.nanoTime());
                    inFlight.ack().get(remaining, TimeUnit.NANOSECONDS);

                    // Dirty checking flushes this UPDATE at commit; the row is durably published-
                    // marked only if the whole batch's transaction commits, which it does because
                    // nothing here rethrows.
                    message.markPublished(OffsetDateTime.now());
                    acked++;
                } catch (InterruptedException e) {
                    // The JVM is shutting down. Restore the flag the wait cleared; the loop in
                    // drainBatch sees it and starts no new wave, and the rest of this one is
                    // resolved with zero waits. Unacked rows stay unpublished, the safe state.
                    Thread.currentThread().interrupt();
                    message.recordFailure("interrupted while awaiting broker acknowledgement");
                    blocked.add(message.getAggregateId());
                    span.error(e);
                } catch (ExecutionException | TimeoutException | RuntimeException e) {
                    // Not rethrown on purpose - see the class javadoc, step 5. A timeout is the
                    // ambiguous case and is treated as a failure: the message may in fact have
                    // reached the broker, so the retry may duplicate it. That is precisely the
                    // duplicate the consumer's inbox exists to absorb, and it is the right way to
                    // be wrong.
                    fail(message, span, blocked, e);
                }
            }
        }
        return acked;
    }

    private void fail(OutboxMessage message, OutboxTracing.PublishSpan span, Set<UUID> blocked,
                      Exception e) {
        log.warn("outbox message {} ({}) failed to publish to {}; leaving it unpublished for retry "
                        + "(attempt {})", message.getId(), message.getEventType(), message.getTopic(),
                message.getAttempts() + 1, e);
        message.recordFailure(describe(e));
        blocked.add(message.getAggregateId());
        // The relay swallows the exception, so a span left to infer success from a normal return
        // would report a green publish for a message that never left.
        span.error(e);
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
     * about why the broker refused; a bare {@link TimeoutException} from the wait has no message
     * at all, so it is given one.
     */
    private String describe(Exception e) {
        if (e instanceof TimeoutException) {
            return "TimeoutException: no broker acknowledgement within " + properties.sendTimeout();
        }
        Throwable cause = e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
        return cause.getClass().getSimpleName() + ": " + cause.getMessage();
    }
}
