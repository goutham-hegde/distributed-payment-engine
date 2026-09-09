package com.dpe.messaging.inbox;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * M6. The inbox dedup decision, in one place, and therefore countable in one place.
 *
 * <p>Before this existed, five handlers across three services each wrote
 * {@code if (inbox.insertIfAbsent(...) == 0) return;} for themselves. That is not a lot of
 * duplication, but it is five copies of <b>the</b> rule that makes at-least-once delivery safe,
 * and there was nowhere to observe it from. Now there is exactly one.
 *
 * <p><b>The guarantee has not moved and has not changed.</b> It is still
 * {@code INSERT ... ON CONFLICT DO NOTHING} against the inbox primary key, still evaluated by
 * Postgres while it holds the row lock, still atomic with the insert. This class adds a counter
 * and a name; it does not add a check. Anything that made the decision in Java here - a
 * {@code SELECT} first, a cache, a set of seen ids - would reintroduce the race the constraint
 * exists to remove.
 *
 * <h2>Why the duplicate count is not an error metric</h2>
 *
 * <p>A rising {@code dpe.inbox.duplicate} is the system <i>working</i>: the broker redelivered,
 * and the inbox absorbed it instead of paying twice. A healthy run shows a non-zero trickle -
 * rebalances, at-least-once retries, a replayed dead letter.
 *
 * <p>What is actually suspicious is <b>zero, forever</b>, and it is worth knowing why: either the
 * broker has genuinely never redelivered anything, or the dedup key is wrong and duplicates are
 * arriving under ids that do not collide - in which case they are being processed as fresh work
 * and the money moves twice, silently. This counter is one of the few places that failure is
 * visible at all.
 *
 * <h2>Cardinality</h2>
 *
 * <p>Tagged by {@code topic} only. Topics are a fixed, declared vocabulary - they come from
 * {@code Topics} constants and a new one requires a code change - so the series count is bounded
 * by the design. The message id, deliberately, is not a tag: that is the unbounded value, and the
 * question "which message was this" is a question for a trace or the inbox table, not a metric.
 */
@Component
public class InboxGate {

    private static final String DUPLICATE = "dpe.inbox.duplicate";
    private static final String ACCEPTED = "dpe.inbox.accepted";

    private final InboxRepository inbox;
    private final MeterRegistry registry;

    public InboxGate(InboxRepository inbox, MeterRegistry registry) {
        this.inbox = inbox;
        this.registry = registry;
    }

    /**
     * Claims a message for processing.
     *
     * <p>Must be called inside the same transaction as the work it guards. That is the whole
     * point and it is not this class's to enforce: if the handler's transaction rolls back, the
     * inbox row rolls back with it and the redelivery is correctly treated as a first delivery.
     * A claim committed separately would mark work done that never happened.
     *
     * @return {@code true} if this is the first delivery and the caller should do the work;
     *         {@code false} if it has already been processed and must be skipped
     */
    public boolean claim(UUID messageId, String topic, String eventType) {
        boolean first = inbox.insertIfAbsent(messageId, topic, eventType) != 0;

        // BOTH counters are touched, and only one is incremented. Registering just the one that
        // fired would mean dpe_inbox_duplicate_total does not exist until the first duplicate
        // arrives - and the healthy reading for this meter is ZERO, which cannot be distinguished
        // from a missing series. The panel would say "No data" on a working system, which is the
        // fastest way to teach an operator to ignore a panel.
        //
        // It matters more here than for most meters, because zero forever is exactly the reading
        // that should raise an eyebrow: either the broker has never redelivered anything, or the
        // dedup key is wrong and duplicates are being processed as fresh work.
        Counter accepted = counter(ACCEPTED, topic);
        Counter duplicate = counter(DUPLICATE, topic);
        (first ? accepted : duplicate).increment();

        return first;
    }

    /**
     * Registered lazily rather than in the constructor because the topics a service consumes are
     * not known here. {@code MeterRegistry.counter} is idempotent - it returns the existing meter
     * for a name and tag set - so this is a map lookup after the first call, not a registration.
     */
    private Counter counter(String name, String topic) {
        return Counter.builder(name)
                .tag("topic", topic)
                .description(DUPLICATE.equals(name)
                        ? "Messages recognised as already processed and skipped"
                        : "Messages claimed for processing on first delivery")
                .register(registry);
    }
}
