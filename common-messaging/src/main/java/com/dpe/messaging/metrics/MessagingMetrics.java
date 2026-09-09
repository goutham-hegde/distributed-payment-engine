package com.dpe.messaging.metrics;

import com.dpe.messaging.deadletter.DeadLetterRepository;
import com.dpe.messaging.outbox.OutboxRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * M6. The health of the messaging spine, as three gauges every service publishes.
 *
 * <p>These are the numbers that answer "is the machinery moving", and they are the same question
 * in all three services, which is why they live here rather than being written out three times.
 *
 * <h2>Why these are gauges of a BACKLOG, not counters of throughput</h2>
 *
 * <p>A relay that has stopped and a system with no traffic produce <b>the same</b> throughput
 * graph: zero. Backlog separates them, and age separates them better still. Getting this wrong is
 * the classic instrumentation mistake - measuring the thing that is easy to count instead of the
 * thing that distinguishes the failure.
 *
 * <h2>Why the scrape does not touch the database</h2>
 *
 * <p>A Micrometer {@link Gauge} built over a lambda calls that lambda <b>on every scrape</b>. Point
 * one at a repository and every ten seconds, forever, Prometheus runs three queries against tables
 * that sit on the write path of every payment - and, worse, the scrape now blocks on Postgres. A
 * slow or unavailable database would then make metrics collection slow or fail, which is precisely
 * when the metrics are the only thing you have.
 *
 * <p>So the gauges here read {@code volatile} fields, and {@link MessagingMetricsScheduler}
 * refreshes them on a timer. Three consequences worth being able to state:
 *
 * <ul>
 *   <li>The scrape is a memory read. It cannot block, and it cannot fail.</li>
 *   <li>The database load is fixed by the refresh interval, not by how many things scrape. Ten
 *       Prometheus servers cost exactly what one costs.</li>
 *   <li>The values are up to one refresh interval stale. For a backlog measured against a 10s
 *       scrape and alert thresholds in the tens of seconds, that is irrelevant - and it is a
 *       deliberate trade, not an oversight.</li>
 * </ul>
 *
 * <p>Each query is also shaped to ride the partial index its own worker already uses -
 * {@code idx_outbox_unpublished} and {@code idx_dead_letters_pending} - so it reads only the
 * ACTIVE set and its cost stays flat as the archive grows. <b>A metric must not degrade the system
 * it measures.</b>
 *
 * <h2>Cardinality</h2>
 *
 * <p>No labels are set here at all. The {@code application} tag arrives from
 * {@code management.metrics.tags} and is the only dimension, so each of these is exactly one time
 * series per service - three per service, nine in the system. Nothing here can grow with traffic.
 */
@Component
public class MessagingMetrics {

    private static final Logger log = LoggerFactory.getLogger(MessagingMetrics.class);

    private final OutboxRepository outbox;
    private final DeadLetterRepository deadLetters;

    /**
     * The published values. {@code AtomicLong} rather than a plain field because the refresher
     * thread writes them and the scrape thread reads them; Micrometer holds a weak reference to
     * the object a gauge reads, which is the other reason these are fields on a {@code @Component}
     * and not locals - a gauge over a garbage-collected object silently reports NaN forever.
     */
    private final AtomicLong outboxBacklog = new AtomicLong();
    private final AtomicLong outboxAgeSeconds = new AtomicLong();
    private final AtomicLong deadLetterDepth = new AtomicLong();

    public MessagingMetrics(OutboxRepository outbox, DeadLetterRepository deadLetters,
                            MeterRegistry registry) {
        this.outbox = outbox;
        this.deadLetters = deadLetters;

        Gauge.builder("dpe.outbox.backlog", outboxBacklog, AtomicLong::get)
                // NO baseUnit. Micrometer APPENDS it to the Prometheus name, so
                // baseUnit("messages") publishes dpe_outbox_backlog_messages - which is not the
                // series any dashboard or alert was written against, and nothing warns. Only
                // `seconds` below earns a suffix, because there the unit is genuinely part of
                // the name Prometheus convention expects.
                .description("Outbox rows written but not yet relayed to the broker")
                .register(registry);

        Gauge.builder("dpe.outbox.age", outboxAgeSeconds, AtomicLong::get)
                .description("Age of the oldest un-relayed outbox row; 0 when the outbox is drained")
                .baseUnit("seconds")
                .register(registry);

        Gauge.builder("dpe.dlq.depth", deadLetterDepth, AtomicLong::get)
                .description("Dead letters recorded and not yet replayed")
                .register(registry);
    }

    /**
     * Re-reads all three from the database. Called on a timer, never by a scrape.
     *
     * <p>Failure is logged and swallowed rather than propagated: the caller is a scheduled task,
     * and a metrics refresh that throws must not become an error in the service it is observing.
     * The gauges keep their previous values, which then go flat - and a flat backlog next to live
     * traffic is itself a readable signal that this refresher has stopped.
     */
    public void refresh() {
        try {
            outboxBacklog.set(outbox.countByPublishedAtIsNull());

            // null when nothing is pending, which is the healthy case and must read as zero
            // rather than as a gap in the series.
            Double age = outbox.oldestUnpublishedAgeSeconds();
            outboxAgeSeconds.set(age == null ? 0L : age.longValue());

            deadLetterDepth.set(deadLetters.countByReplayedAtIsNull());
        } catch (RuntimeException e) {
            log.warn("messaging metrics refresh failed; gauges hold their previous values", e);
        }
    }

    long backlog() {
        return outboxBacklog.get();
    }

    long ageSeconds() {
        return outboxAgeSeconds.get();
    }

    long deadLetters() {
        return deadLetterDepth.get();
    }
}
