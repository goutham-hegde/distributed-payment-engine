package com.dpe.orchestrator.saga;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * M6. What the saga looks like from the outside.
 *
 * <p>The saga is the one thing in this system that fits neither of the standard instrumentation
 * shapes. It is not a request - it outlives the HTTP response, which returned 202 while the work
 * was still running, so no request timer can measure it. It is not a resource, so USE says
 * nothing. What it needs is a rate of starts, a breakdown of terminal outcomes, a duration
 * histogram bucketed by outcome, and a gauge of what is still in flight.
 *
 * <h2>The most important number here is the compensation rate</h2>
 *
 * <p>{@code dpe.saga.terminal{status="COMPENSATED"} / dpe.saga.started}. A compensated saga is
 * <b>not an error</b>: the money went back, every invariant held, and the system did exactly what
 * it was built to do. So it shows up in no error rate, no 5xx graph, and no alert built on either.
 * This ratio is the only place a degrading downstream PSP becomes visible before customers start
 * calling.
 *
 * <p>Duration is split by status for a related reason: a COMPENSATED saga is legitimately slower
 * than a COMPLETED one because it did strictly more work, and a blended histogram hides both.
 *
 * <h2>Why every counter fires after commit, and not when it is called</h2>
 *
 * <p>This is the dual-write problem again, one level down, and it is worth being able to say so.
 * A meter lives in process memory; the saga's state lives in Postgres. There is no transaction
 * spanning both, so the increment and the commit cannot be made atomic - the only choice is
 * <b>which way it is allowed to fail</b>:
 *
 * <ul>
 *   <li>Increment inline, and every rolled-back transaction over-counts. Rollbacks are not rare
 *       here - a redelivered message that loses the inbox race rolls back by design - so the
 *       metric would drift upward permanently and read high forever.</li>
 *   <li>Increment after commit, and a process killed between the commit and the increment
 *       under-counts by one, once, and never again.</li>
 * </ul>
 *
 * <p>The second is strictly better: bounded, rare, and self-correcting in the rate. So every
 * counter and timer here registers a {@link TransactionSynchronization} and fires on
 * {@code afterCommit}. <b>A metric that counts work that did not happen is worse than no metric</b>
 * - it is the number an operator will trust during an incident.
 *
 * <h2>Cardinality</h2>
 *
 * <p>The only label is {@code status} or {@code state}, drawn from {@link SagaStatus} - seven
 * values, fixed at compile time. The transfer id is deliberately absent: that is the unbounded
 * dimension, one series per payment, and "what happened to THIS transfer" is a question for a
 * trace or the {@code saga_steps} table, never for a metric.
 */
@Component
public class SagaMetrics {

    private static final Logger log = LoggerFactory.getLogger(SagaMetrics.class);

    private final SagaInstanceRepository sagas;
    private final MeterRegistry registry;

    private final Counter started;

    /**
     * One gauge per non-terminal state, all reading from this map, all refreshed by ONE query.
     *
     * <p>Pre-populated with every non-terminal state so each series exists from startup at zero.
     * A gauge that only appears once it is non-zero produces a gap rather than a line, and
     * {@code absent()} in an alert then cannot tell "healthy" from "the exporter is gone".
     */
    private final Map<SagaStatus, AtomicLong> inFlight = new EnumMap<>(SagaStatus.class);

    public SagaMetrics(SagaInstanceRepository sagas, MeterRegistry registry) {
        this.sagas = sagas;
        this.registry = registry;

        this.started = Counter.builder("dpe.saga.started")
                .description("Sagas begun")
                .register(registry);

        for (SagaStatus status : SagaStatus.values()) {
            if (status.isTerminal()) {
                // Terminal states get a COUNTER, registered here rather than on first use for
                // the same reason the gauges are: the compensation rate is a ratio, and until
                // COMPENSATED exists as a series the expression returns nothing rather than
                // zero. A dashboard that reads "No data" during the first hour of an incident
                // is indistinguishable from one that is broken.
                terminalCounter(status);
                continue;
            }
            AtomicLong holder = new AtomicLong();
            inFlight.put(status, holder);
            Gauge.builder("dpe.saga.inflight", holder, AtomicLong::get)
                    .tag("state", status.name())
                    // No baseUnit: Micrometer appends it to the Prometheus name, and
                    // dpe_saga_inflight_sagas is not the series the dashboard queries.
                    .description("Sagas currently in this non-terminal state")
                    .register(registry);
        }
    }

    /** A saga was started. Counted only if the transaction that started it commits. */
    public void sagaStarted() {
        afterCommit(started::increment);
    }

    /**
     * A saga reached a terminal state.
     *
     * @param status    COMPLETED, COMPENSATED or FAILED
     * @param startedAt when the saga began, for the duration histogram
     */
    public void sagaFinished(SagaStatus status, OffsetDateTime startedAt) {
        Duration elapsed = startedAt == null
                ? null
                : Duration.between(startedAt.toInstant(), OffsetDateTime.now().toInstant());

        afterCommit(() -> {
            terminalCounter(status).increment();

            if (elapsed != null && !elapsed.isNegative()) {
                Timer.builder("dpe.saga.duration")
                        .tag("status", status.name())
                        .description("Time from saga start to terminal state")
                        // No publishPercentiles here on purpose. The buckets come from
                        // management.metrics.distribution in application.yml, so Prometheus
                        // computes quantiles from _bucket series and they stay aggregatable
                        // across instances. A client-side percentile could not be summed.
                        .register(registry)
                        .record(elapsed);
            }
        });
    }

    /**
     * Re-reads the in-flight gauges. Called on a timer by {@link SagaMetricsScheduler}, never by
     * a scrape - see {@code MessagingMetrics} for why a gauge must not query the database on the
     * scrape path.
     */
    public void refresh() {
        try {
            Map<SagaStatus, Long> counts = new EnumMap<>(SagaStatus.class);
            for (Object[] row : sagas.countInFlightByStatus()) {
                counts.put(SagaStatus.valueOf((String) row[0]), ((Number) row[1]).longValue());
            }
            // Every state is written, including the ones the query did not return. Skipping the
            // absent ones would leave a state stuck at its last non-zero value after it drained -
            // a permanently alarming graph describing a system that is fine.
            inFlight.forEach((status, holder) -> holder.set(counts.getOrDefault(status, 0L)));
        } catch (RuntimeException e) {
            log.warn("saga metrics refresh failed; gauges hold their previous values", e);
        }
    }

    private Counter terminalCounter(SagaStatus status) {
        return Counter.builder("dpe.saga.terminal")
                .tag("status", status.name())
                .description("Sagas reaching a terminal state")
                .register(registry);
    }

    /**
     * Runs the action after the current transaction commits, or immediately if there is no
     * transaction.
     *
     * <p>The fallback matters: {@code SagaOrchestrator.onTimeout} runs inside the sweeper's batch
     * transaction, but a unit test calling the same code without one must still record. Silently
     * dropping the metric when unsynchronised would make the tests here pass while measuring
     * nothing.
     */
    private void afterCommit(Runnable action) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            action.run();
            return;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                action.run();
            }
        });
    }

    long inFlight(SagaStatus status) {
        AtomicLong holder = inFlight.get(status);
        return holder == null ? 0L : holder.get();
    }
}
