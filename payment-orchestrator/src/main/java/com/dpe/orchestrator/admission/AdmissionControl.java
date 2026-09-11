package com.dpe.orchestrator.admission;

import com.dpe.orchestrator.saga.SagaInstanceRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * M8: refuses NEW transfers while the saga pipeline already holds as much work as it can finish.
 *
 * <h2>Why this exists</h2>
 *
 * <p>{@code POST /transfers} answers 202 as soon as three rows commit, and nothing on that path
 * could see the pipeline behind it. The first load test drove the edge at 20-26 transfers/s into a
 * pipeline that settles ~15/s: the edge answered 202 to all of them with zero errors while the
 * difference piled up as outbox rows and consumer lag, every saga crossed its 30 s deadline, and
 * the sweeper compensated 1,401 payments that had never failed - they had waited. A 202 is a
 * promise, and an edge with no idea how full the pipeline is has no way to know it cannot keep it.
 *
 * <h2>What is bounded, and why that and not requests per second</h2>
 *
 * <p>Sagas IN FLIGHT - Little's law's L. L = throughput x time-in-system, so at a fixed throughput a
 * bound on L IS a bound on how long an admitted payment waits, which is the quantity the saga's
 * deadline cares about. A requests-per-second limit bounds arrivals and knows nothing about whether
 * the pipeline is keeping up: a gateway running at half speed would sail under it while the queue
 * grew exactly as before. And because it is counted from {@code saga_instances}, the bound is
 * global - every instance of this service reads the same number.
 *
 * <h2>Where it is checked: after the idempotency claim, never before it</h2>
 *
 * <p>{@link #admit()} is called from {@code TransferService.createTransfer}, which the idempotency
 * gate only reaches for NEW work. A retry of a payment that was already accepted is answered with
 * its original 202 before this code runs, at any load. That ordering is the design: a 503 says
 * "this payment was not accepted", and saying it about a payment that WAS accepted is a false
 * statement - the client would reasonably conclude the money did not move, and it did. A reply is a
 * statement of fact (M7), and that holds for refusals too. The price is that a refusal costs one
 * claim INSERT and a rollback, which is still far cheaper than accepting the work.
 *
 * <h2>How it counts without a query on every request</h2>
 *
 * <p>The count ({@link SagaInstanceRepository#countInFlightByStatus}, which rides the in-flight
 * partial index) is taken at most once per {@code count-interval}, by whichever admission arrives
 * first after it goes stale, inside that request's own transaction - so it costs no extra
 * connection and needs no scheduler thread, which in this service is shared with the relay and can
 * be busy for seconds. Between counts, the estimate is the last count plus every admission this
 * instance has made since. That errs in one direction only: sagas that finish between counts are
 * not subtracted until the next count, so the estimate over-counts and refuses early, never late.
 *
 * <p>It is a SOFT bound, and the ways it can overshoot are bounded and worth knowing:
 * <ul>
 *   <li>admissions in flight on other request threads at the instant of a count are neither in the
 *       count nor kept in the estimate - at most one per bulkhead permit ({@code dpe.bulkhead});</li>
 *   <li>each instance adds its own admissions to a shared count, so N instances can together
 *       overshoot by N x (their admissions in one interval).</li>
 * </ul>
 * Neither matters to correctness. Like Redis, this is a component that may make the system slower
 * or more cautious and can never make it wrong: money is protected by the ledger's constraints,
 * and this only decides how much work is let into the building.
 */
@Component
public class AdmissionControl {

    private static final Logger log = LoggerFactory.getLogger(AdmissionControl.class);

    private final SagaInstanceRepository sagas;
    private final AdmissionProperties properties;
    private final Counter refused;

    /** Sagas in flight at the last count. */
    private final AtomicLong counted = new AtomicLong();
    /** Admissions this instance has made since the last count. */
    private final AtomicLong admittedSince = new AtomicLong();
    /** System.nanoTime() of the last count; never compared unless {@link #everCounted}. */
    private volatile long countedAt;
    private volatile boolean everCounted;
    private final ReentrantLock counting = new ReentrantLock();

    public AdmissionControl(SagaInstanceRepository sagas, AdmissionProperties properties,
                            MeterRegistry registry) {
        this.sagas = sagas;
        this.properties = properties;
        this.refused = Counter.builder("dpe.admission.refused")
                .description("New transfers refused because the pipeline was at its in-flight limit")
                .register(registry);
        // The limit only - NOT the estimate. The estimate is recounted lazily, by the next
        // admission, so on an idle system it would sit at whatever it last was forever: a gauge
        // pinned at 150 on a system with nothing in flight. dpe.saga.inflight is the refreshed,
        // honest in-flight number; plot it against this line.
        Gauge.builder("dpe.admission.limit", properties, p -> p.maxInFlight())
                .description("Sagas that may be in flight before new transfers are refused")
                .register(registry);
    }

    /**
     * Admits one new transfer, or throws.
     *
     * <p>MUST be called inside the transaction that creates the transfer, and only for new work -
     * see the class comment for both.
     *
     * @throws AdmissionRefusedException when the estimate is at the limit
     */
    public void admit() {
        if (!properties.enabled()) {
            return;
        }
        recountIfStale();

        // Increment first and back out on refusal, rather than check-then-increment: two threads
        // that both read limit - 1 would otherwise both be admitted.
        long mine = admittedSince.incrementAndGet();
        long estimate = counted.get() + mine;
        if (estimate > properties.maxInFlight()) {
            admittedSince.decrementAndGet();
            // Counted where it is decided. A metric must not count work a rollback undoes (M6),
            // but a refusal is not work - it IS the rollback, and nothing downstream can reverse it.
            refused.increment();
            log.debug("admission refused: estimate {} over limit {}", estimate - 1,
                    properties.maxInFlight());
            throw new AdmissionRefusedException(estimate - 1, properties.maxInFlight(),
                    properties.retryAfter());
        }
        // An admission whose transaction then rolls back for another reason stays in the estimate
        // until the next count - an over-count, the safe direction, gone within count-interval.
    }

    private void recountIfStale() {
        if (!isStale()) {
            return;
        }
        // One counter at a time. Everybody else proceeds on the current estimate rather than
        // queueing behind a query; a stale-by-milliseconds number is fine for a soft bound.
        if (!counting.tryLock()) {
            return;
        }
        try {
            if (!isStale()) {
                return;
            }
            // Captured BEFORE the query: admissions that commit between here and the query's
            // snapshot are then in both the count and the estimate - an over-count again, never an
            // under-count from this side.
            long before = admittedSince.get();
            long inFlight = sagas.countInFlightByStatus().stream()
                    .mapToLong(row -> ((Number) row[1]).longValue())
                    .sum();
            counted.set(inFlight);
            admittedSince.addAndGet(-before);
            countedAt = System.nanoTime();
            everCounted = true;
        } finally {
            counting.unlock();
        }
    }

    private boolean isStale() {
        return !everCounted || System.nanoTime() - countedAt >= properties.countInterval().toNanos();
    }
}
