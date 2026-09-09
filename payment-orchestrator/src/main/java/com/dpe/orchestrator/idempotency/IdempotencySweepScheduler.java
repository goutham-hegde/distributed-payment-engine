package com.dpe.orchestrator.idempotency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * The clock that drives {@link IdempotencySweeper}, in its own bean for the same two reasons
 * {@code SagaSweepScheduler} and {@code OutboxRelayScheduler} are.
 *
 * <p><b>Testability.</b> A test calls {@code sweeper.sweep()} directly and asserts on the count
 * rather than sleeping and hoping.
 *
 * <p><b>Spring's proxy semantics.</b> {@code @Transactional} is applied by a proxy around the
 * bean, so a call from another method of the same class goes down {@code this} and gets no
 * transaction at all - silently, and passing every single-threaded test. Keeping the timer in a
 * separate bean means the call goes through the proxy and the boundary is real.
 *
 * <p>Every instance of this service runs this timer, and they will overlap. That is harmless:
 * {@code deleteExpired} names its rows in a subquery and the loser of a race deletes nothing.
 * It is wasted work, not a correctness problem - which is the right bar for a garbage collector.
 */
@Component
@ConditionalOnProperty(prefix = "dpe.idempotency", name = "scheduled", havingValue = "true",
        matchIfMissing = true)
public class IdempotencySweepScheduler {

    private static final Logger log = LoggerFactory.getLogger(IdempotencySweepScheduler.class);

    private final IdempotencySweeper sweeper;

    public IdempotencySweepScheduler(IdempotencySweeper sweeper) {
        this.sweeper = sweeper;
    }

    @Scheduled(fixedDelayString = "${dpe.idempotency.sweep-interval:5m}")
    public void poll() {
        try {
            int deleted = sweeper.sweep();
            if (deleted > 0) {
                log.debug("swept {} expired idempotency key(s)", deleted);
            }
        } catch (RuntimeException e) {
            // A fixedDelay method that throws keeps its schedule, but the framework logs it
            // somewhere easy to miss - and a sweeper that has silently stopped looks exactly
            // like a table with nothing to sweep, right up until it does not fit in memory.
            log.error("idempotency sweep failed; will retry on the next interval", e);
        }
    }
}
