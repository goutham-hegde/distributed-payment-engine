package com.dpe.orchestrator.saga;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * The clock that drives {@link SagaTimeoutSweeper}, in its own bean for the same two reasons
 * {@code OutboxRelayScheduler} is.
 *
 * <p><b>Testability.</b> A test calls {@code sweeper.sweep()} directly and asserts on the count
 * instead of sleeping. Tests that want the timer set {@code dpe.saga.scheduled=true}.
 *
 * <p><b>Spring's proxy semantics.</b> {@code @Transactional} is applied by a proxy around the
 * bean, so it only takes effect on calls that go through that proxy. If {@code @Scheduled} and
 * {@code @Transactional} were on the same class, the internal call would bypass the proxy and
 * run with no transaction - the claim's {@code FOR UPDATE} locks would be released the instant
 * the query returned, and two orchestrator instances would compensate the same saga. Here the
 * scheduler holds the sweeper's proxy, so the boundary is real.
 */
@Component
@ConditionalOnProperty(prefix = "dpe.saga", name = "scheduled", havingValue = "true",
        matchIfMissing = true)
public class SagaSweepScheduler {

    private static final Logger log = LoggerFactory.getLogger(SagaSweepScheduler.class);

    private final SagaTimeoutSweeper sweeper;

    public SagaSweepScheduler(SagaTimeoutSweeper sweeper) {
        this.sweeper = sweeper;
    }

    @Scheduled(fixedDelayString = "${dpe.saga.sweep-interval:5s}")
    public void poll() {
        try {
            int swept = sweeper.sweep();
            if (swept > 0) {
                // INFO, not DEBUG. Every row here is a transfer that failed to complete on its
                // own, so a rising count is a genuine incident signal rather than noise.
                log.info("saga sweeper compensated {} stalled saga(s)", swept);
            }
        } catch (RuntimeException e) {
            // A fixedDelay method that throws keeps its schedule, but the framework logs it at a
            // level that is easy to miss - and a sweeper that has silently stopped looks exactly
            // like a system with no stuck sagas.
            log.error("saga sweep failed; will retry on the next interval", e);
        }
    }
}
