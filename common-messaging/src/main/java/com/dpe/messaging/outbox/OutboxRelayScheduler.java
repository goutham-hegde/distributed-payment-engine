package com.dpe.messaging.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * The clock that drives {@link OutboxRelay}. Split into its own bean for two reasons.
 *
 * <p><b>Testability.</b> A test can call {@code relay.drainBatch()} directly and assert on what
 * happened, instead of sleeping and hoping the scheduler fired. Tests that <i>do</i> want the
 * timer set {@code dpe.outbox.scheduled=true} and poll with Awaitility; every other test in the
 * suite leaves it off and is not perturbed by a background thread hammering a broker that is not
 * there.
 *
 * <p><b>Spring's proxy semantics.</b> {@code @Transactional} is applied by a proxy wrapped around
 * the bean, so it only takes effect on a call that goes <i>through</i> that proxy. Had
 * {@code @Scheduled} and {@code @Transactional} been on the same class with the scheduled method
 * calling the transactional one internally, that internal call would bypass the proxy and run
 * with no transaction at all - the claim's {@code FOR UPDATE} locks would be released the instant
 * the query returned, and two relay instances would happily publish the same rows. Here the
 * scheduler holds a reference to the relay's proxy, so the boundary is real. This is the single
 * most common way self-invocation silently disables a transaction.
 */
@Component
@ConditionalOnProperty(prefix = "dpe.outbox", name = "scheduled", havingValue = "true",
        matchIfMissing = true)
public class OutboxRelayScheduler {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelayScheduler.class);

    private final OutboxRelay relay;

    public OutboxRelayScheduler(OutboxRelay relay) {
        this.relay = relay;
    }

    @Scheduled(fixedDelayString = "${dpe.outbox.poll-interval:500ms}")
    public void poll() {
        try {
            int published = relay.drainBatch();
            if (published > 0) {
                log.debug("outbox relay published {} message(s)", published);
            }
        } catch (RuntimeException e) {
            // An exception thrown out of a @Scheduled fixedDelay method does not stop the
            // schedule, but the scheduler logs it at a level that is easy to miss. A relay that
            // has silently stopped draining looks exactly like a system with no traffic, so make
            // the failure loud here rather than trusting the default.
            log.error("outbox relay poll failed; will retry on the next interval", e);
        }
    }
}
