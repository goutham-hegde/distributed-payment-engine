package com.dpe.orchestrator.saga;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Saga timing policy.
 *
 * @param stepTimeout      how long a saga may stay non-terminal before the sweeper forces
 *                         compensation. The most consequential number in the milestone, and the
 *                         trade is genuinely two-sided: too short and a merely slow participant
 *                         gets a perfectly good reserve compensated out from under it, producing
 *                         a spurious failure and a release racing a late commit; too long and a
 *                         customer's money sits in a hold for that duration every time a
 *                         participant dies. It must comfortably exceed the p99 of a full round
 *                         trip - relay poll, broker, consumer poll, gateway latency, and the same
 *                         again coming back.
 * @param sweepInterval    how often to look for expired sagas. One indexed query against a
 *                         partial index holding only in-flight sagas, so it is cheap enough to
 *                         run often. This is also the worst-case extra delay before a stuck
 *                         saga is noticed, on top of the timeout itself.
 * @param maxSweepAttempts how many times the sweeper will try to rescue one saga before leaving
 *                         it alone. Without a cap, a saga that cannot be compensated - a
 *                         participant permanently down, a hold that no longer exists - is swept
 *                         forever, and the log fills with the same failure until the real one is
 *                         invisible. A saga that exhausts this is the thing to alert on; M4's
 *                         dead letter handling is where it gets a home.
 * @param sweepBatchSize   sagas claimed per sweep. Bounded for the same reason the outbox batch
 *                         is: the claim transaction stays open across the whole batch.
 * @param listenGrace      M7: how long the reply listener must have held its partitions before
 *                         the sweeper may time anything out - time to drain replies that queued up
 *                         while this instance could not hear them. See
 *                         {@link ReplyListenerReadiness}.
 */
@ConfigurationProperties(prefix = "dpe.saga")
public record SagaProperties(Duration stepTimeout, Duration sweepInterval, int maxSweepAttempts,
                             int sweepBatchSize, Duration listenGrace) {

    public SagaProperties {
        // Defaults live here as well as in application.yml so a test booting a bare context still
        // gets a working sweeper rather than a NullPointerException on the first tick.
        if (stepTimeout == null) {
            stepTimeout = Duration.ofSeconds(30);
        }
        if (sweepInterval == null) {
            sweepInterval = Duration.ofSeconds(5);
        }
        if (maxSweepAttempts <= 0) {
            maxSweepAttempts = 5;
        }
        if (sweepBatchSize <= 0) {
            sweepBatchSize = 100;
        }
        if (listenGrace == null) {
            listenGrace = Duration.ofSeconds(10);
        }
    }
}
