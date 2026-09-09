package com.dpe.messaging.deadletter;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Retry and dead letter policy. Every value here is a judgement about how long to keep hoping,
 * which is why none of it is a constant buried in the error handler.
 *
 * @param maxAttempts     total deliveries of one record before it is dead-lettered, the first
 *                        one included. The number is a statement about what kind of failure you
 *                        expect: retries only help a TRANSIENT fault, so this should cover a
 *                        pod restart or a brief database failover and no more. Set it high and a
 *                        genuinely poison message occupies its partition for the whole budget;
 *                        set it to 1 and one dropped TCP connection dead-letters a message that
 *                        would have worked on the next try.
 * @param initialInterval delay before the second delivery. Not zero, which is Spring Kafka's
 *                        default: an immediate retry re-runs the request while the downstream is
 *                        still on fire, and ten of them arrive before anything has had time to
 *                        recover. That is a retry storm, and it is how a brief outage becomes a
 *                        long one.
 * @param multiplier      how much longer each successive wait is. Exponential rather than fixed
 *                        because the point of backing off is to give a struggling dependency
 *                        room, and a fixed interval keeps the pressure constant however long the
 *                        outage lasts.
 * @param maxInterval     ceiling on one wait. Without it the delay doubles until the consumer
 *                        exceeds {@code max.poll.interval.ms}, at which point the broker decides
 *                        this consumer is dead and rebalances the group - and the rebalance
 *                        gives the partition to another instance, which starts the retry budget
 *                        again from zero. An unbounded backoff therefore turns into an infinite
 *                        retry with rebalance storms on top. THE BACKOFF MUST FIT INSIDE THE
 *                        POLL INTERVAL: the container is blocked in the listener while it waits,
 *                        so maxAttempts x maxInterval is a duration this consumer is not polling.
 * @param replayBatchSize letters claimed per bulk replay. Bounded like the outbox batch, and for
 *                        the same reason - the claim transaction stays open across every send.
 */
@ConfigurationProperties(prefix = "dpe.dlq")
public record DeadLetterProperties(
        int maxAttempts,
        Duration initialInterval,
        double multiplier,
        Duration maxInterval,
        int replayBatchSize) {

    public DeadLetterProperties {
        if (maxAttempts <= 0) {
            maxAttempts = 4;
        }
        if (initialInterval == null) {
            initialInterval = Duration.ofMillis(500);
        }
        if (multiplier < 1.0) {
            multiplier = 2.0;
        }
        if (maxInterval == null) {
            maxInterval = Duration.ofSeconds(5);
        }
        if (replayBatchSize <= 0) {
            replayBatchSize = 50;
        }
    }
}
